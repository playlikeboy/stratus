package s3_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/stratus-emulator/stratus/internal/clock"
	"github.com/stratus-emulator/stratus/internal/s3"
)

func TestSDKPresignedURLs(t *testing.T) {
	fake := clock.NewFake(time.Now())
	e := start(t, fake)
	e.mustCreateBucket(t, "pre", s3.BucketOptions{})
	presigner := s3sdk.NewPresignClient(e.client)
	ctx := t.Context()

	put, err := presigner.PresignPutObject(ctx, &s3sdk.PutObjectInput{Bucket: aws.String("pre"), Key: aws.String("doc.txt")},
		s3sdk.WithPresignExpires(time.Minute))
	if err != nil {
		t.Fatalf("PresignPutObject: %v", err)
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, put.URL, strings.NewReader("presigned body"))
	if resp := do(t, req); resp.StatusCode != http.StatusOK {
		t.Fatalf("presigned PUT: %d", resp.StatusCode)
	}

	get, err := presigner.PresignGetObject(ctx, &s3sdk.GetObjectInput{
		Bucket: aws.String("pre"), Key: aws.String("doc.txt"),
		ResponseContentType: aws.String("text/x-override"), ResponseContentDisposition: aws.String("inline"),
	}, s3sdk.WithPresignExpires(time.Minute))
	if err != nil {
		t.Fatalf("PresignGetObject: %v", err)
	}
	req, _ = http.NewRequestWithContext(ctx, http.MethodGet, get.URL, nil)
	resp := do(t, req)
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || string(body) != "presigned body" ||
		resp.Header.Get("Content-Type") != "text/x-override" || resp.Header.Get("Content-Disposition") != "inline" {
		t.Fatalf("presigned GET: %d %q %v", resp.StatusCode, body, resp.Header)
	}

	fake.Advance(2 * time.Minute)
	req, _ = http.NewRequestWithContext(ctx, http.MethodGet, get.URL, nil)
	resp = do(t, req)
	body, _ = io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusForbidden || !strings.Contains(string(body), "<Code>AccessDenied</Code>") ||
		!strings.Contains(string(body), "Request has expired") {
		t.Fatalf("expired presigned GET: %d %s", resp.StatusCode, body)
	}
}

func do(t *testing.T, req *http.Request) *http.Response {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.URL, err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// captured collects the messages a notifier receives.
type captured struct {
	mu   sync.Mutex
	msgs []string
	arns []string
}

func (c *captured) notify(queueARN, body string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.arns = append(c.arns, queueARN)
	c.msgs = append(c.msgs, body)
	return nil
}

type eventMessage struct {
	Records []struct {
		EventVersion string `json:"eventVersion"`
		EventSource  string `json:"eventSource"`
		AWSRegion    string `json:"awsRegion"`
		EventTime    string `json:"eventTime"`
		EventName    string `json:"eventName"`
		S3           struct {
			Bucket struct {
				Name string `json:"name"`
				ARN  string `json:"arn"`
			} `json:"bucket"`
			Object struct {
				Key       string `json:"key"`
				Size      int64  `json:"size"`
				ETag      string `json:"eTag"`
				VersionID string `json:"versionId"`
				Sequencer string `json:"sequencer"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}

func TestSDKQueueNotifications(t *testing.T) {
	e := start(t, clock.System{})
	ctx := t.Context()
	var got captured
	e.svc.SetQueueNotifier(got.notify)
	e.mustCreateBucket(t, "events", s3.BucketOptions{})
	const queue = "arn:aws:sqs:eu-central-1:000000000000:uploads"
	if _, err := e.client.PutBucketNotificationConfiguration(ctx, &s3sdk.PutBucketNotificationConfigurationInput{
		Bucket: aws.String("events"),
		NotificationConfiguration: &types.NotificationConfiguration{QueueConfigurations: []types.QueueConfiguration{{
			QueueArn: aws.String(queue), Events: []types.Event{"s3:ObjectCreated:*"},
			Filter: &types.NotificationConfigurationFilter{Key: &types.S3KeyFilter{FilterRules: []types.FilterRule{
				{Name: types.FilterRuleNamePrefix, Value: aws.String("incoming/")},
				{Name: types.FilterRuleNameSuffix, Value: aws.String(".csv")},
			}}},
		}}},
	}); err != nil {
		t.Fatalf("PutBucketNotificationConfiguration: %v", err)
	}
	cfg, err := e.client.GetBucketNotificationConfiguration(ctx, &s3sdk.GetBucketNotificationConfigurationInput{Bucket: aws.String("events")})
	if err != nil || len(cfg.QueueConfigurations) != 1 || aws.ToString(cfg.QueueConfigurations[0].Id) == "" {
		t.Fatalf("GetBucketNotificationConfiguration = %+v, %v", cfg, err)
	}

	e.mustPut(t, "events", "incoming/day one.csv", "a,b")
	e.mustPut(t, "events", "incoming/skip.txt", "no")
	e.mustPut(t, "events", "other/skip.csv", "no")
	if _, err := e.client.CopyObject(ctx, &s3sdk.CopyObjectInput{
		Bucket: aws.String("events"), Key: aws.String("incoming/copy.csv"), CopySource: aws.String("events/other/skip.csv"),
	}); err != nil {
		t.Fatalf("CopyObject: %v", err)
	}

	if len(got.msgs) != 2 {
		t.Fatalf("got %d notifications, want 2: %v", len(got.msgs), got.msgs)
	}
	var msg eventMessage
	if err := json.Unmarshal([]byte(got.msgs[0]), &msg); err != nil || len(msg.Records) != 1 {
		t.Fatalf("message %s: %v", got.msgs[0], err)
	}
	r := msg.Records[0]
	if got.arns[0] != queue || r.EventVersion != "2.1" || r.EventSource != "aws:s3" || r.AWSRegion != identity.Region ||
		r.EventName != "ObjectCreated:Put" || r.EventTime == "" || r.S3.Bucket.Name != "events" ||
		r.S3.Bucket.ARN != "arn:aws:s3:::events" || r.S3.Object.Key != "incoming/day+one.csv" || r.S3.Object.Size != 3 ||
		r.S3.Object.ETag == "" || strings.Contains(r.S3.Object.ETag, `"`) || r.S3.Object.Sequencer == "" {
		t.Fatalf("record = %+v", r)
	}
	_ = json.Unmarshal([]byte(got.msgs[1]), &msg)
	if msg.Records[0].EventName != "ObjectCreated:Copy" {
		t.Fatalf("second event = %s", msg.Records[0].EventName)
	}
}

// The typed Go API configures notifications on creation; a versioned
// bucket's events carry the version id.
func TestQueueNotificationsFromGoAPI(t *testing.T) {
	e := start(t, clock.System{})
	var got captured
	e.svc.SetQueueNotifier(got.notify)
	e.mustCreateBucket(t, "typed", s3.BucketOptions{
		Versioning:    true,
		Notifications: []s3.QueueNotification{{QueueARN: "arn:q", Events: []string{"s3:ObjectCreated:Put"}, Suffix: ".json"}},
	})
	if err := e.svc.PutObject("typed", "a.json", []byte("{}"), s3.ObjectOptions{ContentType: "application/json"}); err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	e.mustPut(t, "typed", "b.txt", "skip")
	if len(got.msgs) != 1 {
		t.Fatalf("got %d notifications, want 1", len(got.msgs))
	}
	var msg eventMessage
	_ = json.Unmarshal([]byte(got.msgs[0]), &msg)
	if msg.Records[0].S3.Object.VersionID == "" || msg.Records[0].S3.Object.Key != "a.json" {
		t.Fatalf("record = %+v", msg.Records[0])
	}
}

func TestSDKLifecycle(t *testing.T) {
	fake := clock.NewFake(epoch.Add(10 * time.Hour))
	e := start(t, fake)
	ctx := t.Context()
	e.mustCreateBucket(t, "life", s3.BucketOptions{})
	if _, err := e.client.PutBucketLifecycleConfiguration(ctx, &s3sdk.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String("life"),
		LifecycleConfiguration: &types.BucketLifecycleConfiguration{Rules: []types.LifecycleRule{{
			ID: aws.String("tmp"), Status: types.ExpirationStatusEnabled,
			Filter:     &types.LifecycleRuleFilter{Prefix: aws.String("tmp/")},
			Expiration: &types.LifecycleExpiration{Days: aws.Int32(1)},
		}}},
	}); err != nil {
		t.Fatalf("PutBucketLifecycleConfiguration: %v", err)
	}
	got, err := e.client.GetBucketLifecycleConfiguration(ctx, &s3sdk.GetBucketLifecycleConfigurationInput{Bucket: aws.String("life")})
	if err != nil || len(got.Rules) != 1 || aws.ToInt32(got.Rules[0].Expiration.Days) != 1 ||
		aws.ToString(got.Rules[0].Filter.Prefix) != "tmp/" {
		t.Fatalf("GetBucketLifecycleConfiguration = %+v, %v", got, err)
	}
	e.mustPut(t, "life", "tmp/a", "a")
	e.mustPut(t, "life", "keep/b", "b")

	// Created at 10:00 on day 0 with 1 day: expires at midnight starting day 2.
	fake.Advance(37 * time.Hour)
	if n := e.svc.ExpireLifecycle(); n != 0 {
		t.Fatalf("expired %d before the rounded-up midnight", n)
	}
	fake.Advance(time.Hour)
	if n := e.svc.ExpireLifecycle(); n != 1 {
		t.Fatalf("expired %d, want 1", n)
	}
	list, _ := e.client.ListObjectsV2(ctx, &s3sdk.ListObjectsV2Input{Bucket: aws.String("life")})
	if len(list.Contents) != 1 || aws.ToString(list.Contents[0].Key) != "keep/b" {
		t.Fatalf("after expiry: %+v", list.Contents)
	}
	if _, err := e.client.DeleteBucketLifecycle(ctx, &s3sdk.DeleteBucketLifecycleInput{Bucket: aws.String("life")}); err != nil {
		t.Fatalf("DeleteBucketLifecycle: %v", err)
	}
	_, err = e.client.GetBucketLifecycleConfiguration(ctx, &s3sdk.GetBucketLifecycleConfigurationInput{Bucket: aws.String("life")})
	wantAPIError(t, err, "NoSuchLifecycleConfiguration")
}

func TestLifecycleNoncurrentFromGoAPI(t *testing.T) {
	fake := clock.NewFake(epoch)
	e := start(t, fake)
	e.mustCreateBucket(t, "hist", s3.BucketOptions{
		Versioning: true,
		Lifecycle:  []s3.LifecycleRule{{NoncurrentDays: 1, ExpiredObjectDeleteMarker: true}},
	})
	for _, body := range []string{"v1", "v2"} {
		if err := e.svc.PutObject("hist", "k", []byte(body), s3.ObjectOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	fake.Advance(25 * time.Hour)
	if n := e.svc.ExpireLifecycle(); n != 1 {
		t.Fatalf("removed %d noncurrent versions, want 1", n)
	}
	versions, _ := e.client.ListObjectVersions(t.Context(), &s3sdk.ListObjectVersionsInput{Bucket: aws.String("hist")})
	if len(versions.Versions) != 1 {
		t.Fatalf("versions left: %+v", versions.Versions)
	}
}
