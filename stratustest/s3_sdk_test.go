package stratustest_test

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/ODudek/stratus/stratustest"
)

const bucketManifest = `
sqs:
  queues:
    - name: uploads
s3:
  buckets:
    - name: packages
      versioning: true
      lifecycle:
        - prefix: tmp/
          expirationDays: 7
      notifications:
        - queue: uploads
          prefix: incoming/
          suffix: .zip
`

func TestBucketFromTheManifestNotifiesItsQueue(t *testing.T) {
	srv := stratustest.Start(t, stratustest.WithManifest(bucketManifest))
	ctx := t.Context()
	objects := s3.New(s3.Options{
		Region: stratustest.Region, BaseEndpoint: aws.String(srv.URL), UsePathStyle: true, Credentials: credentials(),
	})

	for _, key := range []string{"incoming/a.zip", "incoming/skipped.txt", "elsewhere/b.zip"} {
		put, err := objects.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String("packages"), Key: aws.String(key), Body: bytes.NewReader([]byte("data")),
		})
		if err != nil {
			t.Fatalf("PutObject(%s): %v", key, err)
		}
		if aws.ToString(put.VersionId) == "" {
			t.Errorf("PutObject(%s): no version id in a versioned bucket", key)
		}
	}
	got, err := objects.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("packages"), Key: aws.String("incoming/a.zip")})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	body, _ := io.ReadAll(got.Body)
	_ = got.Body.Close()
	if string(body) != "data" {
		t.Errorf("object body = %q", body)
	}

	queues := newClient(srv)
	events, err := queues.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: queueURL(t, queues, "uploads"), MaxNumberOfMessages: 10,
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	if len(events.Messages) != 1 || !strings.Contains(aws.ToString(events.Messages[0].Body), `"key":"incoming/a.zip"`) {
		t.Fatalf("queue has %d events, want exactly the one for incoming/a.zip", len(events.Messages))
	}
}

// A request with no credentials at all is an S3 request.
func TestAnonymousRequestGoesToS3(t *testing.T) {
	srv := stratustest.Start(t, stratustest.WithManifest(bucketManifest))
	if resp := do(t, http.MethodPut, srv.URL+"/packages/raw/key", "payload", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("anonymous PUT: HTTP %d", resp.StatusCode)
	}
	if resp := do(t, http.MethodGet, srv.URL+"/packages/raw/key", "", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("anonymous GET: HTTP %d", resp.StatusCode)
	}
	if resp := do(t, http.MethodGet, srv.URL+"/no-such-bucket/key", "", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET from a missing bucket: HTTP %d, want 404", resp.StatusCode)
	}
}

// Over HTTPS the SDK streams a body it cannot rewind, with its default settings.
func TestStreamedUploadOverTLS(t *testing.T) {
	srv := stratustest.Start(t, stratustest.WithManifest(bucketManifest), stratustest.WithTLS())
	ctx := t.Context()
	objects := s3.New(s3.Options{
		Region: stratustest.Region, BaseEndpoint: aws.String(srv.URL), UsePathStyle: true,
		Credentials: credentials(), HTTPClient: srv.Client(),
	})
	payload := strings.Repeat("stream ", 100_000)
	unseekable := io.MultiReader(strings.NewReader(payload))
	if _, err := objects.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("packages"), Key: aws.String("stream.bin"), Body: unseekable,
	}); err != nil {
		t.Fatalf("streamed PutObject: %v", err)
	}
	head, err := objects.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String("packages"), Key: aws.String("stream.bin")})
	if err != nil || aws.ToInt64(head.ContentLength) != int64(len(payload)) {
		t.Fatalf("HeadObject: length %d, error %v; want %d", aws.ToInt64(head.ContentLength), err, len(payload))
	}
}
