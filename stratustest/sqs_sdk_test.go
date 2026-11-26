package stratustest_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/stratus-emulator/stratus/stratustest"
)

const manifest = `
sqs:
  queues:
    - name: jobs
      visibilityTimeoutSeconds: 30
      deadLetter:
        queue: jobs-dlq
        maxReceiveCount: 2
    - name: jobs-dlq
`

func newClient(srv *stratustest.Server) *sqs.Client {
	return sqs.New(sqs.Options{
		Region:       stratustest.Region,
		BaseEndpoint: aws.String(srv.URL),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
	})
}

func queueURL(t *testing.T, client *sqs.Client, name string) *string {
	t.Helper()
	out, err := client.GetQueueUrl(t.Context(), &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		t.Fatalf("GetQueueUrl(%q): %v", name, err)
	}
	return out.QueueUrl
}

func TestSDKMessageLifecycle(t *testing.T) {
	srv := stratustest.Start(t, stratustest.WithManifest(manifest))
	client := newClient(srv)
	ctx := t.Context()
	url := queueURL(t, client, "jobs")

	sent, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    url,
		MessageBody: aws.String(`{"op":"send"}`),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"tenant": {DataType: aws.String("String"), StringValue: aws.String("acme")},
			"blob":   {DataType: aws.String("Binary"), BinaryValue: []byte{0, 1, 2}},
		},
	})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	got, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:                    url,
		MaxNumberOfMessages:         10,
		MessageAttributeNames:       []string{"All"},
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	if len(got.Messages) != 1 {
		t.Fatalf("received %d messages, want 1", len(got.Messages))
	}
	msg := got.Messages[0]
	if aws.ToString(msg.MessageId) != aws.ToString(sent.MessageId) || aws.ToString(msg.Body) != `{"op":"send"}` {
		t.Errorf("received %q %q, want the sent message", aws.ToString(msg.MessageId), aws.ToString(msg.Body))
	}
	if aws.ToString(msg.MessageAttributes["tenant"].StringValue) != "acme" ||
		string(msg.MessageAttributes["blob"].BinaryValue) != "\x00\x01\x02" {
		t.Errorf("message attributes did not round-trip: %+v", msg.MessageAttributes)
	}
	if aws.ToString(msg.MD5OfMessageAttributes) != aws.ToString(sent.MD5OfMessageAttributes) {
		t.Errorf("attribute digests differ between send and receive")
	}
	if msg.Attributes["ApproximateReceiveCount"] != "1" {
		t.Errorf("ApproximateReceiveCount = %q, want 1", msg.Attributes["ApproximateReceiveCount"])
	}

	// A heartbeat, then the delete.
	if _, err := client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl: url, ReceiptHandle: msg.ReceiptHandle, VisibilityTimeout: 120,
	}); err != nil {
		t.Fatalf("ChangeMessageVisibility: %v", err)
	}
	if _, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: url, ReceiptHandle: msg.ReceiptHandle}); err != nil {
		t.Fatalf("DeleteMessage: %v", err)
	}
	attrs, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       url,
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	if err != nil {
		t.Fatalf("GetQueueAttributes: %v", err)
	}
	if attrs.Attributes["ApproximateNumberOfMessages"] != "0" || attrs.Attributes["ApproximateNumberOfMessagesNotVisible"] != "0" {
		t.Errorf("queue is not empty after the delete: %v", attrs.Attributes)
	}
	if !strings.HasSuffix(attrs.Attributes["QueueArn"], ":"+stratustest.Account+":jobs") {
		t.Errorf("QueueArn = %q", attrs.Attributes["QueueArn"])
	}
}

func TestSDKDeadLetterQueue(t *testing.T) {
	srv := stratustest.Start(t, stratustest.WithManifest(manifest))
	client := newClient(srv)
	ctx := t.Context()
	url := queueURL(t, client, "jobs")

	if _, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: url, MessageBody: aws.String("poison")}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	// Fail the message maxReceiveCount times: receive it and release it at once.
	for i := range 2 {
		got, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: url})
		if err != nil || len(got.Messages) != 1 {
			t.Fatalf("delivery %d: %d messages, error %v", i+1, len(got.Messages), err)
		}
		if _, err := client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
			QueueUrl: url, ReceiptHandle: got.Messages[0].ReceiptHandle, VisibilityTimeout: 0,
		}); err != nil {
			t.Fatalf("ChangeMessageVisibility: %v", err)
		}
	}
	if got, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: url}); err != nil || len(got.Messages) != 0 {
		t.Fatalf("third delivery: %d messages, error %v; want the message gone to the dead-letter queue", len(got.Messages), err)
	}
	dead, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queueURL(t, client, "jobs-dlq")})
	if err != nil || len(dead.Messages) != 1 || aws.ToString(dead.Messages[0].Body) != "poison" {
		t.Fatalf("dead-letter queue: %d messages, error %v; want the poison message", len(dead.Messages), err)
	}
}

func TestSDKLongPollAndDelay(t *testing.T) {
	srv := stratustest.Start(t, stratustest.WithManifest(manifest))
	client := newClient(srv)
	ctx := t.Context()
	url := queueURL(t, client, "jobs")

	go func() {
		time.Sleep(100 * time.Millisecond)
		_, _ = client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: url, MessageBody: aws.String("late")})
	}()
	start := time.Now()
	got, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: url, WaitTimeSeconds: 10})
	if err != nil || len(got.Messages) != 1 {
		t.Fatalf("long poll: %d messages, error %v; want 1", len(got.Messages), err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("long poll took %v: it did not return when the message arrived", elapsed)
	}

	// A delayed message becomes visible to a receiver that is already waiting.
	if _, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl: url, MessageBody: aws.String("delayed"), DelaySeconds: 1,
	}); err != nil {
		t.Fatalf("SendMessage with a delay: %v", err)
	}
	start = time.Now()
	got, err = client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: url, WaitTimeSeconds: 10})
	if err != nil || len(got.Messages) != 1 || aws.ToString(got.Messages[0].Body) != "delayed" {
		t.Fatalf("delayed message: %d messages, error %v", len(got.Messages), err)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond || elapsed > 3*time.Second {
		t.Errorf("delayed message arrived after %v, want about 1s", elapsed)
	}
}

func TestSDKTypedErrors(t *testing.T) {
	srv := stratustest.Start(t)
	client := newClient(srv)
	ctx := t.Context()

	_, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("missing")})
	var notFound *types.QueueDoesNotExist
	if !errors.As(err, &notFound) {
		t.Fatalf("GetQueueUrl of a missing queue: %v, want QueueDoesNotExist", err)
	}

	if _, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("jobs")}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	_, err = client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName:  aws.String("jobs"),
		Attributes: map[string]string{"VisibilityTimeout": "99"},
	})
	var exists *types.QueueNameExists
	if !errors.As(err, &exists) {
		t.Fatalf("CreateQueue with other attributes: %v, want QueueNameExists", err)
	}
}

func TestResetRestoresTheManifest(t *testing.T) {
	srv := stratustest.Start(t, stratustest.WithManifest(manifest))
	client := newClient(srv)
	ctx := t.Context()

	if _, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queueURL(t, client, "jobs"), MessageBody: aws.String("x")}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if _, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("scratch")}); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	resp := do(t, http.MethodPost, srv.URL+"/_stratus/reset", "", nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("reset: HTTP %d", resp.StatusCode)
	}

	list, err := client.ListQueues(ctx, &sqs.ListQueuesInput{})
	if err != nil {
		t.Fatalf("ListQueues: %v", err)
	}
	if len(list.QueueUrls) != 2 || !strings.HasSuffix(list.QueueUrls[0], "/jobs") || !strings.HasSuffix(list.QueueUrls[1], "/jobs-dlq") {
		t.Fatalf("queues after the reset = %v, want exactly the manifest's", list.QueueUrls)
	}
	got, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(list.QueueUrls[0])})
	if err != nil || len(got.Messages) != 0 {
		t.Fatalf("after the reset: %d messages, error %v; want an empty queue", len(got.Messages), err)
	}
}

// A test tool that writes the wire format by hand sends a placeholder
// signature; the emulator must route and serve it all the same.
func TestRawRequestWithAPlaceholderSignature(t *testing.T) {
	srv := stratustest.Start(t, stratustest.WithManifest(manifest))
	headers := map[string]string{
		"Content-Type":  "application/x-amz-json-1.0",
		"X-Amz-Target":  "AmazonSQS.SendMessage",
		"Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/eu-central-1/sqs/aws4_request, SignedHeaders=host, Signature=e2e",
	}
	// The host in the queue URL is not the one the emulator listens on.
	body := `{"QueueUrl":"http://somewhere-else:4566/000000000000/jobs","MessageBody":"raw"}`
	resp := do(t, http.MethodPost, srv.URL+"/", body, headers)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("raw SendMessage: HTTP %d", resp.StatusCode)
	}

	client := newClient(srv)
	got, err := client.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{QueueUrl: queueURL(t, client, "jobs")})
	if err != nil || len(got.Messages) != 1 || aws.ToString(got.Messages[0].Body) != "raw" {
		t.Fatalf("the raw message was not delivered: %d messages, error %v", len(got.Messages), err)
	}
}

func TestControlEndpoints(t *testing.T) {
	srv := stratustest.Start(t)
	for _, path := range []string{"/_stratus/ready", "/_localstack/health"} {
		if resp := do(t, http.MethodGet, srv.URL+path, "", nil); resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s: HTTP %d", path, resp.StatusCode)
		}
	}
	// A request no service claims is refused loudly, not answered by a default.
	unknown := map[string]string{"X-Amz-Target": "Unknown_20200101.Operation"}
	if resp := do(t, http.MethodPost, srv.URL+"/", "{}", unknown); resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("unroutable request: HTTP %d, want 501", resp.StatusCode)
	}
}

func do(t *testing.T, method, url, body string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("building the request: %v", err)
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp
}
