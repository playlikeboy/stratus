package sqs_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/ODudek/stratus/internal/awsapi"
	"github.com/ODudek/stratus/internal/clock"
	"github.com/ODudek/stratus/internal/gateway"
	"github.com/ODudek/stratus/internal/sqs"
)

// queryCall posts a legacy query-protocol request, signed only by a credential
// scope so that the gateway routes it, and returns the status and the XML.
func queryCall(t *testing.T, srv *httptest.Server, path string, form url.Values) (int, string) {
	t.Helper()
	form.Set("Version", "2012-11-05")
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+path, strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/eu-central-1/sqs/aws4_request, SignedHeaders=host, Signature=x")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	body, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(body)
}

func wantContains(t *testing.T, got string, parts ...string) {
	t.Helper()
	for _, part := range parts {
		if !strings.Contains(got, part) {
			t.Fatalf("response lacks %q:\n%s", part, got)
		}
	}
}

func TestQueryProtocol(t *testing.T) {
	svc := sqs.New(awsapi.Identity{Account: awsapi.DefaultAccount, Region: awsapi.DefaultRegion}, clock.System{})
	srv := httptest.NewServer(gateway.New(svc))
	t.Cleanup(srv.Close)
	call := func(path string, kv ...string) (int, string) {
		form := url.Values{}
		for i := 0; i+1 < len(kv); i += 2 {
			form.Set(kv[i], kv[i+1])
		}
		return queryCall(t, srv, path, form)
	}

	status, body := call("/", "Action", "CreateQueue", "QueueName", "jobs",
		"Attribute.1.Name", "VisibilityTimeout", "Attribute.1.Value", "45",
		"Tag.1.Key", "env", "Tag.1.Value", "dev")
	wantContains(t, body, "<CreateQueueResponse", "<QueueUrl>"+srv.URL+"/000000000000/jobs</QueueUrl>", "<RequestId>")
	if status != http.StatusOK {
		t.Fatalf("CreateQueue status = %d", status)
	}
	queueURL := srv.URL + "/000000000000/jobs"

	_, body = call("/", "Action", "ListQueues")
	wantContains(t, body, "<ListQueuesResult><QueueUrl>"+queueURL+"</QueueUrl></ListQueuesResult>")

	_, body = call("/", "Action", "GetQueueAttributes", "QueueUrl", queueURL, "AttributeName.1", "VisibilityTimeout")
	wantContains(t, body, "<Attribute><Name>VisibilityTimeout</Name><Value>45</Value></Attribute>")

	_, body = call("/", "Action", "ListQueueTags", "QueueUrl", queueURL)
	wantContains(t, body, "<Tag><Key>env</Key><Value>dev</Value></Tag>")

	// A queue may be addressed by the path of the request as well.
	_, body = call("/000000000000/jobs", "Action", "SendMessage", "MessageBody", "a < b",
		"MessageAttribute.1.Name", "k", "MessageAttribute.1.Value.DataType", "String", "MessageAttribute.1.Value.StringValue", "v")
	wantContains(t, body, "<SendMessageResult>", "<MessageId>", "<MD5OfMessageAttributes>")

	_, body = call("/", "Action", "SendMessageBatch", "QueueUrl", queueURL,
		"SendMessageBatchRequestEntry.1.Id", "one", "SendMessageBatchRequestEntry.1.MessageBody", "1",
		"SendMessageBatchRequestEntry.2.Id", "two", "SendMessageBatchRequestEntry.2.MessageBody", "2",
		"SendMessageBatchRequestEntry.2.DelaySeconds", "5000")
	wantContains(t, body, "<SendMessageBatchResultEntry><Id>one</Id>", "<BatchResultErrorEntry>", "<Id>two</Id>",
		"<Code>InvalidParameterValue</Code>", "<SenderFault>true</SenderFault>")

	_, body = call("/", "Action", "ReceiveMessage", "QueueUrl", queueURL, "MaxNumberOfMessages", "10",
		"AttributeName.1", "All", "MessageAttributeName.1", "All")
	wantContains(t, body, "<Body>a &lt; b</Body>", "<ReceiptHandle>", "<Attribute><Name>SentTimestamp</Name>",
		"<MessageAttribute><Name>k</Name><Value><DataType>String</DataType><StringValue>v</StringValue></Value></MessageAttribute>")

	status, body = call("/", "Action", "DeleteMessage", "QueueUrl", queueURL, "ReceiptHandle", "stale")
	if status != http.StatusOK || strings.Contains(body, "Result") {
		t.Fatalf("DeleteMessage = %d %s", status, body)
	}

	status, body = call("/", "Action", "GetQueueUrl", "QueueName", "missing")
	wantContains(t, body, "<ErrorResponse", "<Type>Sender</Type>", "<Code>AWS.SimpleQueueService.NonExistentQueue</Code>")
	if status != http.StatusBadRequest {
		t.Fatalf("error status = %d", status)
	}
	status, body = call("/", "Action", "Frobnicate")
	wantContains(t, body, "<Code>InvalidAction</Code>")
	if status != http.StatusBadRequest {
		t.Fatalf("unknown action status = %d", status)
	}
}
