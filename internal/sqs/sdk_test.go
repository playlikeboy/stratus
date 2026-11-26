package sqs_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/stratus-emulator/stratus/internal/awsapi"
	"github.com/stratus-emulator/stratus/internal/clock"
	"github.com/stratus-emulator/stratus/internal/gateway"
	"github.com/stratus-emulator/stratus/internal/sqs"
)

func newSDKClient(t *testing.T) *awssqs.Client {
	t.Helper()
	svc := sqs.New(awsapi.Identity{Account: awsapi.DefaultAccount, Region: awsapi.DefaultRegion}, clock.System{})
	srv := httptest.NewServer(gateway.New(svc))
	t.Cleanup(srv.Close)
	return awssqs.New(awssqs.Options{
		Region:       awsapi.DefaultRegion,
		BaseEndpoint: aws.String(srv.URL),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
	})
}

func createQueue(t *testing.T, c *awssqs.Client, name string, attrs map[string]string) *string {
	t.Helper()
	out, err := c.CreateQueue(t.Context(), &awssqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
	if err != nil {
		t.Fatalf("CreateQueue(%q): %v", name, err)
	}
	return out.QueueUrl
}

func TestSDKSendMessageBatchMixesSuccessAndFailure(t *testing.T) {
	c := newSDKClient(t)
	url := createQueue(t, c, "jobs", nil)

	out, err := c.SendMessageBatch(t.Context(), &awssqs.SendMessageBatchInput{
		QueueUrl: url,
		Entries: []types.SendMessageBatchRequestEntry{
			{Id: aws.String("ok1"), MessageBody: aws.String("one"), MessageAttributes: map[string]types.MessageAttributeValue{
				"k": {DataType: aws.String("String"), StringValue: aws.String("v")},
			}},
			{Id: aws.String("late"), MessageBody: aws.String("two"), DelaySeconds: 1000},
			{Id: aws.String("ok2"), MessageBody: aws.String("three")},
		},
	})
	if err != nil {
		t.Fatalf("SendMessageBatch: %v", err)
	}
	if len(out.Successful) != 2 || len(out.Failed) != 1 {
		t.Fatalf("successful = %d, failed = %d, want 2 and 1", len(out.Successful), len(out.Failed))
	}
	f := out.Failed[0]
	if aws.ToString(f.Id) != "late" || aws.ToString(f.Code) != "InvalidParameterValue" || !f.SenderFault || aws.ToString(f.Message) == "" {
		t.Fatalf("failed entry = %+v", f)
	}
	if out.Successful[0].MD5OfMessageAttributes == nil {
		t.Fatal("the batch result has no MD5OfMessageAttributes")
	}

	got, err := c.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{QueueUrl: url, MaxNumberOfMessages: 10})
	if err != nil || len(got.Messages) != 2 {
		t.Fatalf("ReceiveMessage: %d messages, %v", len(got.Messages), err)
	}
}

func TestSDKDeleteAndChangeVisibilityBatch(t *testing.T) {
	c := newSDKClient(t)
	url := createQueue(t, c, "jobs", nil)
	for _, body := range []string{"a", "b", "c"} {
		if _, err := c.SendMessage(t.Context(), &awssqs.SendMessageInput{QueueUrl: url, MessageBody: aws.String(body)}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := c.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{QueueUrl: url, MaxNumberOfMessages: 10})
	if err != nil || len(got.Messages) != 3 {
		t.Fatalf("ReceiveMessage: %v, %v", got, err)
	}

	vis, err := c.ChangeMessageVisibilityBatch(t.Context(), &awssqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: url,
		Entries: []types.ChangeMessageVisibilityBatchRequestEntry{
			{Id: aws.String("v1"), ReceiptHandle: got.Messages[0].ReceiptHandle, VisibilityTimeout: 120},
			{Id: aws.String("v2"), ReceiptHandle: aws.String("unknown"), VisibilityTimeout: 120},
		},
	})
	if err != nil || len(vis.Successful) != 1 || len(vis.Failed) != 1 || aws.ToString(vis.Failed[0].Id) != "v2" ||
		aws.ToString(vis.Failed[0].Code) != "MessageNotInflight" {
		t.Fatalf("ChangeMessageVisibilityBatch: %+v, %v", vis, err)
	}

	del, err := c.DeleteMessageBatch(t.Context(), &awssqs.DeleteMessageBatchInput{
		QueueUrl: url,
		Entries: []types.DeleteMessageBatchRequestEntry{
			{Id: aws.String("d1"), ReceiptHandle: got.Messages[1].ReceiptHandle},
			{Id: aws.String("d2"), ReceiptHandle: aws.String("")},
		},
	})
	if err != nil || len(del.Successful) != 1 || len(del.Failed) != 1 || aws.ToString(del.Failed[0].Code) != "ReceiptHandleIsInvalid" {
		t.Fatalf("DeleteMessageBatch: %+v, %v", del, err)
	}
}

func TestSDKBatchRequestErrorsAreTyped(t *testing.T) {
	c := newSDKClient(t)
	url := createQueue(t, c, "jobs", nil)
	entry := func(id string) types.SendMessageBatchRequestEntry {
		return types.SendMessageBatchRequestEntry{Id: aws.String(id), MessageBody: aws.String("x")}
	}
	send := func(entries ...types.SendMessageBatchRequestEntry) error {
		if entries == nil {
			entries = []types.SendMessageBatchRequestEntry{} // the SDK rejects a nil list itself
		}
		_, err := c.SendMessageBatch(t.Context(), &awssqs.SendMessageBatchInput{QueueUrl: url, Entries: entries})
		return err
	}

	var empty *types.EmptyBatchRequest
	if err := send(); !errors.As(err, &empty) {
		t.Fatalf("empty batch: %v, want *types.EmptyBatchRequest", err)
	}
	var notDistinct *types.BatchEntryIdsNotDistinct
	if err := send(entry("a"), entry("a")); !errors.As(err, &notDistinct) {
		t.Fatalf("repeated id: %v, want *types.BatchEntryIdsNotDistinct", err)
	}
	var invalid *types.InvalidBatchEntryId
	if err := send(entry("a.b")); !errors.As(err, &invalid) {
		t.Fatalf("bad id: %v, want *types.InvalidBatchEntryId", err)
	}
	var tooMany *types.TooManyEntriesInBatchRequest
	var eleven []types.SendMessageBatchRequestEntry
	for i := range 11 {
		eleven = append(eleven, entry("e"+strconv.Itoa(i)))
	}
	if err := send(eleven...); !errors.As(err, &tooMany) {
		t.Fatalf("11 entries: %v, want *types.TooManyEntriesInBatchRequest", err)
	}
}

func TestSDKFifoRoundTripWithTwoGroups(t *testing.T) {
	c := newSDKClient(t)
	url := createQueue(t, c, "orders.fifo", map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"})

	var firstSeq string
	for _, m := range []struct{ group, body string }{{"A", "a1"}, {"A", "a2"}, {"B", "b1"}} {
		out, err := c.SendMessage(t.Context(), &awssqs.SendMessageInput{
			QueueUrl: url, MessageBody: aws.String(m.body), MessageGroupId: aws.String(m.group),
		})
		if err != nil || aws.ToString(out.SequenceNumber) == "" {
			t.Fatalf("SendMessage(%s): %+v, %v", m.body, out, err)
		}
		if firstSeq == "" {
			firstSeq = *out.SequenceNumber
		}
	}
	dup, err := c.SendMessage(t.Context(), &awssqs.SendMessageInput{
		QueueUrl: url, MessageBody: aws.String("a1"), MessageGroupId: aws.String("A"),
	})
	if err != nil || aws.ToString(dup.SequenceNumber) != firstSeq {
		t.Fatalf("duplicate send: %+v, %v; want the first sequence number %s", dup, err, firstSeq)
	}

	recv := func(max int32) []types.Message {
		out, err := c.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{
			QueueUrl: url, MaxNumberOfMessages: max,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
		})
		if err != nil {
			t.Fatalf("ReceiveMessage: %v", err)
		}
		return out.Messages
	}
	first := recv(1)
	if len(first) != 1 || aws.ToString(first[0].Body) != "a1" || first[0].Attributes["MessageGroupId"] != "A" ||
		first[0].Attributes["SequenceNumber"] != firstSeq || first[0].Attributes["MessageDeduplicationId"] == "" {
		t.Fatalf("first receive = %+v", first)
	}
	second := recv(10) // group A is in flight: only B delivers
	if len(second) != 1 || aws.ToString(second[0].Body) != "b1" {
		t.Fatalf("second receive = %+v, want only b1", second)
	}
	if _, err := c.DeleteMessage(t.Context(), &awssqs.DeleteMessageInput{QueueUrl: url, ReceiptHandle: first[0].ReceiptHandle}); err != nil {
		t.Fatal(err)
	}
	third := recv(10)
	if len(third) != 1 || aws.ToString(third[0].Body) != "a2" {
		t.Fatalf("third receive = %+v, want a2", third)
	}

	attrs, err := c.GetQueueAttributes(t.Context(), &awssqs.GetQueueAttributesInput{
		QueueUrl: url, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	if err != nil || attrs.Attributes["FifoQueue"] != "true" || attrs.Attributes["ContentBasedDeduplication"] != "true" {
		t.Fatalf("GetQueueAttributes: %+v, %v", attrs, err)
	}
}

func TestSDKTraceHeaderAndDeadLetterSource(t *testing.T) {
	c := newSDKClient(t)
	dlq := createQueue(t, c, "dlq", nil)
	attrs, err := c.GetQueueAttributes(t.Context(), &awssqs.GetQueueAttributesInput{
		QueueUrl: dlq, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatal(err)
	}
	dlqArn := attrs.Attributes["QueueArn"]
	url := createQueue(t, c, "src", map[string]string{
		"RedrivePolicy": `{"deadLetterTargetArn":"` + dlqArn + `","maxReceiveCount":"1"}`,
	})
	header := "Root=1-5759e988-bd862e3fe1be46a994272793"
	sent, err := c.SendMessage(t.Context(), &awssqs.SendMessageInput{
		QueueUrl: url, MessageBody: aws.String("x"),
		MessageSystemAttributes: map[string]types.MessageSystemAttributeValue{
			"AWSTraceHeader": {DataType: aws.String("String"), StringValue: aws.String(header)},
		},
	})
	if err != nil || aws.ToString(sent.MD5OfMessageSystemAttributes) == "" {
		t.Fatalf("SendMessage: %+v, %v", sent, err)
	}

	first, err := c.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{
		QueueUrl:                    url,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
	})
	if err != nil || len(first.Messages) != 1 || first.Messages[0].Attributes["AWSTraceHeader"] != header {
		t.Fatalf("first receive: %+v, %v", first, err)
	}
	_, err = c.ChangeMessageVisibility(t.Context(), &awssqs.ChangeMessageVisibilityInput{
		QueueUrl: url, ReceiptHandle: first.Messages[0].ReceiptHandle, VisibilityTimeout: 0,
	})
	if err != nil {
		t.Fatalf("ChangeMessageVisibility: %v", err)
	}
	// maxReceiveCount is 1 and the message was received once: the next receive dead-letters it.
	if again, err := c.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{QueueUrl: url}); err != nil || len(again.Messages) != 0 {
		t.Fatalf("second receive: %+v, %v", again, err)
	}
	dead, err := c.ReceiveMessage(t.Context(), &awssqs.ReceiveMessageInput{
		QueueUrl: dlq, MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
	})
	if err != nil || len(dead.Messages) != 1 || dead.Messages[0].Attributes["DeadLetterQueueSourceArn"] == "" {
		t.Fatalf("dead-letter receive: %+v, %v", dead, err)
	}
}

func TestSDKTags(t *testing.T) {
	c := newSDKClient(t)
	url := createQueue(t, c, "tagged", nil)
	if _, err := c.TagQueue(t.Context(), &awssqs.TagQueueInput{QueueUrl: url, Tags: map[string]string{"env": "dev", "team": "a"}}); err != nil {
		t.Fatalf("TagQueue: %v", err)
	}
	if _, err := c.UntagQueue(t.Context(), &awssqs.UntagQueueInput{QueueUrl: url, TagKeys: []string{"env"}}); err != nil {
		t.Fatalf("UntagQueue: %v", err)
	}
	out, err := c.ListQueueTags(t.Context(), &awssqs.ListQueueTagsInput{QueueUrl: url})
	if err != nil || len(out.Tags) != 1 || out.Tags["team"] != "a" {
		t.Fatalf("ListQueueTags: %+v, %v", out, err)
	}

	created, err := c.CreateQueue(t.Context(), &awssqs.CreateQueueInput{QueueName: aws.String("born-tagged"), Tags: map[string]string{"k": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	if out, err := c.ListQueueTags(t.Context(), &awssqs.ListQueueTagsInput{QueueUrl: created.QueueUrl}); err != nil || out.Tags["k"] != "v" {
		t.Fatalf("tags of a queue created with tags: %+v, %v", out, err)
	}
}

func TestSDKListDeadLetterSourceQueuesAndPaging(t *testing.T) {
	c := newSDKClient(t)
	dlq := createQueue(t, c, "dlq", nil)
	attrs, err := c.GetQueueAttributes(t.Context(), &awssqs.GetQueueAttributesInput{
		QueueUrl: dlq, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatal(err)
	}
	policy := `{"deadLetterTargetArn":"` + attrs.Attributes["QueueArn"] + `","maxReceiveCount":"3"}`
	for _, name := range []string{"src-a", "src-b", "src-c"} {
		createQueue(t, c, name, map[string]string{"RedrivePolicy": policy})
	}

	var sources []string
	var token *string
	for range 5 {
		out, err := c.ListDeadLetterSourceQueues(t.Context(), &awssqs.ListDeadLetterSourceQueuesInput{
			QueueUrl: dlq, MaxResults: aws.Int32(2), NextToken: token,
		})
		if err != nil {
			t.Fatalf("ListDeadLetterSourceQueues: %v", err)
		}
		sources = append(sources, out.QueueUrls...)
		if token = out.NextToken; token == nil {
			break
		}
	}
	if len(sources) != 3 {
		t.Fatalf("source queues = %v, want 3", sources)
	}

	page, err := c.ListQueues(t.Context(), &awssqs.ListQueuesInput{MaxResults: aws.Int32(2)})
	if err != nil || len(page.QueueUrls) != 2 || page.NextToken == nil {
		t.Fatalf("first page = %+v, %v", page, err)
	}
	rest, err := c.ListQueues(t.Context(), &awssqs.ListQueuesInput{MaxResults: aws.Int32(10), NextToken: page.NextToken})
	if err != nil || len(rest.QueueUrls) != 2 || rest.NextToken != nil {
		t.Fatalf("second page = %+v, %v", rest, err)
	}
}
