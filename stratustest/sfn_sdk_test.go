package stratustest_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/ODudek/stratus/stratustest"
)

// A state machine that waits, then sends its input's payload to a queue.
const scheduleManifest = `
sqs:
  queues:
    - name: outbox
    - name: ordered.fifo
      contentBasedDeduplication: true
kinesis:
  streams:
    - name: events
      shards: 2
stepFunctions:
  stateMachines:
    - name: schedule
      substitutions:
        QueueUrl: http://localhost:4566/000000000000/outbox
      definition: |
        {
          "StartAt": "WaitUntil",
          "States": {
            "WaitUntil": {"Type": "Wait", "TimestampPath": "$.send_at", "Next": "Send"},
            "Send": {
              "Type": "Task",
              "Resource": "arn:aws:states:::sqs:sendMessage",
              "Parameters": {"QueueUrl": "${QueueUrl}", "MessageBody.$": "$.payload"},
              "End": true
            }
          }
        }
`

func credentials() aws.CredentialsProvider {
	return aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
	})
}

func TestScheduledSendReachesTheQueue(t *testing.T) {
	srv := stratustest.Start(t, stratustest.WithManifest(scheduleManifest))
	ctx := t.Context()
	machines := sfn.New(sfn.Options{Region: stratustest.Region, BaseEndpoint: aws.String(srv.URL), Credentials: credentials()})
	queues := newClient(srv)

	list, err := machines.ListStateMachines(ctx, &sfn.ListStateMachinesInput{})
	if err != nil || len(list.StateMachines) != 1 {
		t.Fatalf("ListStateMachines: %d machines, error %v; want the manifest's one", len(list.StateMachines), err)
	}
	sendAt := time.Now().Add(time.Second).UTC().Format(time.RFC3339)
	if _, err := machines.StartExecution(ctx, &sfn.StartExecutionInput{
		StateMachineArn: list.StateMachines[0].StateMachineArn,
		Input:           aws.String(`{"send_at":"` + sendAt + `","payload":{"mail":"m-1"}}`),
	}); err != nil {
		t.Fatalf("StartExecution: %v", err)
	}

	url := queueURL(t, queues, "outbox")
	if early, err := queues.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: url}); err != nil || len(early.Messages) != 0 {
		t.Fatalf("before the timestamp: %d messages, error %v; want none", len(early.Messages), err)
	}
	got, err := queues.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: url, WaitTimeSeconds: 10})
	if err != nil || len(got.Messages) != 1 {
		t.Fatalf("after the timestamp: %d messages, error %v; want 1", len(got.Messages), err)
	}
	if body := aws.ToString(got.Messages[0].Body); body != `{"mail":"m-1"}` {
		t.Errorf("message body = %s", body)
	}
}

func TestManifestCreatesStreamsAndFIFOQueues(t *testing.T) {
	srv := stratustest.Start(t, stratustest.WithManifest(scheduleManifest))
	ctx := t.Context()

	streams := kinesis.New(kinesis.Options{Region: stratustest.Region, BaseEndpoint: aws.String(srv.URL), Credentials: credentials()})
	summary, err := streams.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{StreamName: aws.String("events")})
	if err != nil || aws.ToInt32(summary.StreamDescriptionSummary.OpenShardCount) != 2 {
		t.Fatalf("DescribeStreamSummary: %+v, error %v; want 2 shards", summary, err)
	}

	queues := newClient(srv)
	url := queueURL(t, queues, "ordered.fifo")
	for _, body := range []string{"first", "second"} {
		if _, err := queues.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl: url, MessageBody: aws.String(body), MessageGroupId: aws.String("g"),
		}); err != nil {
			t.Fatalf("SendMessage to the FIFO queue: %v", err)
		}
	}
	got, err := queues.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: url, MaxNumberOfMessages: 10})
	if err != nil || len(got.Messages) != 2 || aws.ToString(got.Messages[0].Body) != "first" {
		t.Fatalf("FIFO receive: %d messages, error %v; want first, second in order", len(got.Messages), err)
	}
}
