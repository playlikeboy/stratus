package manifest

import (
	"strings"
	"testing"
	"time"

	"github.com/ODudek/stratus/internal/awsapi"
	"github.com/ODudek/stratus/internal/clock"
	"github.com/ODudek/stratus/internal/kinesis"
	"github.com/ODudek/stratus/internal/s3"
	"github.com/ODudek/stratus/internal/sfn"
	"github.com/ODudek/stratus/internal/sqs"
)

var identity = awsapi.Identity{Account: awsapi.DefaultAccount, Region: awsapi.DefaultRegion}

func TestApplyCreatesQueuesInAnyOrder(t *testing.T) {
	// The dead-letter queue is declared after the queue that points at it.
	m, err := Parse([]byte(`
sqs:
  queues:
    - name: jobs
      visibilityTimeoutSeconds: 900
      deadLetter:
        queue: jobs-dlq
        maxReceiveCount: 5
    - name: jobs-dlq
`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	queues := sqs.New(identity, clock.NewFake(time.Unix(0, 0)))
	if err := m.Apply(Targets{Identity: identity, SQS: queues, Kinesis: kinesis.New(identity, clock.NewFake(time.Unix(0, 0))), S3: s3.New(identity, clock.NewFake(time.Unix(0, 0))), SFN: sfn.New(identity, clock.NewFake(time.Unix(0, 0)))}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	attrs, err := queues.Attributes("jobs")
	if err != nil {
		t.Fatalf("Attributes: %v", err)
	}
	if attrs["VisibilityTimeout"] != "900" {
		t.Errorf("VisibilityTimeout = %q, want 900", attrs["VisibilityTimeout"])
	}
	if !strings.Contains(attrs["RedrivePolicy"], ":jobs-dlq") || !strings.Contains(attrs["RedrivePolicy"], `"maxReceiveCount":5`) {
		t.Errorf("RedrivePolicy = %q, want jobs-dlq with maxReceiveCount 5", attrs["RedrivePolicy"])
	}
	if !queues.Exists("jobs-dlq") {
		t.Error("jobs-dlq was not created")
	}
	// Applying again, as a reset does on a fresh service, must also work twice in a row.
	if err := m.Apply(Targets{Identity: identity, SQS: queues, Kinesis: kinesis.New(identity, clock.NewFake(time.Unix(0, 0))), S3: s3.New(identity, clock.NewFake(time.Unix(0, 0))), SFN: sfn.New(identity, clock.NewFake(time.Unix(0, 0)))}); err != nil {
		t.Fatalf("second Apply: %v", err)
	}
}

func TestParseRejects(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{"an unknown service", "lambda:\n  functions: []\n", "lambda"},
		{"an unknown queue key", "sqs:\n  queues:\n    - name: a\n      fifo: true\n", "fifo"},
		{"a queue without a name", "sqs:\n  queues:\n    - delaySeconds: 1\n", "name is required"},
		{"a duplicate queue", "sqs:\n  queues:\n    - name: a\n    - name: a\n", "declared twice"},
		{"an undeclared dead-letter queue", "sqs:\n  queues:\n    - name: a\n      deadLetter: {queue: b, maxReceiveCount: 3}\n", "not declared"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.yaml))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want one mentioning %q", err, tt.want)
			}
		})
	}
}

func TestParseEmpty(t *testing.T) {
	m, err := Parse(nil)
	if err != nil {
		t.Fatalf("Parse of an empty manifest: %v", err)
	}
	if len(m.SQS.Queues) != 0 {
		t.Fatalf("empty manifest has queues: %+v", m)
	}
}
