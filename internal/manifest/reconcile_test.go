package manifest

import (
	"strings"
	"testing"
	"time"

	"github.com/stratus-emulator/stratus/internal/clock"
	"github.com/stratus-emulator/stratus/internal/dynamodb"
	"github.com/stratus-emulator/stratus/internal/kinesis"
	"github.com/stratus-emulator/stratus/internal/s3"
	"github.com/stratus-emulator/stratus/internal/sfn"
	"github.com/stratus-emulator/stratus/internal/sqs"
)

func newTargets(t *testing.T) Targets {
	t.Helper()
	clk := clock.NewFake(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	return Targets{Identity: identity, SQS: sqs.New(identity, clk), Kinesis: kinesis.New(identity, clk),
		S3: s3.New(identity, clk, t.TempDir()), DynamoDB: dynamodb.New(identity, clk), SFN: sfn.New(identity, clk)}
}

func mustParse(t *testing.T, text string) Manifest {
	t.Helper()
	m, err := Parse([]byte(text))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return m
}

const before = `
sqs:
  queues:
    - name: jobs
      visibilityTimeoutSeconds: 60
      deadLetter: {queue: jobs-dlq, maxReceiveCount: 5}
    - name: jobs-dlq
kinesis:
  streams:
    - name: events
      shards: 2
s3:
  buckets:
    - name: docs
dynamodb:
  tables:
    - name: orders
      partitionKey: {name: id}
stepFunctions:
  stateMachines:
    - name: flow
      definition: '{"StartAt":"A","States":{"A":{"Type":"Pass","End":true}}}'
`

// Applying a changed manifest over resources that exist, as after a
// restore, keeps them and their data and takes the manifest's settings.
func TestApplyOverExistingResourcesTakesTheManifestsSettings(t *testing.T) {
	targets := newTargets(t)
	if err := mustParse(t, before).Apply(targets); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if _, err := targets.SQS.Send("jobs", sqs.SendInput{Body: "kept"}); err != nil {
		t.Fatal(err)
	}
	if err := targets.S3.PutObject("docs", "a.txt", []byte("kept"), s3.ObjectOptions{}); err != nil {
		t.Fatal(err)
	}

	after := mustParse(t, `
sqs:
  queues:
    - name: jobs
      delaySeconds: 5
    - name: jobs-dlq
kinesis:
  streams:
    - name: events
      shards: 2
    - name: audit
s3:
  buckets:
    - name: docs
      versioning: true
      lifecycle:
        - {prefix: tmp/, expirationDays: 1}
dynamodb:
  tables:
    - name: orders
      partitionKey: {name: id}
      ttlAttribute: expires_at
    - name: sessions
      partitionKey: {name: sid}
stepFunctions:
  stateMachines:
    - name: flow
      definition: '{"StartAt":"B","States":{"B":{"Type":"Succeed"}}}'
`)
	if err := after.Apply(targets); err != nil {
		t.Fatalf("Apply of the changed manifest: %v", err)
	}

	attrs, err := targets.SQS.Attributes("jobs")
	if err != nil {
		t.Fatal(err)
	}
	if attrs["VisibilityTimeout"] != "30" || attrs["DelaySeconds"] != "5" || attrs["RedrivePolicy"] != "" {
		t.Errorf("jobs after the change: visibility %s, delay %s, redrive %q; want the default 30, 5 and none",
			attrs["VisibilityTimeout"], attrs["DelaySeconds"], attrs["RedrivePolicy"])
	}
	if attrs["ApproximateNumberOfMessages"] != "1" {
		t.Errorf("jobs lost its message: %s", attrs["ApproximateNumberOfMessages"])
	}
	info, err := targets.S3.Bucket("docs")
	if err != nil {
		t.Fatal(err)
	}
	objects, _, err := targets.S3.ListObjectViews("docs", "", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if info.Versioning != "Enabled" || len(info.Lifecycle) != 1 || len(objects) != 1 {
		t.Errorf("docs after the change: %+v, %d objects", info, len(objects))
	}
	if n, ok := targets.Kinesis.ShardCount("audit"); !ok || n != 1 {
		t.Errorf("the new stream audit: %d shards, exists %v", n, ok)
	}
	for _, name := range []string{"orders", "sessions"} {
		if !targets.DynamoDB.HasTable(name) {
			t.Errorf("table %s is missing", name)
		}
	}
	if ttl := targets.DynamoDB.TTLAttribute("orders"); ttl != "expires_at" {
		t.Errorf("TTL attribute of orders = %q, want expires_at", ttl)
	}
	if def := targets.SFN.Definition("flow"); !strings.Contains(def, `"B"`) {
		t.Errorf("flow's definition after the change: %s", def)
	}
}

func TestApplyRefusesAChangeAResourceCannotTake(t *testing.T) {
	for name, changed := range map[string]string{
		"a table's key":          strings.Replace(before, "partitionKey: {name: id}", "partitionKey: {name: order_id}", 1),
		"a stream's shards":      strings.Replace(before, "shards: 2", "shards: 3", 1),
		"a state machine's type": strings.Replace(before, "- name: flow\n", "- name: flow\n      type: EXPRESS\n", 1),
	} {
		t.Run(name, func(t *testing.T) {
			targets := newTargets(t)
			if err := mustParse(t, before).Apply(targets); err != nil {
				t.Fatalf("first Apply: %v", err)
			}
			err := mustParse(t, changed).Apply(targets)
			if err == nil {
				t.Fatalf("Apply accepted a change of %s", name)
			}
			if !strings.Contains(err.Error(), "-state-dir") {
				t.Errorf("the error does not say how to start over: %v", err)
			}
		})
	}
}
