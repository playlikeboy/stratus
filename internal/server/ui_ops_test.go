package server

import (
	"bytes"
	"encoding/base64"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/ODudek/stratus/internal/manifest"
)

const opsManifest = `
sqs:
  queues:
    - name: jobs
      deadLetter: {queue: jobs-dlq, maxReceiveCount: 3}
    - name: jobs-dlq
kinesis:
  streams:
    - name: events
s3:
  buckets:
    - name: files
dynamodb:
  tables:
    - name: orders
      partitionKey: {name: customer}
      sortKey: {name: order_id, type: N}
      globalIndexes:
        - name: by-status
          partitionKey: {name: status}
stepFunctions:
  stateMachines:
    - name: slow
      definition: '{"StartAt": "W", "States": {"W": {"Type": "Wait", "Seconds": 3600, "End": true}}}'
`

func newOpsServer(t *testing.T) (*Server, string) {
	t.Helper()
	m, err := manifest.Parse([]byte(opsManifest))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	srv, err := New(Config{Manifest: m, DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return srv, ts.URL
}

// op posts an operation as the UI does and returns the refreshed part.
func op(t *testing.T, base, path string, form url.Values) string {
	t.Helper()
	code, body := uiPost(t, base+path, form, true)
	if code != http.StatusOK {
		t.Fatalf("POST %s = %d: %s", path, code, body)
	}
	return body
}

func TestUIQueueOperations(t *testing.T) {
	srv, base := newOpsServer(t)
	body := op(t, base, "/_stratus/ui/sqs/jobs/send", url.Values{"body": {`{"order": 7}`}})
	if !strings.Contains(body, "Sent message") || !strings.Contains(body, "&#34;order&#34;: 7") {
		t.Fatalf("send answered without the notice or the message: %.300s", body)
	}
	op(t, base, "/_stratus/ui/sqs/jobs/send", url.Values{"body": {"second"}})
	if _, page := get(t, base+"/_stratus/ui/sqs/jobs?q=SECOND"); !strings.Contains(page, "1 message") || strings.Contains(page, "order&#34;: 7") {
		t.Errorf("searching for SECOND does not show only the second message")
	}

	msgs, _, _ := srv.targets.SQS.Peek("jobs", "", 10)
	body = op(t, base, "/_stratus/ui/sqs/jobs/delete", url.Values{"id": {msgs[0].ID}})
	if !strings.Contains(body, "Deleted message") {
		t.Fatalf("delete: %.300s", body)
	}
	if left, n, _ := srv.targets.SQS.Peek("jobs", "", 10); n != 1 || left[0].Body != "second" {
		t.Fatalf("after deleting the first message the queue holds %+v", left)
	}

	// A failed message parked in the dead-letter queue goes back to its source.
	op(t, base, "/_stratus/ui/sqs/jobs-dlq/send", url.Values{"body": {"poison"}})
	if _, page := get(t, base+"/_stratus/ui/sqs/jobs-dlq"); !strings.Contains(page, "Dead-letter queue of") {
		t.Errorf("the DLQ's page does not name its source queue")
	}
	body = op(t, base, "/_stratus/ui/sqs/jobs-dlq/redrive", url.Values{"source": {"jobs"}})
	if !strings.Contains(body, "Moved 1 messages back to jobs.") {
		t.Fatalf("redrive: %.300s", body)
	}
	if _, n, _ := srv.targets.SQS.Peek("jobs", "poison", 10); n != 1 {
		t.Fatal("the redriven message is not in jobs")
	}
	if body := op(t, base, "/_stratus/ui/sqs/jobs-dlq/redrive", url.Values{"source": {"jobs-dlq"}}); !strings.Contains(body, "Failed:") {
		t.Errorf("redriving to a queue that does not use this DLQ should fail: %.200s", body)
	}
}

func TestUIStreamShowsKPLAggregatesAndFindsTheirRecords(t *testing.T) {
	srv, base := newOpsServer(t)
	agg := kplAggregate([]string{"user-1", "user-2"}, []struct {
		key  int
		data []byte
	}{{0, gzipped(t, `{"event":"mail_accepted","id":1}`)}, {1, gzipped(t, `{"event":"mail_accepted","id":2}`)}})
	if err := srv.callJSON("Kinesis_20131202.PutRecord", "application/x-amz-json-1.1", map[string]string{
		"StreamName": "events", "PartitionKey": "aggregate", "Data": base64.StdEncoding.EncodeToString(agg),
	}); err != nil {
		t.Fatalf("PutRecord: %v", err)
	}
	op(t, base, "/_stratus/ui/kinesis/events/put", url.Values{"partition_key": {"plain"}, "data": {"hello"}})

	_, page := get(t, base+"/_stratus/ui/kinesis/events")
	for _, want := range []string{"KPL aggregate · 2 records", "KPL → gzip → JSON", "&#34;id&#34;: 2", "hello", "shardId-000000000000"} {
		if !strings.Contains(page, want) {
			t.Errorf("stream page lacks %q", want)
		}
	}
	// The key of a user record inside an aggregate finds the aggregate.
	if _, page := get(t, base+"/_stratus/ui/kinesis/events?key=user-2"); !strings.Contains(page, "1 record") || strings.Contains(page, "hello") {
		t.Errorf("searching by a user record's key does not find only its aggregate")
	}
	if _, page := get(t, base+"/_stratus/ui/kinesis/events?q=mail_accepted"); !strings.Contains(page, "1 record") {
		t.Errorf("searching the decoded data does not find the aggregate")
	}
}

func TestUIBucketOperations(t *testing.T) {
	_, base := newOpsServer(t)
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("object", "reports/")
	fw, _ := mw.CreateFormFile("file", "q3.txt")
	_, _ = io.WriteString(fw, "revenue grew")
	_ = mw.Close()
	req, _ := http.NewRequest(http.MethodPost, base+"/_stratus/ui/s3/files/upload", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("HX-Request", "true")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "Uploaded reports/q3.txt") || !strings.Contains(string(body), "reports/q3.txt") {
		t.Fatalf("upload: %.300s", body)
	}
	if _, page := get(t, base+"/_stratus/ui/s3/files/object?key=reports%2Fq3.txt"); !strings.Contains(page, "revenue grew") {
		t.Errorf("the uploaded object's page does not show its text")
	}
	if _, page := get(t, base+"/_stratus/ui/s3/files?q=nothing"); !strings.Contains(page, "No objects match.") {
		t.Errorf("a key search that matches nothing shows objects")
	}
	if body := op(t, base, "/_stratus/ui/s3/files/delete", url.Values{"object": {"reports/q3.txt"}}); !strings.Contains(body, "Deleted reports/q3.txt.") || strings.Contains(body, "revenue") {
		t.Fatalf("delete: %.300s", body)
	}
}

func TestUITableOperations(t *testing.T) {
	_, base := newOpsServer(t)
	_, page := get(t, base+"/_stratus/ui/dynamodb/orders")
	for _, want := range []string{"customer", "order_id", "by-status", "global"} {
		if !strings.Contains(page, want) {
			t.Errorf("table page lacks %q", want)
		}
	}
	put := func(customer, id, status string) {
		item := `{"customer": {"S": "` + customer + `"}, "order_id": {"N": "` + id + `"}, "status": {"S": "` + status + `"}}`
		if body := op(t, base, "/_stratus/ui/dynamodb/orders/put", url.Values{"item": {item}}); !strings.Contains(body, "Put the item.") {
			t.Fatalf("put: %.300s", body)
		}
	}
	put("c-1", "1", "paid")
	put("c-1", "2", "open")
	put("c-2", "3", "paid")
	if _, page := get(t, base+"/_stratus/ui/dynamodb/orders?key=c-1"); !strings.Contains(page, "2 items") {
		t.Errorf("searching customer c-1 does not find its 2 orders")
	}
	if _, page := get(t, base+"/_stratus/ui/dynamodb/orders?index=by-status&key=paid"); !strings.Contains(page, "2 items") {
		t.Errorf("searching the by-status index for paid does not find 2 orders")
	}
	if body := op(t, base, "/_stratus/ui/dynamodb/orders/put", url.Values{"item": {"not json"}}); !strings.Contains(body, "Failed: not a JSON object") {
		t.Errorf("a broken item should fail with a hint: %.200s", body)
	}
	if body := op(t, base, "/_stratus/ui/dynamodb/orders/put", url.Values{"item": {`{"customer": {"S": "c-9"}}`}}); !strings.Contains(body, "Failed: ValidationException") {
		t.Errorf("an item without its sort key should fail as DynamoDB fails it: %.200s", body)
	}
	op(t, base, "/_stratus/ui/dynamodb/orders/delete", url.Values{"item_key": {`{"customer": {"S": "c-2"}, "order_id": {"N": "3"}}`}})
	if _, page := get(t, base+"/_stratus/ui/dynamodb/orders"); !strings.Contains(page, "2 items") {
		t.Errorf("after deleting one of 3 items the table does not show 2")
	}
}

func TestUIMachineOperations(t *testing.T) {
	_, base := newOpsServer(t)
	if body := op(t, base, "/_stratus/ui/states/slow/start", url.Values{"execution": {"nightly"}, "input": {`{"day": 1}`}}); !strings.Contains(body, "Started an execution.") || !strings.Contains(body, "nightly") {
		t.Fatalf("start: %.300s", body)
	}
	if _, page := get(t, base+"/_stratus/ui/states/slow?status=SUCCEEDED"); !strings.Contains(page, "No execution matches.") {
		t.Errorf("a running execution shows under SUCCEEDED")
	}
	if body := op(t, base, "/_stratus/ui/states/slow/stop", url.Values{"execution": {"nightly"}}); !strings.Contains(body, "Stopped nightly.") || !strings.Contains(body, "ABORTED") {
		t.Fatalf("stop: %.300s", body)
	}
}

func TestUIOperationsNeedHTMX(t *testing.T) {
	_, base := newOpsServer(t)
	if code, _ := uiPost(t, base+"/_stratus/ui/sqs/jobs/purge", url.Values{}, false); code != http.StatusForbidden {
		t.Errorf("an operation without HX-Request = %d, want 403", code)
	}
}
