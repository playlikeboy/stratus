package server

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stratus-emulator/stratus/internal/dynamodb"
	"github.com/stratus-emulator/stratus/internal/s3"
	"github.com/stratus-emulator/stratus/internal/sqs"
)

// uiPostHeaders posts as the UI does and returns the status, a response
// header and the body.
func uiPostHeaders(t *testing.T, target string, form url.Values, header string) (int, string, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, target, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("HX-Request", "true")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", target, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header.Get(header), string(body)
}

func TestUICreatesEveryKindOfResource(t *testing.T) {
	srv, base := newOpsServer(t)
	forms := map[string]url.Values{
		"queue":   {"name": {"emails"}, "dlq": {"jobs-dlq"}, "max_receives": {"4"}},
		"stream":  {"name": {"clicks"}, "shards": {"3"}},
		"bucket":  {"name": {"exports"}, "versioning": {"on"}},
		"table":   {"name": {"sessions"}, "pk": {"id"}, "pk_type": {"S"}, "ttl": {"expires_at"}},
		"machine": {"name": {"noop"}, "definition": {`{"StartAt": "D", "States": {"D": {"Type": "Pass", "End": true}}}`}},
	}
	for kind, form := range forms {
		body := op(t, base, "/_stratus/ui/create/"+kind, form)
		if !strings.Contains(body, "Created ") || strings.Contains(body, "Failed") {
			t.Errorf("create %s: %.200s", kind, body)
		}
	}
	attrs, err := srv.targets.SQS.Attributes("emails")
	if err != nil || !strings.Contains(attrs["RedrivePolicy"], "jobs-dlq") || !strings.Contains(attrs["RedrivePolicy"], `"maxReceiveCount":4`) {
		t.Errorf("emails was created without its redrive policy: %v %v", attrs["RedrivePolicy"], err)
	}
	if info, err := srv.targets.Kinesis.Stream("clicks"); err != nil || len(info.Shards) != 3 {
		t.Errorf("clicks has %d shards, %v", len(info.Shards), err)
	}
	if info, err := srv.targets.S3.Bucket("exports"); err != nil || info.Versioning != "Enabled" {
		t.Errorf("exports versioning = %q, %v", info.Versioning, err)
	}
	if info, err := srv.targets.DynamoDB.Table("sessions"); err != nil || info.TTL != "expires_at" {
		t.Errorf("sessions TTL = %q, %v", info.TTL, err)
	}
	if body := op(t, base, "/_stratus/ui/create/bucket", url.Values{"name": {"Not Valid"}}); !strings.Contains(body, "Failed:") {
		t.Errorf("an invalid bucket name should fail: %.200s", body)
	}
}

func TestUIDeletesResources(t *testing.T) {
	srv, base := newOpsServer(t)
	if err := srv.targets.S3.PutObject("files", "a.txt", []byte("x"), s3.ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/_stratus/ui/sqs/jobs-dlq", "/_stratus/ui/kinesis/events", "/_stratus/ui/s3/files",
		"/_stratus/ui/dynamodb/orders", "/_stratus/ui/states/slow"} {
		code, redirect, body := uiPostHeaders(t, base+path+"/delete-resource", url.Values{}, "HX-Redirect")
		if code != http.StatusOK || redirect != "/_stratus/ui" {
			t.Errorf("delete %s = %d, redirect %q: %.200s", path, code, redirect, body)
		}
		if code, _ := get(t, base+path); code != http.StatusNotFound {
			t.Errorf("%s still answers %d after its deletion", path, code)
		}
	}
}

func TestUIReplacesAMessageKeepingItsAttributes(t *testing.T) {
	srv, base := newOpsServer(t)
	source := "web"
	if _, err := srv.targets.SQS.Send("jobs", sqs.SendInput{Body: "old", Attributes: map[string]sqs.MessageAttribute{
		"source": {DataType: "String", StringValue: &source}}}); err != nil {
		t.Fatal(err)
	}
	msgs, _, _ := srv.targets.SQS.Peek("jobs", "", 10)
	body := op(t, base, "/_stratus/ui/sqs/jobs/replace", url.Values{"id": {msgs[0].ID}, "body": {"new"}})
	if !strings.Contains(body, "Replaced the message") {
		t.Fatalf("replace: %.300s", body)
	}
	after, n, _ := srv.targets.SQS.Peek("jobs", "", 10)
	if n != 1 || after[0].Body != "new" || after[0].ID == msgs[0].ID || *after[0].Attributes["source"].StringValue != "web" {
		t.Fatalf("after the replacement the queue holds %+v", after)
	}
}

func TestUIEditsAndDeletesItems(t *testing.T) {
	srv, base := newOpsServer(t)
	item := `{"customer": {"S": "c-1"}, "order_id": {"N": "1"}, "status": {"S": "open"}}`
	op(t, base, "/_stratus/ui/dynamodb/orders/put", url.Values{"item": {item}})
	edited := strings.Replace(item, "open", "paid", 1)
	op(t, base, "/_stratus/ui/dynamodb/orders/put", url.Values{"item": {edited}})
	items, n, _ := srv.targets.DynamoDB.SearchItems("orders", dynamodbSearchAll, 10)
	if n != 1 || !strings.Contains(items[0], "paid") {
		t.Fatalf("after the edit the table holds %v", items)
	}
	// An item's own Delete button sends the whole item.
	if body := op(t, base, "/_stratus/ui/dynamodb/orders/delete", url.Values{"item": {items[0]}}); strings.Contains(body, "Failed") {
		t.Fatalf("delete by item: %.300s", body)
	}
	if _, n, _ := srv.targets.DynamoDB.SearchItems("orders", dynamodbSearchAll, 10); n != 0 {
		t.Fatalf("the item is still there")
	}
}

func TestUIEditsTextObjects(t *testing.T) {
	srv, base := newOpsServer(t)
	op(t, base, "/_stratus/ui/s3/files/put-text", url.Values{"object": {"notes/today.txt"}, "content": {"first draft"}})
	_, page := get(t, base+"/_stratus/ui/s3/files/object?key=notes%2Ftoday.txt")
	if !strings.Contains(page, "first draft") || !strings.Contains(page, "/object/save") {
		t.Fatalf("the text object's page does not offer an edit: %.300s", page)
	}
	code, refresh, _ := uiPostHeaders(t, base+"/_stratus/ui/s3/files/object/save", url.Values{"key": {"notes/today.txt"}, "content": {"final"}}, "HX-Refresh")
	if code != http.StatusOK || refresh != "true" {
		t.Fatalf("save = %d, HX-Refresh %q", code, refresh)
	}
	if head, _, _, _ := srv.targets.S3.ObjectHead("files", "notes/today.txt", 100); string(head) != "final" {
		t.Fatalf("after saving the object holds %q", head)
	}
	_, redirect, _ := uiPostHeaders(t, base+"/_stratus/ui/s3/files/object/delete", url.Values{"key": {"notes/today.txt"}}, "HX-Redirect")
	if redirect != "/_stratus/ui/s3/files" {
		t.Fatalf("delete redirects to %q, want the bucket", redirect)
	}
	// A compressed object is shown decoded but not offered for editing.
	if err := srv.targets.S3.PutObject("files", "data.gz", gzipped(t, `{"a": 1}`), s3.ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, page := get(t, base+"/_stratus/ui/s3/files/object?key=data.gz"); strings.Contains(page, "/object/save") || !strings.Contains(page, "gzip") {
		t.Errorf("a gzip object should show decoded and without an edit form")
	}
}

var dynamodbSearchAll = dynamodb.ItemSearch{}
