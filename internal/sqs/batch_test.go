package sqs

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

func sendEntries(ids ...string) []SendBatchEntry {
	entries := make([]SendBatchEntry, len(ids))
	for i, id := range ids {
		entries[i] = SendBatchEntry{ID: id, Input: SendInput{Body: "body-" + id}}
	}
	return entries
}

func TestBatchRequestErrors(t *testing.T) {
	s, _ := newService(t)
	mustCreate(t, s, "jobs", nil)
	eleven := make([]string, 11)
	for i := range eleven {
		eleven[i] = "e" + strconv.Itoa(i)
	}
	big := strings.Repeat("x", 600*1024)

	tests := []struct {
		name    string
		entries []SendBatchEntry
		code    string
	}{
		{"empty", nil, "EmptyBatchRequest"},
		{"too many", sendEntries(eleven...), "TooManyEntriesInBatchRequest"},
		{"not distinct", sendEntries("a", "b", "a"), "BatchEntryIdsNotDistinct"},
		{"empty id", sendEntries(""), "InvalidBatchEntryId"},
		{"bad id", sendEntries("a.b"), "InvalidBatchEntryId"},
		{"long id", sendEntries(strings.Repeat("a", 81)), "InvalidBatchEntryId"},
		{"too long", []SendBatchEntry{{ID: "a", Input: SendInput{Body: big}}, {ID: "b", Input: SendInput{Body: big}}}, "BatchRequestTooLong"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := s.SendBatch("jobs", tc.entries)
			wantCode(t, err, tc.code)
		})
	}
	if attrs, _ := s.Attributes("jobs"); attrs["ApproximateNumberOfMessages"] != "0" {
		t.Fatalf("a rejected batch enqueued messages: %v", attrs)
	}
	_, _, err := s.SendBatch("missing", sendEntries("a"))
	wantCode(t, err, "QueueDoesNotExist")
}

func TestDeleteAndVisibilityBatchRequestErrors(t *testing.T) {
	s, _ := newService(t)
	mustCreate(t, s, "jobs", nil)
	_, _, err := s.DeleteBatch("jobs", nil)
	wantCode(t, err, "EmptyBatchRequest")
	_, _, err = s.DeleteBatch("jobs", []DeleteBatchEntry{{ID: "a", Receipt: "r"}, {ID: "a", Receipt: "r"}})
	wantCode(t, err, "BatchEntryIdsNotDistinct")
	_, _, err = s.ChangeVisibilityBatch("jobs", nil)
	wantCode(t, err, "EmptyBatchRequest")
	_, _, err = s.ChangeVisibilityBatch("jobs", []VisibilityBatchEntry{{ID: "a b"}})
	wantCode(t, err, "InvalidBatchEntryId")
}

func TestSendBatchMixesSuccessAndFailure(t *testing.T) {
	s, _ := newService(t)
	mustCreate(t, s, "jobs", nil)
	tooLate := 16 * time.Minute
	entries := []SendBatchEntry{
		{ID: "ok1", Input: SendInput{Body: "one"}},
		{ID: "empty", Input: SendInput{}},
		{ID: "delay", Input: SendInput{Body: "x", Delay: &tooLate}},
		{ID: "dedup", Input: SendInput{Body: "x", DeduplicationID: "d"}},
		{ID: "ok2", Input: SendInput{Body: "two"}},
	}
	done, failed, err := s.SendBatch("jobs", entries)
	if err != nil {
		t.Fatal(err)
	}
	if len(done) != 2 || done[0].ID != "ok1" || done[1].ID != "ok2" || done[0].Output.MessageID == "" {
		t.Fatalf("successful = %+v", done)
	}
	want := map[string]string{"empty": "MissingParameter", "delay": "InvalidParameterValue", "dedup": "InvalidParameterValue"}
	if len(failed) != len(want) {
		t.Fatalf("failed = %+v", failed)
	}
	for _, f := range failed {
		if f.Code != want[f.ID] || !f.SenderFault || f.Message == "" {
			t.Fatalf("failure %+v, want code %s with sender fault", f, want[f.ID])
		}
	}
	wantBodies(t, receive(t, s, "jobs", 10), "one", "two")
}

func TestSendBatchToFifoDeduplicatesInsideTheBatch(t *testing.T) {
	s, _ := newService(t)
	mustCreate(t, s, "o.fifo", fifoAttrs())
	entries := []SendBatchEntry{
		{ID: "a", Input: SendInput{Body: "1", GroupID: "G", DeduplicationID: "d"}},
		{ID: "b", Input: SendInput{Body: "2", GroupID: "G", DeduplicationID: "d"}},
		{ID: "c", Input: SendInput{Body: "3"}},
	}
	done, failed, err := s.SendBatch("o.fifo", entries)
	if err != nil || len(done) != 2 || len(failed) != 1 || failed[0].ID != "c" {
		t.Fatalf("done = %+v, failed = %+v, err = %v", done, failed, err)
	}
	if done[0].Output.MessageID != done[1].Output.MessageID {
		t.Fatal("the repeat inside the batch was not deduplicated")
	}
}

func TestDeleteBatch(t *testing.T) {
	s, _ := newService(t)
	mustCreate(t, s, "jobs", nil)
	mustSend(t, s, "jobs", "one")
	mustSend(t, s, "jobs", "two")
	got := receive(t, s, "jobs", 2)

	done, failed, err := s.DeleteBatch("jobs", []DeleteBatchEntry{
		{ID: "a", Receipt: got[0].Receipt},
		{ID: "b", Receipt: ""},
		{ID: "c", Receipt: got[1].Receipt},
	})
	if err != nil || len(done) != 2 || len(failed) != 1 {
		t.Fatalf("done = %v, failed = %+v, err = %v", done, failed, err)
	}
	if failed[0].ID != "b" || failed[0].Code != "ReceiptHandleIsInvalid" || !failed[0].SenderFault {
		t.Fatalf("failure = %+v", failed[0])
	}
	if attrs, _ := s.Attributes("jobs"); attrs["ApproximateNumberOfMessagesNotVisible"] != "0" {
		t.Fatalf("messages left after the batch delete: %v", attrs)
	}
}

func TestChangeVisibilityBatch(t *testing.T) {
	s, _ := newService(t)
	mustCreate(t, s, "jobs", nil)
	mustSend(t, s, "jobs", "one")
	got := receive(t, s, "jobs", 1)
	zero, tooLong := time.Duration(0), 13*time.Hour

	done, failed, err := s.ChangeVisibilityBatch("jobs", []VisibilityBatchEntry{
		{ID: "ok", Receipt: got[0].Receipt, Timeout: &zero},
		{ID: "unknown", Receipt: "nope", Timeout: &zero},
		{ID: "missing", Receipt: got[0].Receipt},
		{ID: "range", Receipt: got[0].Receipt, Timeout: &tooLong},
		{ID: "blank", Receipt: "", Timeout: &zero},
	})
	if err != nil || len(done) != 1 || done[0] != "ok" {
		t.Fatalf("done = %v, err = %v", done, err)
	}
	codes := map[string]string{}
	for _, f := range failed {
		codes[f.ID] = f.Code
	}
	want := map[string]string{
		"unknown": "MessageNotInflight", "missing": "MissingParameter",
		"range": "InvalidParameterValue", "blank": "ReceiptHandleIsInvalid",
	}
	for id, code := range want {
		if codes[id] != code {
			t.Fatalf("failure codes = %v, want %v", codes, want)
		}
	}
	wantBodies(t, receive(t, s, "jobs", 1), "one") // visible again after the change to 0
}
