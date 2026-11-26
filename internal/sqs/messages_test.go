package sqs

import (
	"strconv"
	"testing"
	"time"
)

// Deleting most of a queue, from its front and its middle, keeps the rest in
// their order and the counts right.
func TestDeletesKeepOrderAndCounts(t *testing.T) {
	s, fake := newService(t)
	mustCreate(t, s, "jobs", map[string]string{"VisibilityTimeout": "10"})
	for i := range 100 {
		mustSend(t, s, "jobs", strconv.Itoa(i))
	}
	var kept []string
	for {
		got := receive(t, s, "jobs", 10)
		if len(got) == 0 {
			break
		}
		for _, m := range got {
			if n, _ := strconv.Atoi(m.Body); n%7 == 3 {
				kept = append(kept, m.Body)
				continue
			}
			if err := s.Delete("jobs", m.Receipt); err != nil {
				t.Fatalf("Delete: %v", err)
			}
		}
	}
	attrs, _ := s.Attributes("jobs")
	if attrs["ApproximateNumberOfMessagesNotVisible"] != strconv.Itoa(len(kept)) || attrs["ApproximateNumberOfMessages"] != "0" {
		t.Fatalf("in flight = %s, visible = %s; want %d, 0", attrs["ApproximateNumberOfMessagesNotVisible"],
			attrs["ApproximateNumberOfMessages"], len(kept))
	}

	fake.Advance(10 * time.Second)
	var again []string
	for _, m := range receive(t, s, "jobs", 10) {
		again = append(again, m.Body)
	}
	for _, m := range receive(t, s, "jobs", 10) {
		again = append(again, m.Body)
	}
	if len(again) != len(kept) {
		t.Fatalf("redelivered %v, want %v", again, kept)
	}
	for i := range kept {
		if again[i] != kept[i] {
			t.Fatalf("redelivered %v, want %v in that order", again, kept)
		}
	}
}

// A receipt handed out before a message moved to the dead-letter queue no
// longer reaches it, in either queue.
func TestReceiptDoesNotFollowMessageToDeadLetterQueue(t *testing.T) {
	s, fake := newService(t)
	mustCreate(t, s, "jobs-dlq", nil)
	mustCreate(t, s, "jobs", map[string]string{
		"VisibilityTimeout": "10",
		"RedrivePolicy":     `{"deadLetterTargetArn":"` + s.arn("jobs-dlq") + `","maxReceiveCount":"1"}`,
	})
	mustSend(t, s, "jobs", "poison")
	first := receive(t, s, "jobs", 1)
	fake.Advance(10 * time.Second)
	if got := receive(t, s, "jobs", 1); len(got) != 0 {
		t.Fatalf("delivered again instead of moving: %+v", got)
	}
	if err := s.Delete("jobs", first[0].Receipt); err != nil {
		t.Fatalf("Delete with the old receipt: %v", err)
	}
	wantCode(t, s.ChangeVisibility("jobs", first[0].Receipt, time.Minute), "MessageNotInflight")
	if dead := receive(t, s, "jobs-dlq", 1); len(dead) != 1 || dead[0].Body != "poison" {
		t.Fatalf("dead-letter queue has %+v, want the poison message", dead)
	}
}
