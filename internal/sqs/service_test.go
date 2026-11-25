package sqs

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ODudek/stratus/internal/awsapi"
	"github.com/ODudek/stratus/internal/clock"
)

var testIdentity = awsapi.Identity{Account: awsapi.DefaultAccount, Region: awsapi.DefaultRegion}

func newService(t *testing.T) (*Service, *clock.Fake) {
	t.Helper()
	fake := clock.NewFake(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	return New(testIdentity, fake), fake
}

func mustCreate(t *testing.T, s *Service, name string, attrs map[string]string) {
	t.Helper()
	if err := s.CreateQueue(name, attrs); err != nil {
		t.Fatalf("CreateQueue(%q): %v", name, err)
	}
}

func mustSend(t *testing.T, s *Service, name, body string) SendOutput {
	t.Helper()
	out, err := s.Send(name, SendInput{Body: body})
	if err != nil {
		t.Fatalf("Send(%q): %v", name, err)
	}
	return out
}

// receive polls without waiting: the fake clock does not move by itself.
func receive(t *testing.T, s *Service, name string, limit int) []Received {
	t.Helper()
	noWait := time.Duration(0)
	out, err := s.Receive(context.Background(), name, ReceiveInput{Max: limit, Wait: &noWait})
	if err != nil {
		t.Fatalf("Receive(%q): %v", name, err)
	}
	return out
}

func wantCode(t *testing.T, err error, code string) {
	t.Helper()
	var apiErr *awsapi.Error
	if !errors.As(err, &apiErr) || apiErr.Code != code {
		t.Fatalf("error = %v, want code %s", err, code)
	}
}

func TestSendReceiveDelete(t *testing.T) {
	s, _ := newService(t)
	mustCreate(t, s, "jobs", nil)
	sent := mustSend(t, s, "jobs", "hello")

	got := receive(t, s, "jobs", 10)
	if len(got) != 1 || got[0].Body != "hello" || got[0].MessageID != sent.MessageID || got[0].ReceiveCount != 1 {
		t.Fatalf("received %+v, want the one sent message with receive count 1", got)
	}
	if again := receive(t, s, "jobs", 10); len(again) != 0 {
		t.Fatalf("an in-flight message was received again: %+v", again)
	}
	if err := s.Delete("jobs", got[0].Receipt); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	attrs, err := s.Attributes("jobs")
	if err != nil {
		t.Fatalf("Attributes: %v", err)
	}
	if attrs["ApproximateNumberOfMessages"] != "0" || attrs["ApproximateNumberOfMessagesNotVisible"] != "0" {
		t.Fatalf("queue is not empty after the delete: %v", attrs)
	}
}

func TestVisibilityTimeoutRedelivers(t *testing.T) {
	s, fake := newService(t)
	mustCreate(t, s, "jobs", map[string]string{"VisibilityTimeout": "60"})
	mustSend(t, s, "jobs", "hello")

	first := receive(t, s, "jobs", 1)
	fake.Advance(59 * time.Second)
	if got := receive(t, s, "jobs", 1); len(got) != 0 {
		t.Fatalf("message visible before its timeout: %+v", got)
	}
	fake.Advance(time.Second)
	second := receive(t, s, "jobs", 1)
	if len(second) != 1 || second[0].ReceiveCount != 2 || second[0].Receipt == first[0].Receipt {
		t.Fatalf("redelivery = %+v, want receive count 2 and a new receipt handle", second)
	}

	// The handle of the first delivery is stale: it must not delete the message.
	if err := s.Delete("jobs", first[0].Receipt); err != nil {
		t.Fatalf("Delete with a stale handle: %v", err)
	}
	attrs, _ := s.Attributes("jobs")
	if attrs["ApproximateNumberOfMessagesNotVisible"] != "1" {
		t.Fatalf("a stale receipt handle deleted the message: %v", attrs)
	}
}

func TestDelay(t *testing.T) {
	s, fake := newService(t)
	mustCreate(t, s, "jobs", nil)
	delay := 30 * time.Second
	if _, err := s.Send("jobs", SendInput{Body: "later", Delay: &delay}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if attrs, _ := s.Attributes("jobs"); attrs["ApproximateNumberOfMessagesDelayed"] != "1" {
		t.Fatalf("attributes = %v, want one delayed message", attrs)
	}
	if got := receive(t, s, "jobs", 1); len(got) != 0 {
		t.Fatalf("delayed message received early: %+v", got)
	}
	fake.Advance(delay)
	if got := receive(t, s, "jobs", 1); len(got) != 1 {
		t.Fatalf("delayed message not received after the delay")
	}
}

func TestDeadLetterQueue(t *testing.T) {
	s, fake := newService(t)
	mustCreate(t, s, "jobs-dlq", nil)
	mustCreate(t, s, "jobs", map[string]string{
		"VisibilityTimeout": "10",
		"RedrivePolicy":     `{"deadLetterTargetArn":"` + s.arn("jobs-dlq") + `","maxReceiveCount":"2"}`,
	})
	sent := mustSend(t, s, "jobs", "poison")

	for i := range 2 {
		if got := receive(t, s, "jobs", 1); len(got) != 1 {
			t.Fatalf("delivery %d: got %d messages, want 1", i+1, len(got))
		}
		fake.Advance(10 * time.Second)
	}
	if got := receive(t, s, "jobs", 1); len(got) != 0 {
		t.Fatalf("message delivered a third time instead of moving to the dead-letter queue: %+v", got)
	}
	dead := receive(t, s, "jobs-dlq", 1)
	if len(dead) != 1 || dead[0].MessageID != sent.MessageID || dead[0].Body != "poison" {
		t.Fatalf("dead-letter queue has %+v, want the poison message", dead)
	}
}

func TestRedrivePolicyNeedsAnExistingTarget(t *testing.T) {
	s, _ := newService(t)
	err := s.CreateQueue("jobs", map[string]string{
		"RedrivePolicy": `{"deadLetterTargetArn":"` + s.arn("missing") + `","maxReceiveCount":3}`,
	})
	wantCode(t, err, "InvalidAttributeValue")
}

func TestCreateQueueIsIdempotentOnlyForEqualAttributes(t *testing.T) {
	s, _ := newService(t)
	mustCreate(t, s, "jobs", map[string]string{"VisibilityTimeout": "45"})
	mustCreate(t, s, "jobs", map[string]string{"VisibilityTimeout": "45"})
	mustCreate(t, s, "jobs", nil)
	wantCode(t, s.CreateQueue("jobs", map[string]string{"VisibilityTimeout": "46"}), "QueueNameExists")
	wantCode(t, s.CreateQueue("jobs.fifo", nil), "InvalidParameterValue")
	wantCode(t, s.CreateQueue("other", map[string]string{"FifoQueue": "true"}), "InvalidAttributeName")
}

func TestChangeVisibility(t *testing.T) {
	s, fake := newService(t)
	mustCreate(t, s, "jobs", nil)
	mustSend(t, s, "jobs", "hello")
	got := receive(t, s, "jobs", 1)

	if err := s.ChangeVisibility("jobs", got[0].Receipt, 0); err != nil {
		t.Fatalf("ChangeVisibility: %v", err)
	}
	again := receive(t, s, "jobs", 1)
	if len(again) != 1 {
		t.Fatalf("message not visible after its visibility was set to 0")
	}
	fake.Advance(defaultVisibility)
	wantCode(t, s.ChangeVisibility("jobs", again[0].Receipt, time.Minute), "MessageNotInflight")
	wantCode(t, s.ChangeVisibility("jobs", "unknown", time.Minute), "MessageNotInflight")
}

func TestUnknownQueue(t *testing.T) {
	s, _ := newService(t)
	_, err := s.Send("missing", SendInput{Body: "x"})
	wantCode(t, err, "QueueDoesNotExist")
	wantCode(t, s.Purge("missing"), "QueueDoesNotExist")
}

func TestReceiveWakesUpOnSend(t *testing.T) {
	s := New(testIdentity, clock.System{})
	mustCreate(t, s, "jobs", nil)

	wait := 10 * time.Second
	done := make(chan []Received, 1)
	go func() {
		out, _ := s.Receive(context.Background(), "jobs", ReceiveInput{Wait: &wait})
		done <- out
	}()
	time.Sleep(50 * time.Millisecond) // let the receiver start waiting
	mustSend(t, s, "jobs", "hello")

	select {
	case out := <-done:
		if len(out) != 1 {
			t.Fatalf("long poll returned %d messages, want 1", len(out))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a waiting receiver did not wake up when a message was sent")
	}
}

func TestMD5OfAttributes(t *testing.T) {
	text := "v"
	a := map[string]MessageAttribute{
		"b": {DataType: "String", StringValue: &text},
		"a": {DataType: "Binary", BinaryValue: []byte{1, 2}},
	}
	if md5OfAttributes(nil) != "" {
		t.Fatal("digest of no attributes must be empty")
	}
	digest := md5OfAttributes(a)
	if len(digest) != 32 {
		t.Fatalf("digest = %q, want 32 hex characters", digest)
	}
	other := "w"
	a["b"] = MessageAttribute{DataType: "String", StringValue: &other}
	if md5OfAttributes(a) == digest {
		t.Fatal("digest did not change with an attribute's value")
	}
}
