package sqs

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"testing"
	"time"
)

func fifoAttrs(extra ...string) map[string]string {
	attrs := map[string]string{"FifoQueue": "true"}
	for i := 0; i+1 < len(extra); i += 2 {
		attrs[extra[i]] = extra[i+1]
	}
	return attrs
}

func fifoSend(t *testing.T, s *Service, name, group, dedup, body string) SendOutput {
	t.Helper()
	out, err := s.Send(name, SendInput{Body: body, GroupID: group, DeduplicationID: dedup})
	if err != nil {
		t.Fatalf("Send(%q, group %q): %v", name, group, err)
	}
	return out
}

func bodies(got []Received) []string {
	out := make([]string, len(got))
	for i, m := range got {
		out[i] = m.Body
	}
	return out
}

func wantBodies(t *testing.T, got []Received, want ...string) {
	t.Helper()
	have := bodies(got)
	if len(have) != len(want) {
		t.Fatalf("received %v, want %v", have, want)
	}
	for i := range want {
		if have[i] != want[i] {
			t.Fatalf("received %v, want %v", have, want)
		}
	}
}

func TestFifoOrderWithinGroupAndIndependentGroups(t *testing.T) {
	s, _ := newService(t)
	mustCreate(t, s, "o.fifo", fifoAttrs())
	var last uint64
	for i, m := range []struct{ group, body string }{{"A", "a1"}, {"B", "b1"}, {"A", "a2"}, {"A", "a3"}, {"B", "b2"}} {
		out := fifoSend(t, s, "o.fifo", m.group, "d"+strconv.Itoa(i), m.body)
		seq, err := strconv.ParseUint(out.SequenceNumber, 10, 64)
		if err != nil || seq <= last {
			t.Fatalf("sequence %q after %d: want a larger number", out.SequenceNumber, last)
		}
		last = seq
	}

	first := receive(t, s, "o.fifo", 1)
	wantBodies(t, first, "a1")
	if first[0].GroupID != "A" || first[0].DeduplicationID != "d0" || first[0].SequenceNumber == "" {
		t.Fatalf("FIFO fields of a received message: %+v", first[0])
	}
	// Group A has a message in flight: only group B delivers, in order.
	wantBodies(t, receive(t, s, "o.fifo", 10), "b1", "b2")
	if err := s.Delete("o.fifo", first[0].Receipt); err != nil {
		t.Fatal(err)
	}
	wantBodies(t, receive(t, s, "o.fifo", 10), "a2", "a3")
}

func TestFifoGroupLockReleasedByVisibilityExpiry(t *testing.T) {
	s, fake := newService(t)
	mustCreate(t, s, "o.fifo", fifoAttrs("VisibilityTimeout", "30"))
	fifoSend(t, s, "o.fifo", "A", "1", "a1")
	fifoSend(t, s, "o.fifo", "A", "2", "a2")

	first := receive(t, s, "o.fifo", 1)
	wantBodies(t, first, "a1")
	fake.Advance(29 * time.Second)
	wantBodies(t, receive(t, s, "o.fifo", 10))
	fake.Advance(time.Second)
	again := receive(t, s, "o.fifo", 1)
	wantBodies(t, again, "a1") // the order holds: a1 again, not a2
	if again[0].ReceiveCount != 2 {
		t.Fatalf("receive count = %d, want 2", again[0].ReceiveCount)
	}
	// Changing the visibility to 0 releases the group too.
	if err := s.ChangeVisibility("o.fifo", again[0].Receipt, 0); err != nil {
		t.Fatal(err)
	}
	wantBodies(t, receive(t, s, "o.fifo", 10), "a1", "a2")
}

func TestFifoDeduplicationInterval(t *testing.T) {
	s, fake := newService(t)
	mustCreate(t, s, "o.fifo", fifoAttrs())
	first := fifoSend(t, s, "o.fifo", "A", "same", "one")

	fake.Advance(4*time.Minute + 59*time.Second)
	dup := fifoSend(t, s, "o.fifo", "A", "same", "two")
	if dup.MessageID != first.MessageID || dup.SequenceNumber != first.SequenceNumber {
		t.Fatalf("duplicate = %+v, want the first message's %+v", dup, first)
	}
	if attrs, _ := s.Attributes("o.fifo"); attrs["ApproximateNumberOfMessages"] != "1" {
		t.Fatalf("a duplicate was enqueued: %v", attrs)
	}

	fake.Advance(2 * time.Second) // 5 minutes and a second after the first send
	again := fifoSend(t, s, "o.fifo", "A", "same", "three")
	if again.MessageID == first.MessageID || again.SequenceNumber == first.SequenceNumber {
		t.Fatalf("a send outside the interval was taken for a duplicate: %+v", again)
	}
	wantBodies(t, receive(t, s, "o.fifo", 10), "one", "three")
}

func TestFifoDeduplicationRemembersDeletedMessages(t *testing.T) {
	s, _ := newService(t)
	mustCreate(t, s, "o.fifo", fifoAttrs())
	fifoSend(t, s, "o.fifo", "A", "x", "one")
	got := receive(t, s, "o.fifo", 1)
	if err := s.Delete("o.fifo", got[0].Receipt); err != nil {
		t.Fatal(err)
	}
	fifoSend(t, s, "o.fifo", "A", "x", "one")
	wantBodies(t, receive(t, s, "o.fifo", 1))
}

func TestFifoContentBasedDeduplication(t *testing.T) {
	s, _ := newService(t)
	mustCreate(t, s, "c.fifo", fifoAttrs("ContentBasedDeduplication", "true"))
	a := fifoSend(t, s, "c.fifo", "A", "", "same body")
	b := fifoSend(t, s, "c.fifo", "B", "", "same body")
	if a.MessageID != b.MessageID {
		t.Fatal("equal bodies were not deduplicated")
	}
	fifoSend(t, s, "c.fifo", "A", "", "other body")
	fifoSend(t, s, "c.fifo", "A", "explicit", "same body") // an explicit id wins over the content

	got := receive(t, s, "c.fifo", 10)
	wantBodies(t, got, "same body", "other body", "same body")
	sum := sha256.Sum256([]byte("same body"))
	if got[0].DeduplicationID != hex.EncodeToString(sum[:]) {
		t.Fatalf("deduplication id = %q, want the SHA-256 of the body", got[0].DeduplicationID)
	}
}

func TestFifoDeduplicationScopeMessageGroup(t *testing.T) {
	s, _ := newService(t)
	mustCreate(t, s, "g.fifo", fifoAttrs("DeduplicationScope", "messageGroup", "FifoThroughputLimit", "perMessageGroupId"))
	a := fifoSend(t, s, "g.fifo", "A", "id", "a")
	b := fifoSend(t, s, "g.fifo", "B", "id", "b")
	if a.MessageID == b.MessageID {
		t.Fatal("the same deduplication id in two groups was taken for a duplicate")
	}
	if dup := fifoSend(t, s, "g.fifo", "A", "id", "a"); dup.MessageID != a.MessageID {
		t.Fatal("a repeat inside a group was not deduplicated")
	}
	attrs, _ := s.Attributes("g.fifo")
	if attrs["DeduplicationScope"] != "messageGroup" || attrs["FifoThroughputLimit"] != "perMessageGroupId" || attrs["FifoQueue"] != "true" {
		t.Fatalf("attributes = %v", attrs)
	}
}

func TestFifoSendValidation(t *testing.T) {
	s, _ := newService(t)
	mustCreate(t, s, "o.fifo", fifoAttrs())
	mustCreate(t, s, "std", nil)
	delay := 5 * time.Second

	_, err := s.Send("o.fifo", SendInput{Body: "x", DeduplicationID: "d"})
	wantCode(t, err, "MissingParameter")
	_, err = s.Send("o.fifo", SendInput{Body: "x", GroupID: "A"})
	wantCode(t, err, "InvalidParameterValue")
	_, err = s.Send("o.fifo", SendInput{Body: "x", GroupID: "A", DeduplicationID: "d", Delay: &delay})
	wantCode(t, err, "InvalidParameterValue")
	_, err = s.Send("o.fifo", SendInput{Body: "x", GroupID: "a b", DeduplicationID: "d"})
	wantCode(t, err, "InvalidParameterValue")
	_, err = s.Send("std", SendInput{Body: "x", DeduplicationID: "d"})
	wantCode(t, err, "InvalidParameterValue")

	// A group id on a standard queue is accepted and ignored.
	if _, err := s.Send("std", SendInput{Body: "x", GroupID: "A"}); err != nil {
		t.Fatal(err)
	}
	got := receive(t, s, "std", 1)
	if got[0].GroupID != "" || got[0].SequenceNumber != "" {
		t.Fatalf("a standard queue kept FIFO fields: %+v", got[0])
	}
}

func TestFifoQueueCreation(t *testing.T) {
	s, _ := newService(t)
	wantCode(t, s.CreateQueue("a.fifo", nil), "InvalidParameterValue")
	wantCode(t, s.CreateQueue("a", fifoAttrs()), "InvalidParameterValue")
	wantCode(t, s.CreateQueue("a.fifo", map[string]string{"FifoQueue": "false"}), "InvalidParameterValue")
	wantCode(t, s.CreateQueue("a.fifo", fifoAttrs("DeduplicationScope", "bogus")), "InvalidAttributeValue")
	wantCode(t, s.CreateQueue("a.fifo", fifoAttrs("FifoThroughputLimit", "perMessageGroupId")), "InvalidAttributeValue")
	wantCode(t, s.CreateQueue("s", map[string]string{"FifoQueue": "false", "DeduplicationScope": "queue"}), "InvalidAttributeName")

	mustCreate(t, s, "a.fifo", fifoAttrs("ContentBasedDeduplication", "true"))
	mustCreate(t, s, "a.fifo", fifoAttrs("ContentBasedDeduplication", "true"))
	wantCode(t, s.CreateQueue("a.fifo", fifoAttrs("ContentBasedDeduplication", "false")), "QueueNameExists")
	wantCode(t, s.SetAttributes("a.fifo", map[string]string{"FifoQueue": "false"}), "InvalidAttributeName")
	if err := s.SetAttributes("a.fifo", map[string]string{"ContentBasedDeduplication": "false"}); err != nil {
		t.Fatal(err)
	}
}

func TestDeadLetterQueueMustMatchTheQueueType(t *testing.T) {
	s, _ := newService(t)
	mustCreate(t, s, "std-dlq", nil)
	mustCreate(t, s, "dlq.fifo", fifoAttrs())
	policy := func(target string) map[string]string {
		return map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"` + s.arn(target) + `","maxReceiveCount":"2"}`}
	}
	wantCode(t, s.CreateQueue("src.fifo", fifoAttrsWith(policy("std-dlq"))), "InvalidAttributeValue")
	wantCode(t, s.CreateQueue("src", policy("dlq.fifo")), "InvalidAttributeValue")
	mustCreate(t, s, "src.fifo", fifoAttrsWith(policy("dlq.fifo")))
}

func fifoAttrsWith(extra map[string]string) map[string]string {
	attrs := fifoAttrs()
	for k, v := range extra {
		attrs[k] = v
	}
	return attrs
}

func TestFifoDeadLetterMove(t *testing.T) {
	s, fake := newService(t)
	mustCreate(t, s, "dlq.fifo", fifoAttrs())
	mustCreate(t, s, "src.fifo", fifoAttrsWith(map[string]string{
		"VisibilityTimeout": "10",
		"RedrivePolicy":     `{"deadLetterTargetArn":"` + s.arn("dlq.fifo") + `","maxReceiveCount":"1"}`,
	}))
	fifoSend(t, s, "src.fifo", "A", "1", "poison")
	receive(t, s, "src.fifo", 1)
	fake.Advance(10 * time.Second)
	wantBodies(t, receive(t, s, "src.fifo", 1))

	dead := receive(t, s, "dlq.fifo", 1)
	wantBodies(t, dead, "poison")
	if dead[0].DeadLetterSource != s.arn("src.fifo") || dead[0].GroupID != "A" || dead[0].SequenceNumber == "" {
		t.Fatalf("dead-lettered message = %+v", dead[0])
	}
}
