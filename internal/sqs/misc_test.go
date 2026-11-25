package sqs

import (
	"strings"
	"testing"
	"time"
)

func TestRetentionDropsOldMessagesLazily(t *testing.T) {
	s, fake := newService(t)
	mustCreate(t, s, "jobs", map[string]string{"MessageRetentionPeriod": "60"})
	mustSend(t, s, "jobs", "old")
	fake.Advance(30 * time.Second)
	mustSend(t, s, "jobs", "young")

	fake.Advance(31 * time.Second) // "old" is 61 s old, "young" 31 s
	attrs, _ := s.Attributes("jobs")
	if attrs["ApproximateNumberOfMessages"] != "1" || attrs["MessageRetentionPeriod"] != "60" {
		t.Fatalf("attributes = %v, want only the young message", attrs)
	}
	wantBodies(t, receive(t, s, "jobs", 10), "young")

	// An in-flight message expires too, and a send touches the queue as well.
	fake.Advance(time.Hour)
	mustSend(t, s, "jobs", "new")
	if attrs, _ := s.Attributes("jobs"); attrs["ApproximateNumberOfMessagesNotVisible"] != "0" {
		t.Fatalf("an expired in-flight message is still counted: %v", attrs)
	}
	wantBodies(t, receive(t, s, "jobs", 10), "new")
}

func TestRetentionAndSizeAttributesAreValidated(t *testing.T) {
	s, _ := newService(t)
	for _, attrs := range []map[string]string{
		{"MessageRetentionPeriod": "59"},
		{"MessageRetentionPeriod": "1209601"},
		{"MessageRetentionPeriod": "x"},
		{"MaximumMessageSize": "1023"},
		{"MaximumMessageSize": "1048577"},
	} {
		wantCode(t, s.CreateQueue("q", attrs), "InvalidAttributeValue")
	}
	mustCreate(t, s, "q", map[string]string{"MessageRetentionPeriod": "1209600", "MaximumMessageSize": "1024"})
	attrs, _ := s.Attributes("q")
	if attrs["MessageRetentionPeriod"] != "1209600" || attrs["MaximumMessageSize"] != "1024" {
		t.Fatalf("attributes = %v", attrs)
	}
	mustCreate(t, s, "d", nil)
	attrs, _ = s.Attributes("d")
	if attrs["MessageRetentionPeriod"] != "345600" || attrs["MaximumMessageSize"] != "1048576" {
		t.Fatalf("defaults = %v", attrs)
	}
}

func TestMaximumMessageSizeCountsAttributes(t *testing.T) {
	s, _ := newService(t)
	mustCreate(t, s, "q", map[string]string{"MaximumMessageSize": "1024"})
	mustSend(t, s, "q", strings.Repeat("x", 1024))
	_, err := s.Send("q", SendInput{Body: strings.Repeat("x", 1025)})
	wantCode(t, err, "InvalidParameterValue")

	value := strings.Repeat("v", 100)
	_, err = s.Send("q", SendInput{
		Body:       strings.Repeat("x", 1000),
		Attributes: map[string]MessageAttribute{"a": {DataType: "String", StringValue: &value}},
	})
	wantCode(t, err, "InvalidParameterValue")
}

func TestTraceHeaderSystemAttribute(t *testing.T) {
	s, _ := newService(t)
	mustCreate(t, s, "q", nil)
	header := "Root=1-5759e988-bd862e3fe1be46a994272793"
	out, err := s.Send("q", SendInput{Body: "x", SystemAttributes: map[string]MessageAttribute{
		"AWSTraceHeader": {DataType: "String", StringValue: &header},
	}})
	if err != nil || out.MD5OfSystemAttributes == "" {
		t.Fatalf("Send: %+v, %v", out, err)
	}
	if got := receive(t, s, "q", 1); got[0].TraceHeader != header {
		t.Fatalf("trace header = %q", got[0].TraceHeader)
	}
	_, err = s.Send("q", SendInput{Body: "x", SystemAttributes: map[string]MessageAttribute{
		"Other": {DataType: "String", StringValue: &header},
	}})
	wantCode(t, err, "InvalidParameterValue")
}

func TestTags(t *testing.T) {
	s, _ := newService(t)
	if err := s.CreateQueueWithTags("q", nil, map[string]string{"env": "dev"}); err != nil {
		t.Fatal(err)
	}
	if err := s.TagQueue("q", map[string]string{"env": "test", "team": "a"}); err != nil {
		t.Fatal(err)
	}
	tags, err := s.ListQueueTags("q")
	if err != nil || len(tags) != 2 || tags["env"] != "test" || tags["team"] != "a" {
		t.Fatalf("tags = %v, %v", tags, err)
	}
	if err := s.UntagQueue("q", []string{"env", "absent"}); err != nil {
		t.Fatal(err)
	}
	if tags, _ := s.ListQueueTags("q"); len(tags) != 1 || tags["team"] != "a" {
		t.Fatalf("tags after untag = %v", tags)
	}
	wantCode(t, s.TagQueue("q", nil), "MissingParameter")
	wantCode(t, s.TagQueue("q", map[string]string{"aws:x": "1"}), "InvalidParameterValue")
	wantCode(t, s.TagQueue("missing", map[string]string{"a": "1"}), "QueueDoesNotExist")
	_, err = s.ListQueueTags("missing")
	wantCode(t, err, "QueueDoesNotExist")

	many := map[string]string{}
	for i := range 51 {
		many[strings.Repeat("k", i+1)] = "v"
	}
	wantCode(t, s.TagQueue("q", many), "InvalidParameterValue")
}

func TestListDeadLetterSourceQueues(t *testing.T) {
	s, _ := newService(t)
	mustCreate(t, s, "dlq", nil)
	policy := map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"` + s.arn("dlq") + `","maxReceiveCount":"3"}`}
	mustCreate(t, s, "b-src", policy)
	mustCreate(t, s, "a-src", policy)
	mustCreate(t, s, "other", nil)

	names, next, err := s.ListDeadLetterSourceQueues("dlq", 0, "")
	if err != nil || next != "" || len(names) != 2 || names[0] != "a-src" || names[1] != "b-src" {
		t.Fatalf("sources = %v, %q, %v", names, next, err)
	}
	_, _, err = s.ListDeadLetterSourceQueues("missing", 0, "")
	wantCode(t, err, "QueueDoesNotExist")
}

func TestListQueuesPagination(t *testing.T) {
	s, _ := newService(t)
	for _, name := range []string{"e", "a", "c", "b", "d"} {
		mustCreate(t, s, name, nil)
	}
	var all []string
	token := ""
	for pages := 0; ; pages++ {
		names, next, err := s.ListQueuesPage("", 2, token)
		if err != nil || len(names) > 2 || pages > 3 {
			t.Fatalf("page %d = %v, %v", pages, names, err)
		}
		all = append(all, names...)
		if next == "" {
			break
		}
		token = next
	}
	if strings.Join(all, "") != "abcde" {
		t.Fatalf("pages together = %v, want a..e once each", all)
	}
	_, _, err := s.ListQueuesPage("", 2, "!!not base64!!")
	wantCode(t, err, "InvalidParameterValue")
}

func TestDeadLetterSourceArnOnStandardQueues(t *testing.T) {
	s, fake := newService(t)
	mustCreate(t, s, "dlq", nil)
	mustCreate(t, s, "src", map[string]string{
		"VisibilityTimeout": "5",
		"RedrivePolicy":     `{"deadLetterTargetArn":"` + s.arn("dlq") + `","maxReceiveCount":"1"}`,
	})
	mustSend(t, s, "src", "poison")
	receive(t, s, "src", 1)
	fake.Advance(5 * time.Second)
	receive(t, s, "src", 1)
	dead := receive(t, s, "dlq", 1)
	if len(dead) != 1 || dead[0].DeadLetterSource != s.arn("src") {
		t.Fatalf("dead-letter queue has %+v", dead)
	}
}
