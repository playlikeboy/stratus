package server

import (
	"strings"
	"testing"
)

func TestQueuePageNamesAttributesForPeople(t *testing.T) {
	_, base := newOpsServer(t)
	_, page := get(t, base+"/_stratus/ui/sqs/jobs")
	for _, want := range []string{"Delayed messages", "Visibility timeout", "30 seconds", "Message retention", "4 days",
		"Maximum message size", "Standard", "after 3 receives"} {
		if !strings.Contains(page, want) {
			t.Errorf("queue page lacks %q", want)
		}
	}
	for _, raw := range []string{"ApproximateNumberOfMessagesDelayed", "VisibilityTimeout", "QueueArn", "RedrivePolicy"} {
		if strings.Contains(page, raw) {
			t.Errorf("queue page shows the API name %s", raw)
		}
	}
}

func TestLabels(t *testing.T) {
	cases := map[string]string{
		splitWords("ApproximateNumberOfMessages"): "Approximate number of messages",
		splitWords("KmsMasterKeyId"):              "Kms master key id",
		humanEnum("PROVISIONED"):                  "Provisioned",
		humanEnum("ON_DEMAND"):                    "On demand",
		humanEnum("perMessageGroupId"):            "Per message group id",
		seconds("345600"):                         "4 days",
		seconds("90"):                             "90 seconds",
		seconds("0"):                              "none",
		retention(24):                             "1 day",
		keyType("N"):                              "Number",
		projection("INCLUDE", []string{"total"}):  "Keys and total",
		statusLabel("TIMED_OUT"):                  "Timed out",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}
