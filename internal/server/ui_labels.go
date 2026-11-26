package server

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// The UI names things the way people read them, not the way the API spells
// them: "Visibility timeout · 30 seconds", not "VisibilityTimeout 30".

// queueAttributeLabels are the queue attributes the UI shows, in order, with
// how to show each value. RedrivePolicy is shown as the dead-letter queue.
var queueAttributeLabels = []struct {
	name, label string
	format      func(string) string
}{
	{"ApproximateNumberOfMessages", "Visible messages", plain},
	{"ApproximateNumberOfMessagesNotVisible", "Messages in flight", plain},
	{"ApproximateNumberOfMessagesDelayed", "Delayed messages", plain},
	{"FifoQueue", "Type", func(v string) string {
		if v == "true" {
			return "FIFO"
		}
		return "Standard"
	}},
	{"ContentBasedDeduplication", "Content-based deduplication", yesNo},
	{"DeduplicationScope", "Deduplication scope", humanEnum},
	{"FifoThroughputLimit", "Throughput limit", humanEnum},
	{"VisibilityTimeout", "Visibility timeout", seconds},
	{"DelaySeconds", "Delivery delay", seconds},
	{"ReceiveMessageWaitTimeSeconds", "Receive wait (long polling)", seconds},
	{"MessageRetentionPeriod", "Message retention", seconds},
	{"MaximumMessageSize", "Maximum message size", func(v string) string {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return v
		}
		return formatBytes(n)
	}},
	{"SqsManagedSseEnabled", "Encryption (SSE-SQS)", yesNo},
	{"KmsMasterKeyId", "KMS key", plain},
	{"CreatedTimestamp", "Created", epoch},
	{"LastModifiedTimestamp", "Last modified", epoch},
	{"QueueArn", "ARN", plain},
}

// labelledAttributes turns a queue's attributes into what the UI shows:
// the known ones labelled and formatted, in a fixed order, then any other
// with its name split into words.
func labelledAttributes(attrs map[string]string) []attribute {
	var out []attribute
	seen := map[string]bool{"RedrivePolicy": true, "RedriveAllowPolicy": true, "Policy": true}
	if attrs["FifoQueue"] == "" {
		out = append(out, attribute{"Type", "Standard"})
	}
	for _, a := range queueAttributeLabels {
		seen[a.name] = true
		if v, ok := attrs[a.name]; ok {
			out = append(out, attribute{a.label, a.format(v)})
		}
	}
	for name, v := range attrs {
		if !seen[name] {
			out = append(out, attribute{splitWords(name), v})
		}
	}
	return out
}

func plain(v string) string { return v }

func yesNo(v string) string {
	if v == "true" {
		return "on"
	}
	return "off"
}

// seconds shows a number of seconds in the largest unit that fits evenly.
func seconds(v string) string {
	n, err := strconv.Atoi(v)
	if err != nil {
		return v
	}
	return humanDuration(time.Duration(n) * time.Second)
}

func humanDuration(d time.Duration) string {
	units := []struct {
		size       time.Duration
		one, other string
	}{{24 * time.Hour, "day", "days"}, {time.Hour, "hour", "hours"}, {time.Minute, "minute", "minutes"}, {time.Second, "second", "seconds"}}
	if d == 0 {
		return "none"
	}
	for _, u := range units {
		if d%u.size == 0 {
			n := int64(d / u.size)
			if n == 1 {
				return "1 " + u.one
			}
			return fmt.Sprintf("%d %s", n, u.other)
		}
	}
	return d.String()
}

func epoch(v string) string {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return v
	}
	return time.Unix(n, 0).Local().Format("2006-01-02 15:04:05")
}

// humanEnum shows an API constant as words: "PROVISIONED" as "Provisioned",
// "perMessageGroupId" as "Per message group id".
func humanEnum(v string) string {
	if v == "" {
		return v
	}
	if strings.ToUpper(v) == v {
		v = strings.ToLower(strings.ReplaceAll(v, "_", " "))
	} else {
		v = strings.ToLower(splitWords(v))
	}
	return strings.ToUpper(v[:1]) + v[1:]
}

// splitWords splits a CamelCase name into words: "ApproximateNumberOfMessages"
// becomes "Approximate number of messages".
func splitWords(name string) string {
	var b strings.Builder
	runes := []rune(name)
	for i, r := range runes {
		if i > 0 && unicode.IsUpper(r) && (unicode.IsLower(runes[i-1]) ||
			(i+1 < len(runes) && unicode.IsLower(runes[i+1]))) {
			b.WriteByte(' ')
			r = unicode.ToLower(r)
		}
		b.WriteRune(r)
	}
	return b.String()
}

// keyType names a DynamoDB key type.
func keyType(t string) string {
	return map[string]string{"S": "String", "N": "Number", "B": "Binary"}[t]
}

// projection describes what a secondary index holds.
func projection(typ string, nonKey []string) string {
	switch typ {
	case "KEYS_ONLY":
		return "Keys only"
	case "INCLUDE":
		return "Keys and " + strings.Join(nonKey, ", ")
	}
	return "All attributes"
}

// executionStatuses are the statuses an execution can have, with their names.
var executionStatuses = []choice{
	{"RUNNING", "Running"}, {"SUCCEEDED", "Succeeded"}, {"FAILED", "Failed"},
	{"TIMED_OUT", "Timed out"}, {"ABORTED", "Stopped"},
}

func statusLabel(s string) string {
	for _, c := range executionStatuses {
		if c.Value == s {
			return c.Label
		}
	}
	return humanEnum(s)
}

// retention shows a stream's retention period.
func retention(hours int) string { return humanDuration(time.Duration(hours) * time.Hour) }
