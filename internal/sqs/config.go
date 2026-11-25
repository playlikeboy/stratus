package sqs

import (
	"encoding/json"
	"maps"
	"strconv"
	"strings"
	"time"
)

const (
	defaultVisibility = 30 * time.Second
	maxVisibility     = 12 * time.Hour
	maxDelay          = 15 * time.Minute
	maxReceiveWait    = 20 * time.Second
	maxMessageBytes   = 1 << 20
	maxReceiveBatch   = 10
)

// storedAttributes are accepted and returned, but change nothing in how a
// queue behaves here.
var storedAttributes = map[string]bool{
	"MessageRetentionPeriod":       true,
	"MaximumMessageSize":           true,
	"Policy":                       true,
	"RedriveAllowPolicy":           true,
	"KmsMasterKeyId":               true,
	"KmsDataKeyReusePeriodSeconds": true,
	"SqsManagedSseEnabled":         true,
}

// config is a queue's attributes. A config is never changed in place: every
// change builds a new one.
type config struct {
	visibility  time.Duration
	delay       time.Duration
	receiveWait time.Duration
	redrive     *redrive
	stored      map[string]string
}

type redrive struct {
	target          string // queue name
	maxReceiveCount int
}

func defaultConfig() config {
	return config{
		visibility: defaultVisibility,
		stored: map[string]string{
			"MessageRetentionPeriod": "345600",
			"MaximumMessageSize":     strconv.Itoa(maxMessageBytes),
		},
	}
}

func (c config) equal(o config) bool {
	if c.visibility != o.visibility || c.delay != o.delay || c.receiveWait != o.receiveWait {
		return false
	}
	if (c.redrive == nil) != (o.redrive == nil) {
		return false
	}
	if c.redrive != nil && *c.redrive != *o.redrive {
		return false
	}
	return maps.Equal(c.stored, o.stored)
}

// withAttributes returns c changed by attrs, as CreateQueue and
// SetQueueAttributes take them. The caller holds s.mu.
func (s *Service) withAttributes(c config, attrs map[string]string) (config, error) {
	next := c
	next.stored = maps.Clone(c.stored)
	for name, value := range attrs {
		var err error
		switch name {
		case "VisibilityTimeout":
			next.visibility, err = parseSeconds(name, value, maxVisibility)
		case "DelaySeconds":
			next.delay, err = parseSeconds(name, value, maxDelay)
		case "ReceiveMessageWaitTimeSeconds":
			next.receiveWait, err = parseSeconds(name, value, maxReceiveWait)
		case "RedrivePolicy":
			next.redrive, err = s.parseRedrive(value)
		default:
			if !storedAttributes[name] {
				return config{}, errInvalidAttributeName(name)
			}
			next.stored[name] = value
		}
		if err != nil {
			return config{}, err
		}
	}
	return next, nil
}

func parseSeconds(name, value string, limit time.Duration) (time.Duration, error) {
	n, err := strconv.Atoi(value)
	if err != nil || n < 0 || time.Duration(n)*time.Second > limit {
		return 0, errInvalidAttributeValue(name,
			"must be a number of seconds from 0 to "+strconv.Itoa(int(limit/time.Second))+".")
	}
	return time.Duration(n) * time.Second, nil
}

func (s *Service) parseRedrive(value string) (*redrive, error) {
	if value == "" {
		return nil, nil
	}
	var policy struct {
		DeadLetterTargetArn string      `json:"deadLetterTargetArn"`
		MaxReceiveCount     json.Number `json:"maxReceiveCount"`
	}
	if err := json.Unmarshal([]byte(value), &policy); err != nil {
		return nil, errInvalidAttributeValue("RedrivePolicy", "it is not valid JSON.")
	}
	count, err := strconv.Atoi(policy.MaxReceiveCount.String())
	if err != nil || count < 1 || count > 1000 {
		return nil, errInvalidAttributeValue("RedrivePolicy", "maxReceiveCount must be from 1 to 1000.")
	}
	target := policy.DeadLetterTargetArn[strings.LastIndex(policy.DeadLetterTargetArn, ":")+1:]
	if policy.DeadLetterTargetArn != s.arn(target) {
		return nil, errInvalidAttributeValue("RedrivePolicy",
			"deadLetterTargetArn must be an ARN of a queue in this account and region.")
	}
	if _, ok := s.queues[target]; !ok {
		return nil, errInvalidAttributeValue("RedrivePolicy", "Dead letter target does not exist.")
	}
	return &redrive{target: target, maxReceiveCount: count}, nil
}

// attributesLocked renders q's attributes as GetQueueAttributes returns them.
func (s *Service) attributesLocked(q *queue, now time.Time) map[string]string {
	visible, inFlight, delayed := 0, 0, 0
	for _, m := range q.messages {
		switch {
		case !m.visibleAt.After(now):
			visible++
		case m.receiveCount == 0:
			delayed++
		default:
			inFlight++
		}
	}
	out := maps.Clone(q.cfg.stored)
	out["QueueArn"] = s.arn(q.name)
	out["CreatedTimestamp"] = strconv.FormatInt(q.createdAt.Unix(), 10)
	out["LastModifiedTimestamp"] = strconv.FormatInt(q.modifiedAt.Unix(), 10)
	out["VisibilityTimeout"] = seconds(q.cfg.visibility)
	out["DelaySeconds"] = seconds(q.cfg.delay)
	out["ReceiveMessageWaitTimeSeconds"] = seconds(q.cfg.receiveWait)
	out["ApproximateNumberOfMessages"] = strconv.Itoa(visible)
	out["ApproximateNumberOfMessagesNotVisible"] = strconv.Itoa(inFlight)
	out["ApproximateNumberOfMessagesDelayed"] = strconv.Itoa(delayed)
	if r := q.cfg.redrive; r != nil {
		policy, _ := json.Marshal(map[string]any{
			"deadLetterTargetArn": s.arn(r.target),
			"maxReceiveCount":     r.maxReceiveCount,
		})
		out["RedrivePolicy"] = string(policy)
	}
	return out
}

func seconds(d time.Duration) string {
	return strconv.Itoa(int(d / time.Second))
}
