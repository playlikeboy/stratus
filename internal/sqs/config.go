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
	maxReceiveBatch   = 10

	minRetention     = 60 * time.Second
	maxRetention     = 14 * 24 * time.Hour
	defaultRetention = 4 * 24 * time.Hour

	minMessageBytes = 1024
	maxMessageBytes = 1 << 20

	scopeQueue        = "queue"
	scopeMessageGroup = "messageGroup"
	perQueue          = "perQueue"
	perMessageGroup   = "perMessageGroupId"
)

// storedAttributes are accepted and returned, but change nothing in how a
// queue behaves here.
var storedAttributes = map[string]bool{
	"Policy":                       true,
	"RedriveAllowPolicy":           true,
	"KmsMasterKeyId":               true,
	"KmsDataKeyReusePeriodSeconds": true,
	"SqsManagedSseEnabled":         true,
}

// fifoAttributes only exist on FIFO queues.
var fifoAttributes = map[string]bool{
	"ContentBasedDeduplication": true,
	"DeduplicationScope":        true,
	"FifoThroughputLimit":       true,
}

// config is a queue's attributes. A config is never changed in place: every
// change builds a new one.
type config struct {
	visibility  time.Duration
	delay       time.Duration
	receiveWait time.Duration
	retention   time.Duration
	maxSize     int
	redrive     *redrive

	fifo         bool // fixed when the queue is created
	contentDedup bool
	dedupScope   string
	throughput   string

	stored map[string]string
}

type redrive struct {
	target          string // queue name
	maxReceiveCount int
}

func defaultConfig() config {
	return config{
		visibility: defaultVisibility,
		retention:  defaultRetention,
		maxSize:    maxMessageBytes,
		dedupScope: scopeQueue,
		throughput: perQueue,
		stored:     map[string]string{},
	}
}

func (c config) equal(o config) bool {
	same := c.visibility == o.visibility && c.delay == o.delay && c.receiveWait == o.receiveWait &&
		c.retention == o.retention && c.maxSize == o.maxSize && c.fifo == o.fifo &&
		c.contentDedup == o.contentDedup && c.dedupScope == o.dedupScope && c.throughput == o.throughput
	if !same || (c.redrive == nil) != (o.redrive == nil) {
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
		if err := s.applyAttribute(&next, name, value); err != nil {
			return config{}, err
		}
	}
	if next.throughput == perMessageGroup && next.dedupScope != scopeMessageGroup {
		return config{}, errInvalidAttributeValue("FifoThroughputLimit",
			"perMessageGroupId needs DeduplicationScope messageGroup.")
	}
	return next, nil
}

// applyAttribute sets one attribute on next, which is already a private copy.
func (s *Service) applyAttribute(next *config, name, value string) error {
	var err error
	switch name {
	case "VisibilityTimeout":
		next.visibility, err = parseSeconds(name, value, 0, maxVisibility)
	case "DelaySeconds":
		next.delay, err = parseSeconds(name, value, 0, maxDelay)
	case "ReceiveMessageWaitTimeSeconds":
		next.receiveWait, err = parseSeconds(name, value, 0, maxReceiveWait)
	case "MessageRetentionPeriod":
		next.retention, err = parseSeconds(name, value, minRetention, maxRetention)
	case "MaximumMessageSize":
		next.maxSize, err = parseBytes(name, value)
	case "RedrivePolicy":
		next.redrive, err = s.parseRedrive(value, next.fifo)
	case "FifoQueue":
		err = checkFifoFlag(next.fifo, value)
	case "ContentBasedDeduplication":
		next.contentDedup, err = parseBool(name, value, next.fifo)
	case "DeduplicationScope":
		next.dedupScope, err = parseChoice(name, value, next.fifo, scopeQueue, scopeMessageGroup)
	case "FifoThroughputLimit":
		next.throughput, err = parseChoice(name, value, next.fifo, perQueue, perMessageGroup)
	default:
		if !storedAttributes[name] {
			return errInvalidAttributeName(name)
		}
		next.stored[name] = value
	}
	return err
}

// checkFifoFlag accepts FifoQueue only when it states what the queue already is:
// the type of a queue cannot change.
func checkFifoFlag(fifo bool, value string) error {
	switch value {
	case "true", "false":
		if (value == "true") == fifo {
			return nil
		}
		return errInvalidAttributeName("FifoQueue")
	}
	return errInvalidAttributeValue("FifoQueue", "must be true or false.")
}

func parseBool(name, value string, fifo bool) (bool, error) {
	if !fifo {
		return false, errInvalidAttributeName(name)
	}
	switch value {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	return false, errInvalidAttributeValue(name, "must be true or false.")
}

func parseChoice(name, value string, fifo bool, choices ...string) (string, error) {
	if !fifo {
		return "", errInvalidAttributeName(name)
	}
	for _, c := range choices {
		if value == c {
			return value, nil
		}
	}
	return "", errInvalidAttributeValue(name, "must be one of "+strings.Join(choices, ", ")+".")
}

func parseSeconds(name, value string, lo, hi time.Duration) (time.Duration, error) {
	n, err := strconv.Atoi(value)
	if err != nil || time.Duration(n)*time.Second < lo || time.Duration(n)*time.Second > hi {
		return 0, errInvalidAttributeValue(name, "must be a number of seconds from "+
			strconv.Itoa(int(lo/time.Second))+" to "+strconv.Itoa(int(hi/time.Second))+".")
	}
	return time.Duration(n) * time.Second, nil
}

func parseBytes(name, value string) (int, error) {
	n, err := strconv.Atoi(value)
	if err != nil || n < minMessageBytes || n > maxMessageBytes {
		return 0, errInvalidAttributeValue(name, "must be a number of bytes from "+
			strconv.Itoa(minMessageBytes)+" to "+strconv.Itoa(maxMessageBytes)+".")
	}
	return n, nil
}

func (s *Service) parseRedrive(value string, fifo bool) (*redrive, error) {
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
	dlq, ok := s.queues[target]
	if !ok {
		return nil, errInvalidAttributeValue("RedrivePolicy", "Dead letter target does not exist.")
	}
	if dlq.cfg.fifo != fifo {
		return nil, errInvalidAttributeValue("RedrivePolicy",
			"the dead-letter queue must be of the same type as the source queue (FIFO or standard).")
	}
	return &redrive{target: target, maxReceiveCount: count}, nil
}

// attributesLocked renders q's attributes as GetQueueAttributes returns them.
func (s *Service) attributesLocked(q *queue, now time.Time) map[string]string {
	visible, inFlight, delayed := 0, 0, 0
	for _, m := range q.messages {
		switch {
		case m.deleted:
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
	out["MessageRetentionPeriod"] = seconds(q.cfg.retention)
	out["MaximumMessageSize"] = strconv.Itoa(q.cfg.maxSize)
	out["ApproximateNumberOfMessages"] = strconv.Itoa(visible)
	out["ApproximateNumberOfMessagesNotVisible"] = strconv.Itoa(inFlight)
	out["ApproximateNumberOfMessagesDelayed"] = strconv.Itoa(delayed)
	if q.cfg.fifo {
		out["FifoQueue"] = "true"
		out["ContentBasedDeduplication"] = strconv.FormatBool(q.cfg.contentDedup)
		out["DeduplicationScope"] = q.cfg.dedupScope
		out["FifoThroughputLimit"] = q.cfg.throughput
	}
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
