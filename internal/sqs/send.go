package sqs

import (
	"strconv"
	"strings"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

const traceHeaderName = "AWSTraceHeader"

func (s *Service) Send(name string, in SendInput) (SendOutput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queueLocked(name)
	if err != nil {
		return SendOutput{}, err
	}
	return s.sendLocked(q, in)
}

// sendLocked validates a message against q's type and limits and enqueues it.
// On a FIFO queue a repeat of a message seen in the last five minutes is
// accepted, answered with the first one's result and not enqueued.
func (s *Service) sendLocked(q *queue, in SendInput) (SendOutput, error) {
	if err := validateSend(q, in); err != nil {
		return SendOutput{}, err
	}
	now := s.clock.Now()
	delay := q.cfg.delay
	m := &message{
		id:         awsapi.NewID(),
		body:       in.Body,
		md5OfBody:  md5Hex(in.Body),
		attributes: in.Attributes,
		sentAt:     now,
	}
	if sv := in.SystemAttributes[traceHeaderName].StringValue; sv != nil {
		m.traceHeader = *sv
	}
	out := SendOutput{
		MessageID:             m.id,
		MD5OfBody:             m.md5OfBody,
		MD5OfAttributes:       md5OfAttributes(in.Attributes),
		MD5OfSystemAttributes: md5OfAttributes(in.SystemAttributes),
	}
	if q.cfg.fifo {
		group, dedup, err := q.cfg.fifoFields(in)
		if err != nil {
			return SendOutput{}, err
		}
		key := q.cfg.dedupKey(group, dedup)
		if first, dup := q.fifo.duplicate(key, now); dup {
			return first, nil
		}
		m.groupID, m.dedupID, m.sequence = group, dedup, q.fifo.nextSequence()
		out.SequenceNumber = m.sequence
		q.fifo.remember(key, out, now)
	} else if in.Delay != nil {
		delay = *in.Delay
	}
	m.visibleAt = now.Add(delay)
	q.append(m)
	wake(q)
	return out, nil
}

// validateSend runs the checks that do not change q.
func validateSend(q *queue, in SendInput) error {
	if in.Body == "" {
		return errMissingParameter("MessageBody")
	}
	if err := validateAttributes(in.Attributes); err != nil {
		return err
	}
	if err := validateSystemAttributes(in.SystemAttributes); err != nil {
		return err
	}
	if messageSize(in.Body, in.Attributes) > q.cfg.maxSize {
		return errInvalidParameter("One or more parameters are invalid. Reason: Message must be shorter than " +
			strconv.Itoa(q.cfg.maxSize) + " bytes.")
	}
	if in.Delay != nil && (*in.Delay < 0 || *in.Delay > maxDelay) {
		return errInvalidParameter("Value " + seconds(*in.Delay) + " for parameter DelaySeconds is invalid. " +
			"Reason: Must be between 0 and " + seconds(maxDelay) + ", if provided.")
	}
	if !q.cfg.fifo {
		if in.DeduplicationID != "" {
			return errNotForQueueType("MessageDeduplicationId", in.DeduplicationID)
		}
		return nil
	}
	if in.Delay != nil && *in.Delay > 0 {
		return errNotForQueueType("DelaySeconds", seconds(*in.Delay))
	}
	return nil
}

// messageSize is what counts against MaximumMessageSize: the body and the
// names, data types and values of the message attributes.
func messageSize(body string, attrs map[string]MessageAttribute) int {
	size := len(body)
	for name, a := range attrs {
		size += len(name) + len(a.DataType) + len(a.BinaryValue)
		if a.StringValue != nil {
			size += len(*a.StringValue)
		}
	}
	return size
}

func validateAttributes(attrs map[string]MessageAttribute) error {
	if len(attrs) > 10 {
		return errInvalidParameter("Number of message attributes [" + strconv.Itoa(len(attrs)) +
			"] exceeds the allowed maximum [10].")
	}
	for name, attr := range attrs {
		kind, _, _ := strings.Cut(attr.DataType, ".")
		switch kind {
		case "String", "Number":
			if attr.StringValue == nil {
				return errInvalidParameter("Message attribute '" + name + "' must have a StringValue.")
			}
		case "Binary":
			if attr.BinaryValue == nil {
				return errInvalidParameter("Message attribute '" + name + "' must have a BinaryValue.")
			}
		default:
			return errInvalidParameter("Message attribute '" + name +
				"' has an invalid DataType; it must start with String, Number or Binary.")
		}
	}
	return nil
}

// validateSystemAttributes accepts AWSTraceHeader, a String, and nothing else.
func validateSystemAttributes(attrs map[string]MessageAttribute) error {
	for name, attr := range attrs {
		if name != traceHeaderName {
			return errInvalidParameter("Message system attribute '" + name + "' is not supported; " +
				"the only one is " + traceHeaderName + ".")
		}
		if attr.DataType != "String" || attr.StringValue == nil || *attr.StringValue == "" {
			return errInvalidParameter("Message system attribute '" + name + "' must be a String with a StringValue.")
		}
	}
	return nil
}
