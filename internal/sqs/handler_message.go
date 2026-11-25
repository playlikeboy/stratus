package sqs

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
)

// messageFields are the members a message has in SendMessage and in an entry of
// SendMessageBatch.
type messageFields struct {
	MessageBody             string
	DelaySeconds            *int
	MessageAttributes       map[string]MessageAttribute
	MessageSystemAttributes map[string]MessageAttribute
	MessageDeduplicationId  string //nolint:revive // the wire name
	MessageGroupId          string //nolint:revive // the wire name
}

// sendInput converts the wire members; whether the delay is acceptable is the
// service's decision, so that a batch can fail just the entry that has it wrong.
func (f messageFields) sendInput() SendInput {
	in := SendInput{
		Body:             f.MessageBody,
		Attributes:       f.MessageAttributes,
		SystemAttributes: f.MessageSystemAttributes,
		GroupID:          f.MessageGroupId,
		DeduplicationID:  f.MessageDeduplicationId,
	}
	if f.DelaySeconds != nil {
		delay := time.Duration(*f.DelaySeconds) * time.Second
		in.Delay = &delay
	}
	return in
}

type sendMessageInput struct {
	QueueUrl string //nolint:revive // the wire name
	messageFields
}

type sendResult struct {
	MessageId                    string //nolint:revive // the wire name
	MD5OfMessageBody             string
	MD5OfMessageAttributes       string `json:",omitempty"`
	MD5OfMessageSystemAttributes string `json:",omitempty"`
	SequenceNumber               string `json:",omitempty"`
}

func newSendResult(out SendOutput) sendResult {
	return sendResult{
		MessageId:                    out.MessageID,
		MD5OfMessageBody:             out.MD5OfBody,
		MD5OfMessageAttributes:       out.MD5OfAttributes,
		MD5OfMessageSystemAttributes: out.MD5OfSystemAttributes,
		SequenceNumber:               out.SequenceNumber,
	}
}

func (s *Service) sendMessage(_ *http.Request, in sendMessageInput) (any, error) {
	out, err := s.Send(queueName(in.QueueUrl), in.sendInput())
	if err != nil {
		return nil, err
	}
	return newSendResult(out), nil
}

type receiveMessageInput struct {
	QueueUrl                    string //nolint:revive // the wire name
	MaxNumberOfMessages         *int
	VisibilityTimeout           *int
	WaitTimeSeconds             *int
	AttributeNames              []string // the deprecated spelling of the next field
	MessageSystemAttributeNames []string
	MessageAttributeNames       []string
	ReceiveRequestAttemptId     string //nolint:revive // accepted and ignored
}

type receivedMessage struct {
	MessageId              string //nolint:revive // the wire name
	ReceiptHandle          string
	MD5OfBody              string
	Body                   string
	Attributes             map[string]string           `json:",omitempty"`
	MD5OfMessageAttributes string                      `json:",omitempty"`
	MessageAttributes      map[string]MessageAttribute `json:",omitempty"`
}

func (s *Service) receiveMessage(r *http.Request, in receiveMessageInput) (any, error) {
	visibility, err := optionalSeconds("VisibilityTimeout", in.VisibilityTimeout, maxVisibility)
	if err != nil {
		return nil, err
	}
	wait, err := optionalSeconds("WaitTimeSeconds", in.WaitTimeSeconds, maxReceiveWait)
	if err != nil {
		return nil, err
	}
	limit := 0
	if in.MaxNumberOfMessages != nil {
		if limit = *in.MaxNumberOfMessages; limit == 0 {
			limit = -1 // an explicit 0 is invalid, unlike an absent value
		}
	}
	received, err := s.Receive(r.Context(), queueName(in.QueueUrl), ReceiveInput{
		Max:        limit,
		Visibility: visibility,
		Wait:       wait,
	})
	if err != nil {
		return nil, err
	}
	if len(received) == 0 {
		return empty{}, nil
	}
	systemNames := slices.Concat(in.MessageSystemAttributeNames, in.AttributeNames)
	messages := make([]receivedMessage, 0, len(received))
	for _, m := range received {
		attrs := selectAttributes(m.Attributes, in.MessageAttributeNames)
		messages = append(messages, receivedMessage{
			MessageId:              m.MessageID,
			ReceiptHandle:          m.Receipt,
			MD5OfBody:              m.MD5OfBody,
			Body:                   m.Body,
			Attributes:             s.systemAttributes(m, systemNames),
			MD5OfMessageAttributes: md5OfAttributes(attrs),
			MessageAttributes:      attrs,
		})
	}
	return map[string][]receivedMessage{"Messages": messages}, nil
}

// systemAttributes returns the message system attributes a receiver asked for,
// by name or all with "All". Those a message does not have are left out.
func (s *Service) systemAttributes(m Received, names []string) map[string]string {
	if len(names) == 0 {
		return nil
	}
	all := map[string]string{
		"SenderId":                         s.identity.Account,
		"SentTimestamp":                    strconv.FormatInt(m.SentAt.UnixMilli(), 10),
		"ApproximateReceiveCount":          strconv.Itoa(m.ReceiveCount),
		"ApproximateFirstReceiveTimestamp": strconv.FormatInt(m.FirstReceivedAt.UnixMilli(), 10),
	}
	for name, value := range map[string]string{
		traceHeaderName:            m.TraceHeader,
		"MessageGroupId":           m.GroupID,
		"MessageDeduplicationId":   m.DeduplicationID,
		"SequenceNumber":           m.SequenceNumber,
		"DeadLetterQueueSourceArn": m.DeadLetterSource,
	} {
		if value != "" {
			all[name] = value
		}
	}
	if slices.Contains(names, "All") {
		return all
	}
	out := map[string]string{}
	for _, name := range names {
		if value, ok := all[name]; ok {
			out[name] = value
		}
	}
	return out
}

// selectAttributes returns the attributes a receiver asked for: by name, by
// "prefix.*", or all of them with "All" or ".*".
func selectAttributes(attrs map[string]MessageAttribute, names []string) map[string]MessageAttribute {
	out := map[string]MessageAttribute{}
	for name, attr := range attrs {
		for _, want := range names {
			prefix, isPrefix := strings.CutSuffix(want, ".*")
			if want == "All" || want == name || (isPrefix && strings.HasPrefix(name, prefix)) {
				out[name] = attr
				break
			}
		}
	}
	return out
}

type deleteMessageInput struct {
	QueueUrl      string //nolint:revive // the wire name
	ReceiptHandle string
}

func (s *Service) deleteMessage(_ *http.Request, in deleteMessageInput) (any, error) {
	return empty{}, s.Delete(queueName(in.QueueUrl), in.ReceiptHandle)
}

type changeMessageVisibilityInput struct {
	QueueUrl          string //nolint:revive // the wire name
	ReceiptHandle     string
	VisibilityTimeout *int
}

func (s *Service) changeMessageVisibility(_ *http.Request, in changeMessageVisibilityInput) (any, error) {
	if in.VisibilityTimeout == nil {
		return nil, errMissingParameter("VisibilityTimeout")
	}
	timeout, err := optionalSeconds("VisibilityTimeout", in.VisibilityTimeout, maxVisibility)
	if err != nil {
		return nil, err
	}
	return empty{}, s.ChangeVisibility(queueName(in.QueueUrl), in.ReceiptHandle, *timeout)
}
