// Package sqs emulates Amazon SQS standard queues over the JSON protocol.
package sqs

import (
	"context"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ODudek/stratus/internal/awsapi"
	"github.com/ODudek/stratus/internal/clock"
)

var validName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)

// Service holds every queue of one emulator.
type Service struct {
	identity awsapi.Identity
	clock    clock.Clock

	mu     sync.Mutex
	queues map[string]*queue
}

type queue struct {
	name       string
	createdAt  time.Time
	modifiedAt time.Time
	cfg        config
	messages   []*message
	// changed is closed, and replaced, whenever a waiting receiver should look
	// at the queue again.
	changed chan struct{}
}

type message struct {
	id              string
	body            string
	md5OfBody       string
	attributes      map[string]MessageAttribute // never changed after the send
	sentAt          time.Time
	visibleAt       time.Time
	firstReceivedAt time.Time
	receiveCount    int
	receipt         string
}

// MessageAttribute is a message attribute, in its wire form.
type MessageAttribute struct {
	DataType    string  `json:"DataType"`
	StringValue *string `json:"StringValue,omitempty"`
	BinaryValue []byte  `json:"BinaryValue,omitempty"`
}

// SendInput is one message to send.
type SendInput struct {
	Body       string
	Delay      *time.Duration // nil: the queue's delay
	Attributes map[string]MessageAttribute
}

// SendOutput describes a sent message.
type SendOutput struct {
	MessageID       string
	MD5OfBody       string
	MD5OfAttributes string
}

// ReceiveInput is one receive call.
type ReceiveInput struct {
	Max        int            // 0: one message
	Visibility *time.Duration // nil: the queue's visibility timeout
	Wait       *time.Duration // nil: the queue's receive wait time
}

// Received is a message handed to a consumer.
type Received struct {
	MessageID       string
	Receipt         string
	Body            string
	MD5OfBody       string
	Attributes      map[string]MessageAttribute
	SentAt          time.Time
	FirstReceivedAt time.Time
	ReceiveCount    int
}

func New(identity awsapi.Identity, c clock.Clock) *Service {
	return &Service{identity: identity, clock: c, queues: map[string]*queue{}}
}

func (s *Service) Name() string         { return "sqs" }
func (s *Service) TargetPrefix() string { return targetPrefix }

// Reset drops every queue; receivers waiting on them fail with QueueDoesNotExist.
func (s *Service) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, q := range s.queues {
		close(q.changed)
	}
	s.queues = map[string]*queue{}
}

func (s *Service) arn(name string) string { return s.identity.ARN("sqs", name) }

// CreateQueue creates a queue, or does nothing when one of that name already
// has exactly these attributes.
func (s *Service) CreateQueue(name string, attrs map[string]string) error {
	if !validName.MatchString(name) {
		return errInvalidParameter("Queue name can only include alphanumeric characters, " +
			"hyphens, or underscores, 1 to 80 in length (FIFO queues are not supported).")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if q, ok := s.queues[name]; ok {
		cfg, err := s.withAttributes(q.cfg, attrs)
		if err != nil {
			return err
		}
		if !cfg.equal(q.cfg) {
			return errQueueNameExists()
		}
		return nil
	}
	cfg, err := s.withAttributes(defaultConfig(), attrs)
	if err != nil {
		return err
	}
	now := s.clock.Now()
	s.queues[name] = &queue{name: name, createdAt: now, modifiedAt: now, cfg: cfg, changed: make(chan struct{})}
	return nil
}

func (s *Service) DeleteQueue(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queueLocked(name)
	if err != nil {
		return err
	}
	close(q.changed)
	delete(s.queues, name)
	return nil
}

// Exists reports whether the queue exists.
func (s *Service) Exists(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.queues[name]
	return ok
}

// ListQueues returns the sorted names of the queues with the prefix.
func (s *Service) ListQueues(prefix string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.queues))
	for name := range s.queues {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

func (s *Service) SetAttributes(name string, attrs map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queueLocked(name)
	if err != nil {
		return err
	}
	cfg, err := s.withAttributes(q.cfg, attrs)
	if err != nil {
		return err
	}
	q.cfg = cfg
	q.modifiedAt = s.clock.Now()
	return nil
}

// Attributes returns every attribute of the queue.
func (s *Service) Attributes(name string) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queueLocked(name)
	if err != nil {
		return nil, err
	}
	return s.attributesLocked(q, s.clock.Now()), nil
}

func (s *Service) Send(name string, in SendInput) (SendOutput, error) {
	if in.Body == "" {
		return SendOutput{}, errMissingParameter("MessageBody")
	}
	if len(in.Body) > maxMessageBytes {
		return SendOutput{}, errInvalidParameter("One or more parameters are invalid. " +
			"Reason: Message must be shorter than 1048576 bytes.")
	}
	if err := validateAttributes(in.Attributes); err != nil {
		return SendOutput{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queueLocked(name)
	if err != nil {
		return SendOutput{}, err
	}
	delay := q.cfg.delay
	if in.Delay != nil {
		delay = *in.Delay
	}
	now := s.clock.Now()
	m := &message{
		id:         awsapi.NewID(),
		body:       in.Body,
		md5OfBody:  md5Hex(in.Body),
		attributes: in.Attributes,
		sentAt:     now,
		visibleAt:  now.Add(delay),
	}
	q.messages = append(q.messages, m)
	wake(q)
	return SendOutput{MessageID: m.id, MD5OfBody: m.md5OfBody, MD5OfAttributes: md5OfAttributes(m.attributes)}, nil
}

func validateAttributes(attrs map[string]MessageAttribute) error {
	if len(attrs) > 10 {
		return errInvalidParameter("Number of message attributes [" +
			seconds(time.Duration(len(attrs))*time.Second) + "] exceeds the allowed maximum [10].")
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

// Receive returns up to in.Max visible messages and hides them for the
// visibility timeout. With nothing to return it waits for the wait time, for a
// message to arrive or become visible, or for ctx to end.
func (s *Service) Receive(ctx context.Context, name string, in ReceiveInput) ([]Received, error) {
	limit := in.Max
	if limit == 0 {
		limit = 1
	}
	if limit < 1 || limit > maxReceiveBatch {
		return nil, errInvalidParameter("Value for parameter MaxNumberOfMessages is invalid. " +
			"Reason: Must be between 1 and 10, if provided.")
	}

	var deadline time.Time
	for {
		s.mu.Lock()
		q, err := s.queueLocked(name)
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		now := s.clock.Now()
		if deadline.IsZero() {
			wait := q.cfg.receiveWait
			if in.Wait != nil {
				wait = *in.Wait
			}
			deadline = now.Add(wait)
		}
		visibility := q.cfg.visibility
		if in.Visibility != nil {
			visibility = *in.Visibility
		}
		out := s.receiveLocked(q, limit, visibility, now)
		changed := q.changed
		pause := deadline.Sub(now)
		if next := nextVisible(q, now); !next.IsZero() && next.Sub(now) < pause {
			pause = next.Sub(now)
		}
		s.mu.Unlock()

		if len(out) > 0 || pause <= 0 {
			return out, nil
		}
		timer := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, nil
		case <-changed:
			timer.Stop()
		case <-timer.C:
		}
	}
}

// receiveLocked hands out the visible messages of q, moving to the dead-letter
// queue those already received maxReceiveCount times.
func (s *Service) receiveLocked(q *queue, limit int, visibility time.Duration, now time.Time) []Received {
	dlq := s.deadLetterLocked(q)
	var out []Received
	kept := make([]*message, 0, len(q.messages))
	for _, m := range q.messages {
		if len(out) == limit || m.visibleAt.After(now) {
			kept = append(kept, m)
			continue
		}
		if dlq != nil && m.receiveCount >= q.cfg.redrive.maxReceiveCount {
			m.receipt = ""
			dlq.messages = append(dlq.messages, m)
			wake(dlq)
			continue
		}
		m.receiveCount++
		if m.firstReceivedAt.IsZero() {
			m.firstReceivedAt = now
		}
		m.receipt = awsapi.NewID()
		m.visibleAt = now.Add(visibility)
		kept = append(kept, m)
		out = append(out, Received{
			MessageID:       m.id,
			Receipt:         m.receipt,
			Body:            m.body,
			MD5OfBody:       m.md5OfBody,
			Attributes:      m.attributes,
			SentAt:          m.sentAt,
			FirstReceivedAt: m.firstReceivedAt,
			ReceiveCount:    m.receiveCount,
		})
	}
	q.messages = kept
	return out
}

func (s *Service) deadLetterLocked(q *queue) *queue {
	if q.cfg.redrive == nil || q.cfg.redrive.target == q.name {
		return nil
	}
	return s.queues[q.cfg.redrive.target]
}

// nextVisible is when the next hidden message of q becomes visible; zero when
// none is hidden.
func nextVisible(q *queue, now time.Time) time.Time {
	var next time.Time
	for _, m := range q.messages {
		if m.visibleAt.After(now) && (next.IsZero() || m.visibleAt.Before(next)) {
			next = m.visibleAt
		}
	}
	return next
}

// Delete removes the message the receipt handle was issued for. A handle that
// is no longer current deletes nothing, as in SQS.
func (s *Service) Delete(name, receipt string) error {
	if receipt == "" {
		return errReceiptHandleInvalid()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queueLocked(name)
	if err != nil {
		return err
	}
	q.messages = slices.DeleteFunc(q.messages, func(m *message) bool { return m.receipt == receipt })
	return nil
}

// ChangeVisibility hides an in-flight message for timeout, counted from now.
func (s *Service) ChangeVisibility(name, receipt string, timeout time.Duration) error {
	if receipt == "" {
		return errReceiptHandleInvalid()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queueLocked(name)
	if err != nil {
		return err
	}
	now := s.clock.Now()
	for _, m := range q.messages {
		if m.receipt != receipt {
			continue
		}
		if !m.visibleAt.After(now) {
			return errMessageNotInflight()
		}
		m.visibleAt = now.Add(timeout)
		wake(q)
		return nil
	}
	return errMessageNotInflight()
}

// Purge removes every message of the queue.
func (s *Service) Purge(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queueLocked(name)
	if err != nil {
		return err
	}
	q.messages = nil
	return nil
}

func (s *Service) queueLocked(name string) (*queue, error) {
	q, ok := s.queues[name]
	if !ok {
		return nil, errQueueNotFound()
	}
	return q, nil
}

func wake(q *queue) {
	close(q.changed)
	q.changed = make(chan struct{})
}
