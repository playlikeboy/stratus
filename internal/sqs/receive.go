package sqs

import (
	"context"
	"time"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

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
		if len(out) == 0 && pause > 0 {
			if next := nextVisible(q, now); !next.IsZero() && next.Sub(now) < pause {
				pause = next.Sub(now)
			}
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
// queue those already received maxReceiveCount times. On a FIFO queue a group
// with a message that is hidden (in flight or delayed) delivers nothing, so
// that the order of the group holds; the groups are independent of each other.
// The walk stops once limit messages are found: the rest of the queue cannot
// change what is delivered.
func (s *Service) receiveLocked(q *queue, limit int, visibility time.Duration, now time.Time) []Received {
	dlq := s.deadLetterLocked(q)
	var out []Received
	var blocked map[string]bool // FIFO groups that must not deliver now
	if q.cfg.fifo {
		blocked = map[string]bool{}
	}
	var moved []*message
	end := len(q.messages) // how far the walk went
	for i, m := range q.messages {
		if len(out) == limit {
			end = i
			break
		}
		switch {
		case m.deleted:
		case m.visibleAt.After(now):
			if q.cfg.fifo {
				blocked[m.groupID] = true
			}
		case q.cfg.fifo && blocked[m.groupID]:
		case dlq != nil && m.receiveCount >= q.cfg.redrive.maxReceiveCount:
			moved = append(moved, m)
		default:
			out = append(out, q.deliver(m, visibility, now))
		}
	}
	q.compactPrefix(end)
	for _, m := range moved { // after the walk: removing one may reslice q.messages
		s.moveToDeadLetter(q, dlq, m)
	}
	return out
}

// deliver marks m as received now and hides it.
func (q *queue) deliver(m *message, visibility time.Duration, now time.Time) Received {
	m.receiveCount++
	if m.firstReceivedAt.IsZero() {
		m.firstReceivedAt = now
	}
	if m.receipt != "" {
		delete(q.receipts, m.receipt)
	}
	m.receipt = awsapi.NewID()
	q.receipts[m.receipt] = m
	m.visibleAt = now.Add(visibility)
	return Received{
		MessageID:        m.id,
		Receipt:          m.receipt,
		Body:             m.body,
		MD5OfBody:        m.md5OfBody,
		Attributes:       m.attributes,
		SentAt:           m.sentAt,
		FirstReceivedAt:  m.firstReceivedAt,
		ReceiveCount:     m.receiveCount,
		TraceHeader:      m.traceHeader,
		GroupID:          m.groupID,
		DeduplicationID:  m.dedupID,
		SequenceNumber:   m.sequence,
		DeadLetterSource: m.deadLetterSource,
	}
}

// moveToDeadLetter moves m from q to the end of dlq, remembering where it came
// from. A FIFO dead-letter queue numbers it again.
func (s *Service) moveToDeadLetter(q, dlq *queue, m *message) {
	q.remove(m)
	c := *m // m stays in q.messages, marked deleted, until it is dropped from there
	c.deleted = false
	c.receipt = ""
	c.deadLetterSource = s.arn(q.name)
	if dlq.cfg.fifo {
		c.sequence = dlq.fifo.nextSequence()
	}
	dlq.append(&c)
	wake(dlq)
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
		if !m.deleted && m.visibleAt.After(now) && (next.IsZero() || m.visibleAt.Before(next)) {
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
	return deleteLocked(q, receipt)
}

// deleteLocked removes the message with the receipt handle. Removing an
// in-flight message frees its FIFO group, so waiting receivers look again.
func deleteLocked(q *queue, receipt string) error {
	if receipt == "" {
		return errReceiptHandleInvalid()
	}
	if m, ok := q.receipts[receipt]; ok {
		q.remove(m)
		wake(q)
	}
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
	return s.changeVisibilityLocked(q, receipt, timeout)
}

func (s *Service) changeVisibilityLocked(q *queue, receipt string, timeout time.Duration) error {
	if receipt == "" {
		return errReceiptHandleInvalid()
	}
	if timeout < 0 || timeout > maxVisibility {
		return errInvalidParameter("Value " + seconds(timeout) + " for parameter VisibilityTimeout is invalid. " +
			"Reason: Must be between 0 and " + seconds(maxVisibility) + ", if provided.")
	}
	m, ok := q.receipts[receipt]
	now := s.clock.Now()
	if !ok || !m.visibleAt.After(now) {
		return errMessageNotInflight()
	}
	m.visibleAt = now.Add(timeout)
	wake(q)
	return nil
}

// Purge removes every message of the queue.
func (s *Service) Purge(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queueLocked(name)
	if err != nil {
		return err
	}
	q.clearMessages()
	return nil
}
