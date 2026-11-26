package sqs

import "time"

// PeekedMessage is a message as a look at the queue shows it.
type PeekedMessage struct {
	ID           string
	Body         string
	SentAt       time.Time
	ReceiveCount int
	State        string // "visible", "in flight" or "delayed"
	GroupID      string // FIFO queues only
	Attributes   map[string]MessageAttribute
}

// Peek returns up to max messages of the queue, in delivery order, without
// receiving them: their visibility and receive counts stay as they are.
func (s *Service) Peek(name string, max int) ([]PeekedMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queueLocked(name)
	if err != nil {
		return nil, err
	}
	now := s.clock.Now()
	var out []PeekedMessage
	for _, m := range q.messages {
		if len(out) == max {
			break
		}
		if m.deleted {
			continue
		}
		state := "visible"
		switch {
		case !m.visibleAt.After(now):
		case m.receiveCount == 0:
			state = "delayed"
		default:
			state = "in flight"
		}
		out = append(out, PeekedMessage{
			ID: m.id, Body: m.body, SentAt: m.sentAt, ReceiveCount: m.receiveCount,
			State: state, GroupID: m.groupID, Attributes: m.attributes,
		})
	}
	return out, nil
}
