package sqs

// append adds m at the end of q's messages.
func (q *queue) append(m *message) {
	if len(q.messages) == q.deleted || m.sentAt.Before(q.oldestSentAt) {
		q.oldestSentAt = m.sentAt
	}
	q.messages = append(q.messages, m)
}

// remove marks m, a message of q, deleted. Deleted messages at the front of
// the queue leave it at once, the others once they are half of it, so that
// removing one costs O(1) amortized and not a walk of the queue.
func (q *queue) remove(m *message) {
	if m.receipt != "" && q.receipts[m.receipt] == m {
		delete(q.receipts, m.receipt)
	}
	m.deleted = true
	q.deleted++
	for len(q.messages) > 0 && q.messages[0].deleted {
		q.messages[0] = nil
		q.messages = q.messages[1:]
		q.deleted--
	}
	if q.deleted > 32 && q.deleted > len(q.messages)/2 {
		q.compact()
	}
}

// compact drops the deleted messages from q.messages.
func (q *queue) compact() {
	n := 0
	for _, m := range q.messages {
		if !m.deleted {
			q.messages[n] = m
			n++
		}
	}
	clear(q.messages[n:])
	q.messages = q.messages[:n]
	q.deleted = 0
}

// compactPrefix drops the deleted messages from the first end of q.messages,
// which a receive has just walked, keeping the order of the rest. Messages
// hidden at the front would otherwise make every receive walk the deleted
// ones behind them again.
func (q *queue) compactPrefix(end int) {
	w := end
	for i := end - 1; i >= 0; i-- {
		if m := q.messages[i]; !m.deleted {
			w--
			q.messages[w] = m
		}
	}
	clear(q.messages[:w])
	q.messages = q.messages[w:]
	q.deleted -= w
}

// clearMessages empties q.
func (q *queue) clearMessages() {
	q.messages = nil
	q.receipts = map[string]*message{}
	q.deleted = 0
}
