package sqs

import "time"

// dropExpired removes the messages that are older than the queue's retention
// period. It runs whenever the queue is touched, so that no background
// goroutine is needed and a fake clock decides what has expired; a queue with
// nothing that old is not walked.
func dropExpired(q *queue, now time.Time) {
	limit := now.Add(-q.cfg.retention)
	if len(q.messages) == 0 || !q.oldestSentAt.Before(limit) {
		return
	}
	n := 0
	var oldest time.Time
	for _, m := range q.messages {
		if m.deleted {
			continue
		}
		if m.sentAt.Before(limit) {
			if m.receipt != "" && q.receipts[m.receipt] == m {
				delete(q.receipts, m.receipt)
			}
			continue
		}
		if n == 0 || m.sentAt.Before(oldest) {
			oldest = m.sentAt
		}
		q.messages[n] = m
		n++
	}
	clear(q.messages[n:])
	q.messages = q.messages[:n]
	q.deleted = 0
	q.oldestSentAt = oldest
}
