package sqs

import "time"

// dropExpired removes the messages that are older than the queue's retention
// period. It runs whenever the queue is touched, so that no background
// goroutine is needed and a fake clock decides what has expired.
func dropExpired(q *queue, now time.Time) {
	limit := now.Add(-q.cfg.retention)
	n := 0
	for _, m := range q.messages {
		if m.sentAt.Before(limit) {
			continue
		}
		q.messages[n] = m
		n++
	}
	clear(q.messages[n:])
	q.messages = q.messages[:n]
}
