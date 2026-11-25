package sqs

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"
)

const (
	// dedupInterval is how long SQS remembers a deduplication id.
	dedupInterval = 5 * time.Minute

	maxFifoIDLength = 128

	// firstSequence is where the sequence numbers of a queue start; real ones are
	// 20-digit numbers too, and clients only ever treat them as opaque strings.
	firstSequence uint64 = 18_000_000_000_000_000_000
)

// fifoState is what a FIFO queue remembers besides its messages: the sequence
// counter and the deduplication ids of the last five minutes.
type fifoState struct {
	sequence uint64
	seen     map[string]dedupEntry
	// order lists the entries of seen by expiry, oldest first, so that expired
	// ones are forgotten from the front without scanning the map.
	order []dedupRecord
}

type dedupEntry struct {
	out     SendOutput // what the first send returned
	expires time.Time
}

type dedupRecord struct {
	key     string
	expires time.Time
}

// nextSequence hands out the sequence number of the next accepted message.
func (f *fifoState) nextSequence() string {
	n := firstSequence + f.sequence
	f.sequence++
	return strconv.FormatUint(n, 10)
}

// duplicate returns the result of the first send with this key, if it is still
// inside the deduplication interval.
func (f *fifoState) duplicate(key string, now time.Time) (SendOutput, bool) {
	f.forget(now)
	entry, ok := f.seen[key]
	return entry.out, ok
}

// remember records a send so that a repeat of it inside the interval is a duplicate.
func (f *fifoState) remember(key string, out SendOutput, now time.Time) {
	if f.seen == nil {
		f.seen = map[string]dedupEntry{}
	}
	expires := now.Add(dedupInterval)
	f.seen[key] = dedupEntry{out: out, expires: expires}
	f.order = append(f.order, dedupRecord{key: key, expires: expires})
}

// forget drops the entries whose interval is over. An entry that a later send
// replaced stays: only the record of the entry itself is removed.
func (f *fifoState) forget(now time.Time) {
	n := 0
	for ; n < len(f.order) && !f.order[n].expires.After(now); n++ {
		rec := f.order[n]
		if entry, ok := f.seen[rec.key]; ok && entry.expires.Equal(rec.expires) {
			delete(f.seen, rec.key)
		}
	}
	f.order = f.order[n:]
}

// fifoFields checks and returns the group and deduplication ids of a message
// sent to a FIFO queue; the deduplication id is the body's SHA-256 when the
// queue deduplicates on content and the sender gave none.
func (c config) fifoFields(in SendInput) (group, dedup string, err error) {
	if in.GroupID == "" {
		return "", "", errMissingParameter("MessageGroupId")
	}
	if err := validFifoID("MessageGroupId", in.GroupID); err != nil {
		return "", "", err
	}
	dedup = in.DeduplicationID
	switch {
	case dedup != "":
		if err := validFifoID("MessageDeduplicationId", dedup); err != nil {
			return "", "", err
		}
	case c.contentDedup:
		sum := sha256.Sum256([]byte(in.Body))
		dedup = hex.EncodeToString(sum[:])
	default:
		return "", "", errInvalidParameter("The queue should either have ContentBasedDeduplication " +
			"enabled or MessageDeduplicationId provided explicitly.")
	}
	return in.GroupID, dedup, nil
}

// dedupKey is what a send is compared by: the deduplication id, within its
// message group when the queue deduplicates per group.
func (c config) dedupKey(group, dedup string) string {
	if c.dedupScope == scopeMessageGroup {
		return group + "\x00" + dedup
	}
	return dedup
}

// validFifoID accepts up to 128 printable ASCII characters, the rule SQS has for
// message group and deduplication ids.
func validFifoID(name, value string) error {
	ok := len(value) <= maxFifoIDLength
	for i := 0; ok && i < len(value); i++ {
		ok = value[i] >= 0x21 && value[i] <= 0x7e
	}
	if !ok {
		return errInvalidParameter("Value " + value + " for parameter " + name + " is invalid. " +
			"Reason: Must be 1 to 128 alphanumeric or punctuation characters.")
	}
	return nil
}

// errNotForQueueType rejects a parameter the queue's type has no use for.
func errNotForQueueType(name, value string) error {
	return errInvalidParameter("Value " + value + " for parameter " + name + " is invalid. " +
		"Reason: The request include parameter that is not valid for this queue type.")
}
