package sqs

import (
	"errors"
	"net/http"
	"regexp"
	"time"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

const (
	maxBatchEntries = 10
	// maxBatchBytes is the most a SendMessageBatch request may carry in message
	// bodies and attributes together.
	maxBatchBytes = 1 << 20
)

var validBatchID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)

// BatchFailure is the result of a batch entry that failed; the other entries of
// the batch are unaffected.
type BatchFailure struct {
	ID          string
	Code        string
	Message     string
	SenderFault bool
}

// SendBatchEntry is one message of SendBatch.
type SendBatchEntry struct {
	ID    string
	Input SendInput
}

// SendBatchResult is a message SendBatch enqueued.
type SendBatchResult struct {
	ID     string
	Output SendOutput
}

// SendBatch sends up to ten messages. A batch that is malformed as a whole
// returns an error; otherwise every entry ends up in exactly one of the two
// results, in the order of the entries.
func (s *Service) SendBatch(name string, entries []SendBatchEntry) ([]SendBatchResult, []BatchFailure, error) {
	ids := make([]string, len(entries))
	size := 0
	for i, e := range entries {
		ids[i] = e.ID
		size += messageSize(e.Input.Body, e.Input.Attributes)
	}
	var done []SendBatchResult
	failed, err := s.batch(name, "SendMessageBatchRequestEntry", ids, size, func(q *queue, i int) error {
		out, err := s.sendLocked(q, entries[i].Input)
		if err == nil {
			done = append(done, SendBatchResult{ID: entries[i].ID, Output: out})
		}
		return err
	})
	return done, failed, err
}

// DeleteBatchEntry is one message of DeleteBatch.
type DeleteBatchEntry struct {
	ID      string
	Receipt string
}

// DeleteBatch deletes up to ten messages; it returns the ids of the entries
// that succeeded and the failures.
func (s *Service) DeleteBatch(name string, entries []DeleteBatchEntry) ([]string, []BatchFailure, error) {
	ids := make([]string, len(entries))
	for i, e := range entries {
		ids[i] = e.ID
	}
	var done []string
	failed, err := s.batch(name, "DeleteMessageBatchRequestEntry", ids, 0, func(q *queue, i int) error {
		err := deleteLocked(q, entries[i].Receipt)
		if err == nil {
			done = append(done, entries[i].ID)
		}
		return err
	})
	return done, failed, err
}

// VisibilityBatchEntry is one message of ChangeVisibilityBatch.
type VisibilityBatchEntry struct {
	ID      string
	Receipt string
	Timeout *time.Duration // nil: the entry has no VisibilityTimeout
}

// ChangeVisibilityBatch changes the visibility of up to ten in-flight messages;
// it returns the ids of the entries that succeeded and the failures.
func (s *Service) ChangeVisibilityBatch(name string, entries []VisibilityBatchEntry) ([]string, []BatchFailure, error) {
	ids := make([]string, len(entries))
	for i, e := range entries {
		ids[i] = e.ID
	}
	var done []string
	failed, err := s.batch(name, "ChangeMessageVisibilityBatchRequestEntry", ids, 0, func(q *queue, i int) error {
		if entries[i].Timeout == nil {
			return errMissingParameter("VisibilityTimeout")
		}
		err := s.changeVisibilityLocked(q, entries[i].Receipt, *entries[i].Timeout)
		if err == nil {
			done = append(done, entries[i].ID)
		}
		return err
	})
	return done, failed, err
}

// batch checks the batch as a whole, then applies each entry under one lock and
// collects the failures.
func (s *Service) batch(name, entryType string, ids []string, size int, apply func(q *queue, i int) error) ([]BatchFailure, error) {
	if err := validateBatch(entryType, ids, size); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queueLocked(name)
	if err != nil {
		return nil, err
	}
	var failed []BatchFailure
	for i, id := range ids {
		if err := apply(q, i); err != nil {
			failed = append(failed, failureOf(id, err))
		}
	}
	return failed, nil
}

// validateBatch applies the rules every batch operation has: one to ten
// entries, valid and distinct ids, and a bounded total size.
func validateBatch(entryType string, ids []string, size int) error {
	switch {
	case len(ids) == 0:
		return errEmptyBatch(entryType)
	case len(ids) > maxBatchEntries:
		return errTooManyEntries(len(ids))
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if !validBatchID.MatchString(id) {
			return errInvalidBatchEntryID()
		}
		if seen[id] {
			return errBatchIDsNotDistinct(id)
		}
		seen[id] = true
	}
	if size > maxBatchBytes {
		return errBatchTooLong(size)
	}
	return nil
}

// failureOf describes err as the failure of one batch entry.
func failureOf(id string, err error) BatchFailure {
	var apiErr *awsapi.Error
	if !errors.As(err, &apiErr) {
		apiErr = &awsapi.Error{Status: http.StatusInternalServerError, Code: "InternalFailure", Message: err.Error()}
	}
	return BatchFailure{ID: id, Code: apiErr.Code, Message: apiErr.Message, SenderFault: apiErr.Status < http.StatusInternalServerError}
}
