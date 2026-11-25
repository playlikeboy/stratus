package sqs

import (
	"encoding/base64"
	"maps"
	"slices"
	"strings"
)

const (
	maxTags        = 50
	maxTagKey      = 128
	maxTagValue    = 256
	maxListResults = 1000
)

// addTags adds tags to q, all or none.
func (q *queue) addTags(tags map[string]string) error {
	for key, value := range tags {
		if key == "" || len(key) > maxTagKey || strings.HasPrefix(strings.ToLower(key), "aws:") {
			return errInvalidParameter("Tag keys must be 1 to 128 characters and must not start with aws:.")
		}
		if len(value) > maxTagValue {
			return errInvalidParameter("Tag values must be at most 256 characters.")
		}
	}
	next := maps.Clone(q.tags)
	if next == nil {
		next = map[string]string{}
	}
	maps.Copy(next, tags)
	if len(next) > maxTags {
		return errInvalidParameter("Too many tags: a queue has at most 50.")
	}
	q.tags = next
	return nil
}

// TagQueue adds tags to the queue, replacing the value of a key it already has.
func (s *Service) TagQueue(name string, tags map[string]string) error {
	if len(tags) == 0 {
		return errMissingParameter("Tags")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queueLocked(name)
	if err != nil {
		return err
	}
	return q.addTags(tags)
}

// UntagQueue removes the tags with these keys; a key the queue has no tag for is ignored.
func (s *Service) UntagQueue(name string, keys []string) error {
	if len(keys) == 0 {
		return errMissingParameter("TagKeys")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queueLocked(name)
	if err != nil {
		return err
	}
	next := maps.Clone(q.tags)
	for _, key := range keys {
		delete(next, key)
	}
	q.tags = next
	return nil
}

// ListQueueTags returns the tags of the queue.
func (s *Service) ListQueueTags(name string) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q, err := s.queueLocked(name)
	if err != nil {
		return nil, err
	}
	return maps.Clone(q.tags), nil
}

// ListQueues returns the sorted names of the queues with the prefix.
func (s *Service) ListQueues(prefix string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.namesLocked(func(q *queue) bool { return strings.HasPrefix(q.name, prefix) })
}

// ListQueuesPage is ListQueues cut into pages of at most max names (0: no
// limit). It returns the token of the next page, empty after the last one.
func (s *Service) ListQueuesPage(prefix string, max int, token string) (names []string, next string, err error) {
	return paginate(s.ListQueues(prefix), max, token)
}

// ListDeadLetterSourceQueues returns, in pages like ListQueuesPage, the queues
// whose redrive policy names this queue as their dead-letter queue.
func (s *Service) ListDeadLetterSourceQueues(name string, max int, token string) (names []string, next string, err error) {
	s.mu.Lock()
	if _, err := s.queueLocked(name); err != nil {
		s.mu.Unlock()
		return nil, "", err
	}
	all := s.namesLocked(func(q *queue) bool { return q.cfg.redrive != nil && q.cfg.redrive.target == name })
	s.mu.Unlock()
	return paginate(all, max, token)
}

func (s *Service) namesLocked(keep func(*queue) bool) []string {
	names := make([]string, 0, len(s.queues))
	for name, q := range s.queues {
		if keep(q) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

// paginate returns the page of sorted names that follows token. The token is the
// last name of the previous page, encoded so that clients treat it as opaque.
func paginate(names []string, max int, token string) ([]string, string, error) {
	if token != "" {
		last, err := base64.RawURLEncoding.DecodeString(token)
		if err != nil {
			return nil, "", errInvalidParameter("The NextToken is invalid.")
		}
		i, _ := slices.BinarySearch(names, string(last))
		if i < len(names) && names[i] == string(last) {
			i++
		}
		names = names[i:]
	}
	if max <= 0 || len(names) <= max {
		return names, "", nil
	}
	page := names[:max]
	return page, base64.RawURLEncoding.EncodeToString([]byte(page[max-1])), nil
}
