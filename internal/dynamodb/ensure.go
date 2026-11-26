package dynamodb

import (
	"errors"
	"fmt"
	"reflect"
)

// ErrSpecMismatch is the error of EnsureTable for a table whose keys or
// indexes differ from the spec: DynamoDB cannot change them in place.
var ErrSpecMismatch = errors.New("the table has other keys or indexes")

// EnsureTable creates the table, or, when one of that name exists, as after
// a restore, checks that it has the spec's keys and indexes and gives it the
// spec's TTL attribute. Its items stay.
func (s *Service) EnsureTable(spec TableSpec) error {
	norm, err := spec.normalised()
	if err != nil {
		return err
	}
	s.mu.Lock()
	t, ok := s.tables[norm.Name]
	if ok {
		defer s.mu.Unlock()
		if !reflect.DeepEqual(shape(t.spec), shape(norm)) {
			return fmt.Errorf("%w than the manifest declares", ErrSpecMismatch)
		}
		t.ttl = norm.TTLAttribute
		return nil
	}
	s.mu.Unlock()
	return s.CreateTable(spec)
}

// shape is what of a spec cannot change once the table exists: its keys and
// indexes.
func shape(spec TableSpec) TableSpec {
	spec.TTLAttribute, spec.BillingMode, spec.ReadCapacity, spec.WriteCapacity = "", "", 0, 0
	return spec
}

// HasTable reports whether the table exists.
func (s *Service) HasTable(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.tables[name]
	return ok
}

// TTLAttribute is the TTL attribute of a table, empty when TTL is disabled
// or the table does not exist.
func (s *Service) TTLAttribute(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.tables[name]; ok {
		return t.ttl
	}
	return ""
}
