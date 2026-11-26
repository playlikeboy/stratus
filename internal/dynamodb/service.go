// Package dynamodb emulates Amazon DynamoDB over the awsJson1.0 protocol:
// tables with local and global secondary indexes, the item operations with
// the full expression language, batches, transactions and TTL.
//
// Every operation, and every transaction as a whole, runs under one lock, so
// each is atomic. Tables are ACTIVE as soon as they are created and are never
// throttled.
package dynamodb

import (
	"slices"
	"sync"

	"github.com/stratus-emulator/stratus/internal/awsapi"
	"github.com/stratus-emulator/stratus/internal/clock"
	"github.com/stratus-emulator/stratus/internal/dynamodb/attr"
)

// Service holds every table of one emulator.
type Service struct {
	identity awsapi.Identity
	clock    clock.Clock

	mu     sync.Mutex
	tables map[string]*table
}

func New(identity awsapi.Identity, c clock.Clock) *Service {
	return &Service{identity: identity, clock: c, tables: map[string]*table{}}
}

func (s *Service) Name() string         { return "dynamodb" }
func (s *Service) TargetPrefix() string { return targetPrefix }

// Reset drops every table.
func (s *Service) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tables = map[string]*table{}
}

// CreateTable creates a table, ACTIVE at once. It fails with a
// ValidationException for an invalid spec and a ResourceInUseException when
// the table exists.
func (s *Service) CreateTable(spec TableSpec) error {
	_, err := s.createTable(spec)
	return err
}

func (s *Service) createTable(spec TableSpec) (*table, error) {
	norm, err := spec.normalised()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.tables[norm.Name]; ok {
		return nil, errTableInUse(norm.Name)
	}
	t := newTable(norm, s.identity.ARN("dynamodb", "table/"+norm.Name), awsapi.NewID(), s.clock.Now())
	s.tables[norm.Name] = t
	return t, nil
}

// table returns the named table; the caller holds the lock.
func (s *Service) table(name string) (*table, error) {
	if name == "" {
		return nil, errValidation("1 validation error detected: Value null at 'tableName' failed to satisfy " +
			"constraint: Member must not be null")
	}
	t, ok := s.tables[name]
	if !ok {
		return nil, errTableNotFound(name)
	}
	return t, nil
}

func (s *Service) tableNames() []string {
	names := make([]string, 0, len(s.tables))
	for name := range s.tables {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// ExpireTTL deletes the items whose TTL attribute is in the past by the
// service's clock and returns how many it deleted. DynamoDB does this in the
// background; the emulator does it only when asked, so tests stay
// deterministic. Like DynamoDB it ignores a TTL attribute that is not a
// number, and one more than five years in the past.
func (s *Service) ExpireTTL() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock.Now().Unix()
	oldest := attr.NumberFromInt(now - 5*365*24*3600)
	cutoff := attr.NumberFromInt(now)
	deleted := 0
	for _, t := range s.tables {
		if t.ttl == "" {
			continue
		}
		for _, item := range t.all() {
			v, ok := item[t.ttl]
			if !ok || v.Type != attr.N {
				continue
			}
			if n := v.Number(); n.Cmp(cutoff) < 0 && n.Cmp(oldest) >= 0 {
				t.remove(t.keyOf(item))
				deleted++
			}
		}
	}
	return deleted
}
