package dynamodb

import (
	"encoding/json"
	"slices"
	"strings"
)

// TableSummary is what an overview of the emulator shows of a table.
type TableSummary struct {
	Name         string
	Items        int
	Indexes      int    // local and global secondary indexes
	TTLAttribute string // empty when TTL is disabled
}

// Tables summarises every table, by name.
func (s *Service) Tables() []TableSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]TableSummary, 0, len(s.tables))
	for _, t := range s.tables {
		out = append(out, TableSummary{
			Name:         t.spec.Name,
			Items:        t.count,
			Indexes:      len(t.spec.LocalIndexes) + len(t.spec.GlobalIndexes),
			TTLAttribute: t.ttl,
		})
	}
	slices.SortFunc(out, func(a, b TableSummary) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// PeekItems returns up to max items of the table in key order, as DynamoDB
// JSON, and how many items the table holds.
func (s *Service) PeekItems(name string, max int) (items []string, total int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(name)
	if err != nil {
		return nil, 0, err
	}
	for _, enc := range t.order {
		for _, item := range t.parts[enc].items {
			if len(items) == max {
				return items, t.count, nil
			}
			b, err := json.MarshalIndent(item, "", "  ")
			if err != nil {
				return nil, 0, err
			}
			items = append(items, string(b))
		}
	}
	return items, t.count, nil
}
