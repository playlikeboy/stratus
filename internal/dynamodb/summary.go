package dynamodb

import (
	"encoding/base64"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/ODudek/stratus/internal/dynamodb/attr"
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

// TableInfo is a table's definition and size.
type TableInfo struct {
	Spec      TableSpec
	ARN       string
	Created   time.Time
	Items     int
	SizeBytes int64
	TTL       string // the TTL attribute; empty when TTL is disabled
}

// Table describes one table.
func (s *Service) Table(name string) (TableInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(name)
	if err != nil {
		return TableInfo{}, err
	}
	return TableInfo{Spec: t.spec, ARN: t.arn, Created: t.createdAt, Items: t.count, SizeBytes: t.sizeBytes(), TTL: t.ttl}, nil
}

// ItemSearch picks the items a look at a table shows. Zero matches every item.
type ItemSearch struct {
	// Index is a secondary index whose partition key PartitionKey applies to;
	// empty: the table's own key.
	Index string
	// PartitionKey is the value the partition key must have, as typed: the
	// string, or the number.
	PartitionKey string
	// Contains is text the item's DynamoDB JSON must contain, ignoring case.
	Contains string
}

// SearchItems returns up to max matching items in the table's key order, as
// DynamoDB JSON, and how many items match in all.
func (s *Service) SearchItems(name string, q ItemSearch, max int) (items []string, matched int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(name)
	if err != nil {
		return nil, 0, err
	}
	keyName := t.spec.PartitionKey.Name
	if q.Index != "" {
		idx, ok := indexSpec(t.spec, q.Index)
		if !ok {
			return nil, 0, errValidation("The table does not have the specified index: %s", q.Index)
		}
		if idx.PartitionKey.Name != "" {
			keyName = idx.PartitionKey.Name
		}
	}
	needle := strings.ToLower(q.Contains)
	for _, enc := range t.order {
		for _, item := range t.parts[enc].items {
			if q.Index != "" || q.PartitionKey != "" {
				v, ok := item[keyName]
				if !ok || (q.PartitionKey != "" && keyText(v) != q.PartitionKey) {
					continue // an item without the index's key is not in the index
				}
			}
			b, err := json.MarshalIndent(item, "", "  ")
			if err != nil {
				return nil, 0, err
			}
			if needle != "" && !strings.Contains(strings.ToLower(string(b)), needle) {
				continue
			}
			matched++
			if len(items) < max {
				items = append(items, string(b))
			}
		}
	}
	return items, matched, nil
}

// indexSpec finds a local or global secondary index by name.
func indexSpec(spec TableSpec, name string) (IndexSpec, bool) {
	for _, idx := range append(slices.Clone(spec.LocalIndexes), spec.GlobalIndexes...) {
		if idx.Name == name {
			return idx, true
		}
	}
	return IndexSpec{}, false
}

// keyText is a key value as a person types it: the string, the number or the
// base64 of the bytes.
func keyText(v attr.Value) string {
	if v.Type == attr.B {
		return base64.StdEncoding.EncodeToString(v.B)
	}
	return v.S
}
