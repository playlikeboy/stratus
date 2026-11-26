package dynamodb

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/stratus-emulator/stratus/internal/dynamodb/attr"
)

// snapshotFormat is the version of what Snapshot writes. Restore reads only
// this one: a state written by another version is refused, never half read.
const snapshotFormat = 1

type snapshot struct {
	Format int             `json:"format"`
	Tables []tableSnapshot `json:"tables"`
}

type tableSnapshot struct {
	Spec      TableSpec   `json:"spec"`
	ARN       string      `json:"arn"`
	ID        string      `json:"id"`
	CreatedAt time.Time   `json:"createdAt"`
	TTL       string      `json:"ttl,omitempty"` // the TTL attribute now, which UpdateTimeToLive may have changed
	Items     []attr.Item `json:"items"`
}

// Snapshot returns every table with its items, for Restore to bring back in
// another emulator.
func (s *Service) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := snapshot{Format: snapshotFormat, Tables: make([]tableSnapshot, 0, len(s.tables))}
	for _, t := range s.tables {
		snap.Tables = append(snap.Tables, tableSnapshot{
			Spec: t.spec, ARN: t.arn, ID: t.id, CreatedAt: t.createdAt, TTL: t.ttl, Items: t.all(),
		})
	}
	sort.Slice(snap.Tables, func(i, j int) bool { return snap.Tables[i].Spec.Name < snap.Tables[j].Spec.Name })
	return json.Marshal(snap)
}

// Restore replaces every table with those of a snapshot.
func (s *Service) Restore(data []byte) error {
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("reading the DynamoDB state: %w", err)
	}
	if snap.Format != snapshotFormat {
		return fmt.Errorf("the DynamoDB state is in format %d; this stratus reads format %d", snap.Format, snapshotFormat)
	}
	tables := make(map[string]*table, len(snap.Tables))
	for _, ts := range snap.Tables {
		t := newTable(ts.Spec, ts.ARN, ts.ID, ts.CreatedAt)
		t.ttl = ts.TTL
		for _, item := range ts.Items {
			t.put(item)
		}
		tables[ts.Spec.Name] = t
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tables = tables
	return nil
}
