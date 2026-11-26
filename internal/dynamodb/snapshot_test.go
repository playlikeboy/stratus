package dynamodb

import (
	"testing"
	"time"

	"github.com/stratus-emulator/stratus/internal/awsapi"
	"github.com/stratus-emulator/stratus/internal/clock"
	"github.com/stratus-emulator/stratus/internal/dynamodb/attr"
)

// numValue is an N value.
func numValue(s string) attr.Value { return attr.Value{Type: attr.N, S: s} }

func TestASnapshotKeepsTablesAndItems(t *testing.T) {
	identity := awsapi.Identity{Account: awsapi.DefaultAccount, Region: awsapi.DefaultRegion}
	clk := clock.NewFake(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	s := New(identity, clk)
	spec := TableSpec{
		Name:         "orders",
		PartitionKey: KeyAttribute{Name: "customer", Type: "S"},
		SortKey:      &KeyAttribute{Name: "id", Type: "N"},
		LocalIndexes: []IndexSpec{{Name: "by-status", SortKey: &KeyAttribute{Name: "status", Type: "S"},
			Projection: Projection{Type: ProjectAll}}},
		TTLAttribute: "expires",
	}
	if err := s.CreateTable(spec); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	orders := s.tables["orders"]
	orders.ttl = "expires_at" // changed by UpdateTimeToLive after the creation
	for _, item := range []attr.Item{
		{"customer": attr.Str("c-1"), "id": numValue("2"), "status": attr.Str("new"), "total": numValue("9.5")},
		{"customer": attr.Str("c-1"), "id": numValue("1"), "status": attr.Str("paid")},
		{"customer": attr.Str("c-2"), "id": numValue("1"), "lines": attr.List([]attr.Value{attr.Str("a")})},
	} {
		orders.put(item)
	}

	data, err := s.Snapshot(nil)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	next := New(identity, clk)
	if err := next.Restore(data, nil); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	got, ok := next.tables["orders"]
	if !ok {
		t.Fatal("no table orders after Restore")
	}
	if got.arn != orders.arn || got.id != orders.id || !got.createdAt.Equal(orders.createdAt) || got.ttl != "expires_at" {
		t.Errorf("table after Restore: arn %s id %s created %v ttl %q", got.arn, got.id, got.createdAt, got.ttl)
	}
	if len(got.spec.LocalIndexes) != 1 || got.spec.LocalIndexes[0].Name != "by-status" {
		t.Errorf("local indexes after Restore: %+v", got.spec.LocalIndexes)
	}
	if got.count != 3 {
		t.Fatalf("%d items after Restore, want 3", got.count)
	}
	item := got.get(attr.Item{"customer": attr.Str("c-1"), "id": numValue("2")})
	if item == nil || !attr.Equal(item["total"], numValue("9.5")) {
		t.Fatalf("item c-1/2 after Restore: %v", item)
	}
	all := got.all()
	if !attr.Equal(all[0]["id"], numValue("1")) || !attr.Equal(all[1]["id"], numValue("2")) {
		t.Errorf("items of c-1 are not in sort key order after Restore: %v", all)
	}
}

func TestDynamoDBRestoreRefusesAnotherFormat(t *testing.T) {
	s := New(awsapi.Identity{}, clock.NewFake(time.Unix(0, 0)))
	if err := s.Restore([]byte(`{"format":99}`), nil); err == nil {
		t.Fatal("Restore of format 99 succeeded")
	}
}
