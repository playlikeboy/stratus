package dynamodb

import (
	"slices"
	"strings"
	"time"

	"github.com/stratus-emulator/stratus/internal/dynamodb/attr"
)

// table is one table and its items. Items of a partition are kept sorted by
// the sort key; partitions are kept in the order of their encoded keys, which
// is the order Scan walks them in.
type table struct {
	spec      TableSpec
	arn       string
	id        string
	createdAt time.Time
	ttl       string // the TTL attribute, empty when TTL is disabled
	types     map[string]attr.Type

	parts map[string]*partition
	order []string // the encoded partition keys, sorted
	count int
}

type partition struct {
	items []attr.Item
}

func newTable(spec TableSpec, arn, id string, createdAt time.Time) *table {
	t := &table{
		spec: spec, arn: arn, id: id, createdAt: createdAt, ttl: spec.TTLAttribute,
		types: map[string]attr.Type{}, parts: map[string]*partition{},
	}
	for _, k := range spec.keyAttributes() {
		t.types[k.Name] = attr.Type(k.Type)
	}
	return t
}

// base returns the table's own key schema as an index, for code that treats
// the table and its indexes alike.
func (t *table) base() *index {
	ix := &index{pk: t.spec.PartitionKey.Name, projection: Projection{Type: ProjectAll}}
	if t.spec.SortKey != nil {
		ix.sk = t.spec.SortKey.Name
	}
	return ix
}

// index is a key schema the items can be read through: the table's own, a
// local or a global secondary index.
type index struct {
	name       string // empty for the table itself
	global     bool
	pk, sk     string
	projection Projection
}

func (t *table) index(name string) *index {
	for i := range t.spec.allIndexes() {
		spec := t.spec.indexAt(i)
		if spec.Name != name {
			continue
		}
		ix := &index{name: name, global: i >= len(t.spec.LocalIndexes), pk: spec.PartitionKey.Name,
			projection: spec.Projection}
		if spec.SortKey != nil {
			ix.sk = spec.SortKey.Name
		}
		return ix
	}
	return nil
}

// encodeKey turns a key value into a map key; the type is part of it so that
// the string "1" and the number 1 differ.
func encodeKey(v attr.Value) string {
	if v.Type == attr.B {
		return "B" + string(v.B)
	}
	return string(v.Type) + v.S
}

// keyOf returns the primary key attributes of item.
func (t *table) keyOf(item attr.Item) attr.Item {
	key := attr.Item{t.spec.PartitionKey.Name: item[t.spec.PartitionKey.Name]}
	if sk := t.spec.SortKey; sk != nil {
		key[sk.Name] = item[sk.Name]
	}
	return key
}

// locate finds the position of key in its partition: the index where it is,
// or where it would be inserted.
func (t *table) locate(key attr.Item) (p *partition, i int, found bool) {
	p = t.parts[encodeKey(key[t.spec.PartitionKey.Name])]
	if p == nil {
		return nil, 0, false
	}
	if t.spec.SortKey == nil {
		return p, 0, len(p.items) > 0
	}
	name := t.spec.SortKey.Name
	i, found = slices.BinarySearchFunc(p.items, key[name], func(item attr.Item, v attr.Value) int {
		c, _ := attr.Compare(item[name], v)
		return c
	})
	return p, i, found
}

// get returns the item of key, or nil.
func (t *table) get(key attr.Item) attr.Item {
	p, i, found := t.locate(key)
	if !found {
		return nil
	}
	return p.items[i]
}

// put stores item, replacing the item of the same key.
func (t *table) put(item attr.Item) {
	p, i, found := t.locate(item)
	if found {
		p.items[i] = item
		return
	}
	if p == nil {
		enc := encodeKey(item[t.spec.PartitionKey.Name])
		p = &partition{}
		t.parts[enc] = p
		at, _ := slices.BinarySearch(t.order, enc)
		t.order = slices.Insert(t.order, at, enc)
	}
	p.items = slices.Insert(p.items, i, item)
	t.count++
}

// remove deletes the item of key, if there is one.
func (t *table) remove(key attr.Item) {
	p, i, found := t.locate(key)
	if !found {
		return
	}
	p.items = slices.Delete(p.items, i, i+1)
	t.count--
	if len(p.items) == 0 {
		enc := encodeKey(key[t.spec.PartitionKey.Name])
		delete(t.parts, enc)
		at, _ := slices.BinarySearch(t.order, enc)
		t.order = slices.Delete(t.order, at, at+1)
	}
}

// all returns every item, partition by partition.
func (t *table) all() []attr.Item {
	out := make([]attr.Item, 0, t.count)
	for _, enc := range t.order {
		out = append(out, t.parts[enc].items...)
	}
	return out
}

func (t *table) sizeBytes() int64 {
	var n int64
	for _, item := range t.all() {
		n += int64(attr.ItemSize(item))
	}
	return n
}

// keyNames lists the attributes that identify an item read through ix, in
// the order they sort by: the index keys, then the table keys not among them.
func (t *table) keyNames(ix *index) []string {
	names := []string{ix.pk}
	if ix.sk != "" {
		names = append(names, ix.sk)
	}
	for _, n := range []string{t.spec.PartitionKey.Name, t.base().sk} {
		if n != "" && !slices.Contains(names, n) {
			names = append(names, n)
		}
	}
	return names
}

// isPartitionKey reports whether name, one of keyNames(ix), orders by its
// encoding, as partitions do, rather than by value.
func (t *table) isPartitionKey(ix *index, name string) bool {
	return name == ix.pk || (name == t.spec.PartitionKey.Name && name != ix.sk)
}

// compareBy orders two items, or an item and a key, by the attributes of
// keyNames(ix).
func (t *table) compareBy(ix *index, a, b attr.Item) int {
	for _, name := range t.keyNames(ix) {
		var c int
		if t.isPartitionKey(ix, name) {
			c = strings.Compare(encodeKey(a[name]), encodeKey(b[name]))
		} else {
			c, _ = attr.Compare(a[name], b[name])
		}
		if c != 0 {
			return c
		}
	}
	return 0
}
