package dynamodb

import (
	"slices"

	"github.com/stratus-emulator/stratus/internal/dynamodb/attr"
)

// inIndex reports whether item appears in ix. Secondary indexes are sparse:
// an item missing one of the index keys is not in the index.
func (t *table) inIndex(ix *index, item attr.Item) bool {
	for _, k := range []string{ix.pk, ix.sk} {
		if k == "" {
			continue
		}
		if _, ok := item[k]; !ok {
			return false
		}
	}
	return true
}

// projectIndex returns the attributes of item that ix holds: every one for
// ALL, else the table and index keys plus an INCLUDE's non-key attributes.
func (t *table) projectIndex(ix *index, item attr.Item) attr.Item {
	if ix.projection.Type == ProjectAll {
		return item
	}
	out := attr.Item{}
	for _, k := range append(t.keyNames(ix), ix.projection.NonKeyAttributes...) {
		if v, ok := item[k]; ok {
			out[k] = v
		}
	}
	return out
}

// partitionItems returns the items whose ix partition key is pk, in index
// order. The table's own partitions are kept sorted; a secondary index is
// sorted on demand, which is fine at an emulator's sizes.
func (t *table) partitionItems(ix *index, pk attr.Value) []attr.Item {
	var source []attr.Item
	if ix.global {
		source = t.all()
	} else if p := t.parts[encodeKey(pk)]; p != nil {
		source = p.items
	}
	if ix.name == "" {
		return source
	}
	var out []attr.Item
	for _, item := range source {
		if t.inIndex(ix, item) && attr.Equal(item[ix.pk], pk) {
			out = append(out, item)
		}
	}
	t.sortBy(ix, out)
	return out
}

// scanItems returns every item of ix, in index order.
func (t *table) scanItems(ix *index) []attr.Item {
	if ix.name == "" {
		return t.all()
	}
	var out []attr.Item
	for _, item := range t.all() {
		if t.inIndex(ix, item) {
			out = append(out, item)
		}
	}
	t.sortBy(ix, out)
	return out
}

func (t *table) sortBy(ix *index, items []attr.Item) {
	slices.SortStableFunc(items, func(a, b attr.Item) int { return t.compareBy(ix, a, b) })
}

// keyOfIndex returns the attributes that identify item read through ix: the
// LastEvaluatedKey of a page that ends at item.
func (t *table) keyOfIndex(ix *index, item attr.Item) attr.Item {
	key := attr.Item{}
	for _, k := range t.keyNames(ix) {
		key[k] = item[k]
	}
	return key
}

// checkStartKey checks an ExclusiveStartKey: exactly the attributes of
// keyNames(ix), of the right types.
func (t *table) checkStartKey(ix *index, key attr.Item) error {
	names := t.keyNames(ix)
	if len(key) != len(names) {
		return errValidation("The provided starting key is invalid: The provided key element does not match the schema")
	}
	for _, k := range names {
		if v, ok := key[k]; !ok || v.Type != t.types[k] {
			return errValidation("The provided starting key is invalid: The provided key element does not " +
				"match the schema")
		}
	}
	return nil
}
