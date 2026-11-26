package expr

import (
	"cmp"
	"slices"

	"github.com/stratus-emulator/stratus/internal/dynamodb/attr"
)

// Apply returns the item the update makes of item; item itself is left
// untouched. Like DynamoDB, every SET value is computed from the item as it
// was before the update.
func (u *Update) Apply(item attr.Item) (attr.Item, error) {
	values := make([]attr.Value, len(u.set))
	for i, a := range u.set {
		v, err := required(item, a.value)
		if err != nil {
			return nil, err
		}
		values[i] = v
	}
	root := attr.Map(item)
	var err error
	for i, a := range u.set {
		v := values[i]
		root, err = modify(root, a.path, true, func(attr.Value, bool) (attr.Value, bool, error) {
			return v, true, nil
		})
		if err != nil {
			return nil, err
		}
	}
	// Removing list elements from the highest index down keeps the indexes
	// of the others valid, so a[1] and a[3] remove the elements the client
	// meant.
	removes := slices.Clone(u.remove)
	slices.SortFunc(removes, func(a, b Path) int { return comparePaths(b, a) })
	for _, p := range removes {
		if root, err = modify(root, p, false, removeChange); err != nil {
			return nil, err
		}
	}
	for _, a := range u.add {
		if root, err = modify(root, a.path, true, addChange(a.value)); err != nil {
			return nil, err
		}
	}
	for _, a := range u.delete {
		if root, err = modify(root, a.path, false, deleteChange(a.value)); err != nil {
			return nil, err
		}
	}
	return root.M, nil
}

func removeChange(old attr.Value, _ bool) (attr.Value, bool, error) { return old, false, nil }

// addChange adds a number to a number, or the members of a set to a set of
// the same type; a missing attribute takes the value as it is.
func addChange(v attr.Value) change {
	return func(old attr.Value, exists bool) (attr.Value, bool, error) {
		switch {
		case !exists:
			return v, true, nil
		case old.Type == attr.N && v.Type == attr.N:
			n, err := old.Number().Add(v.Number())
			return attr.Num(n), true, err
		case old.Type == v.Type && v.IsSet():
			return attr.Union(old, v), true, nil
		}
		return old, false, errOperandType
	}
}

// deleteChange removes members from a set; a set left empty is removed, as
// DynamoDB stores no empty sets.
func deleteChange(v attr.Value) change {
	return func(old attr.Value, exists bool) (attr.Value, bool, error) {
		switch {
		case !exists:
			return old, false, nil
		case old.Type != v.Type:
			return old, false, errOperandType
		}
		rest := attr.Difference(old, v)
		return rest, rest.Len() > 0, nil
	}
}

func comparePaths(a, b Path) int {
	for i := range min(len(a), len(b)) {
		x, y := a[i], b[i]
		switch {
		case x.IsIndex && y.IsIndex:
			if c := cmp.Compare(x.Index, y.Index); c != 0 {
				return c
			}
		case x.IsIndex != y.IsIndex:
			if x.IsIndex {
				return 1
			}
			return -1
		default:
			if c := cmp.Compare(x.Name, y.Name); c != 0 {
				return c
			}
		}
	}
	return cmp.Compare(len(a), len(b))
}
