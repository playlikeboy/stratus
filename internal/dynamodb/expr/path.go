package expr

import (
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/stratus-emulator/stratus/internal/dynamodb/attr"
)

// Step is one element of a document path: a map key, or a list index.
type Step struct {
	Name    string
	Index   int
	IsIndex bool
}

// Path is a document path, such as a.b[2].c.
type Path []Step

// Top is the top-level attribute the path starts at.
func (p Path) Top() string { return p[0].Name }

// String renders p as DynamoDB does in error messages: [a, b, [2]].
func (p Path) String() string {
	parts := make([]string, len(p))
	for i, s := range p {
		if s.IsIndex {
			parts[i] = "[" + strconv.Itoa(s.Index) + "]"
		} else {
			parts[i] = s.Name
		}
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// overlaps reports whether one of p and q is a prefix of the other, which
// DynamoDB forbids between two paths of one update or projection.
func overlaps(p, q Path) bool {
	n := min(len(p), len(q))
	return slices.Equal(p[:n], q[:n])
}

// checkOverlap rejects a list of paths in which two overlap.
func checkOverlap(kind string, paths []Path) error {
	for i := range paths {
		for j := i + 1; j < len(paths); j++ {
			if overlaps(paths[i], paths[j]) {
				return attr.Errorf("Invalid %s: Two document paths overlap with each other; "+
					"must remove or rewrite one of these paths; path one: %s, path two: %s",
					kind, paths[i], paths[j])
			}
		}
	}
	return nil
}

// Get returns the value at p in item.
func Get(item attr.Item, p Path) (attr.Value, bool) {
	v, ok := item[p.Top()]
	for _, s := range p[1:] {
		if !ok {
			return attr.Value{}, false
		}
		switch {
		case s.IsIndex && v.Type == attr.L && s.Index < len(v.L):
			v = v.L[s.Index]
		case !s.IsIndex && v.Type == attr.M:
			v, ok = v.M[s.Name]
		default:
			return attr.Value{}, false
		}
	}
	return v, ok
}

// change computes the new value of a path from the old one (exists is false
// when there is none); keep false removes the path.
type change func(old attr.Value, exists bool) (v attr.Value, keep bool, err error)

var errInvalidPath = attr.Errorf("The document path provided in the update expression is invalid for update")

// modify returns a copy of cur, a map or list, with the value at p changed by
// fn. Only the containers along p are copied, so cur is left untouched. When
// a container on the way is missing, modify fails if create is set (the
// change would add a value) and leaves cur as it is otherwise.
func modify(cur attr.Value, p Path, create bool, fn change) (attr.Value, error) {
	s, rest := p[0], p[1:]
	switch {
	case s.IsIndex && cur.Type == attr.L:
		return modifyList(cur, s.Index, rest, create, fn)
	case !s.IsIndex && cur.Type == attr.M:
		return modifyMap(cur, s.Name, rest, create, fn)
	case create:
		return cur, errInvalidPath
	default:
		return cur, nil
	}
}

func modifyMap(cur attr.Value, name string, rest Path, create bool, fn change) (attr.Value, error) {
	m := maps.Clone(cur.M)
	if m == nil {
		m = map[string]attr.Value{}
	}
	old, exists := m[name]
	if len(rest) > 0 {
		if !exists {
			if create {
				return cur, errInvalidPath
			}
			return cur, nil
		}
		child, err := modify(old, rest, create, fn)
		if err != nil {
			return cur, err
		}
		m[name] = child
		return attr.Map(m), nil
	}
	v, keep, err := fn(old, exists)
	if err != nil {
		return cur, err
	}
	if keep {
		m[name] = v
	} else {
		delete(m, name)
	}
	return attr.Map(m), nil
}

func modifyList(cur attr.Value, i int, rest Path, create bool, fn change) (attr.Value, error) {
	l := slices.Clone(cur.L)
	exists := i < len(l)
	if len(rest) > 0 {
		if !exists {
			if create {
				return cur, errInvalidPath
			}
			return cur, nil
		}
		child, err := modify(l[i], rest, create, fn)
		if err != nil {
			return cur, err
		}
		l[i] = child
		return attr.List(l), nil
	}
	var old attr.Value
	if exists {
		old = l[i]
	}
	v, keep, err := fn(old, exists)
	switch {
	case err != nil:
		return cur, err
	case keep && exists:
		l[i] = v
	case keep:
		l = append(l, v) // an index past the end appends, as DynamoDB does
	case exists:
		l = slices.Delete(l, i, i+1)
	}
	return attr.List(l), nil
}
