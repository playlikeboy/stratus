package expr

import (
	"maps"
	"slices"

	"github.com/ODudek/stratus/internal/dynamodb/attr"
)

const projectionKind = "ProjectionExpression"

// ParseProjection parses a ProjectionExpression: comma-separated paths, no
// two of which overlap.
func ParseProjection(s string, ctx *Context) ([]Path, error) {
	p, err := newParser(projectionKind, s, ctx)
	if err != nil {
		return nil, err
	}
	for {
		if _, err := p.parsePath(); err != nil {
			return nil, err
		}
		if p.peek().kind != tComma {
			break
		}
		p.next()
	}
	if err := p.finish(); err != nil {
		return nil, err
	}
	if err := checkOverlap(projectionKind, p.paths); err != nil {
		return nil, err
	}
	return p.paths, nil
}

// Project returns the parts of item the paths select. Like DynamoDB, the
// selected elements of a list keep their order and close up: projecting
// l[1] and l[3] gives a list of two elements.
func Project(item attr.Item, paths []Path) attr.Item {
	tree := &selection{}
	for _, p := range paths {
		tree.add(p)
	}
	v, ok := tree.apply(attr.Map(item))
	if !ok {
		return attr.Item{}
	}
	return v.M
}

// selection is a tree of the selected paths.
type selection struct {
	whole   bool // the value is selected in full
	names   map[string]*selection
	indexes map[int]*selection
}

func (s *selection) add(p Path) {
	if len(p) == 0 {
		s.whole = true
		return
	}
	var child *selection
	if p[0].IsIndex {
		child = childOf(&s.indexes, p[0].Index)
	} else {
		child = childOf(&s.names, p[0].Name)
	}
	child.add(p[1:])
}

func childOf[K comparable](m *map[K]*selection, k K) *selection {
	if *m == nil {
		*m = map[K]*selection{}
	}
	if c, ok := (*m)[k]; ok {
		return c
	}
	c := &selection{}
	(*m)[k] = c
	return c
}

// apply returns the selected parts of v; ok is false when nothing is selected.
func (s *selection) apply(v attr.Value) (attr.Value, bool) {
	if s.whole {
		return v, true
	}
	switch {
	case v.Type == attr.M && s.names != nil:
		out := map[string]attr.Value{}
		for name, child := range s.names {
			if e, ok := v.M[name]; ok {
				if sel, ok := child.apply(e); ok {
					out[name] = sel
				}
			}
		}
		return attr.Map(out), len(out) > 0
	case v.Type == attr.L && s.indexes != nil:
		var out []attr.Value
		for _, i := range slices.Sorted(maps.Keys(s.indexes)) {
			if i < len(v.L) {
				if sel, ok := s.indexes[i].apply(v.L[i]); ok {
					out = append(out, sel)
				}
			}
		}
		return attr.List(out), len(out) > 0
	}
	return attr.Value{}, false
}
