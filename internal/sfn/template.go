package sfn

import (
	"fmt"
	"sort"
	"strings"
)

// template is a compiled Parameters, ResultSelector or Result-like value:
// literals with "key.$" fields that are filled in from an env.
type template struct {
	lit    any
	fields []tfield
	elems  []*template
	kind   tkind
}

type tkind int

const (
	tLiteral tkind = iota
	tObject
	tArray
)

type tfield struct {
	key string
	t   *template
	x   expr // set for a "key.$" field
}

// compileTemplate validates v, a decoded JSON value, and prepares it.
func compileTemplate(v any) (*template, error) {
	switch n := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(n))
		for k := range n {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		t := &template{kind: tObject}
		for _, k := range keys {
			f, err := compileField(k, n[k])
			if err != nil {
				return nil, err
			}
			t.fields = append(t.fields, f)
		}
		return t, nil
	case []any:
		t := &template{kind: tArray}
		for _, e := range n {
			c, err := compileTemplate(e)
			if err != nil {
				return nil, err
			}
			t.elems = append(t.elems, c)
		}
		return t, nil
	}
	return &template{lit: v}, nil
}

func compileField(key string, v any) (tfield, error) {
	name, dynamic := strings.CutSuffix(key, ".$")
	if !dynamic {
		t, err := compileTemplate(v)
		return tfield{key: key, t: t}, err
	}
	s, ok := v.(string)
	if !ok {
		return tfield{}, fmt.Errorf("the value of %q must be a string path or intrinsic function", key)
	}
	x, err := parseValueExpr(s)
	if err != nil {
		return tfield{}, fmt.Errorf("field %q: %w", key, err)
	}
	return tfield{key: name, x: x}, nil
}

// eval builds the value; it never modifies the template or the env.
func (t *template) eval(e *env) (any, error) {
	switch t.kind {
	case tObject:
		out := make(map[string]any, len(t.fields))
		for _, f := range t.fields {
			var v any
			var err error
			if f.x != nil {
				v, err = f.x.eval(e)
			} else {
				v, err = f.t.eval(e)
			}
			if err != nil {
				return nil, err
			}
			out[f.key] = v
		}
		return out, nil
	case tArray:
		out := make([]any, len(t.elems))
		for i, el := range t.elems {
			v, err := el.eval(e)
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	}
	return t.lit, nil
}
