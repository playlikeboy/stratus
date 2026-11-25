package sfn

import (
	"fmt"
	"maps"
	"strconv"
	"strings"
)

// step is one hop of a JSONPath: an object key or an array index.
type step struct {
	key     string
	index   int
	isIndex bool
}

// path is a parsed JSONPath of the supported subset: $, .name, ['name'], [n]
// and [-n]. Anything richer is rejected when the definition is created.
type path struct {
	raw   string
	steps []step
}

func unsupportedPath(raw, reason string) error {
	return fmt.Errorf("unsupported JSONPath %q: %s; stratus supports $, .name, ['name'], [n] and [-n]", raw, reason)
}

// parsePath parses a path that starts with a single "$".
func parsePath(raw string) (path, error) {
	if !strings.HasPrefix(raw, "$") {
		return path{}, fmt.Errorf("invalid JSONPath %q: it must start with $", raw)
	}
	p := path{raw: raw}
	i := 1
	for i < len(raw) {
		var (
			st   step
			next int
			err  error
		)
		switch raw[i] {
		case '.':
			st, next, err = parseDot(raw, i)
		case '[':
			st, next, err = parseBracket(raw, i)
		default:
			err = unsupportedPath(raw, "unexpected character "+strconv.QuoteRune(rune(raw[i])))
		}
		if err != nil {
			return path{}, err
		}
		p.steps = append(p.steps, st)
		i = next
	}
	return p, nil
}

func parseDot(raw string, i int) (step, int, error) {
	if i+1 < len(raw) && raw[i+1] == '.' {
		return step{}, 0, unsupportedPath(raw, "recursive descent (..) is not supported")
	}
	j := i + 1
	for j < len(raw) && raw[j] != '.' && raw[j] != '[' {
		j++
	}
	name := raw[i+1 : j]
	if name == "" {
		return step{}, 0, unsupportedPath(raw, "empty member name")
	}
	if strings.ContainsAny(name, "*?@(),:'\" ") {
		return step{}, 0, unsupportedPath(raw, "wildcards, filters, slices and unions are not supported")
	}
	return step{key: name}, j, nil
}

func parseBracket(raw string, i int) (step, int, error) {
	if i+1 < len(raw) && raw[i+1] == '\'' {
		end := strings.Index(raw[i+2:], "'")
		if end < 0 || i+2+end+1 >= len(raw) || raw[i+2+end+1] != ']' {
			return step{}, 0, unsupportedPath(raw, "unterminated ['name']")
		}
		return step{key: raw[i+2 : i+2+end]}, i + 2 + end + 2, nil
	}
	end := strings.IndexByte(raw[i:], ']')
	if end < 0 {
		return step{}, 0, unsupportedPath(raw, "missing ]")
	}
	n, err := strconv.Atoi(raw[i+1 : i+end])
	if err != nil {
		return step{}, 0, unsupportedPath(raw, "wildcards, filters, slices and unions are not supported")
	}
	return step{index: n, isIndex: true}, i + end + 1, nil
}

// get reads the value the path points at; ok is false when it is not there.
func (p path) get(v any) (any, bool) {
	for _, st := range p.steps {
		if st.isIndex {
			arr, isArr := v.([]any)
			if !isArr {
				return nil, false
			}
			idx := st.index
			if idx < 0 {
				idx += len(arr)
			}
			if idx < 0 || idx >= len(arr) {
				return nil, false
			}
			v = arr[idx]
			continue
		}
		obj, isObj := v.(map[string]any)
		if !isObj {
			return nil, false
		}
		next, found := obj[st.key]
		if !found {
			return nil, false
		}
		v = next
	}
	return v, true
}

// onlyKeys reports whether the path has no array index, which is all a
// ResultPath may use.
func (p path) onlyKeys() bool {
	for _, st := range p.steps {
		if st.isIndex {
			return false
		}
	}
	return true
}

// set returns a copy of root with val placed where the path points, creating
// the objects on the way. root itself is never modified.
func (p path) set(root, val any) any { return setAt(root, p.steps, val) }

func setAt(cur any, steps []step, val any) any {
	if len(steps) == 0 {
		return val
	}
	obj, _ := cur.(map[string]any)
	out := make(map[string]any, len(obj)+1)
	maps.Copy(out, obj)
	out[steps[0].key] = setAt(out[steps[0].key], steps[1:], val)
	return out
}

// ref is a path into the state input ("$...") or the context object ("$$...").
type ref struct {
	ctx bool
	p   path
}

func parseRef(raw string) (ref, error) {
	if strings.HasPrefix(raw, "$$") {
		p, err := parsePath(raw[1:])
		return ref{ctx: true, p: path{raw: raw, steps: p.steps}}, err
	}
	p, err := parsePath(raw)
	return ref{p: p}, err
}

func (r ref) eval(e *env) (any, bool) {
	if r.ctx {
		return r.p.get(e.ctx)
	}
	return r.p.get(e.input)
}

// env is what a path or an intrinsic function reads: the state input and the
// context object.
type env struct {
	input any
	ctx   map[string]any
}
