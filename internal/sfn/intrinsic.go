package sfn

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ODudek/stratus/internal/awsapi"
)

// expr is a value computed from the state input and the context object: a
// path, a literal or an intrinsic function call.
type expr interface {
	eval(e *env) (any, error)
}

type pathExpr struct{ r ref }

func (x pathExpr) eval(e *env) (any, error) {
	v, ok := x.r.eval(e)
	if !ok {
		return nil, runtimeError("the JSONPath %s could not be found in the input", x.r.p.raw)
	}
	return v, nil
}

// litExpr is a literal argument; raw keeps the escapes States.Format needs.
type litExpr struct {
	val any
	raw string
}

func (x litExpr) eval(*env) (any, error) { return x.val, nil }

type callExpr struct {
	name string
	args []expr
}

// intrinsics lists the supported functions with their argument counts
// (max < 0 means any number).
var intrinsics = map[string]struct {
	min, max int
	fn       func(args []any) (any, error)
}{
	"States.Format":       {1, -1, nil}, // handled by callExpr.eval: it needs the raw template
	"States.StringToJson": {1, 1, fnStringToJSON},
	"States.JsonToString": {1, 1, fnJSONToString},
	"States.UUID":         {0, 0, func([]any) (any, error) { return awsapi.NewID(), nil }},
	"States.Array":        {0, -1, func(a []any) (any, error) { return append([]any{}, a...), nil }},
}

func intrinsicFailure(format string, args ...any) error {
	return &IntegrationError{Name: "States.IntrinsicFailure", Cause: fmt.Sprintf(format, args...)}
}

func fnStringToJSON(a []any) (any, error) {
	s, ok := a[0].(string)
	if !ok {
		return nil, intrinsicFailure("States.StringToJson needs a string, got %s", encodeJSON(a[0]))
	}
	v, err := decodeJSON(s)
	if err != nil {
		return nil, intrinsicFailure("States.StringToJson: %v", err)
	}
	return v, nil
}

func fnJSONToString(a []any) (any, error) { return encodeJSON(a[0]), nil }

func (c callExpr) eval(e *env) (any, error) {
	args := make([]any, len(c.args))
	for i, a := range c.args {
		if lit, ok := a.(litExpr); ok && i == 0 && c.name == "States.Format" {
			args[i] = lit.raw
			continue
		}
		v, err := a.eval(e)
		if err != nil {
			return nil, err
		}
		args[i] = v
	}
	if c.name == "States.Format" {
		return format(args)
	}
	return intrinsics[c.name].fn(args)
}

// format implements States.Format: "{}" takes the next argument, and a
// backslash escapes "{", "}" and "\".
func format(args []any) (any, error) {
	tpl, ok := args[0].(string)
	if !ok {
		return nil, intrinsicFailure("States.Format needs a string template")
	}
	rest := args[1:]
	var b strings.Builder
	for i := 0; i < len(tpl); i++ {
		switch {
		case tpl[i] == '\\' && i+1 < len(tpl) && strings.IndexByte(`{}\`, tpl[i+1]) >= 0:
			i++
			b.WriteByte(tpl[i])
		case tpl[i] == '{' && i+1 < len(tpl) && tpl[i+1] == '}':
			if len(rest) == 0 {
				return nil, intrinsicFailure("States.Format has more {} placeholders than arguments")
			}
			b.WriteString(stringify(rest[0]))
			rest = rest[1:]
			i++
		default:
			b.WriteByte(tpl[i])
		}
	}
	return b.String(), nil
}

func stringify(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return encodeJSON(v)
}

// parseValueExpr compiles the value of a "key.$" field: a path or an
// intrinsic function call.
func parseValueExpr(s string) (expr, error) {
	switch {
	case strings.HasPrefix(s, "States."):
		p := &iparser{s: s}
		x, err := p.call()
		if err != nil {
			return nil, fmt.Errorf("intrinsic function %q: %w", s, err)
		}
		if p.skipSpace(); p.i != len(s) {
			return nil, fmt.Errorf("intrinsic function %q: unexpected text after the call", s)
		}
		return x, nil
	case strings.HasPrefix(s, "$"):
		r, err := parseRef(s)
		if err != nil {
			return nil, err
		}
		return pathExpr{r}, nil
	}
	return nil, fmt.Errorf("%q is neither a path nor an intrinsic function", s)
}

type iparser struct {
	s string
	i int
}

func (p *iparser) skipSpace() {
	for p.i < len(p.s) && p.s[p.i] == ' ' {
		p.i++
	}
}

func (p *iparser) call() (expr, error) {
	open := strings.IndexByte(p.s[p.i:], '(')
	if open < 0 {
		return nil, fmt.Errorf("missing (")
	}
	name := p.s[p.i : p.i+open]
	spec, ok := intrinsics[name]
	if !ok {
		return nil, fmt.Errorf("%s is not supported by stratus", name)
	}
	p.i += open + 1
	var args []expr
	for {
		p.skipSpace()
		if p.i < len(p.s) && p.s[p.i] == ')' && len(args) == 0 {
			p.i++
			break
		}
		a, err := p.arg()
		if err != nil {
			return nil, err
		}
		args = append(args, a)
		p.skipSpace()
		if p.i >= len(p.s) {
			return nil, fmt.Errorf("missing )")
		}
		c := p.s[p.i]
		p.i++
		if c == ')' {
			break
		}
		if c != ',' {
			return nil, fmt.Errorf("expected , or ) but found %q", c)
		}
	}
	if len(args) < spec.min || (spec.max >= 0 && len(args) > spec.max) {
		return nil, fmt.Errorf("%s takes %d to %d arguments, got %d", name, spec.min, spec.max, len(args))
	}
	return callExpr{name: name, args: args}, nil
}

func (p *iparser) arg() (expr, error) {
	if p.i >= len(p.s) {
		return nil, fmt.Errorf("missing argument")
	}
	switch c := p.s[p.i]; {
	case c == '\'':
		return p.literal()
	case c == '$':
		return p.pathArg()
	case strings.HasPrefix(p.s[p.i:], "States."):
		return p.call()
	}
	tok := p.token()
	switch tok {
	case "true":
		return litExpr{val: true, raw: tok}, nil
	case "false":
		return litExpr{val: false, raw: tok}, nil
	case "null":
		return litExpr{val: nil, raw: tok}, nil
	}
	if _, err := strconv.ParseFloat(tok, 64); err != nil {
		return nil, fmt.Errorf("unexpected argument %q", tok)
	}
	v, _ := decodeJSON(tok)
	return litExpr{val: v, raw: tok}, nil
}

func (p *iparser) token() string {
	start := p.i
	for p.i < len(p.s) && !strings.ContainsRune(",) ", rune(p.s[p.i])) {
		p.i++
	}
	return p.s[start:p.i]
}

// literal reads a quoted string. Only \' is resolved here; States.Format
// resolves the other escapes itself, so raw keeps them.
func (p *iparser) literal() (expr, error) {
	var raw strings.Builder
	for p.i++; p.i < len(p.s); p.i++ {
		switch c := p.s[p.i]; {
		case c == '\'':
			p.i++
			return litExpr{val: unescape(raw.String()), raw: raw.String()}, nil
		case c == '\\' && p.i+1 < len(p.s):
			p.i++
			if p.s[p.i] != '\'' {
				raw.WriteByte('\\')
			}
			raw.WriteByte(p.s[p.i])
		default:
			raw.WriteByte(c)
		}
	}
	return nil, fmt.Errorf("unterminated string literal")
}

func unescape(raw string) string {
	var b strings.Builder
	for i := 0; i < len(raw); i++ {
		if raw[i] == '\\' && i+1 < len(raw) {
			i++
		}
		b.WriteByte(raw[i])
	}
	return b.String()
}

// pathArg reads a path argument, which may hold commas inside brackets.
func (p *iparser) pathArg() (expr, error) {
	start, depth := p.i, 0
	for ; p.i < len(p.s); p.i++ {
		c := p.s[p.i]
		if c == '[' {
			depth++
		} else if c == ']' {
			depth--
		} else if depth == 0 && (c == ',' || c == ')') {
			break
		}
	}
	r, err := parseRef(strings.TrimSpace(p.s[start:p.i]))
	if err != nil {
		return nil, err
	}
	return pathExpr{r}, nil
}
