package expr

import (
	"slices"
	"strconv"
	"strings"

	"github.com/ODudek/stratus/internal/dynamodb/attr"
)

// Context holds the placeholders of one request and records which of them its
// expressions use, because DynamoDB rejects both undefined and unused ones.
type Context struct {
	names      map[string]string
	values     map[string]attr.Value
	usedNames  map[string]bool
	usedValues map[string]bool
}

// NewContext returns the placeholders of a request; either map may be nil.
func NewContext(names map[string]string, values map[string]attr.Value) *Context {
	return &Context{names: names, values: values, usedNames: map[string]bool{}, usedValues: map[string]bool{}}
}

// CheckUnused fails when a placeholder was given but no expression used it.
// Call it after parsing every expression of the request.
func (c *Context) CheckUnused() error {
	if unused := unusedKeys(c.names, c.usedNames); len(unused) > 0 {
		return attr.Errorf("Value provided in ExpressionAttributeNames unused in expressions: keys: {%s}",
			strings.Join(unused, ", "))
	}
	if unused := unusedKeys(c.values, c.usedValues); len(unused) > 0 {
		return attr.Errorf("Value provided in ExpressionAttributeValues unused in expressions: keys: {%s}",
			strings.Join(unused, ", "))
	}
	return nil
}

func unusedKeys[V any](given map[string]V, used map[string]bool) []string {
	var out []string
	for k := range given {
		if !used[k] {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

// reserved are the keywords of the expression grammar, which cannot be used
// as bare attribute names.
var reserved = []string{"and", "or", "not", "between", "in", "set", "remove", "add", "delete"}

// parser walks the tokens of one expression.
type parser struct {
	kind  string // the expression's parameter name, for error messages
	toks  []token
	pos   int
	ctx   *Context
	paths []Path // every document path read, in order
	// undefined is the first placeholder missing from the context. It is
	// reported only once the expression has parsed, as DynamoDB reports
	// syntax errors first.
	undefined error
}

func newParser(kind, s string, ctx *Context) (*parser, error) {
	if strings.TrimSpace(s) == "" {
		return nil, attr.Errorf("Invalid %s: The expression can not be empty;", kind)
	}
	toks, err := lex(kind, s)
	if err != nil {
		return nil, err
	}
	return &parser{kind: kind, toks: toks, ctx: ctx}, nil
}

func (p *parser) peekAt(n int) token {
	if p.pos+n < len(p.toks) {
		return p.toks[p.pos+n]
	}
	return token{kind: tEOF}
}

func (p *parser) peek() token { return p.peekAt(0) }

func (p *parser) next() token {
	t := p.peek()
	if p.pos < len(p.toks) {
		p.pos++
	}
	return t
}

// unexpected reports the token at the current position as a syntax error.
func (p *parser) unexpected() error {
	return syntaxError(p.kind, p.peek().text, p.toks[:p.pos])
}

func (p *parser) expect(k tokenKind) error {
	if p.peek().kind != k {
		return p.unexpected()
	}
	p.next()
	return nil
}

// finish checks that every token was used and every placeholder defined.
func (p *parser) finish() error {
	if p.peek().kind != tEOF {
		return p.unexpected()
	}
	return p.undefined
}

func (p *parser) noteUndefined(err error) {
	if p.undefined == nil {
		p.undefined = err
	}
}

// parsePath reads name ( "." name | "[" index "]" )*.
func (p *parser) parsePath() (Path, error) {
	first, err := p.pathName()
	if err != nil {
		return nil, err
	}
	path := Path{{Name: first}}
	for {
		switch p.peek().kind {
		case tDot:
			p.next()
			name, err := p.pathName()
			if err != nil {
				return nil, err
			}
			path = append(path, Step{Name: name})
		case tLBracket:
			p.next()
			t := p.peek()
			i, err := strconv.Atoi(t.text)
			if t.kind != tInt || err != nil {
				return nil, p.unexpected()
			}
			p.next()
			if err := p.expect(tRBracket); err != nil {
				return nil, err
			}
			path = append(path, Step{Index: i, IsIndex: true})
		default:
			p.paths = append(p.paths, path)
			return path, nil
		}
	}
}

// pathName reads one name of a path: a bare name or a #placeholder.
func (p *parser) pathName() (string, error) {
	t := p.peek()
	switch {
	case t.kind == tName:
		p.next()
		name, ok := p.ctx.names[t.text]
		if !ok {
			p.noteUndefined(attr.Errorf("Invalid %s: An expression attribute name used in the document path "+
				"is not defined; attribute name: %s", p.kind, t.text))
		}
		p.ctx.usedNames[t.text] = true
		return name, nil
	case t.kind == tIdent && !slices.Contains(reserved, strings.ToLower(t.text)):
		p.next()
		return t.text, nil
	}
	return "", p.unexpected()
}

// value reads a :placeholder and returns its value.
func (p *parser) value() (attr.Value, error) {
	t := p.peek()
	if t.kind != tValue {
		return attr.Value{}, p.unexpected()
	}
	p.next()
	v, ok := p.ctx.values[t.text]
	if !ok {
		// A stand-in value lets the parse go on to find any syntax error.
		p.noteUndefined(attr.Errorf("Invalid %s: An expression attribute value used in expression "+
			"is not defined; attribute value: %s", p.kind, t.text))
		v = attr.Value{Type: attr.NULL}
	}
	p.ctx.usedValues[t.text] = true
	return v, nil
}

// arguments reads "(" operand ("," operand)* ")" with read for each operand.
func (p *parser) arguments(read func() (operand, error)) ([]operand, error) {
	if err := p.expect(tLParen); err != nil {
		return nil, err
	}
	var args []operand
	for {
		a, err := read()
		if err != nil {
			return nil, err
		}
		args = append(args, a)
		if p.peek().kind != tComma {
			break
		}
		p.next()
	}
	return args, p.expect(tRParen)
}

func (p *parser) operandCount(fn string, args []operand, want int) error {
	if len(args) != want {
		return attr.Errorf("Invalid %s: Incorrect number of operands for operator or function; "+
			"operator or function: %s, number of operands: %d", p.kind, fn, len(args))
	}
	return nil
}

// operandType reports a placeholder of the wrong type, unless a placeholder
// is undefined: that is the real error, and v may be its stand-in.
func (p *parser) operandType(fn string, v attr.Value) error {
	if p.undefined != nil {
		return p.undefined
	}
	return attr.Errorf("Invalid %s: Incorrect operand type for operator or function; "+
		"operator or function: %s, operand type: %s", p.kind, fn, v.Type)
}
