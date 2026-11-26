package expr

import (
	"slices"
	"strings"

	"github.com/stratus-emulator/stratus/internal/dynamodb/attr"
)

// maxInOperands is DynamoDB's limit on the list of an IN.
const maxInOperands = 100

// Condition is a parsed ConditionExpression, FilterExpression or
// KeyConditionExpression.
type Condition struct {
	root  cond
	paths []Path
}

// ParseCondition parses s; kind is the parameter name used in errors, for
// example "ConditionExpression".
func ParseCondition(kind, s string, ctx *Context) (*Condition, error) {
	p, err := newParser(kind, s, ctx)
	if err != nil {
		return nil, err
	}
	root, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if err := p.finish(); err != nil {
		return nil, err
	}
	return &Condition{root: root, paths: p.paths}, nil
}

// Eval reports whether item satisfies the condition. A nil Condition always
// holds.
func (c *Condition) Eval(item attr.Item) (bool, error) {
	if c == nil {
		return true, nil
	}
	return c.root.eval(item)
}

// Paths returns every document path the condition reads.
func (c *Condition) Paths() []Path { return c.paths }

type cond interface {
	eval(item attr.Item) (bool, error)
}

type logical struct {
	and  bool
	l, r cond
}

func (c logical) eval(item attr.Item) (bool, error) {
	l, err := c.l.eval(item)
	if err != nil || l != c.and {
		return l, err // AND stops at false, OR at true
	}
	return c.r.eval(item)
}

type negation struct{ c cond }

func (c negation) eval(item attr.Item) (bool, error) {
	ok, err := c.c.eval(item)
	return !ok, err
}

// parseOr, parseAnd, parseNot and parsePrimary follow DynamoDB's precedence:
// comparisons, BETWEEN, IN and functions bind tightest, then NOT, AND, OR.
func (p *parser) parseOr() (cond, error) {
	l, err := p.parseAnd()
	for err == nil && p.peek().keyword("or") {
		p.next()
		var r cond
		if r, err = p.parseAnd(); err == nil {
			l = logical{and: false, l: l, r: r}
		}
	}
	return l, err
}

func (p *parser) parseAnd() (cond, error) {
	l, err := p.parseNot()
	for err == nil && p.peek().keyword("and") {
		p.next()
		var r cond
		if r, err = p.parseNot(); err == nil {
			l = logical{and: true, l: l, r: r}
		}
	}
	return l, err
}

func (p *parser) parseNot() (cond, error) {
	if p.peek().keyword("not") {
		p.next()
		c, err := p.parseNot()
		return negation{c}, err
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (cond, error) {
	t := p.peek()
	if t.kind == tLParen {
		p.next()
		c, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		return c, p.expect(tRParen)
	}
	if t.kind == tIdent && p.peekAt(1).kind == tLParen && t.text != "size" {
		return p.parseFunction(t.text)
	}
	l, err := p.conditionOperand()
	if err != nil {
		return nil, err
	}
	switch t := p.peek(); {
	case t.kind == tCmp:
		p.next()
		r, err := p.conditionOperand()
		return comparison{op: t.text, l: l, r: r}, err
	case t.keyword("between"):
		return p.parseBetween(l)
	case t.keyword("in"):
		return p.parseIn(l)
	}
	return nil, p.unexpected()
}

func (p *parser) parseBetween(v operand) (cond, error) {
	p.next()
	lo, err := p.conditionOperand()
	if err != nil {
		return nil, err
	}
	if !p.peek().keyword("and") {
		return nil, p.unexpected()
	}
	p.next()
	hi, err := p.conditionOperand()
	if err != nil {
		return nil, err
	}
	lc, lok := lo.(valueOperand)
	hc, hok := hi.(valueOperand)
	if lok && hok {
		if c, ok := attr.Compare(lc.v, hc.v); ok && c > 0 {
			return nil, attr.Errorf("Invalid %s: The BETWEEN operator requires upper bound to be greater "+
				"than or equal to lower bound; lower bound operand: AttributeValue: %s, upper bound "+
				"operand: AttributeValue: %s", p.kind, lc.v, hc.v)
		}
	}
	return between{v: v, lo: lo, hi: hi}, nil
}

func (p *parser) parseIn(v operand) (cond, error) {
	p.next()
	list, err := p.arguments(p.conditionOperand)
	if err != nil {
		return nil, err
	}
	if len(list) > maxInOperands {
		return nil, attr.Errorf("Invalid %s: The IN operator is provided with too many operands; "+
			"number of operands: %d", p.kind, len(list))
	}
	return in{v: v, list: list}, nil
}

type comparison struct {
	op   string
	l, r operand
}

func (c comparison) eval(item attr.Item) (bool, error) {
	l, lok, _ := c.l.eval(item)
	r, rok, _ := c.r.eval(item)
	if c.op == "<>" {
		return !lok || !rok || !attr.Equal(l, r), nil
	}
	if !lok || !rok {
		return false, nil
	}
	if c.op == "=" {
		return attr.Equal(l, r), nil
	}
	n, ok := attr.Compare(l, r)
	if !ok {
		return false, nil
	}
	switch c.op {
	case "<":
		return n < 0, nil
	case "<=":
		return n <= 0, nil
	case ">":
		return n > 0, nil
	default:
		return n >= 0, nil
	}
}

type between struct{ v, lo, hi operand }

func (c between) eval(item attr.Item) (bool, error) {
	v, ok1, _ := c.v.eval(item)
	lo, ok2, _ := c.lo.eval(item)
	hi, ok3, _ := c.hi.eval(item)
	if !ok1 || !ok2 || !ok3 {
		return false, nil
	}
	a, ok := attr.Compare(lo, v)
	b, ok2 := attr.Compare(v, hi)
	return ok && ok2 && a <= 0 && b <= 0, nil
}

type in struct {
	v    operand
	list []operand
}

func (c in) eval(item attr.Item) (bool, error) {
	v, ok, _ := c.v.eval(item)
	if !ok {
		return false, nil
	}
	return slices.ContainsFunc(c.list, func(o operand) bool {
		e, ok, _ := o.eval(item)
		return ok && attr.Equal(v, e)
	}), nil
}

// function is one of the boolean functions of a condition.
type function struct {
	name string
	args []operand
}

func (p *parser) parseFunction(name string) (cond, error) {
	arity := map[string]int{
		"attribute_exists": 1, "attribute_not_exists": 1, "attribute_type": 2, "begins_with": 2, "contains": 2,
	}
	want, ok := arity[name]
	if !ok {
		_, err := p.conditionFunctionOperand(name)
		return nil, err
	}
	p.next()
	args, err := p.arguments(p.conditionOperand)
	if err != nil {
		return nil, err
	}
	if err := p.operandCount(name, args, want); err != nil {
		return nil, err
	}
	if err := p.checkFunction(name, args); err != nil {
		return nil, err
	}
	return function{name: name, args: args}, nil
}

// checkFunction checks what can be checked before evaluation: the operands
// that must be paths, and the types of placeholder operands.
func (p *parser) checkFunction(name string, args []operand) error {
	if _, ok := args[0].(pathOperand); !ok && name != "begins_with" && name != "contains" {
		return p.needsPath(name)
	}
	switch name {
	case "attribute_type":
		t, ok := args[1].(valueOperand)
		if !ok || t.v.Type != attr.S {
			return p.operandType(name, t.v)
		}
		if !slices.Contains(attr.Types, attr.Type(t.v.S)) {
			return attr.Errorf("Invalid %s: Invalid attribute type name found; type: %s, "+
				"valid types: { B,NULL,SS,BOOL,L,BS,N,NS,S,M }", p.kind, t.v.S)
		}
	case "begins_with":
		for _, a := range args {
			if c, ok := a.(valueOperand); ok && c.v.Type != attr.S && c.v.Type != attr.B {
				return p.operandType(name, c.v)
			}
		}
	}
	return nil
}

func (c function) eval(item attr.Item) (bool, error) {
	a, aok, _ := c.args[0].eval(item)
	if c.name == "attribute_exists" || c.name == "attribute_not_exists" {
		return aok == (c.name == "attribute_exists"), nil
	}
	b, bok, _ := c.args[1].eval(item)
	if !aok || !bok {
		return false, nil
	}
	switch c.name {
	case "attribute_type":
		return strings.EqualFold(string(a.Type), b.S), nil
	case "begins_with":
		return attr.HasPrefix(a, b), nil
	default:
		return attr.Contains(a, b), nil
	}
}
