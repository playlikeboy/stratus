package expr

import (
	"github.com/stratus-emulator/stratus/internal/dynamodb/attr"
)

// operand is something that yields a value from an item: a path, a
// placeholder value, size(), or an update's arithmetic and functions.
type operand interface {
	// eval returns the operand's value in item; ok is false when there is
	// none (a missing path).
	eval(item attr.Item) (v attr.Value, ok bool, err error)
}

type pathOperand struct{ path Path }

func (o pathOperand) eval(item attr.Item) (attr.Value, bool, error) {
	v, ok := Get(item, o.path)
	return v, ok, nil
}

type valueOperand struct{ v attr.Value }

func (o valueOperand) eval(attr.Item) (attr.Value, bool, error) { return o.v, true, nil }

// sizeOperand is size(path): the length of a string or binary, or the member
// count of a set, list or map. It has no value for other types.
type sizeOperand struct{ path Path }

func (o sizeOperand) eval(item attr.Item) (attr.Value, bool, error) {
	v, ok := Get(item, o.path)
	if !ok {
		return attr.Value{}, false, nil
	}
	var n int
	switch v.Type {
	case attr.S:
		n = len(v.S)
	case attr.B:
		n = len(v.B)
	case attr.L, attr.M, attr.SS, attr.NS, attr.BS:
		n = v.Len()
	default:
		return attr.Value{}, false, nil
	}
	return attr.Num(attr.NumberFromInt(int64(n))), true, nil
}

// conditionOperand reads an operand of a condition: size(path), a path or a
// :placeholder.
func (p *parser) conditionOperand() (operand, error) {
	t := p.peek()
	switch {
	case t.kind == tValue:
		v, err := p.value()
		return valueOperand{v}, err
	case t.kind == tIdent && p.peekAt(1).kind == tLParen:
		return p.conditionFunctionOperand(t.text)
	default:
		path, err := p.parsePath()
		return pathOperand{path}, err
	}
}

// conditionFunctionOperand reads a function used as a value; only size is.
func (p *parser) conditionFunctionOperand(name string) (operand, error) {
	switch name {
	case "size":
		p.next()
		args, err := p.arguments(p.conditionOperand)
		if err != nil {
			return nil, err
		}
		if err := p.operandCount(name, args, 1); err != nil {
			return nil, err
		}
		path, ok := args[0].(pathOperand)
		if !ok {
			return nil, p.needsPath(name)
		}
		return sizeOperand{path.path}, nil
	case "attribute_exists", "attribute_not_exists", "attribute_type", "begins_with", "contains":
		return nil, attr.Errorf("Invalid %s: The function is not allowed to be used this way in an "+
			"expression; function: %s", p.kind, name)
	case "if_not_exists", "list_append":
		return nil, attr.Errorf("Invalid %s: The function is not allowed in a condition expression; "+
			"function: %s", p.kind, name)
	}
	return nil, p.unknownFunction(name)
}

func (p *parser) needsPath(fn string) error {
	return attr.Errorf("Invalid %s: Operator or function requires a document path; "+
		"operator or function: %s", p.kind, fn)
}

func (p *parser) unknownFunction(name string) error {
	return attr.Errorf("Invalid %s: Invalid function name; function: %s", p.kind, name)
}

// arithmetic is a + b or a - b in an update's SET; both must be numbers.
type arithmetic struct {
	minus bool
	l, r  operand
}

func (o arithmetic) eval(item attr.Item) (attr.Value, bool, error) {
	l, err := required(item, o.l)
	if err != nil {
		return attr.Value{}, false, err
	}
	r, err := required(item, o.r)
	if err != nil {
		return attr.Value{}, false, err
	}
	if l.Type != attr.N || r.Type != attr.N {
		return attr.Value{}, false, errOperandType
	}
	var n attr.Number
	if o.minus {
		n, err = l.Number().Sub(r.Number())
	} else {
		n, err = l.Number().Add(r.Number())
	}
	return attr.Num(n), err == nil, err
}

// ifNotExists is if_not_exists(path, fallback).
type ifNotExists struct {
	path     Path
	fallback operand
}

func (o ifNotExists) eval(item attr.Item) (attr.Value, bool, error) {
	if v, ok := Get(item, o.path); ok {
		return v, true, nil
	}
	v, err := required(item, o.fallback)
	return v, err == nil, err
}

// listAppend is list_append(a, b): the elements of a, then those of b.
type listAppend struct{ l, r operand }

func (o listAppend) eval(item attr.Item) (attr.Value, bool, error) {
	l, err := required(item, o.l)
	if err != nil {
		return attr.Value{}, false, err
	}
	r, err := required(item, o.r)
	if err != nil {
		return attr.Value{}, false, err
	}
	if l.Type != attr.L || r.Type != attr.L {
		return attr.Value{}, false, errOperandType
	}
	out := make([]attr.Value, 0, len(l.L)+len(r.L))
	return attr.List(append(append(out, l.L...), r.L...)), true, nil
}

var (
	errOperandType = attr.Errorf("An operand in the update expression has an incorrect data type")
	errNoAttribute = attr.Errorf("The provided expression refers to an attribute that does not exist in the item")
)

// required evaluates an operand of an update, which must have a value.
func required(item attr.Item, o operand) (attr.Value, error) {
	v, ok, err := o.eval(item)
	switch {
	case err != nil:
		return attr.Value{}, err
	case !ok:
		return attr.Value{}, errNoAttribute
	}
	return v, nil
}
