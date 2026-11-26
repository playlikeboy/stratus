package expr

import (
	"strings"

	"github.com/stratus-emulator/stratus/internal/dynamodb/attr"
)

const updateKind = "UpdateExpression"

// Update is a parsed UpdateExpression.
type Update struct {
	set    []setAction
	remove []Path
	add    []valueAction
	delete []valueAction
}

type setAction struct {
	path  Path
	value operand
}

// valueAction is an ADD or DELETE: a path and a placeholder value.
type valueAction struct {
	path  Path
	value attr.Value
}

// ParseUpdate parses an UpdateExpression: SET, REMOVE, ADD and DELETE
// clauses, each at most once, in any order.
func ParseUpdate(s string, ctx *Context) (*Update, error) {
	p, err := newParser(updateKind, s, ctx)
	if err != nil {
		return nil, err
	}
	u := &Update{}
	seen := map[string]bool{}
	for p.peek().kind != tEOF {
		t := p.next()
		clause := strings.ToUpper(t.text)
		if t.kind != tIdent || !strings.Contains(" SET REMOVE ADD DELETE ", " "+clause+" ") {
			p.pos--
			return nil, p.unexpected()
		}
		if seen[clause] {
			return nil, attr.Errorf("Invalid UpdateExpression: The \"%s\" section can only be used once "+
				"in an update expression;", clause)
		}
		seen[clause] = true
		if err := p.clause(u, clause); err != nil {
			return nil, err
		}
	}
	if err := p.finish(); err != nil {
		return nil, err
	}
	if err := checkOverlap(updateKind, u.Paths()); err != nil {
		return nil, err
	}
	return u, nil
}

// clause reads the comma-separated actions of one clause.
func (p *parser) clause(u *Update, clause string) error {
	for {
		var err error
		switch clause {
		case "SET":
			err = p.setAction(u)
		case "REMOVE":
			var path Path
			if path, err = p.parsePath(); err == nil {
				u.remove = append(u.remove, path)
			}
		case "ADD":
			err = p.valueAction(clause, &u.add)
		default:
			err = p.valueAction(clause, &u.delete)
		}
		if err != nil {
			return err
		}
		if p.peek().kind != tComma {
			return nil
		}
		p.next()
	}
}

func (p *parser) setAction(u *Update) error {
	path, err := p.parsePath()
	if err != nil {
		return err
	}
	if t := p.peek(); t.kind != tCmp || t.text != "=" {
		return p.unexpected()
	}
	p.next()
	v, err := p.setValue()
	if err != nil {
		return err
	}
	u.set = append(u.set, setAction{path: path, value: v})
	return nil
}

// setValue reads operand [ ("+" | "-") operand ].
func (p *parser) setValue() (operand, error) {
	l, err := p.setOperand()
	if err != nil {
		return nil, err
	}
	t := p.peek()
	if t.kind != tPlus && t.kind != tMinus {
		return l, nil
	}
	p.next()
	r, err := p.setOperand()
	if err != nil {
		return nil, err
	}
	for _, o := range []operand{l, r} {
		if c, ok := o.(valueOperand); ok && c.v.Type != attr.N {
			return nil, p.operandType(t.text, c.v)
		}
	}
	return arithmetic{minus: t.kind == tMinus, l: l, r: r}, nil
}

// setOperand reads a path, a :placeholder, if_not_exists(path, operand) or
// list_append(operand, operand).
func (p *parser) setOperand() (operand, error) {
	t := p.peek()
	switch {
	case t.kind == tValue:
		v, err := p.value()
		return valueOperand{v}, err
	case t.kind != tIdent || p.peekAt(1).kind != tLParen:
		path, err := p.parsePath()
		return pathOperand{path}, err
	}
	switch t.text {
	case "if_not_exists", "list_append":
	case "attribute_exists", "attribute_not_exists", "attribute_type", "begins_with", "contains", "size":
		return nil, attr.Errorf("Invalid UpdateExpression: The function is not allowed in an update "+
			"expression; function: %s", t.text)
	default:
		return nil, p.unknownFunction(t.text)
	}
	p.next()
	args, err := p.arguments(p.setOperand)
	if err != nil {
		return nil, err
	}
	if err := p.operandCount(t.text, args, 2); err != nil {
		return nil, err
	}
	if t.text == "list_append" {
		for _, a := range args {
			if c, ok := a.(valueOperand); ok && c.v.Type != attr.L {
				return nil, p.operandType(t.text, c.v)
			}
		}
		return listAppend{l: args[0], r: args[1]}, nil
	}
	path, ok := args[0].(pathOperand)
	if !ok {
		return nil, p.needsPath(t.text)
	}
	return ifNotExists{path: path.path, fallback: args[1]}, nil
}

// valueAction reads path :placeholder for ADD (a number or a set) or DELETE
// (a set).
func (p *parser) valueAction(clause string, into *[]valueAction) error {
	path, err := p.parsePath()
	if err != nil {
		return err
	}
	v, err := p.value()
	if err != nil {
		return err
	}
	if !v.IsSet() && (clause == "DELETE" || v.Type != attr.N) {
		if p.undefined != nil {
			return p.undefined
		}
		return attr.Errorf("Invalid UpdateExpression: Incorrect operand type for operator or function; "+
			"operator: %s, operand type: %s", clause, typeWord(v.Type))
	}
	*into = append(*into, valueAction{path: path, value: v})
	return nil
}

// typeWord names a type as DynamoDB does in operand type errors.
func typeWord(t attr.Type) string {
	return map[attr.Type]string{
		attr.S: "STRING", attr.N: "NUMBER", attr.B: "BINARY", attr.BOOL: "BOOLEAN", attr.NULL: "NULL",
		attr.L: "LIST", attr.M: "MAP",
	}[t]
}

// Paths returns every path the update writes, in clause order.
func (u *Update) Paths() []Path {
	var out []Path
	for _, a := range u.set {
		out = append(out, a.path)
	}
	out = append(out, u.remove...)
	for _, a := range u.add {
		out = append(out, a.path)
	}
	for _, a := range u.delete {
		out = append(out, a.path)
	}
	return out
}
