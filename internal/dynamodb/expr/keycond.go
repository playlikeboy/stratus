package expr

import (
	"slices"

	"github.com/stratus-emulator/stratus/internal/dynamodb/attr"
)

const keyKind = "KeyConditionExpression"

// KeyCondition is a parsed KeyConditionExpression: an equality on the
// partition key and at most one condition on the sort key.
type KeyCondition struct {
	Partition KeyPart
	Sort      *KeyPart // nil: every sort key matches
}

// KeyPart is the condition on one key attribute.
type KeyPart struct {
	Name  string
	Op    string // =, <, <=, >, >=, BETWEEN or begins_with
	Value attr.Value
	Upper attr.Value // the upper bound of a BETWEEN
}

// Match reports whether v satisfies the condition.
func (k KeyPart) Match(v attr.Value) bool {
	switch k.Op {
	case "begins_with":
		return attr.HasPrefix(v, k.Value)
	case "BETWEEN":
		lo, ok1 := attr.Compare(k.Value, v)
		hi, ok2 := attr.Compare(v, k.Upper)
		return ok1 && ok2 && lo <= 0 && hi <= 0
	}
	c, ok := attr.Compare(v, k.Value)
	if !ok {
		return false
	}
	return map[string]bool{"=": c == 0, "<": c < 0, "<=": c <= 0, ">": c > 0, ">=": c >= 0}[k.Op]
}

// ParseKeyCondition parses s against the key schema of the table or index
// queried: partitionKey and sortKey (empty when there is none).
func ParseKeyCondition(s string, ctx *Context, partitionKey, sortKey string) (KeyCondition, error) {
	c, err := ParseCondition(keyKind, s, ctx)
	if err != nil {
		return KeyCondition{}, err
	}
	var parts []KeyPart
	if err := collectKeyParts(c.root, &parts); err != nil {
		return KeyCondition{}, err
	}
	return assignKeyParts(parts, partitionKey, sortKey)
}

// collectKeyParts flattens the ANDs of a key condition; anything else but
// simple conditions on a key is rejected.
func collectKeyParts(c cond, into *[]KeyPart) error {
	switch c := c.(type) {
	case logical:
		if !c.and {
			return errKeyOperator("OR")
		}
		if err := collectKeyParts(c.l, into); err != nil {
			return err
		}
		return collectKeyParts(c.r, into)
	case negation:
		return errKeyOperator("NOT")
	case in:
		return errKeyOperator("IN")
	case comparison:
		if c.op == "<>" {
			return errKeyOperator(c.op)
		}
		if _, ok := c.l.(valueOperand); ok {
			flipped := map[string]string{"=": "=", "<": ">", "<=": ">=", ">": "<", ">=": "<="}[c.op]
			return addKeyPart(into, flipped, c.r, c.l, nil)
		}
		return addKeyPart(into, c.op, c.l, c.r, nil)
	case between:
		return addKeyPart(into, "BETWEEN", c.v, c.lo, c.hi)
	case function:
		if c.name != "begins_with" {
			return errKeyOperator(c.name)
		}
		return addKeyPart(into, c.name, c.args[0], c.args[1], nil)
	}
	return errKeyOperator("?")
}

func addKeyPart(into *[]KeyPart, op string, key, value, upper operand) error {
	path, ok := key.(pathOperand)
	if !ok {
		return attr.Errorf("Invalid KeyConditionExpression: The key condition must compare a key attribute with a value")
	}
	if len(path.path) != 1 {
		return attr.Errorf("KeyConditionExpressions cannot have conditions on nested attributes")
	}
	part := KeyPart{Name: path.path.Top(), Op: op}
	var vok bool
	if part.Value, vok = constant(value); !vok {
		return attr.Errorf("Invalid KeyConditionExpression: The key condition must compare a key attribute with a value")
	}
	if upper != nil {
		if part.Upper, vok = constant(upper); !vok {
			return attr.Errorf("Invalid KeyConditionExpression: The key condition must compare a key attribute with a value")
		}
	}
	*into = append(*into, part)
	return nil
}

func constant(o operand) (attr.Value, bool) {
	c, ok := o.(valueOperand)
	return c.v, ok
}

func errKeyOperator(op string) error {
	return attr.Errorf("Invalid operator used in KeyConditionExpression: %s", op)
}

func assignKeyParts(parts []KeyPart, partitionKey, sortKey string) (KeyCondition, error) {
	if len(parts) > 2 {
		return KeyCondition{}, attr.Errorf("KeyConditionExpressions must only contain one condition per key")
	}
	var kc KeyCondition
	found := false
	hasPartition := slices.ContainsFunc(parts, func(p KeyPart) bool { return p.Name == partitionKey })
	for _, p := range parts {
		switch {
		case p.Name == partitionKey && !found:
			if p.Op != "=" {
				return KeyCondition{}, attr.Errorf("Query key condition not supported")
			}
			kc.Partition, found = p, true
		case p.Name == sortKey && sortKey != "" && kc.Sort == nil:
			kc.Sort = &p
		case p.Name == partitionKey || p.Name == sortKey:
			return KeyCondition{}, attr.Errorf("KeyConditionExpressions must only contain one condition per key")
		default:
			// A condition on a non-key attribute: name the key it should be on.
			missing := sortKey
			if !hasPartition || sortKey == "" {
				missing = partitionKey
			}
			return KeyCondition{}, attr.Errorf("Query condition missed key schema element: %s", missing)
		}
	}
	if !found {
		return KeyCondition{}, attr.Errorf("Query condition missed key schema element: %s", partitionKey)
	}
	return kc, nil
}
