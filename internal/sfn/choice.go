package sfn

import (
	"fmt"
	"strings"
	"time"
)

// rule is one Choice rule: a boolean combination (And, Or, Not) or a single
// comparison of a Variable.
type rule struct {
	and, or []*rule
	not     *rule
	cmp     *comparison
	next    string
}

// comparison tests the value at variable. kind is String, Numeric, Boolean,
// Timestamp, Matches or one of the Is* type tests.
type comparison struct {
	variable ref
	kind     string
	rel      string // Equals, LessThan, GreaterThan, LessThanEquals, GreaterThanEquals
	operand  any
	operandP *ref // set for the "...Path" variants
	flag     bool // the boolean of the Is* tests
}

var relations = map[string]bool{
	"Equals": true, "LessThan": true, "GreaterThan": true,
	"LessThanEquals": true, "GreaterThanEquals": true,
}

var typeTests = map[string]bool{
	"IsNull": true, "IsPresent": true, "IsNumeric": true,
	"IsString": true, "IsBoolean": true, "IsTimestamp": true,
}

// parseRule compiles a Choice rule; top marks a rule of the Choices list,
// which must name a Next state.
func parseRule(v any, top bool) (*rule, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("a Choice rule must be an object")
	}
	r := &rule{}
	if next, has := m["Next"]; has {
		s, isStr := next.(string)
		if !isStr || !top {
			return nil, fmt.Errorf("Next is allowed only on a top-level Choice rule, as a string")
		}
		r.next = s
	} else if top {
		return nil, fmt.Errorf("a Choice rule needs a Next")
	}
	for _, key := range []string{"And", "Or", "Not", "Variable"} {
		if _, has := m[key]; has {
			return r, r.parseBody(key, m)
		}
	}
	return nil, fmt.Errorf("a Choice rule needs And, Or, Not or a Variable with a comparison")
}

func (r *rule) parseBody(key string, m map[string]any) error {
	switch key {
	case "And", "Or":
		list, ok := m[key].([]any)
		if !ok || len(list) == 0 {
			return fmt.Errorf("%s needs a non-empty array of rules", key)
		}
		for _, item := range list {
			sub, err := parseRule(item, false)
			if err != nil {
				return err
			}
			if key == "And" {
				r.and = append(r.and, sub)
			} else {
				r.or = append(r.or, sub)
			}
		}
	case "Not":
		sub, err := parseRule(m["Not"], false)
		if err != nil {
			return err
		}
		r.not = sub
	default:
		c, err := parseComparison(m)
		if err != nil {
			return err
		}
		r.cmp = c
	}
	return nil
}

func parseComparison(m map[string]any) (*comparison, error) {
	s, ok := m["Variable"].(string)
	if !ok {
		return nil, fmt.Errorf("Variable must be a path string")
	}
	v, err := parseRef(s)
	if err != nil {
		return nil, err
	}
	c := &comparison{variable: v}
	for key, operand := range m {
		if key == "Variable" || key == "Next" || key == "Comment" {
			continue
		}
		if c.kind != "" {
			return nil, fmt.Errorf("a rule on Variable %s has more than one comparison", s)
		}
		if err := c.setOperator(key, operand); err != nil {
			return nil, err
		}
	}
	if c.kind == "" {
		return nil, fmt.Errorf("the rule on Variable %s has no comparison", s)
	}
	return c, nil
}

func (c *comparison) setOperator(key string, operand any) error {
	switch {
	case typeTests[key]:
		b, ok := operand.(bool)
		if !ok {
			return fmt.Errorf("%s needs true or false", key)
		}
		c.kind, c.flag = key, b
		return nil
	case key == "StringMatches":
		if _, ok := operand.(string); !ok {
			return fmt.Errorf("StringMatches needs a string")
		}
		c.kind, c.operand = "Matches", operand
		return nil
	}
	for _, kind := range []string{"String", "Numeric", "Boolean", "Timestamp"} {
		rest, ok := strings.CutPrefix(key, kind)
		if !ok {
			continue
		}
		rel, viaPath := strings.CutSuffix(rest, "Path")
		if !relations[rel] || (kind == "Boolean" && rel != "Equals") {
			break
		}
		c.kind, c.rel = kind, rel
		return c.setOperand(key, operand, viaPath)
	}
	return fmt.Errorf("unknown Choice operator %q", key)
}

func (c *comparison) setOperand(key string, operand any, viaPath bool) error {
	if viaPath {
		s, ok := operand.(string)
		if !ok {
			return fmt.Errorf("%s needs a path string", key)
		}
		r, err := parseRef(s)
		if err != nil {
			return err
		}
		c.operandP = &r
		return nil
	}
	valid := false
	switch c.kind {
	case "String":
		_, valid = operand.(string)
	case "Numeric":
		_, valid = toFloat(operand)
	case "Boolean":
		_, valid = operand.(bool)
	case "Timestamp":
		s, isStr := operand.(string)
		_, err := time.Parse(time.RFC3339, s)
		valid = isStr && err == nil
	}
	if !valid {
		return fmt.Errorf("%s has an operand of the wrong type: %s", key, encodeJSON(operand))
	}
	c.operand = operand
	return nil
}

func (r *rule) eval(e *env) (bool, error) {
	switch {
	case r.and != nil:
		for _, sub := range r.and {
			if ok, err := sub.eval(e); err != nil || !ok {
				return false, err
			}
		}
		return true, nil
	case r.or != nil:
		for _, sub := range r.or {
			if ok, err := sub.eval(e); err != nil || ok {
				return ok, err
			}
		}
		return false, nil
	case r.not != nil:
		ok, err := r.not.eval(e)
		return !ok, err
	}
	return r.cmp.eval(e)
}

func (c *comparison) eval(e *env) (bool, error) {
	v, found := c.variable.eval(e)
	if c.kind == "IsPresent" {
		return found == c.flag, nil
	}
	if !found {
		return false, runtimeError("the Choice Variable %s could not be found in the input", c.variable.p.raw)
	}
	if typeTests[c.kind] {
		return typeTest(c.kind, v) == c.flag, nil
	}
	operand := c.operand
	if c.operandP != nil {
		o, ok := c.operandP.eval(e)
		if !ok {
			return false, runtimeError("the Choice operand path %s could not be found in the input", c.operandP.p.raw)
		}
		operand = o
	}
	return compare(c, v, operand), nil
}

func typeTest(kind string, v any) bool {
	switch kind {
	case "IsNull":
		return v == nil
	case "IsString":
		_, ok := v.(string)
		return ok
	case "IsNumeric":
		_, ok := toFloat(v)
		return ok
	case "IsBoolean":
		_, ok := v.(bool)
		return ok
	}
	s, ok := v.(string) // IsTimestamp
	if !ok {
		return false
	}
	_, err := time.Parse(time.RFC3339, s)
	return err == nil
}

// compare applies the comparison; a value of the wrong type is simply not a
// match, as in the AWS service.
func compare(c *comparison, v, operand any) bool {
	switch c.kind {
	case "Matches":
		s, ok := v.(string)
		return ok && wildcardMatch(operand.(string), s)
	case "String":
		a, aok := v.(string)
		b, bok := operand.(string)
		return aok && bok && relation(c.rel, strings.Compare(a, b))
	case "Numeric":
		a, aok := toFloat(v)
		b, bok := toFloat(operand)
		return aok && bok && relation(c.rel, cmpFloat(a, b))
	case "Boolean":
		a, aok := v.(bool)
		b, bok := operand.(bool)
		return aok && bok && a == b
	}
	a, aok := parseTime(v)
	b, bok := parseTime(operand)
	return aok && bok && relation(c.rel, a.Compare(b))
}

func parseTime(v any) (time.Time, bool) {
	s, ok := v.(string)
	if !ok {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s)
	return t, err == nil
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func relation(rel string, cmp int) bool {
	switch rel {
	case "Equals":
		return cmp == 0
	case "LessThan":
		return cmp < 0
	case "GreaterThan":
		return cmp > 0
	case "LessThanEquals":
		return cmp <= 0
	}
	return cmp >= 0 // GreaterThanEquals
}

// wildcardMatch implements StringMatches: "*" matches any run of characters
// and a backslash escapes "*" and "\".
func wildcardMatch(pattern, s string) bool {
	var parts []string // literal pieces between wildcards
	var cur strings.Builder
	for i := 0; i < len(pattern); i++ {
		switch {
		case pattern[i] == '\\' && i+1 < len(pattern):
			i++
			cur.WriteByte(pattern[i])
		case pattern[i] == '*':
			parts = append(parts, cur.String())
			cur.Reset()
		default:
			cur.WriteByte(pattern[i])
		}
	}
	parts = append(parts, cur.String())
	if len(parts) == 1 {
		return s == parts[0]
	}
	rest, ok := strings.CutPrefix(s, parts[0])
	if !ok {
		return false
	}
	for _, mid := range parts[1 : len(parts)-1] {
		i := strings.Index(rest, mid)
		if i < 0 {
			return false
		}
		rest = rest[i+len(mid):]
	}
	return strings.HasSuffix(rest, parts[len(parts)-1])
}
