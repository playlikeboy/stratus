package expr

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ODudek/stratus/internal/dynamodb/attr"
)

const fixture = `{
	"pk": {"S": "u1"}, "n": {"N": "5"}, "s": {"S": "hello"},
	"l": {"L": [{"N": "1"}, {"S": "two"}, {"M": {"x": {"N": "3"}}}]},
	"m": {"M": {"a": {"S": "b"}, "nested": {"M": {"c": {"N": "1"}}}}},
	"ss": {"SS": ["a", "b"]}, "ns": {"NS": ["1", "2"]}, "b": {"B": "AAE="},
	"t": {"BOOL": true}, "z": {"NULL": true}, "status": {"S": "open"}
}`

func item(t *testing.T, s string) attr.Item {
	t.Helper()
	var it attr.Item
	if err := json.Unmarshal([]byte(s), &it); err != nil {
		t.Fatalf("decoding %s: %v", s, err)
	}
	return it
}

func encode(t *testing.T, it attr.Item) string {
	t.Helper()
	out, err := json.Marshal(it)
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	return string(out)
}

// values holds the placeholder values the tests use.
func values(t *testing.T) map[string]attr.Value {
	t.Helper()
	m := item(t, `{
		":one": {"N": "1"}, ":five": {"N": "5"}, ":ten": {"N": "10"}, ":hello": {"S": "hello"},
		":he": {"S": "he"}, ":a": {"S": "a"}, ":S": {"S": "S"}, ":SS": {"S": "SS"}, ":bad": {"S": "X"},
		":open": {"S": "open"}, ":list": {"L": [{"N": "9"}]}, ":set": {"SS": ["c"]}, ":ns": {"NS": ["5"]},
		":bin": {"B": "AA=="}, ":three": {"N": "3"}
	}`)
	return m
}

func names() map[string]string { return map[string]string{"#s": "status", "#m": "m", "#n": "n"} }

func TestConditionEval(t *testing.T) {
	it := item(t, fixture)
	tests := []struct {
		expr string
		want bool
	}{
		{"n = :five", true},
		{"n <> :five", false},
		{"missing <> :five", true},
		{"missing = :five", false},
		{"n < :ten AND n > :one", true},
		{"n <= :five AND n >= :five", true},
		{"n > :ten OR s = :hello", true},
		{"NOT n = :five", false},
		{"not (n = :one or n = :ten)", true},
		{"n BETWEEN :one AND :ten", true},
		{"n between :one and :five", true},
		{"n IN (:one, :ten)", false},
		{"n IN (:one, :five)", true},
		{"attribute_exists(m.nested.c)", true},
		{"attribute_exists(l[2].x)", true},
		{"attribute_exists(l[3])", false},
		{"attribute_not_exists(missing)", true},
		{"attribute_type(ss, :SS)", true},
		{"attribute_type(s, :SS)", false},
		{"begins_with(s, :he)", true},
		{"begins_with(n, :he)", false},
		{"contains(s, :he)", true},
		{"contains(ss, :a)", true},
		{"contains(ns, :one)", true},
		{"contains(ns, :ten)", false},
		{"contains(l, :one)", true},
		{"size(s) = :five", true},
		{"size(l) = :three", true},
		{"size(ss) < :three", true},
		{"size(n) = :one", false},
		{"#s = :open", true},
		{"#m.a = :a", false},
		{"#m.nested.c = :one", true},
		{"m.nested.c = :one AND l[0] = :one", true},
		{"s < :ten", false},
		{"((n = :five))", true},
		{"n = :five AND s = :hello OR n = :one", true},
		{"n = :one AND s = :hello OR n = :five", true},
		{"n = :one AND (s = :hello OR n = :five)", false},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := NewContext(names(), values(t))
			c, err := ParseCondition("ConditionExpression", tt.expr, ctx)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			got, err := c.Eval(it)
			if err != nil || got != tt.want {
				t.Errorf("Eval = %v, %v, want %v", got, err, tt.want)
			}
		})
	}
}

func TestConditionErrors(t *testing.T) {
	tests := []struct {
		expr, wantErr string
	}{
		{"", "can not be empty"},
		{"n =", `Syntax error; token: "<EOF>"`},
		{"n = :five AND", "Syntax error"},
		{"n == :five", "Syntax error"},
		{"n = :five)", "Syntax error"},
		{"(n = :five", "Syntax error"},
		{"n $ :five", `Syntax error; token: "$"`},
		{"n = :undefined", "attribute value: :undefined"},
		{"#undefined = :five", "attribute name: #undefined"},
		{"#undefined = :five AND", "Syntax error"}, // syntax is reported before placeholders
		{"begins_with(s, :undefined)", "attribute value: :undefined"},
		{"n BETWEEN :ten AND :one", "requires upper bound to be greater than or equal to lower bound"},
		{"nosuch(n)", "Invalid function name; function: nosuch"},
		{"attribute_exists(:five)", "requires a document path"},
		{"attribute_exists(n, s)", "Incorrect number of operands"},
		{"attribute_type(n, :bad)", "Invalid attribute type name found; type: X"},
		{"begins_with(s, :one)", "Incorrect operand type"},
		{"size(n)", "Syntax error"},
		{"attribute_exists(n) = :five", "Syntax error"},
		{"if_not_exists(n, :one) = :five", "not allowed in a condition expression"},
		{"l[x] = :one", "Syntax error"},
		{"and = :one", "Syntax error"},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := NewContext(names(), values(t))
			_, err := ParseCondition("ConditionExpression", tt.expr, ctx)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want one containing %q", err, tt.wantErr)
			}
			if !strings.HasPrefix(tt.wantErr, "Syntax") {
				return
			}
			if !strings.HasPrefix(err.Error(), "Invalid ConditionExpression: ") {
				t.Errorf("error %q does not name the expression", err)
			}
		})
	}
}

func TestUnusedPlaceholders(t *testing.T) {
	ctx := NewContext(map[string]string{"#a": "a", "#b": "b"}, values(t))
	if _, err := ParseCondition("ConditionExpression", "#a = :one", ctx); err != nil {
		t.Fatal(err)
	}
	err := ctx.CheckUnused()
	if err == nil || !strings.Contains(err.Error(), "ExpressionAttributeNames unused in expressions: keys: {#b}") {
		t.Fatalf("CheckUnused = %v, want #b reported unused", err)
	}
	ctx = NewContext(nil, map[string]attr.Value{":x": attr.Str("x"), ":y": attr.Str("y")})
	if _, err := ParseCondition("ConditionExpression", "a = :x", ctx); err != nil {
		t.Fatal(err)
	}
	err = ctx.CheckUnused()
	if err == nil || !strings.Contains(err.Error(), "ExpressionAttributeValues unused in expressions: keys: {:y}") {
		t.Fatalf("CheckUnused = %v, want :y reported unused", err)
	}
}
