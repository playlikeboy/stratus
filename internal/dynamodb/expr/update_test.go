package expr

import (
	"strings"
	"testing"
)

func TestUpdateApply(t *testing.T) {
	base := `{"pk":{"S":"k"},"n":{"N":"5"},"l":{"L":[{"N":"1"},{"N":"2"},{"N":"3"},{"N":"4"}]},` +
		`"m":{"M":{"a":{"S":"b"}}},"ss":{"SS":["a","b"]}}`
	tests := []struct {
		name, expr, want string
	}{
		{"set new", "SET x = :one",
			`{"l":{"L":[{"N":"1"},{"N":"2"},{"N":"3"},{"N":"4"}]},"m":{"M":{"a":{"S":"b"}}},"n":{"N":"5"},"pk":{"S":"k"},"ss":{"SS":["a","b"]},"x":{"N":"1"}}`},
		{"arithmetic", "SET n = n + :ten, y = n - :one",
			`{"l":{"L":[{"N":"1"},{"N":"2"},{"N":"3"},{"N":"4"}]},"m":{"M":{"a":{"S":"b"}}},"n":{"N":"15"},"pk":{"S":"k"},"ss":{"SS":["a","b"]},"y":{"N":"4"}}`},
		{"if_not_exists", "SET n = if_not_exists(n, :one), c = if_not_exists(c, :ten)",
			`{"c":{"N":"10"},"l":{"L":[{"N":"1"},{"N":"2"},{"N":"3"},{"N":"4"}]},"m":{"M":{"a":{"S":"b"}}},"n":{"N":"5"},"pk":{"S":"k"},"ss":{"SS":["a","b"]}}`},
		{"list_append", "SET l = list_append(l, :list)",
			`{"l":{"L":[{"N":"1"},{"N":"2"},{"N":"3"},{"N":"4"},{"N":"9"}]},"m":{"M":{"a":{"S":"b"}}},"n":{"N":"5"},"pk":{"S":"k"},"ss":{"SS":["a","b"]}}`},
		{"nested set and list index", "SET m.c = :one, l[1] = :ten, l[10] = :five",
			`{"l":{"L":[{"N":"1"},{"N":"10"},{"N":"3"},{"N":"4"},{"N":"5"}]},"m":{"M":{"a":{"S":"b"},"c":{"N":"1"}}},"n":{"N":"5"},"pk":{"S":"k"},"ss":{"SS":["a","b"]}}`},
		{"remove", "REMOVE n, m.a, l[0], l[2], nosuch",
			`{"l":{"L":[{"N":"2"},{"N":"4"}]},"m":{"M":{}},"pk":{"S":"k"},"ss":{"SS":["a","b"]}}`},
		{"add", "ADD n :one, ss :set, newset :ns, cnt :five",
			`{"cnt":{"N":"5"},"l":{"L":[{"N":"1"},{"N":"2"},{"N":"3"},{"N":"4"}]},"m":{"M":{"a":{"S":"b"}}},"n":{"N":"6"},"newset":{"NS":["5"]},"pk":{"S":"k"},"ss":{"SS":["a","b","c"]}}`},
		{"delete", "DELETE ss :a2",
			`{"l":{"L":[{"N":"1"},{"N":"2"},{"N":"3"},{"N":"4"}]},"m":{"M":{"a":{"S":"b"}}},"n":{"N":"5"},"pk":{"S":"k"}}`},
		{"clauses in any order", "remove m set x = :one",
			`{"l":{"L":[{"N":"1"},{"N":"2"},{"N":"3"},{"N":"4"}]},"n":{"N":"5"},"pk":{"S":"k"},"ss":{"SS":["a","b"]},"x":{"N":"1"}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vals := values(t)
			vals[":a2"] = item(t, `{"v":{"SS":["a","b"]}}`)["v"]
			ctx := NewContext(nil, vals)
			u, err := ParseUpdate(tt.expr, ctx)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			orig := item(t, base)
			got, err := u.Apply(orig)
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if s := encode(t, got); s != tt.want {
				t.Errorf("Apply =\n%s\nwant\n%s", s, tt.want)
			}
			if s := encode(t, orig); s != encode(t, item(t, base)) {
				t.Errorf("Apply changed its input: %s", s)
			}
		})
	}
}

func TestUpdateErrors(t *testing.T) {
	base := `{"pk":{"S":"k"},"n":{"N":"5"},"s":{"S":"x"},"ss":{"SS":["a"]}}`
	tests := []struct {
		expr, wantErr string
		atApply       bool
	}{
		{expr: "SET", wantErr: "Syntax error"},
		{expr: "SET a = :one,", wantErr: "Syntax error"},
		{expr: "SET a :one", wantErr: "Syntax error"},
		{expr: "UPSERT a = :one", wantErr: "Syntax error"},
		{expr: "SET a = :one SET b = :one", wantErr: `The "SET" section can only be used once`},
		{expr: "SET a = :one REMOVE a", wantErr: "Two document paths overlap"},
		{expr: "SET a.b = :one, a = :five", wantErr: "Two document paths overlap"},
		{expr: "SET a = :hello + :one", wantErr: "Incorrect operand type"},
		{expr: "ADD a :hello", wantErr: "operator: ADD, operand type: STRING"},
		{expr: "DELETE ss :one", wantErr: "operator: DELETE, operand type: NUMBER"},
		{expr: "SET a = size(s)", wantErr: "not allowed in an update expression"},
		{expr: "SET a = :nope", wantErr: "attribute value: :nope"},
		{expr: "SET a = s + :one", wantErr: "incorrect data type", atApply: true},
		{expr: "SET a = absent + :one", wantErr: "refers to an attribute that does not exist", atApply: true},
		{expr: "SET nope.such = :one", wantErr: "document path provided in the update expression is invalid", atApply: true},
		{expr: "ADD s :one", wantErr: "incorrect data type", atApply: true},
		{expr: "DELETE ss :ns", wantErr: "incorrect data type", atApply: true},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			u, err := ParseUpdate(tt.expr, NewContext(nil, values(t)))
			if tt.atApply {
				if err != nil {
					t.Fatalf("parse: %v", err)
				}
				_, err = u.Apply(item(t, base))
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want one containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestProjection(t *testing.T) {
	it := item(t, fixture)
	tests := []struct {
		expr, want string
	}{
		{"pk, n", `{"n":{"N":"5"},"pk":{"S":"u1"}}`},
		{"m.nested.c, l[2].x, l[0]", `{"l":{"L":[{"N":"1"},{"M":{"x":{"N":"3"}}}]},"m":{"M":{"nested":{"M":{"c":{"N":"1"}}}}}}`},
		{"#m.a, absent, l[9]", `{"m":{"M":{"a":{"S":"b"}}}}`},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			ctx := NewContext(map[string]string{"#m": "m"}, nil)
			paths, err := ParseProjection(tt.expr, ctx)
			if err != nil {
				t.Fatal(err)
			}
			if got := encode(t, Project(it, paths)); got != tt.want {
				t.Errorf("Project = %s, want %s", got, tt.want)
			}
		})
	}
	if _, err := ParseProjection("a, a.b", NewContext(nil, nil)); err == nil ||
		!strings.Contains(err.Error(), "Invalid ProjectionExpression: Two document paths overlap") {
		t.Errorf("overlapping projection error = %v", err)
	}
}

func TestKeyCondition(t *testing.T) {
	tests := []struct {
		expr, wantErr, wantSortOp string
	}{
		{expr: "pk = :a", wantSortOp: ""},
		{expr: "pk = :a AND sk BETWEEN :one AND :ten", wantSortOp: "BETWEEN"},
		{expr: "begins_with(sk, :he) AND pk = :a", wantSortOp: "begins_with"},
		{expr: ":five > sk AND pk = :a", wantSortOp: "<"},
		{expr: "pk = :a OR sk = :one", wantErr: "Invalid operator used in KeyConditionExpression: OR"},
		{expr: "pk = :a AND sk <> :one", wantErr: "Invalid operator used in KeyConditionExpression: <>"},
		{expr: "pk = :a AND sk IN (:one)", wantErr: "Invalid operator used in KeyConditionExpression: IN"},
		{expr: "pk < :a", wantErr: "Query key condition not supported"},
		{expr: "sk = :one", wantErr: "Query condition missed key schema element: pk"},
		{expr: "pk = :a AND extra = :one", wantErr: "Query condition missed key schema element: sk"},
		{expr: "pk = :a AND sk = :one AND sk = :ten", wantErr: "only contain one condition per key"},
		{expr: "pk.x = :a", wantErr: "nested attributes"},
		{expr: "attribute_exists(pk)", wantErr: "Invalid operator used in KeyConditionExpression: attribute_exists"},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			kc, err := ParseKeyCondition(tt.expr, NewContext(nil, values(t)), "pk", "sk")
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want one containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			gotOp := ""
			if kc.Sort != nil {
				gotOp = kc.Sort.Op
			}
			if kc.Partition.Name != "pk" || gotOp != tt.wantSortOp {
				t.Errorf("key condition = %+v, sort op %q, want %q", kc.Partition, gotOp, tt.wantSortOp)
			}
		})
	}
}
