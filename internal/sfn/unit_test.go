package sfn

import (
	"strings"
	"testing"
	"time"
)

func mustJSON(t *testing.T, s string) any {
	t.Helper()
	v, err := decodeJSON(s)
	if err != nil {
		t.Fatalf("decodeJSON(%s): %v", s, err)
	}
	return v
}

func TestPathGet(t *testing.T) {
	doc := mustJSON(t, `{"a":{"b":[10,20,30],"c d":"x"},"n":null}`)
	tests := []struct {
		path string
		want string
		ok   bool
	}{
		{"$", `{"a":{"b":[10,20,30],"c d":"x"},"n":null}`, true},
		{"$.a.b", `[10,20,30]`, true},
		{"$['a']['c d']", `"x"`, true},
		{"$.a.b[0]", `10`, true},
		{"$.a.b[-1]", `30`, true},
		{"$.a.b[3]", ``, false},
		{"$.a.b[-4]", ``, false},
		{"$.n", `null`, true},
		{"$.missing", ``, false},
		{"$.a.b.c", ``, false},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			p, err := parsePath(tt.path)
			if err != nil {
				t.Fatal(err)
			}
			got, ok := p.get(doc)
			if ok != tt.ok || (ok && encodeJSON(got) != tt.want) {
				t.Errorf("get = %s, %v; want %s, %v", encodeJSON(got), ok, tt.want, tt.ok)
			}
		})
	}
}

func TestPathRejectsRichSyntax(t *testing.T) {
	for _, raw := range []string{"a.b", "$..a", "$.*", "$.a[*]", "$.a[1:2]", "$.a[?(@.x)]", "$.a[1,2]", "$.a[", "$.", "$['a'"} {
		t.Run(raw, func(t *testing.T) {
			if _, err := parsePath(raw); err == nil {
				t.Errorf("parsePath(%q) succeeded", raw)
			}
		})
	}
}

func TestPathSetDoesNotMutate(t *testing.T) {
	root := mustJSON(t, `{"a":{"x":1}}`)
	p, _ := parsePath("$.a.y.z")
	got := p.set(root, "v")
	if encodeJSON(got) != `{"a":{"x":1,"y":{"z":"v"}}}` {
		t.Errorf("set = %s", encodeJSON(got))
	}
	if encodeJSON(root) != `{"a":{"x":1}}` {
		t.Errorf("root was modified: %s", encodeJSON(root))
	}
}

func evalTemplate(t *testing.T, tmpl, input string) (string, error) {
	t.Helper()
	c, err := compileTemplate(mustJSON(t, tmpl))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ctx := map[string]any{
		"Execution": map[string]any{"Id": "exec-1", "Name": "n1"},
		"State":     map[string]any{"Name": "S"},
		"Task":      map[string]any{"Token": "tok"},
	}
	out, err := c.eval(&env{input: mustJSON(t, input), ctx: ctx})
	if err != nil {
		return "", err
	}
	return encodeJSON(out), nil
}

func TestTemplates(t *testing.T) {
	tests := []struct {
		name, tmpl, input, want string
	}{
		{"literal", `{"a":1,"b":[true,null]}`, `{}`, `{"a":1,"b":[true,null]}`},
		{"path", `{"x.$":"$.a.b"}`, `{"a":{"b":7}}`, `{"x":7}`},
		{"nested and array", `{"o":{"y.$":"$.v"},"l":[{"z.$":"$.v"}]}`, `{"v":"s"}`, `{"l":[{"z":"s"}],"o":{"y":"s"}}`},
		{"context", `{"id.$":"$$.Execution.Id","st.$":"$$.State.Name","t.$":"$$.Task.Token"}`, `{}`, `{"id":"exec-1","st":"S","t":"tok"}`},
		{"format", `{"m.$":"States.Format('hi {} and {}', $.a, $.n)"}`, `{"a":"x","n":3}`, `{"m":"hi x and 3"}`},
		{"format escapes", `{"m.$":"States.Format('\\{ {}\\}', $.a)"}`, `{"a":"x"}`, `{"m":"{ x}"}`},
		{"format object", `{"m.$":"States.Format('{}', $.o)"}`, `{"o":{"k":1}}`, `{"m":"{\"k\":1}"}`},
		{"string to json", `{"v.$":"States.StringToJson($.s)"}`, `{"s":"{\"a\":[1]}"}`, `{"v":{"a":[1]}}`},
		{"json to string", `{"v.$":"States.JsonToString($.o)"}`, `{"o":{"a":1}}`, `{"v":"{\"a\":1}"}`},
		{"array", `{"v.$":"States.Array($.a, 'b', 3, true, null)"}`, `{"a":1}`, `{"v":[1,"b",3,true,null]}`},
		{"nested call", `{"v.$":"States.JsonToString(States.Array($.a))"}`, `{"a":1}`, `{"v":"[1]"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evalTemplate(t, tt.tmpl, tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("got %s, want %s", got, tt.want)
			}
		})
	}
}

func TestTemplateUUID(t *testing.T) {
	got, err := evalTemplate(t, `{"u.$":"States.UUID()"}`, `{}`)
	if err != nil || len(got) != len(`{"u":"00000000-0000-0000-0000-000000000000"}`) {
		t.Errorf("got %s, %v", got, err)
	}
}

func TestTemplateErrors(t *testing.T) {
	for name, tmpl := range map[string]string{
		"not a string":     `{"a.$":1}`,
		"not a path":       `{"a.$":"nope"}`,
		"unknown function": `{"a.$":"States.MathAdd(1,2)"}`,
		"wildcard path":    `{"a.$":"$.x[*]"}`,
		"arity":            `{"a.$":"States.UUID(1)"}`,
		"unclosed":         `{"a.$":"States.Array(1"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := compileTemplate(mustJSON(t, tmpl)); err == nil {
				t.Error("expected a compile error")
			}
		})
	}
	if _, err := evalTemplate(t, `{"a.$":"$.missing"}`, `{}`); err == nil || !strings.Contains(err.Error(), "States.Runtime") {
		t.Errorf("missing path error = %v", err)
	}
}

func TestPipeline(t *testing.T) {
	tests := []struct {
		name  string
		state string
		input string
		want  string
	}{
		{"result replaces input", `{"Type":"Pass","Result":{"r":1},"End":true}`, `{"a":1}`, `{"r":1}`},
		{"result path", `{"Type":"Pass","Result":"R","ResultPath":"$.out","End":true}`, `{"a":1}`, `{"a":1,"out":"R"}`},
		{"result path null", `{"Type":"Pass","Result":"R","ResultPath":null,"End":true}`, `{"a":1}`, `{"a":1}`},
		{"input path then output path", `{"Type":"Pass","InputPath":"$.x","OutputPath":"$.k","End":true}`, `{"x":{"k":[1]},"y":2}`, `[1]`},
		{"parameters from input path", `{"Type":"Pass","InputPath":"$.x","Parameters":{"v.$":"$.k"},"End":true}`, `{"x":{"k":5}}`, `{"v":5}`},
		{"result path merges into raw input", `{"Type":"Pass","InputPath":"$.x","Parameters":{"v.$":"$.k"},"ResultPath":"$.res","End":true}`,
			`{"x":{"k":5},"keep":1}`, `{"keep":1,"res":{"v":5},"x":{"k":5}}`},
		{"output path after result path", `{"Type":"Pass","Result":9,"ResultPath":"$.n","OutputPath":"$.n","End":true}`, `{}`, `9`},
		{"null input path", `{"Type":"Pass","InputPath":null,"End":true}`, `{"a":1}`, `{}`},
		{"null output path", `{"Type":"Pass","OutputPath":null,"End":true}`, `{"a":1}`, `{}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def := `{"StartAt":"S","States":{"S":` + tt.state + `}}`
			d, err := parseDefinition(def, func(string) bool { return true })
			if err != nil {
				t.Fatal(err)
			}
			e := &execution{m: &machine{def: d, name: "m"}, name: "e"}
			out, err := e.runPass(d.states["S"], mustJSON(t, tt.input), time.Unix(0, 0))
			if err != nil {
				t.Fatal(err)
			}
			if got := encodeJSON(out.output); got != tt.want {
				t.Errorf("got %s, want %s", got, tt.want)
			}
		})
	}
}
