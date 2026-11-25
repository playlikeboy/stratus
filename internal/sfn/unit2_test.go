package sfn

import (
	"strings"
	"testing"
	"time"
)

func TestChoiceRules(t *testing.T) {
	input := `{"s":"abc","n":5,"f":1.5,"b":true,"ts":"2026-01-02T00:00:00Z","nul":null,"lim":"abd","num":3,"t2":"2026-01-01T00:00:00Z"}`
	tests := []struct {
		name, rule string
		want       bool
	}{
		{"StringEquals", `{"Variable":"$.s","StringEquals":"abc"}`, true},
		{"StringEquals no", `{"Variable":"$.s","StringEquals":"x"}`, false},
		{"StringLessThan", `{"Variable":"$.s","StringLessThan":"abd"}`, true},
		{"StringGreaterThanEquals", `{"Variable":"$.s","StringGreaterThanEquals":"abc"}`, true},
		{"StringEqualsPath", `{"Variable":"$.s","StringEqualsPath":"$.s"}`, true},
		{"StringLessThanPath", `{"Variable":"$.s","StringLessThanPath":"$.lim"}`, true},
		{"StringMatches wildcard", `{"Variable":"$.s","StringMatches":"a*c"}`, true},
		{"StringMatches miss", `{"Variable":"$.s","StringMatches":"b*"}`, false},
		{"StringMatches middle", `{"Variable":"$.s","StringMatches":"*b*"}`, true},
		{"NumericEquals", `{"Variable":"$.n","NumericEquals":5}`, true},
		{"NumericLessThan float", `{"Variable":"$.f","NumericLessThan":2}`, true},
		{"NumericGreaterThanPath", `{"Variable":"$.n","NumericGreaterThanPath":"$.num"}`, true},
		{"NumericLessThanEquals", `{"Variable":"$.n","NumericLessThanEquals":5}`, true},
		{"type mismatch is false", `{"Variable":"$.s","NumericEquals":5}`, false},
		{"BooleanEquals", `{"Variable":"$.b","BooleanEquals":true}`, true},
		{"TimestampGreaterThan", `{"Variable":"$.ts","TimestampGreaterThan":"2026-01-01T00:00:00Z"}`, true},
		{"TimestampLessThanPath", `{"Variable":"$.t2","TimestampLessThanPath":"$.ts"}`, true},
		{"TimestampEquals offset", `{"Variable":"$.ts","TimestampEquals":"2026-01-02T01:00:00+01:00"}`, true},
		{"IsPresent true", `{"Variable":"$.s","IsPresent":true}`, true},
		{"IsPresent missing", `{"Variable":"$.zzz","IsPresent":false}`, true},
		{"IsNull", `{"Variable":"$.nul","IsNull":true}`, true},
		{"IsString", `{"Variable":"$.s","IsString":true}`, true},
		{"IsNumeric false", `{"Variable":"$.s","IsNumeric":true}`, false},
		{"IsBoolean", `{"Variable":"$.b","IsBoolean":true}`, true},
		{"IsTimestamp", `{"Variable":"$.ts","IsTimestamp":true}`, true},
		{"IsTimestamp not", `{"Variable":"$.s","IsTimestamp":false}`, true},
		{"And", `{"And":[{"Variable":"$.n","NumericEquals":5},{"Variable":"$.s","StringEquals":"abc"}]}`, true},
		{"And fails", `{"And":[{"Variable":"$.n","NumericEquals":5},{"Variable":"$.s","StringEquals":"x"}]}`, false},
		{"Or", `{"Or":[{"Variable":"$.n","NumericEquals":1},{"Variable":"$.s","StringEquals":"abc"}]}`, true},
		{"Not", `{"Not":{"Variable":"$.n","NumericEquals":1}}`, true},
		{"nested", `{"Not":{"Or":[{"Variable":"$.n","NumericEquals":1},{"And":[{"Variable":"$.b","BooleanEquals":false}]}]}}`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := parseRule(mustJSON(t, tt.rule), false)
			if err != nil {
				t.Fatal(err)
			}
			got, err := r.eval(&env{input: mustJSON(t, input)})
			if err != nil || got != tt.want {
				t.Errorf("eval = %v, %v; want %v", got, err, tt.want)
			}
		})
	}
}

func TestChoiceRuleErrors(t *testing.T) {
	for name, rule := range map[string]string{
		"no comparison":     `{"Variable":"$.a"}`,
		"two comparisons":   `{"Variable":"$.a","StringEquals":"x","NumericEquals":1}`,
		"unknown operator":  `{"Variable":"$.a","StringContains":"x"}`,
		"bad operand type":  `{"Variable":"$.a","NumericEquals":"x"}`,
		"bad timestamp":     `{"Variable":"$.a","TimestampEquals":"yesterday"}`,
		"boolean ordering":  `{"Variable":"$.a","BooleanLessThan":true}`,
		"empty And":         `{"And":[]}`,
		"empty rule":        `{}`,
		"wildcard variable": `{"Variable":"$.a[*]","StringEquals":"x"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseRule(mustJSON(t, rule), false); err == nil {
				t.Error("expected an error")
			}
		})
	}
	r, _ := parseRule(mustJSON(t, `{"Variable":"$.gone","StringEquals":"x"}`), false)
	if _, err := r.eval(&env{input: map[string]any{}}); err == nil {
		t.Error("a missing variable must be a States.Runtime error")
	}
}

func TestErrorMatches(t *testing.T) {
	tests := []struct {
		list []string
		name string
		want bool
	}{
		{[]string{"Boom"}, "Boom", true},
		{[]string{"Boom"}, "Other", false},
		{[]string{"States.ALL"}, "Anything", true},
		{[]string{"States.ALL"}, "States.Timeout", true},
		{[]string{"States.ALL"}, "States.Runtime", false},
		{[]string{"States.Runtime"}, "States.Runtime", true},
		{[]string{"States.TaskFailed"}, "Custom", true},
		{[]string{"States.TaskFailed"}, "States.Timeout", false},
		{[]string{"States.Timeout"}, "States.Timeout", true},
		{[]string{"A", "B"}, "B", true},
	}
	for _, tt := range tests {
		if got := errorMatches(tt.list, tt.name); got != tt.want {
			t.Errorf("errorMatches(%v, %q) = %v, want %v", tt.list, tt.name, got, tt.want)
		}
	}
}

func TestRetryBackoff(t *testing.T) {
	r := &retrier{interval: 2, backoff: 2, maxAttempts: 4, maxDelay: 10}
	want := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second}
	for i, w := range want {
		if got := r.delay(i); got != w {
			t.Errorf("delay(%d) = %v, want %v", i, got, w)
		}
	}
}

func TestNextRetryExhaustsPerRetrier(t *testing.T) {
	rs := []*retrier{
		{errors: []string{"A"}, interval: 1, backoff: 1, maxAttempts: 1},
		{errors: []string{"States.ALL"}, interval: 5, backoff: 1, maxAttempts: 2},
	}
	counts := []int{0, 0}
	if i, d, ok := nextRetry(rs, counts, "A"); !ok || i != 0 || d != time.Second {
		t.Errorf("first retry = %d %v %v", i, d, ok)
	}
	counts[0] = 1
	if _, _, ok := nextRetry(rs, counts, "A"); ok {
		t.Error("retrier A has used its attempt; the ALL retrier must not take over")
	}
	if i, d, ok := nextRetry(rs, counts, "B"); !ok || i != 1 || d != 5*time.Second {
		t.Errorf("ALL retry = %d %v %v", i, d, ok)
	}
	counts[1] = 2
	if _, _, ok := nextRetry(rs, counts, "B"); ok {
		t.Error("attempts exhausted")
	}
	if matchCatch([]*catcher{{errors: []string{"X"}, next: "n"}}, "Y") != nil {
		t.Error("catch must not match another error")
	}
}

func TestDefinitionValidation(t *testing.T) {
	task := func(extra string) string {
		return `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage"` + extra + `,"End":true}}}`
	}
	tests := []struct {
		name, def, want string
	}{
		{"not json", `{`, "not valid JSON"},
		{"no StartAt", `{"States":{"A":{"Type":"Succeed"}}}`, "StartAt"},
		{"StartAt missing state", `{"StartAt":"X","States":{"A":{"Type":"Succeed"}}}`, `"X" is not a state`},
		{"Next missing", `{"StartAt":"A","States":{"A":{"Type":"Pass","Next":"B"}}}`, `"B"`},
		{"Default missing", `{"StartAt":"A","States":{"A":{"Type":"Choice","Choices":[{"Variable":"$.a","IsNull":true,"Next":"A"}],"Default":"Z"}}}`, `"Z"`},
		{"Choice Next missing", `{"StartAt":"A","States":{"A":{"Type":"Choice","Choices":[{"Variable":"$.a","IsNull":true,"Next":"Q"}]}}}`, `"Q"`},
		{"Catch Next missing", task(`,"Catch":[{"ErrorEquals":["States.ALL"],"Next":"Nope"}]`), `"Nope"`},
		{"unterminated", `{"StartAt":"A","States":{"A":{"Type":"Pass"}}}`, "terminal"},
		{"both Next and End", `{"StartAt":"A","States":{"A":{"Type":"Pass","Next":"A","End":true}}}`, "exactly one"},
		{"unreachable", `{"StartAt":"A","States":{"A":{"Type":"Succeed"},"B":{"Type":"Succeed"}}}`, "not reachable"},
		{"unknown type", `{"StartAt":"A","States":{"A":{"Type":"Dance"}}}`, "unknown state type"},
		{"unknown field", `{"StartAt":"A","States":{"A":{"Type":"Succeed","Bogus":1}}}`, "Bogus"},
		{"Next on Succeed", `{"StartAt":"A","States":{"A":{"Type":"Succeed","Next":"A"}}}`, "Next"},
		{"Parallel", `{"StartAt":"A","States":{"A":{"Type":"Parallel","Branches":[],"End":true}}}`, "Parallel state type is not supported"},
		{"Map", `{"StartAt":"A","States":{"A":{"Type":"Map","End":true}}}`, "Map state type is not supported"},
		{"JSONata", `{"QueryLanguage":"JSONata","StartAt":"A","States":{"A":{"Type":"Succeed"}}}`, "JSONata"},
		{"Assign", `{"StartAt":"A","States":{"A":{"Type":"Pass","Assign":{},"End":true}}}`, "Assign"},
		{"rich path", `{"StartAt":"A","States":{"A":{"Type":"Pass","InputPath":"$.a[*]","End":true}}}`, "unsupported JSONPath"},
		{"ResultPath index", `{"StartAt":"A","States":{"A":{"Type":"Pass","ResultPath":"$.a[0]","End":true}}}`, "array indexes"},
		{"wait two timers", `{"StartAt":"A","States":{"A":{"Type":"Wait","Seconds":1,"SecondsPath":"$.s","End":true}}}`, "exactly one"},
		{"wait bad timestamp", `{"StartAt":"A","States":{"A":{"Type":"Wait","Timestamp":"soon","End":true}}}`, "RFC3339"},
		{"no integration", `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::lambda:invoke","End":true}}}`, "arn:aws:states:::lambda:invoke"},
		{"sync pattern", `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage.sync","End":true}}}`, ".sync"},
		{"ALL not alone", task(`,"Retry":[{"ErrorEquals":["States.ALL","X"]}]`), "States.ALL"},
		{"ALL not last", task(`,"Retry":[{"ErrorEquals":["States.ALL"]},{"ErrorEquals":["X"]}]`), "last"},
		{"bad backoff", task(`,"Retry":[{"ErrorEquals":["X"],"BackoffRate":0.5}]`), "BackoffRate"},
		{"heartbeat without token", task(`,"HeartbeatSeconds":5`), "waitForTaskToken"},
		{"Retry unknown field", task(`,"Retry":[{"ErrorEquals":["X"],"Foo":1}]`), "Foo"},
		{"fail both error forms", `{"StartAt":"F","States":{"F":{"Type":"Fail","Error":"a","ErrorPath":"$.e"}}}`, "mutually exclusive"},
		{"bad intrinsic", `{"StartAt":"A","States":{"A":{"Type":"Pass","Parameters":{"a.$":"States.Nope()"},"End":true}}}`, "States.Nope"},
	}
	has := func(r string) bool {
		return r == "arn:aws:states:::sqs:sendMessage" || strings.HasSuffix(r, "waitForTaskToken")
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseDefinition(tt.def, has)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

func TestDefinitionAccepts(t *testing.T) {
	def := `{
	  "Comment":"all state types","StartAt":"P","TimeoutSeconds":60,"Version":"1.0",
	  "States":{
	    "P":{"Type":"Pass","Result":{"a":1},"Next":"W"},
	    "W":{"Type":"Wait","SecondsPath":"$.a","Next":"C"},
	    "C":{"Type":"Choice","Choices":[{"Variable":"$.a","NumericEquals":1,"Next":"T"}],"Default":"F"},
	    "T":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","TimeoutSeconds":5,
	         "Retry":[{"ErrorEquals":["States.TaskFailed"],"MaxAttempts":0},{"ErrorEquals":["States.ALL"]}],
	         "Catch":[{"ErrorEquals":["States.ALL"],"Next":"F","ResultPath":"$.err"}],"Next":"S"},
	    "S":{"Type":"Succeed"},
	    "F":{"Type":"Fail","Error":"E","Cause":"c"}
	  }}`
	if _, err := parseDefinition(def, func(string) bool { return true }); err != nil {
		t.Fatal(err)
	}
}
