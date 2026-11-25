package sfn

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"time"
)

// retrier is one Retry entry of a Task.
type retrier struct {
	errors      []string
	interval    float64
	maxAttempts int
	backoff     float64
	maxDelay    float64 // 0 means no cap
	fullJitter  bool
}

// catcher is one Catch entry of a Task.
type catcher struct {
	errors     []string
	next       string
	resultPath resultPath
}

// errorMatches tells whether an ErrorEquals list covers the error called
// name. States.ALL covers every error but States.Runtime, and
// States.TaskFailed covers every error a task raised, but not a timeout.
func errorMatches(list []string, name string) bool {
	for _, want := range list {
		switch {
		case want == name:
			return true
		case want == errAll && name != errRuntime:
			return true
		case want == errTaskFailed && name != errTimeout && !strings.HasPrefix(name, "States."):
			return true
		}
	}
	return false
}

// delay is the wait before retry number attempt (0 for the first retry).
func (r *retrier) delay(attempt int) time.Duration {
	secs := r.interval * math.Pow(r.backoff, float64(attempt))
	if r.maxDelay > 0 {
		secs = math.Min(secs, r.maxDelay)
	}
	if r.fullJitter {
		secs *= rand.Float64() //nolint:gosec // jitter, not security
	}
	return time.Duration(secs * float64(time.Second))
}

// nextRetry finds the retrier that covers the error. ok is false when none
// does, or when the covering one has used up its attempts; counts holds the
// retries made so far per retrier.
func nextRetry(rs []*retrier, counts []int, name string) (idx int, wait time.Duration, ok bool) {
	for i, r := range rs {
		if !errorMatches(r.errors, name) {
			continue
		}
		if counts[i] >= r.maxAttempts {
			return 0, 0, false
		}
		return i, r.delay(counts[i]), true
	}
	return 0, 0, false
}

func matchCatch(cs []*catcher, name string) *catcher {
	for _, c := range cs {
		if errorMatches(c.errors, name) {
			return c
		}
	}
	return nil
}

// errorOutput is the standard error output a Catch hands to its next state.
func errorOutput(ie *IntegrationError) map[string]any {
	return map[string]any{"Error": ie.Name, "Cause": ie.Cause}
}

func parseErrorEquals(m map[string]any, what string) ([]string, error) {
	list, ok := m["ErrorEquals"].([]any)
	if !ok || len(list) == 0 {
		return nil, fmt.Errorf("%s needs a non-empty ErrorEquals array", what)
	}
	names := make([]string, len(list))
	for i, v := range list {
		s, isStr := v.(string)
		if !isStr || s == "" {
			return nil, fmt.Errorf("%s: ErrorEquals holds only error names", what)
		}
		names[i] = s
	}
	if slices.Contains(names, errAll) && len(names) > 1 {
		return nil, fmt.Errorf("%s: States.ALL must be the only ErrorEquals entry", what)
	}
	return names, nil
}

func parseRetry(v any) ([]*retrier, error) {
	list, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("Retry must be an array")
	}
	out := make([]*retrier, 0, len(list))
	for i, item := range list {
		what := fmt.Sprintf("Retry[%d]", i)
		r, err := parseRetrier(item, what)
		if err != nil {
			return nil, err
		}
		if slices.Contains(r.errors, errAll) && i != len(list)-1 {
			return nil, fmt.Errorf("%s: States.ALL must be in the last Retry entry", what)
		}
		out = append(out, r)
	}
	return out, nil
}

func parseRetrier(item any, what string) (*retrier, error) {
	m, ok := item.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an object", what)
	}
	if err := onlyFields(m, what, "ErrorEquals", "IntervalSeconds", "MaxAttempts", "BackoffRate", "MaxDelaySeconds", "JitterStrategy"); err != nil {
		return nil, err
	}
	names, err := parseErrorEquals(m, what)
	if err != nil {
		return nil, err
	}
	r := &retrier{errors: names, interval: 1, maxAttempts: 3, backoff: 2}
	for _, f := range []struct {
		key string
		dst *float64
		min float64
	}{{"IntervalSeconds", &r.interval, 0}, {"BackoffRate", &r.backoff, 1}, {"MaxDelaySeconds", &r.maxDelay, 0}} {
		if v, has := m[f.key]; has {
			n, isNum := toFloat(v)
			if !isNum || n < f.min || (f.key != "BackoffRate" && n <= 0) {
				return nil, fmt.Errorf("%s: %s is out of range", what, f.key)
			}
			*f.dst = n
		}
	}
	if v, has := m["MaxAttempts"]; has {
		n, isNum := toFloat(v)
		if !isNum || n < 0 || n != math.Trunc(n) {
			return nil, fmt.Errorf("%s: MaxAttempts must be a non-negative integer", what)
		}
		r.maxAttempts = int(n)
	}
	if v, has := m["JitterStrategy"]; has {
		if v != "FULL" && v != "NONE" {
			return nil, fmt.Errorf("%s: JitterStrategy must be FULL or NONE", what)
		}
		r.fullJitter = v == "FULL"
	}
	return r, nil
}

func parseCatch(v any) ([]*catcher, error) {
	list, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("Catch must be an array")
	}
	out := make([]*catcher, 0, len(list))
	for i, item := range list {
		what := fmt.Sprintf("Catch[%d]", i)
		m, isObj := item.(map[string]any)
		if !isObj {
			return nil, fmt.Errorf("%s must be an object", what)
		}
		if err := onlyFields(m, what, "ErrorEquals", "Next", "ResultPath", "Comment"); err != nil {
			return nil, err
		}
		names, err := parseErrorEquals(m, what)
		if err != nil {
			return nil, err
		}
		next, isStr := m["Next"].(string)
		if !isStr || next == "" {
			return nil, fmt.Errorf("%s needs a Next", what)
		}
		c := &catcher{errors: names, next: next}
		if c.resultPath, err = parseResultPath(m); err != nil {
			return nil, fmt.Errorf("%s: %w", what, err)
		}
		if slices.Contains(names, errAll) && i != len(list)-1 {
			return nil, fmt.Errorf("%s: States.ALL must be in the last Catch entry", what)
		}
		out = append(out, c)
	}
	return out, nil
}
