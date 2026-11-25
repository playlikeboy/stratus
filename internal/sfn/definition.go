package sfn

import (
	"fmt"
	"sort"
	"time"
)

// Definition is a compiled state machine definition.
type Definition struct {
	startAt string
	states  map[string]*state
	timeout time.Duration // 0 means no limit
}

var topLevelFields = []string{"Comment", "StartAt", "States", "TimeoutSeconds", "Version", "QueryLanguage"}

// parseDefinition compiles and validates an Amazon States Language document.
// hasIntegration tells whether a Task resource has a registered integration.
func parseDefinition(text string, hasIntegration func(string) bool) (*Definition, error) {
	v, err := decodeJSON(text)
	if err != nil {
		return nil, fmt.Errorf("the definition is not valid JSON: %w", err)
	}
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("the definition must be a JSON object")
	}
	if err := checkQueryLanguage(m); err != nil {
		return nil, err
	}
	for k := range m {
		if !contains(topLevelFields, k) {
			return nil, fmt.Errorf("the definition has an unsupported or unknown field %s", k)
		}
	}
	d := &Definition{states: map[string]*state{}}
	if d.startAt, ok = m["StartAt"].(string); !ok || d.startAt == "" {
		return nil, fmt.Errorf("the definition needs a StartAt")
	}
	if t, has := m["TimeoutSeconds"]; has {
		n, isNum := toFloat(t)
		if !isNum || n <= 0 {
			return nil, fmt.Errorf("TimeoutSeconds must be a positive number")
		}
		d.timeout = time.Duration(n * float64(time.Second))
	}
	raw, ok := m["States"].(map[string]any)
	if !ok || len(raw) == 0 {
		return nil, fmt.Errorf("the definition needs a non-empty States object")
	}
	for _, name := range sortedKeys(raw) {
		st, err := parseState(name, raw[name])
		if err != nil {
			return nil, fmt.Errorf("state %q: %w", name, err)
		}
		d.states[name] = st
	}
	if err := d.validateGraph(); err != nil {
		return nil, err
	}
	return d, d.checkIntegrations(hasIntegration)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// validateGraph checks that StartAt and every transition target exist and
// that every state can be reached.
func (d *Definition) validateGraph() error {
	if _, ok := d.states[d.startAt]; !ok {
		return fmt.Errorf("StartAt %q is not a state of the machine", d.startAt)
	}
	for _, name := range sortedKeys(d.states) {
		for _, t := range d.states[name].targets() {
			if _, ok := d.states[t]; !ok {
				return fmt.Errorf("state %q transitions to %q, which is not a state of the machine", name, t)
			}
		}
	}
	seen := map[string]bool{d.startAt: true}
	queue := []string{d.startAt}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		for _, t := range d.states[name].targets() {
			if !seen[t] {
				seen[t] = true
				queue = append(queue, t)
			}
		}
	}
	for _, name := range sortedKeys(d.states) {
		if !seen[name] {
			return fmt.Errorf("state %q is not reachable from StartAt", name)
		}
	}
	return nil
}

// checkIntegrations names the first Task resource nothing is registered for.
func (d *Definition) checkIntegrations(has func(string) bool) error {
	for _, name := range sortedKeys(d.states) {
		st := d.states[name]
		if st.typ == "Task" && !has(st.baseResource) {
			return fmt.Errorf("state %q: no integration is registered for the Task resource %q", name, st.baseResource)
		}
	}
	return nil
}
