package sfn

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// optRef is an InputPath or OutputPath: absent means "$", null means the
// state sees (or emits) an empty object.
type optRef struct {
	set, null bool
	r         ref
}

// resultPath says where a state's result goes in its input: absent means "$"
// (replace the input), null discards the result.
type resultPath struct {
	set, null bool
	p         path
}

// state is one compiled state of a machine.
type state struct {
	name, typ  string
	next       string
	end        bool
	inputPath  optRef
	outputPath optRef
	resultPath resultPath

	parameters     *template
	resultSelector *template
	result         any
	hasResult      bool

	// Wait
	seconds       *float64
	secondsPath   *ref
	timestamp     *time.Time
	timestampPath *ref

	// Choice
	choices []*rule
	def     string

	// Fail
	errName, cause   string
	errPath, causePt *ref

	// Task
	resource, baseResource string
	waitToken              bool
	retry                  []*retrier
	catch                  []*catcher
	timeout, heartbeat     *float64
	timeoutPath, hbPath    *ref
}

// targets lists every state this state can move to, for graph validation.
func (s *state) targets() []string {
	var out []string
	if s.next != "" {
		out = append(out, s.next)
	}
	for _, c := range s.choices {
		out = append(out, c.next)
	}
	if s.def != "" {
		out = append(out, s.def)
	}
	for _, c := range s.catch {
		out = append(out, c.next)
	}
	return out
}

// common fields every state type may carry.
var commonFields = []string{"Type", "Comment"}

// fieldsByType lists the fields a state of each type may have.
var fieldsByType = map[string][]string{
	"Pass":    {"Next", "End", "InputPath", "OutputPath", "ResultPath", "Parameters", "Result"},
	"Wait":    {"Next", "End", "InputPath", "OutputPath", "Seconds", "SecondsPath", "Timestamp", "TimestampPath"},
	"Choice":  {"Choices", "Default", "InputPath", "OutputPath"},
	"Succeed": {"InputPath", "OutputPath"},
	"Fail":    {"Error", "Cause", "ErrorPath", "CausePath"},
	"Task": {"Resource", "Next", "End", "InputPath", "OutputPath", "ResultPath", "Parameters", "ResultSelector",
		"Retry", "Catch", "TimeoutSeconds", "TimeoutSecondsPath", "HeartbeatSeconds", "HeartbeatSecondsPath"},
}

// onlyFields rejects any key of m that is not allowed, sorted so the error is
// stable.
func onlyFields(m map[string]any, what string, allowed ...string) error {
	var bad []string
	for k := range m {
		if !contains(allowed, k) && !contains(commonFields, k) {
			bad = append(bad, k)
		}
	}
	if len(bad) == 0 {
		return nil
	}
	sort.Strings(bad)
	return fmt.Errorf("%s has unsupported or unknown fields: %s", what, strings.Join(bad, ", "))
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// unsupportedStateFields are JSONata and variable features.
var unsupportedStateFields = []string{"Assign", "Output", "Arguments", "Credentials"}

func parseState(name string, v any) (*state, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("must be an object")
	}
	typ, _ := m["Type"].(string)
	switch typ {
	case "":
		return nil, fmt.Errorf("has no Type")
	case "Parallel", "Map":
		return nil, fmt.Errorf("the %s state type is not supported by stratus", typ)
	}
	allowed, known := fieldsByType[typ]
	if !known {
		return nil, fmt.Errorf("has an unknown state type %q", typ)
	}
	for _, f := range unsupportedStateFields {
		if _, has := m[f]; has {
			return nil, fmt.Errorf("the %s field is not supported by stratus (JSONata and variables are out of scope)", f)
		}
	}
	if err := checkQueryLanguage(m); err != nil {
		return nil, err
	}
	if err := onlyFields(m, "the state", append(allowed, "QueryLanguage")...); err != nil {
		return nil, err
	}
	s := &state{name: name, typ: typ}
	if err := s.parseCommon(m); err != nil {
		return nil, err
	}
	var err error
	switch typ {
	case "Pass":
		err = s.parsePass(m)
	case "Wait":
		err = s.parseWait(m)
	case "Choice":
		err = s.parseChoice(m)
	case "Fail":
		err = s.parseFail(m)
	case "Task":
		err = s.parseTask(m)
	}
	if err != nil {
		return nil, err
	}
	return s, s.checkTermination(m)
}

func checkQueryLanguage(m map[string]any) error {
	if ql, has := m["QueryLanguage"]; has && ql != "JSONPath" {
		return fmt.Errorf("QueryLanguage %s is not supported by stratus: only JSONPath is", encodeJSON(ql))
	}
	return nil
}

// checkTermination makes sure a state that continues has exactly one of Next
// and End, and that terminal types have neither.
func (s *state) checkTermination(m map[string]any) error {
	switch s.typ {
	case "Choice", "Succeed", "Fail":
		return nil
	}
	if _, hasEnd := m["End"]; hasEnd && !s.end {
		return fmt.Errorf("End must be true when present")
	}
	if (s.next != "") == s.end {
		return fmt.Errorf("must have exactly one of Next and \"End\": true (terminal states must be marked)")
	}
	return nil
}

func (s *state) parseCommon(m map[string]any) error {
	var err error
	if s.inputPath, err = parseOptRef(m, "InputPath"); err != nil {
		return err
	}
	if s.outputPath, err = parseOptRef(m, "OutputPath"); err != nil {
		return err
	}
	if s.resultPath, err = parseResultPath(m); err != nil {
		return err
	}
	if v, has := m["Next"]; has {
		next, ok := v.(string)
		if !ok || next == "" {
			return fmt.Errorf("Next must be a state name")
		}
		s.next = next
	}
	if v, has := m["End"]; has {
		s.end, _ = v.(bool)
	}
	if v, has := m["Parameters"]; has {
		if s.parameters, err = compileTemplate(v); err != nil {
			return fmt.Errorf("Parameters: %w", err)
		}
	}
	if v, has := m["ResultSelector"]; has {
		if s.resultSelector, err = compileTemplate(v); err != nil {
			return fmt.Errorf("ResultSelector: %w", err)
		}
	}
	return nil
}

func parseOptRef(m map[string]any, key string) (optRef, error) {
	v, has := m[key]
	if !has {
		return optRef{}, nil
	}
	if v == nil {
		return optRef{set: true, null: true}, nil
	}
	s, ok := v.(string)
	if !ok {
		return optRef{}, fmt.Errorf("%s must be a path string or null", key)
	}
	r, err := parseRef(s)
	if err != nil {
		return optRef{}, fmt.Errorf("%s: %w", key, err)
	}
	return optRef{set: true, r: r}, nil
}

func parseResultPath(m map[string]any) (resultPath, error) {
	v, has := m["ResultPath"]
	if !has {
		return resultPath{}, nil
	}
	if v == nil {
		return resultPath{set: true, null: true}, nil
	}
	s, ok := v.(string)
	if !ok {
		return resultPath{}, fmt.Errorf("ResultPath must be a path string or null")
	}
	p, err := parsePath(s)
	if err != nil {
		return resultPath{}, fmt.Errorf("ResultPath: %w", err)
	}
	if !p.onlyKeys() {
		return resultPath{}, fmt.Errorf("ResultPath %q: array indexes are not allowed in a ResultPath", s)
	}
	return resultPath{set: true, p: p}, nil
}

func (s *state) parsePass(m map[string]any) error {
	if v, has := m["Result"]; has {
		s.result, s.hasResult = v, true
	}
	return nil
}
