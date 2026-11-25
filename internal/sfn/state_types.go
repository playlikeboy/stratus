package sfn

import (
	"fmt"
	"time"
)

func optionalRef(m map[string]any, key string) (*ref, error) {
	v, has := m[key]
	if !has {
		return nil, nil
	}
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("%s must be a path string", key)
	}
	r, err := parseRef(s)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", key, err)
	}
	return &r, nil
}

func optionalSecs(m map[string]any, key string) (*float64, error) {
	v, has := m[key]
	if !has {
		return nil, nil
	}
	n, ok := toFloat(v)
	if !ok || n <= 0 {
		return nil, fmt.Errorf("%s must be a positive number", key)
	}
	return &n, nil
}

func (s *state) parseWait(m map[string]any) error {
	var err error
	given := 0
	for _, k := range []string{"Seconds", "SecondsPath", "Timestamp", "TimestampPath"} {
		if _, has := m[k]; has {
			given++
		}
	}
	if given != 1 {
		return fmt.Errorf("a Wait state needs exactly one of Seconds, SecondsPath, Timestamp and TimestampPath")
	}
	if v, has := m["Seconds"]; has {
		n, ok := toFloat(v)
		if !ok || n < 0 {
			return fmt.Errorf("Seconds must be a non-negative number")
		}
		s.seconds = &n
	}
	if s.secondsPath, err = optionalRef(m, "SecondsPath"); err != nil {
		return err
	}
	if s.timestampPath, err = optionalRef(m, "TimestampPath"); err != nil {
		return err
	}
	if v, has := m["Timestamp"]; has {
		str, _ := v.(string)
		t, perr := time.Parse(time.RFC3339, str)
		if perr != nil {
			return fmt.Errorf("Timestamp %s is not an RFC3339 timestamp", encodeJSON(v))
		}
		s.timestamp = &t
	}
	return nil
}

func (s *state) parseChoice(m map[string]any) error {
	list, ok := m["Choices"].([]any)
	if !ok || len(list) == 0 {
		return fmt.Errorf("a Choice state needs a non-empty Choices array")
	}
	for i, item := range list {
		r, err := parseRule(item, true)
		if err != nil {
			return fmt.Errorf("Choices[%d]: %w", i, err)
		}
		s.choices = append(s.choices, r)
	}
	if v, has := m["Default"]; has {
		d, isStr := v.(string)
		if !isStr || d == "" {
			return fmt.Errorf("Default must be a state name")
		}
		s.def = d
	}
	return nil
}

func (s *state) parseFail(m map[string]any) error {
	var err error
	if s.errPath, err = optionalRef(m, "ErrorPath"); err != nil {
		return err
	}
	if s.causePt, err = optionalRef(m, "CausePath"); err != nil {
		return err
	}
	for _, f := range []struct {
		key  string
		dst  *string
		path *ref
	}{{"Error", &s.errName, s.errPath}, {"Cause", &s.cause, s.causePt}} {
		v, has := m[f.key]
		if has && f.path != nil {
			return fmt.Errorf("%s and %sPath are mutually exclusive", f.key, f.key)
		}
		if has {
			str, ok := v.(string)
			if !ok {
				return fmt.Errorf("%s must be a string", f.key)
			}
			*f.dst = str
		}
	}
	return nil
}

func (s *state) parseTask(m map[string]any) error {
	resource, ok := m["Resource"].(string)
	if !ok || resource == "" {
		return fmt.Errorf("a Task state needs a Resource")
	}
	var err error
	s.resource = resource
	if s.baseResource, s.waitToken, err = splitResource(resource); err != nil {
		return err
	}
	if v, has := m["Retry"]; has {
		if s.retry, err = parseRetry(v); err != nil {
			return err
		}
	}
	if v, has := m["Catch"]; has {
		if s.catch, err = parseCatch(v); err != nil {
			return err
		}
	}
	return s.parseTaskTimers(m)
}

func (s *state) parseTaskTimers(m map[string]any) error {
	var err error
	if s.timeout, err = optionalSecs(m, "TimeoutSeconds"); err != nil {
		return err
	}
	if s.heartbeat, err = optionalSecs(m, "HeartbeatSeconds"); err != nil {
		return err
	}
	if s.timeoutPath, err = optionalRef(m, "TimeoutSecondsPath"); err != nil {
		return err
	}
	if s.hbPath, err = optionalRef(m, "HeartbeatSecondsPath"); err != nil {
		return err
	}
	if (s.timeout != nil && s.timeoutPath != nil) || (s.heartbeat != nil && s.hbPath != nil) {
		return fmt.Errorf("TimeoutSeconds and HeartbeatSeconds are mutually exclusive with their Path forms")
	}
	if (s.heartbeat != nil || s.hbPath != nil) && !s.waitToken {
		return fmt.Errorf("HeartbeatSeconds is supported only with the .waitForTaskToken pattern")
	}
	return nil
}
