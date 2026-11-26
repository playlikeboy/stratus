package sfn

import (
	"errors"
	"fmt"
)

// ErrTypeMismatch is the error of EnsureStateMachine for a machine of another
// type: Step Functions cannot change it in place.
var ErrTypeMismatch = errors.New("the state machine is of another type")

// EnsureStateMachine creates the machine, or, when one of that name exists,
// as after a restore, gives it the definition when it differs, as an update
// does: its running executions keep the version they started with.
func (s *Service) EnsureStateMachine(name, definition string, opts ...CreateOption) error {
	cfg := createConfig{typ: typeStandard}
	for _, o := range opts {
		o(&cfg)
	}
	s.mu.Lock()
	old, ok := s.machines[name]
	s.mu.Unlock()
	if !ok {
		return s.CreateStateMachine(name, definition, opts...)
	}
	if old.typ != cfg.typ {
		return fmt.Errorf("%w: %s, the manifest declares %s", ErrTypeMismatch, old.typ, cfg.typ)
	}
	if sameJSON(old.definition, definition) && (cfg.roleARN == "" || cfg.roleARN == old.roleARN) {
		return nil
	}
	_, err := s.updateStateMachine(old.arn, definition, cfg.roleARN)
	return err
}

// Definition is the definition of a state machine, empty when it does not exist.
func (s *Service) Definition(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.machines[name]; ok {
		return m.definition
	}
	return ""
}
