// Package sfn emulates AWS Step Functions: state machines, executions and an
// interpreter for the JSONPath flavour of the Amazon States Language. Task
// states call integrations that the composition root plugs in.
package sfn

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ODudek/stratus/internal/awsapi"
	"github.com/ODudek/stratus/internal/clock"
)

const (
	typeStandard = "STANDARD"
	typeExpress  = "EXPRESS"
)

// Execution statuses.
const (
	statusRunning   = "RUNNING"
	statusSucceeded = "SUCCEEDED"
	statusFailed    = "FAILED"
	statusAborted   = "ABORTED"
	statusTimedOut  = "TIMED_OUT"
)

// Service is the Step Functions service. All state is in memory.
type Service struct {
	identity awsapi.Identity
	clock    clock.Clock

	mu           sync.Mutex
	machines     map[string]*machine
	executions   map[string]*execution
	tasks        map[string]*taskWait
	integrations map[string]Integration
	seq          int64 // orders executions that start in the same millisecond

	wakeMu sync.Mutex
	wakeCh chan struct{}
}

// machine is one state machine version. It is never modified: an update
// replaces it, so a running execution keeps the definition it started with.
type machine struct {
	name, arn  string
	definition string
	roleARN    string
	typ        string
	def        *Definition
	created    time.Time
	updated    time.Time
}

func New(identity awsapi.Identity, c clock.Clock) *Service {
	return &Service{
		identity:     identity,
		clock:        c,
		machines:     map[string]*machine{},
		executions:   map[string]*execution{},
		tasks:        map[string]*taskWait{},
		integrations: map[string]Integration{},
		wakeCh:       make(chan struct{}),
	}
}

func (s *Service) Name() string         { return "states" }
func (s *Service) TargetPrefix() string { return "AWSStepFunctions." }

// Wake makes every waiting execution re-check the clock at once, so a test
// can advance a clock.Fake and call Wake instead of sleeping.
func (s *Service) Wake() {
	s.wakeMu.Lock()
	defer s.wakeMu.Unlock()
	close(s.wakeCh)
	s.wakeCh = make(chan struct{})
}

func (s *Service) wakeChan() <-chan struct{} {
	s.wakeMu.Lock()
	defer s.wakeMu.Unlock()
	return s.wakeCh
}

// Reset stops every running execution, waits for its goroutine to exit and
// drops all state machines and executions. Registered integrations stay.
func (s *Service) Reset() {
	s.mu.Lock()
	done := s.cancelRunningLocked(func(*execution) bool { return true })
	s.mu.Unlock()
	waitAll(done)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.machines = map[string]*machine{}
	s.executions = map[string]*execution{}
	s.tasks = map[string]*taskWait{}
}

// cancelRunningLocked aborts the running executions that match and returns
// their goroutines' done channels, to be waited on once s.mu is released.
func (s *Service) cancelRunningLocked(match func(*execution) bool) []<-chan struct{} {
	var done []<-chan struct{}
	for _, e := range s.executions {
		if e.status != statusRunning || !match(e) {
			continue
		}
		e.closeLocked(statusAborted, "", "States.Aborted", "The state machine was deleted or the emulator was reset.")
		e.cancel()
		done = append(done, e.done)
	}
	return done
}

func waitAll(done []<-chan struct{}) {
	for _, d := range done {
		<-d
	}
}

// CreateOption tunes CreateStateMachine.
type CreateOption func(*createConfig)

type createConfig struct {
	roleARN string
	typ     string
}

// WithRoleARN sets the role ARN reported by DescribeStateMachine.
func WithRoleARN(arn string) CreateOption { return func(c *createConfig) { c.roleARN = arn } }

// WithType sets the machine type, "STANDARD" (the default) or "EXPRESS".
// Both run the same way; stratus does not emulate Express limits.
func WithType(typ string) CreateOption { return func(c *createConfig) { c.typ = typ } }

// StateMachineARN is the ARN of the state machine called name.
func (s *Service) StateMachineARN(name string) string {
	return s.identity.ARN("states", "stateMachine:"+name)
}

func (s *Service) executionARN(machineName, name string) string {
	return s.identity.ARN("states", "execution:"+machineName+":"+name)
}

// CreateStateMachine creates a state machine from an Amazon States Language
// definition. Creating the same name with the same definition and type again
// is a no-op; a different one fails with StateMachineAlreadyExists. A Task
// resource without a registered integration is an InvalidDefinition.
func (s *Service) CreateStateMachine(name, definition string, opts ...CreateOption) error {
	cfg := createConfig{typ: typeStandard}
	for _, o := range opts {
		o(&cfg)
	}
	if err := validateMachineName(name); err != nil {
		return err
	}
	if cfg.typ != typeStandard && cfg.typ != typeExpress {
		return errValidation("Value '" + cfg.typ + "' at 'type' failed to satisfy constraint: it must be STANDARD or EXPRESS")
	}
	def, err := s.compile(definition)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.machines[name]; ok {
		if old.typ == cfg.typ && sameJSON(old.definition, definition) {
			return nil
		}
		return errMachineExists(name)
	}
	now := s.clock.Now()
	s.machines[name] = &machine{
		name: name, arn: s.StateMachineARN(name), definition: definition,
		roleARN: cfg.roleARN, typ: cfg.typ, def: def, created: now, updated: now,
	}
	return nil
}

// compile validates a definition against the integrations registered now.
func (s *Service) compile(definition string) (*Definition, error) {
	s.mu.Lock()
	known := make(map[string]bool, len(s.integrations))
	for r := range s.integrations {
		known[r] = true
	}
	s.mu.Unlock()
	def, err := parseDefinition(definition, func(r string) bool { return known[r] })
	if err != nil {
		return nil, errInvalidDefinition(err.Error())
	}
	return def, nil
}

func (s *Service) updateStateMachine(arn, definition, roleARN string) (time.Time, error) {
	name, err := s.machineName(arn)
	if err != nil {
		return time.Time{}, err
	}
	var def *Definition
	if definition != "" {
		if def, err = s.compile(definition); err != nil {
			return time.Time{}, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.machines[name]
	if !ok {
		return time.Time{}, errMachineNotFound(arn)
	}
	next := *old
	next.updated = s.clock.Now()
	if def != nil {
		next.definition, next.def = definition, def
	}
	if roleARN != "" {
		next.roleARN = roleARN
	}
	s.machines[name] = &next
	return next.updated, nil
}

// DeleteStateMachine removes the machine, aborts its running executions and
// waits for their goroutines to exit.
func (s *Service) deleteStateMachine(arn string) error {
	name, err := s.machineName(arn)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if _, ok := s.machines[name]; !ok {
		s.mu.Unlock()
		return nil // deleting a missing machine succeeds, as in AWS
	}
	delete(s.machines, name)
	done := s.cancelRunningLocked(func(e *execution) bool { return e.m.name == name })
	s.mu.Unlock()
	waitAll(done)
	return nil
}

func (s *Service) machineName(arn string) (string, error) {
	prefix := s.identity.ARN("states", "stateMachine:")
	name, ok := strings.CutPrefix(arn, prefix)
	if !ok || name == "" {
		return "", errInvalidArn(arn)
	}
	return name, nil
}

func (s *Service) describeMachine(arn string) (machine, error) {
	name, err := s.machineName(arn)
	if err != nil {
		return machine{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.machines[name]
	if !ok {
		return machine{}, errMachineNotFound(arn)
	}
	return *m, nil
}

func (s *Service) listMachines() []machine {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]machine, 0, len(s.machines))
	for _, m := range s.machines {
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

const invalidNameChars = "<>{}[]?*\"#%\\^|~$&,;:/ \t\n"

func validateMachineName(name string) error {
	switch {
	case name == "" || len(name) > 80:
		return errInvalidName(name, "length must be between 1 and 80")
	case strings.ContainsAny(name, invalidNameChars):
		return errInvalidName(name, "contains invalid characters")
	}
	return nil
}

// context returns the base context of one execution goroutine.
func newExecContext() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}
