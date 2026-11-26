package sfn

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

// execution is one run of a state machine. Its fields are guarded by
// Service.mu, except the immutable ones set when it starts.
type execution struct {
	s        *Service
	m        *machine
	arn      string
	name     string
	rawInput string
	input    any
	started  time.Time
	deadline time.Time // zero when the machine has no TimeoutSeconds
	seq      int64
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{} // closed when the goroutine has exited

	status  string
	stopped time.Time
	output  string
	errName string
	cause   string
	history []event
}

// StartExecution starts an execution of the machine with the given ARN and
// returns its ARN. An empty name gets a generated one; an empty input is {}.
// Reusing the name of a running execution with the same input returns that
// execution; any other reuse is ExecutionAlreadyExists.
func (s *Service) StartExecution(machineARN, name, input string) (string, error) {
	if input == "" {
		input = "{}"
	}
	parsed, err := decodeJSON(input)
	if err != nil {
		return "", errInvalidInput(err.Error())
	}
	machineName, err := s.machineName(machineARN)
	if err != nil {
		return "", err
	}
	if name != "" {
		if err := validateMachineName(name); err != nil {
			return "", err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.machines[machineName]
	if !ok {
		return "", errMachineNotFound(machineARN)
	}
	if err := s.checkIntegrationsLocked(m); err != nil {
		return "", err
	}
	if name == "" {
		name = newName()
	}
	arn := s.executionARN(machineName, name)
	if old, exists := s.executions[arn]; exists {
		if old.status == statusRunning && sameJSON(old.rawInput, input) {
			return arn, nil
		}
		return "", errExecutionExists(name)
	}
	e := s.newExecutionLocked(m, arn, name, input, parsed)
	go e.run()
	return arn, nil
}

func newName() string { return awsapi.NewID() }

func (s *Service) checkIntegrationsLocked(m *machine) error {
	for _, name := range sortedKeys(m.def.states) {
		st := m.def.states[name]
		if _, ok := s.integrations[st.baseResource]; st.typ == "Task" && !ok {
			return errInvalidDefinition("state \"" + name + "\": no integration is registered for the Task resource \"" + st.baseResource + "\"")
		}
	}
	return nil
}

func (s *Service) newExecutionLocked(m *machine, arn, name, rawInput string, input any) *execution {
	ctx, cancel := newExecContext()
	s.seq++
	e := &execution{
		s: s, m: m, arn: arn, name: name, rawInput: rawInput, input: input,
		started: s.clock.Now(), seq: s.seq, ctx: ctx, cancel: cancel,
		done: make(chan struct{}), status: statusRunning,
	}
	if m.def.timeout > 0 {
		e.deadline = e.started.Add(m.def.timeout)
	}
	e.addLocked("ExecutionStarted", "executionStartedEventDetails",
		map[string]any{"input": rawInput, "inputDetails": map[string]any{"truncated": false}, "roleArn": m.roleARN})
	s.executions[arn] = e
	return e
}

// closeLocked records the final outcome of a running execution.
func (e *execution) closeLocked(status, output, errName, cause string) {
	e.status, e.output, e.errName, e.cause = status, output, errName, cause
	e.stopped = e.s.clock.Now()
	switch status {
	case statusSucceeded:
		e.addLocked("ExecutionSucceeded", "executionSucceededEventDetails",
			map[string]any{"output": output, "outputDetails": map[string]any{"truncated": false}})
	case statusFailed:
		e.addLocked("ExecutionFailed", "executionFailedEventDetails", map[string]any{"error": errName, "cause": cause})
	case statusTimedOut:
		e.addLocked("ExecutionTimedOut", "executionTimedOutEventDetails", map[string]any{"error": errTimeout})
	default:
		e.addLocked("ExecutionAborted", "executionAbortedEventDetails", map[string]any{"error": errName, "cause": cause})
	}
	for _, tk := range e.s.tasks {
		if tk.exec == e && tk.outcome == nil {
			tk.timedOut = true
		}
	}
}

// finish records the outcome from the execution's own goroutine, unless a
// stop already did.
func (e *execution) finish(status, output, errName, cause string) {
	e.s.mu.Lock()
	defer e.s.mu.Unlock()
	if e.status == statusRunning {
		e.closeLocked(status, output, errName, cause)
	}
}

// stopExecution aborts a running execution; stopping a finished one is a
// no-op that reports its stop date.
func (s *Service) stopExecution(arn, errName, cause string) (time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.lookupExecutionLocked(arn)
	if err != nil {
		return time.Time{}, err
	}
	if e.status == statusRunning {
		e.closeLocked(statusAborted, "", errName, cause)
		e.cancel()
	}
	return e.stopped, nil
}

func (s *Service) lookupExecutionLocked(arn string) (*execution, error) {
	prefix := s.identity.ARN("states", "execution:")
	if !strings.HasPrefix(arn, prefix) {
		return nil, errInvalidArn(arn)
	}
	e, ok := s.executions[arn]
	if !ok {
		return nil, errExecutionNotFound(arn)
	}
	return e, nil
}

// execView is a copy of an execution that is safe to read without the lock.
type execView struct {
	arn, name, machineARN string
	status, input, output string
	errName, cause        string
	started, stopped      time.Time
	history               []event
}

func (e *execution) viewLocked() execView {
	return execView{
		arn: e.arn, name: e.name, machineARN: e.m.arn, status: e.status, input: e.rawInput,
		output: e.output, errName: e.errName, cause: e.cause, started: e.started, stopped: e.stopped,
		history: append([]event(nil), e.history...),
	}
}

func (s *Service) describeExecution(arn string) (execView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.lookupExecutionLocked(arn)
	if err != nil {
		return execView{}, err
	}
	return e.viewLocked(), nil
}

// listExecutions returns the executions of a machine, newest first.
func (s *Service) listExecutions(machineARN, status string) ([]execView, error) {
	name, err := s.machineName(machineARN)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.machines[name]; !ok {
		return nil, errMachineNotFound(machineARN)
	}
	var picked []*execution
	for _, e := range s.executions {
		if e.m.name == name && (status == "" || e.status == status) {
			picked = append(picked, e)
		}
	}
	sort.Slice(picked, func(i, j int) bool { return picked[i].seq > picked[j].seq })
	out := make([]execView, len(picked))
	for i, e := range picked {
		out[i] = e.viewLocked()
		out[i].history = nil
	}
	return out, nil
}
