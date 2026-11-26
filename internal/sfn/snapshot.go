package sfn

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// snapshotFormat is the version of what Snapshot writes. Restore reads only
// this one: a state written by another version is refused, never half read.
const snapshotFormat = 1

type snapshot struct {
	Format     int                 `json:"format"`
	Seq        int64               `json:"seq"`
	Machines   []machineSnapshot   `json:"machines"`
	Executions []executionSnapshot `json:"executions"`
}

// machineSnapshot is one version of a state machine.
type machineSnapshot struct {
	Name       string    `json:"name"`
	Definition string    `json:"definition"`
	RoleARN    string    `json:"roleArn,omitempty"`
	Type       string    `json:"type"`
	Created    time.Time `json:"created"`
	Updated    time.Time `json:"updated"`
}

type executionSnapshot struct {
	ARN  string `json:"arn"`
	Name string `json:"name"`
	// Machine is the version the execution started with, which an update of
	// the machine since then does not change.
	Machine  machineSnapshot `json:"machine"`
	Input    string          `json:"input"`
	Started  time.Time       `json:"started"`
	Deadline time.Time       `json:"deadline"`
	Seq      int64           `json:"seq"`
	Status   string          `json:"status"`
	Stopped  time.Time       `json:"stopped"`
	Output   string          `json:"output,omitempty"`
	Error    string          `json:"error,omitempty"`
	Cause    string          `json:"cause,omitempty"`
	History  []eventSnapshot `json:"history"`
	// State, StateInput and Entered are where a running execution is, for
	// Restore to resume it there.
	State      string    `json:"state,omitempty"`
	StateInput string    `json:"stateInput,omitempty"`
	Entered    time.Time `json:"entered"`
}

type eventSnapshot struct {
	ID      int64          `json:"id"`
	Type    string         `json:"type"`
	At      time.Time      `json:"at"`
	Key     string         `json:"key,omitempty"`
	Details map[string]any `json:"details,omitempty"`
}

func snapshotMachine(m *machine) machineSnapshot {
	return machineSnapshot{Name: m.name, Definition: m.definition, RoleARN: m.roleARN, Type: m.typ,
		Created: m.created, Updated: m.updated}
}

// Snapshot returns the state machines and their executions. A running
// execution is recorded at the state it is in; a task waiting for a token
// is not kept, so a resumed Task state calls its integration again.
// keep says which state machines, by name, to include with their
// executions; nil includes every one.
func (s *Service) Snapshot(keep func(name string) bool) ([]byte, error) {
	kept := func(name string) bool { return keep == nil || keep(name) }
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := snapshot{Format: snapshotFormat, Seq: s.seq}
	for _, m := range s.machines {
		if !kept(m.name) {
			continue
		}
		snap.Machines = append(snap.Machines, snapshotMachine(m))
	}
	sort.Slice(snap.Machines, func(i, j int) bool { return snap.Machines[i].Name < snap.Machines[j].Name })
	for _, e := range s.executions {
		if !kept(e.m.name) {
			continue
		}
		es := executionSnapshot{
			ARN: e.arn, Name: e.name, Machine: snapshotMachine(e.m), Input: e.rawInput, Started: e.started,
			Deadline: e.deadline, Seq: e.seq, Status: e.status, Stopped: e.stopped, Output: e.output,
			Error: e.errName, Cause: e.cause,
		}
		for _, ev := range e.history {
			es.History = append(es.History, eventSnapshot{ID: ev.id, Type: ev.typ, At: ev.at, Key: ev.key, Details: ev.details})
		}
		if e.status == statusRunning && e.at.state != "" {
			es.State, es.StateInput, es.Entered = e.at.state, encodeJSON(e.at.input), e.at.entered
		}
		snap.Executions = append(snap.Executions, es)
	}
	sort.Slice(snap.Executions, func(i, j int) bool { return snap.Executions[i].Seq < snap.Executions[j].Seq })
	return json.Marshal(snap)
}

// resumption is a restored running execution and where it goes on from.
type resumption struct {
	e       *execution
	state   string
	input   any
	entered time.Time
}

// Restore replaces every machine and execution with those of a snapshot and
// resumes the running executions where they were. The integrations their
// Task states call must be registered before.
// keep says which resources of the snapshot, by name, to bring back; nil brings every one.
func (s *Service) Restore(data []byte, keep func(name string) bool) error {
	kept := func(name string) bool { return keep == nil || keep(name) }
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("reading the Step Functions state: %w", err)
	}
	if snap.Format != snapshotFormat {
		return fmt.Errorf("the Step Functions state is in format %d; this stratus reads format %d", snap.Format, snapshotFormat)
	}
	machines := make(map[string]*machine, len(snap.Machines))
	for _, ms := range snap.Machines {
		if !kept(ms.Name) {
			continue
		}
		m, err := s.restoreMachine(ms)
		if err != nil {
			return err
		}
		machines[ms.Name] = m
	}
	executions := make(map[string]*execution, len(snap.Executions))
	var resume []resumption
	for _, es := range snap.Executions {
		if !kept(es.Machine.Name) {
			continue
		}
		e, r, err := s.restoreExecution(es, machines)
		if err != nil {
			return fmt.Errorf("execution %s: %w", es.ARN, err)
		}
		executions[es.ARN] = e
		if r != nil {
			resume = append(resume, *r)
		}
	}

	s.mu.Lock()
	done := s.cancelRunningLocked(func(*execution) bool { return true })
	s.machines, s.executions, s.tasks, s.seq = machines, executions, map[string]*taskWait{}, snap.Seq
	s.mu.Unlock()
	waitAll(done)
	for _, r := range resume {
		go r.e.runFrom(r.state, r.input, r.entered)
	}
	return nil
}

func (s *Service) restoreMachine(ms machineSnapshot) (*machine, error) {
	def, err := s.compile(ms.Definition)
	if err != nil {
		return nil, fmt.Errorf("state machine %q: %w", ms.Name, err)
	}
	return &machine{name: ms.Name, arn: s.StateMachineARN(ms.Name), definition: ms.Definition,
		roleARN: ms.RoleARN, typ: ms.Type, def: def, created: ms.Created, updated: ms.Updated}, nil
}

func (s *Service) restoreExecution(es executionSnapshot, machines map[string]*machine) (*execution, *resumption, error) {
	m := machines[es.Machine.Name]
	if m == nil || m.definition != es.Machine.Definition || !m.updated.Equal(es.Machine.Updated) {
		version, err := s.restoreMachine(es.Machine)
		if err != nil {
			return nil, nil, err
		}
		m = version
	}
	input, err := decodeJSON(es.Input)
	if err != nil {
		return nil, nil, fmt.Errorf("reading its input: %w", err)
	}
	ctx, cancel := newExecContext()
	e := &execution{
		s: s, m: m, arn: es.ARN, name: es.Name, rawInput: es.Input, input: input, started: es.Started,
		deadline: es.Deadline, seq: es.Seq, ctx: ctx, cancel: cancel, done: make(chan struct{}),
		status: es.Status, stopped: es.Stopped, output: es.Output, errName: es.Error, cause: es.Cause,
	}
	for _, ev := range es.History {
		e.history = append(e.history, event{id: ev.ID, typ: ev.Type, at: ev.At, key: ev.Key, details: ev.Details})
	}
	if e.status != statusRunning {
		cancel()
		close(e.done)
		return e, nil, nil
	}
	if es.State == "" {
		return e, &resumption{e: e, state: m.def.startAt, input: input}, nil
	}
	if _, ok := m.def.states[es.State]; !ok {
		return nil, nil, fmt.Errorf("it is in state %q, which its definition does not have", es.State)
	}
	stateInput, err := decodeJSON(es.StateInput)
	if err != nil {
		return nil, nil, fmt.Errorf("reading the input of its state: %w", err)
	}
	e.at = position{state: es.State, input: stateInput, entered: es.Entered}
	return e, &resumption{e: e, state: es.State, input: stateInput, entered: es.Entered}, nil
}
