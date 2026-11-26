package sfn

import (
	"cmp"
	"slices"
	"strings"
	"time"
)

// MachineSummary is what an overview of the emulator shows of a state machine.
type MachineSummary struct {
	Name      string
	Type      string
	Running   int
	Succeeded int
	Failed    int // failed, timed out or aborted
}

// StateMachines summarises every state machine and its executions, by name.
func (s *Service) StateMachines() []MachineSummary {
	s.mu.Lock()
	defer s.mu.Unlock()
	byName := make(map[string]*MachineSummary, len(s.machines))
	out := make([]MachineSummary, 0, len(s.machines))
	for _, m := range s.machines {
		out = append(out, MachineSummary{Name: m.name, Type: m.typ})
	}
	for i := range out {
		byName[out[i].Name] = &out[i]
	}
	for _, e := range s.executions {
		sum, ok := byName[e.m.name]
		if !ok {
			continue
		}
		switch e.status {
		case statusRunning:
			sum.Running++
		case statusSucceeded:
			sum.Succeeded++
		default:
			sum.Failed++
		}
	}
	slices.SortFunc(out, func(a, b MachineSummary) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// ExecutionView is an execution as a look at its state machine shows it.
type ExecutionView struct {
	Name, Status  string
	Started       time.Time
	Stopped       time.Time // zero while running
	Input, Output string
	Error, Cause  string
}

// MachineDetail returns a state machine's definition and its executions,
// newest first, up to max.
func (s *Service) MachineDetail(name string, max int) (definition string, executions []ExecutionView, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.machines[name]
	if !ok {
		return "", nil, errMachineNotFound(s.StateMachineARN(name))
	}
	var all []*execution
	for _, e := range s.executions {
		if e.m.name == name {
			all = append(all, e)
		}
	}
	slices.SortFunc(all, func(a, b *execution) int {
		if c := b.started.Compare(a.started); c != 0 {
			return c
		}
		return cmp.Compare(b.seq, a.seq)
	})
	for _, e := range all[:min(len(all), max)] {
		executions = append(executions, ExecutionView{
			Name: e.name, Status: e.status, Started: e.started, Stopped: e.stopped,
			Input: e.rawInput, Output: e.output, Error: e.errName, Cause: e.cause,
		})
	}
	return m.definition, executions, nil
}
