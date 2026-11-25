package sfn

import (
	"strings"
	"time"
)

const tokenPrefix = "stratus-task-token:"

// taskWait is a task that waits for SendTaskSuccess or SendTaskFailure.
// Its fields are guarded by Service.mu.
type taskWait struct {
	token    string
	exec     *execution
	notify   chan struct{} // poked by a completion or a heartbeat
	outcome  *taskOutcome
	lastBeat time.Time
	timedOut bool
}

type taskOutcome struct {
	output any
	err    *IntegrationError
}

func (s *Service) newTask(e *execution) *taskWait {
	s.mu.Lock()
	defer s.mu.Unlock()
	tk := &taskWait{token: newTokenValue(), exec: e, notify: make(chan struct{}, 1), lastBeat: s.clock.Now()}
	s.tasks[tk.token] = tk
	return tk
}

// releaseTask retires a task whose attempt is over. A task that never got a
// result stays known as timed out, so a late SendTask* reports TaskTimedOut.
func (s *Service) releaseTask(tk *taskWait) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tk.outcome != nil {
		delete(s.tasks, tk.token)
		return
	}
	tk.timedOut = true
}

// awaitToken waits for the task's result, its timeout, or a missed heartbeat.
func (e *execution) awaitToken(st *state, tk *taskWait, timeout, heartbeat time.Duration) (any, error) {
	var timeoutAt time.Time
	if timeout > 0 {
		timeoutAt = e.s.clock.Now().Add(timeout)
	}
	for {
		e.s.mu.Lock()
		out, last := tk.outcome, tk.lastBeat
		e.s.mu.Unlock()
		if out != nil {
			if out.err != nil {
				return nil, e.taskFailed(st, out.err)
			}
			return out.output, nil
		}
		now := e.s.clock.Now()
		deadline := timeoutAt
		if heartbeat > 0 && (deadline.IsZero() || last.Add(heartbeat).Before(deadline)) {
			deadline = last.Add(heartbeat)
		}
		if !deadline.IsZero() && !now.Before(deadline) {
			return nil, e.taskFailed(st, &IntegrationError{Name: errTimeout, Cause: "The task timed out."})
		}
		if err := e.block(deadline, tk.notify); err != nil {
			return nil, err
		}
	}
}

func poke(tk *taskWait) {
	select {
	case tk.notify <- struct{}{}:
	default:
	}
}

// findTaskLocked resolves a token: InvalidToken for a malformed one,
// TaskDoesNotExist for an unknown or finished one, TaskTimedOut for one that
// timed out or whose execution ended.
func (s *Service) findTaskLocked(token string) (*taskWait, error) {
	if !strings.HasPrefix(token, tokenPrefix) {
		return nil, errInvalidToken()
	}
	tk, ok := s.tasks[token]
	switch {
	case !ok:
		return nil, errTaskNotFound()
	case tk.timedOut:
		return nil, errTaskTimedOut()
	}
	return tk, nil
}

func (s *Service) sendTaskSuccess(token, output string) error {
	v, err := decodeJSON(output)
	if err != nil {
		return errInvalidOutput(err.Error())
	}
	return s.completeTask(token, &taskOutcome{output: v})
}

func (s *Service) sendTaskFailure(token, errName, cause string) error {
	return s.completeTask(token, &taskOutcome{err: &IntegrationError{Name: errName, Cause: cause}})
}

func (s *Service) completeTask(token string, out *taskOutcome) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tk, err := s.findTaskLocked(token)
	if err != nil {
		return err
	}
	if tk.outcome == nil {
		tk.outcome = out
	}
	poke(tk)
	return nil
}

func (s *Service) sendTaskHeartbeat(token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tk, err := s.findTaskLocked(token)
	if err != nil {
		return err
	}
	tk.lastBeat = s.clock.Now()
	poke(tk)
	return nil
}
