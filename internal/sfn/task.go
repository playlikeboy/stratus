package sfn

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

// runTask runs a Task state with its Retry and Catch policies.
func (e *execution) runTask(st *state, input any, entered time.Time) (outcome, error) {
	counts := make([]int, len(st.retry))
	retries := 0
	for {
		out, err := e.attemptTask(st, input, entered, retries)
		if err == nil {
			return st.outcome(out), nil
		}
		var ie *IntegrationError
		if !errors.As(err, &ie) {
			return outcome{}, err // cancelled or timed out as a whole
		}
		if i, wait, ok := nextRetry(st.retry, counts, ie.Name); ok {
			counts[i]++
			retries++
			if err := e.sleepUntil(e.s.clock.Now().Add(wait)); err != nil {
				return outcome{}, err
			}
			continue
		}
		if c := matchCatch(st.catch, ie.Name); c != nil {
			return outcome{output: applyResultPath(c.resultPath, input, errorOutput(ie)), next: c.next}, nil
		}
		return outcome{}, ie
	}
}

// attemptTask makes one try of the Task: parameters, the call, and the
// ResultSelector / ResultPath / OutputPath tail.
func (e *execution) attemptTask(st *state, input any, entered time.Time, retries int) (any, error) {
	ev := e.newEnv(st, input, entered, retries)
	eff, err := applyInputPath(st.inputPath, input, ev)
	if err != nil {
		return nil, err
	}
	ev.input = eff
	timeout, heartbeat, err := taskTimers(st, ev)
	if err != nil {
		return nil, err
	}
	var tk *taskWait
	if st.waitToken {
		tk = e.s.newTask(e)
		defer e.s.releaseTask(tk)
		ev.ctx["Task"] = map[string]any{"Token": tk.token}
	}
	params := eff
	if st.parameters != nil {
		if params, err = st.parameters.eval(ev); err != nil {
			return nil, err
		}
	}
	fn, ok := e.s.integration(st.baseResource)
	if !ok {
		return nil, runtimeError("no integration is registered for the Task resource %q", st.baseResource)
	}
	result, err := e.callTask(st, fn, params, tk, timeout, heartbeat)
	if err != nil {
		return nil, err
	}
	return finishData(st, input, result, ev)
}

// taskTimers reads TimeoutSeconds and HeartbeatSeconds, or their Path forms.
func taskTimers(st *state, ev *env) (timeout, heartbeat time.Duration, err error) {
	read := func(fixed *float64, path *ref) (time.Duration, error) {
		if fixed != nil {
			return secondsToDuration(*fixed), nil
		}
		if path == nil {
			return 0, nil
		}
		v, ok := path.eval(ev)
		n, isNum := toFloat(v)
		if !ok || !isNum || n <= 0 {
			return 0, runtimeError("the path %s must point at a positive number of seconds", path.p.raw)
		}
		return secondsToDuration(n), nil
	}
	if timeout, err = read(st.timeout, st.timeoutPath); err != nil {
		return 0, 0, err
	}
	heartbeat, err = read(st.heartbeat, st.hbPath)
	return timeout, heartbeat, err
}

// callTask calls the integration and, for .waitForTaskToken, waits for the
// task to be completed through SendTask*.
func (e *execution) callTask(st *state, fn Integration, params any, tk *taskWait, timeout, heartbeat time.Duration) (any, error) {
	body := json.RawMessage(encodeJSON(params))
	scheduled := map[string]any{
		"resource": st.baseResource, "resourceType": resourceType(st.baseResource),
		"region": e.s.identity.Region, "parameters": string(body),
	}
	if timeout > 0 {
		scheduled["timeoutInSeconds"] = int64(timeout / time.Second)
	}
	e.add("TaskScheduled", "taskScheduledEventDetails", scheduled)

	var (
		out json.RawMessage
		err error
		res any
	)
	if tk != nil {
		out, err = fn(e.ctx, body)
	} else {
		out, err = e.invoke(fn, body, timeout)
	}
	switch {
	case err != nil:
		err = e.taskFailed(st, classify(err))
	case tk != nil:
		res, err = e.awaitToken(st, tk, timeout, heartbeat)
	default:
		res, err = decodeResult(out)
	}
	if err != nil {
		return nil, err
	}
	e.add("TaskSucceeded", "taskSucceededEventDetails", map[string]any{
		"resource": st.baseResource, "resourceType": resourceType(st.baseResource), "output": encodeJSON(res),
		"outputDetails": map[string]any{"truncated": false},
	})
	return res, nil
}

func decodeResult(out json.RawMessage) (any, error) {
	if len(strings.TrimSpace(string(out))) == 0 {
		return nil, nil
	}
	v, err := decodeJSON(string(out))
	if err != nil {
		return nil, &IntegrationError{Name: errTaskFailed, Cause: "the integration returned invalid JSON: " + err.Error()}
	}
	return v, nil
}

// resourceType is the service part of "arn:aws:states:::sqs:sendMessage".
func resourceType(resource string) string {
	rest, ok := strings.CutPrefix(resource, "arn:aws:states:::")
	if !ok {
		return "lambda"
	}
	service, _, _ := strings.Cut(rest, ":")
	return service
}

// taskFailed records a failed task and passes the error on, unless it ends
// the whole execution.
func (e *execution) taskFailed(st *state, err error) error {
	var ie *IntegrationError
	if errors.As(err, &ie) {
		e.add("TaskFailed", "taskFailedEventDetails", map[string]any{
			"resource": st.baseResource, "resourceType": resourceType(st.baseResource),
			"error": ie.Name, "cause": ie.Cause,
		})
	}
	return err
}

// invoke calls fn. With a timeout it runs the call beside the clock and
// cancels it when the clock gets there first.
func (e *execution) invoke(fn Integration, body json.RawMessage, timeout time.Duration) (json.RawMessage, error) {
	if timeout == 0 {
		return fn(e.ctx, body)
	}
	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()
	var (
		out  json.RawMessage
		err  error
		done = make(chan struct{})
	)
	go func() {
		defer close(done)
		out, err = fn(ctx, body)
	}()
	deadline := e.s.clock.Now().Add(timeout)
	for {
		select {
		case <-done:
			return out, err
		default:
		}
		if !e.s.clock.Now().Before(deadline) {
			cancel()
			<-done
			return nil, &IntegrationError{Name: errTimeout, Cause: "The task timed out."}
		}
		if berr := e.block(deadline, done); berr != nil {
			cancel()
			<-done
			return nil, berr
		}
	}
}

// newTokenValue is the opaque task token handed to a .waitForTaskToken task.
func newTokenValue() string { return tokenPrefix + awsapi.NewID() }
