package sfn

import (
	"errors"
	"time"
)

// outcome is where a state sends the execution: to next with output, or, with
// end, to the end of the execution.
type outcome struct {
	output any
	next   string
	end    bool
}

// run walks the machine from StartAt until a terminal state, a failure, a
// stop or a reset. It is the body of the execution's goroutine.
func (e *execution) run() {
	defer close(e.done)
	name, input := e.m.def.startAt, e.input
	for {
		if e.ctx.Err() != nil {
			return
		}
		// Checked between states too: a loop that never waits would otherwise
		// outrun its timeout and grow its history without end.
		if !e.deadline.IsZero() && !e.s.clock.Now().Before(e.deadline) {
			e.finish(statusTimedOut, "", "", "")
			return
		}
		if e.historyLen() >= maxHistoryEvents {
			e.finish(statusFailed, "", errRuntime, "The execution reached the maximum number of history events (25000).")
			return
		}
		out, err := e.runState(e.m.def.states[name], input)
		if err != nil {
			e.fail(err)
			return
		}
		if out.end {
			e.finish(statusSucceeded, encodeJSON(out.output), "", "")
			return
		}
		name, input = out.next, out.output
	}
}

// fail records why the execution could not continue. A cancelled execution
// has already been closed by whoever cancelled it.
func (e *execution) fail(err error) {
	var ie *IntegrationError
	switch {
	case errors.Is(err, errCancelled):
	case errors.Is(err, errExecTimeout):
		e.finish(statusTimedOut, "", "", "")
	case errors.As(err, &ie):
		e.finish(statusFailed, "", ie.Name, ie.Cause)
	default:
		e.finish(statusFailed, "", errRuntime, err.Error())
	}
}

func (e *execution) runState(st *state, input any) (outcome, error) {
	entered := e.s.clock.Now()
	e.stateEntered(st, input)
	var (
		out outcome
		err error
	)
	switch st.typ {
	case "Pass":
		out, err = e.runPass(st, input, entered)
	case "Wait":
		out, err = e.runWait(st, input, entered)
	case "Choice":
		out, err = e.runChoice(st, input, entered)
	case "Succeed":
		out, err = e.runSucceed(st, input, entered)
	case "Fail":
		err = e.runFail(st, input, entered)
	case "Task":
		out, err = e.runTask(st, input, entered)
	}
	if err != nil {
		return outcome{}, err
	}
	e.stateExited(st, out.output)
	return out, nil
}

func iso(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }

// newEnv builds what paths read: the state input and the context object.
func (e *execution) newEnv(st *state, input any, entered time.Time, retries int) *env {
	return &env{input: input, ctx: map[string]any{
		"Execution": map[string]any{
			"Id": e.arn, "Name": e.name, "StartTime": iso(e.started), "Input": e.input, "RoleArn": e.m.roleARN,
		},
		"StateMachine": map[string]any{"Id": e.m.arn, "Name": e.m.name},
		"State":        map[string]any{"Name": st.name, "EnteredTime": iso(entered), "RetryCount": retries},
	}}
}

func (st *state) outcome(output any) outcome {
	return outcome{output: output, next: st.next, end: st.end}
}

func (e *execution) runPass(st *state, input any, entered time.Time) (outcome, error) {
	ev := e.newEnv(st, input, entered, 0)
	eff, err := applyInputPath(st.inputPath, input, ev)
	if err != nil {
		return outcome{}, err
	}
	ev.input = eff
	result := eff
	if st.parameters != nil {
		if result, err = st.parameters.eval(ev); err != nil {
			return outcome{}, err
		}
	}
	if st.hasResult {
		result = st.result
	}
	out, err := finishData(st, input, result, ev)
	return st.outcome(out), err
}

func (e *execution) runWait(st *state, input any, entered time.Time) (outcome, error) {
	ev := e.newEnv(st, input, entered, 0)
	eff, err := applyInputPath(st.inputPath, input, ev)
	if err != nil {
		return outcome{}, err
	}
	ev.input = eff
	until, err := waitUntil(st, entered, ev)
	if err != nil {
		return outcome{}, err
	}
	if err := e.sleepUntil(until); err != nil {
		return outcome{}, err
	}
	out, err := applyOutputPath(st.outputPath, eff, ev)
	return st.outcome(out), err
}

// waitUntil is the moment a Wait state ends.
func waitUntil(st *state, entered time.Time, ev *env) (time.Time, error) {
	switch {
	case st.seconds != nil:
		return entered.Add(secondsToDuration(*st.seconds)), nil
	case st.timestamp != nil:
		return *st.timestamp, nil
	case st.secondsPath != nil:
		v, ok := st.secondsPath.eval(ev)
		n, isNum := toFloat(v)
		if !ok || !isNum || n < 0 {
			return time.Time{}, runtimeError("SecondsPath %s must point at a non-negative number", st.secondsPath.p.raw)
		}
		return entered.Add(secondsToDuration(n)), nil
	}
	v, ok := st.timestampPath.eval(ev)
	s, _ := v.(string)
	t, err := time.Parse(time.RFC3339, s)
	if !ok || err != nil {
		return time.Time{}, runtimeError("TimestampPath %s must point at an RFC3339 timestamp", st.timestampPath.p.raw)
	}
	return t, nil
}

func secondsToDuration(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }

func (e *execution) runChoice(st *state, input any, entered time.Time) (outcome, error) {
	ev := e.newEnv(st, input, entered, 0)
	eff, err := applyInputPath(st.inputPath, input, ev)
	if err != nil {
		return outcome{}, err
	}
	ev.input = eff
	next := st.def
	for _, r := range st.choices {
		ok, rerr := r.eval(ev)
		if rerr != nil {
			return outcome{}, rerr
		}
		if ok {
			next = r.next
			break
		}
	}
	if next == "" {
		return outcome{}, &IntegrationError{Name: "States.NoChoiceMatched", Cause: "No Choice rule matched and the state has no Default."}
	}
	out, err := applyOutputPath(st.outputPath, eff, ev)
	return outcome{output: out, next: next}, err
}

func (e *execution) runSucceed(st *state, input any, entered time.Time) (outcome, error) {
	ev := e.newEnv(st, input, entered, 0)
	eff, err := applyInputPath(st.inputPath, input, ev)
	if err != nil {
		return outcome{}, err
	}
	out, err := applyOutputPath(st.outputPath, eff, ev)
	return outcome{output: out, end: true}, err
}

func (e *execution) runFail(st *state, input any, entered time.Time) error {
	ev := e.newEnv(st, input, entered, 0)
	name, cause := st.errName, st.cause
	for _, f := range []struct {
		path *ref
		dst  *string
	}{{st.errPath, &name}, {st.causePt, &cause}} {
		if f.path == nil {
			continue
		}
		v, ok := f.path.eval(ev)
		s, isStr := v.(string)
		if !ok || !isStr {
			return runtimeError("the path %s of a Fail state must point at a string", f.path.p.raw)
		}
		*f.dst = s
	}
	return &IntegrationError{Name: name, Cause: cause}
}
