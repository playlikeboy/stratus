package sfn

import (
	"runtime"
	"strings"
	"testing"
)

const longWait = `{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":3600,"End":true}}}`

// runningDone starts n executions of machine name and returns their done channels.
func runningDone(t *testing.T, f *fixture, name string, n int) []<-chan struct{} {
	t.Helper()
	if err := f.svc.CreateStateMachine(name, longWait); err != nil {
		t.Fatal(err)
	}
	var arns []string
	for range n {
		arn, err := f.svc.StartExecution(f.svc.StateMachineARN(name), "", `{}`)
		if err != nil {
			t.Fatal(err)
		}
		arns = append(arns, arn)
	}
	var done []<-chan struct{}
	for _, arn := range arns {
		f.waitEvent(t, arn, "WaitStateEntered")
		f.svc.mu.Lock()
		done = append(done, f.svc.executions[arn].done)
		f.svc.mu.Unlock()
	}
	return done
}

func allClosed(done []<-chan struct{}) bool {
	for _, d := range done {
		select {
		case <-d:
		default:
			return false
		}
	}
	return true
}

func TestDeleteAndResetStopRunningExecutionsWithoutLeaks(t *testing.T) {
	f := newFixture(t)
	baseline := runtime.NumGoroutine()

	deleted := runningDone(t, f, "doomed", 4)
	kept := runningDone(t, f, "survivor", 3)
	if runtime.NumGoroutine() < baseline+7 {
		t.Fatalf("expected at least 7 execution goroutines, have %d over the baseline", runtime.NumGoroutine()-baseline)
	}

	if err := f.svc.deleteStateMachine(f.svc.StateMachineARN("doomed")); err != nil {
		t.Fatal(err)
	}
	// deleteStateMachine waits, so the goroutines are gone when it returns.
	if !allClosed(deleted) {
		t.Fatal("DeleteStateMachine returned before its executions' goroutines exited")
	}
	if allClosed(kept) {
		t.Fatal("deleting one machine stopped another machine's executions")
	}

	f.svc.Reset()
	if !allClosed(kept) {
		t.Fatal("Reset returned before the goroutines exited")
	}
	eventually(t, "goroutines to drain", func() bool { return runtime.NumGoroutine() <= baseline })
	if len(f.svc.listMachines()) != 0 {
		t.Error("Reset kept state machines")
	}
}

func TestResetStopsATaskWaitingForItsToken(t *testing.T) {
	f := newFixture(t)
	arn := f.start(t, tokenDef(`"End": true`), `{"who":"x"}`)
	eventually(t, "the call", func() bool { return f.rec.count() == 1 })
	f.svc.mu.Lock()
	done := f.svc.executions[arn].done
	f.svc.mu.Unlock()
	f.svc.Reset()
	select {
	case <-done:
	default:
		t.Fatal("goroutine still running after Reset")
	}
	if err := f.svc.sendTaskSuccess(tokenOf(t, f.rec.call(0)), `{}`); !hasCode(err, "TaskDoesNotExist") {
		t.Errorf("token after Reset: %v", err)
	}
}

func TestCreateStateMachineIsIdempotent(t *testing.T) {
	f := newFixture(t)
	if err := f.svc.CreateStateMachine("m", longWait); err != nil {
		t.Fatal(err)
	}
	reformatted := strings.ReplaceAll(longWait, `"W":`, "\n  \"W\" :")
	if err := f.svc.CreateStateMachine("m", reformatted); err != nil {
		t.Errorf("same definition, other formatting: %v", err)
	}
	other := strings.Replace(longWait, "3600", "1", 1)
	if err := f.svc.CreateStateMachine("m", other); !hasCode(err, "StateMachineAlreadyExists") {
		t.Errorf("different definition: %v", err)
	}
	if err := f.svc.CreateStateMachine("m", longWait, WithType(typeExpress)); !hasCode(err, "StateMachineAlreadyExists") {
		t.Errorf("different type: %v", err)
	}
}

func TestCreateStateMachineRejectsUnregisteredIntegration(t *testing.T) {
	f := newFixture(t)
	def := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::sns:publish","End":true}}}`
	err := f.svc.CreateStateMachine("m", def)
	if !hasCode(err, "InvalidDefinition") || !strings.Contains(err.Error(), "arn:aws:states:::sns:publish") {
		t.Fatalf("error = %v", err)
	}
	f.svc.RegisterIntegration("arn:aws:states:::sns:publish", f.rec.fn)
	if err := f.svc.CreateStateMachine("m", def); err != nil {
		t.Errorf("after registering: %v", err)
	}
}

func TestStartExecutionNameReuse(t *testing.T) {
	f := newFixture(t)
	if err := f.svc.CreateStateMachine("m", longWait); err != nil {
		t.Fatal(err)
	}
	arn := f.svc.StateMachineARN("m")
	first, err := f.svc.StartExecution(arn, "run-1", `{"a":1}`)
	if err != nil {
		t.Fatal(err)
	}
	again, err := f.svc.StartExecution(arn, "run-1", `{ "a": 1 }`)
	if err != nil || again != first {
		t.Errorf("same name and input: %q, %v", again, err)
	}
	if _, err := f.svc.StartExecution(arn, "run-1", `{"a":2}`); !hasCode(err, "ExecutionAlreadyExists") {
		t.Errorf("different input: %v", err)
	}
	if _, err := f.svc.StartExecution(arn, "bad name", `{}`); !hasCode(err, "InvalidName") {
		t.Errorf("bad name: %v", err)
	}
	if _, err := f.svc.StartExecution(arn, "", `{nope`); !hasCode(err, "InvalidExecutionInput") {
		t.Errorf("bad input: %v", err)
	}
	if _, err := f.svc.StartExecution(f.svc.StateMachineARN("ghost"), "", `{}`); !hasCode(err, "StateMachineDoesNotExist") {
		t.Errorf("missing machine: %v", err)
	}
}

func TestExecutionHistory(t *testing.T) {
	f := newFixture(t)
	arn := f.start(t, scheduledSend, scheduledInput(0))
	v := f.waitStatus(t, arn, statusSucceeded)
	var types []string
	for _, ev := range v.history {
		types = append(types, ev.typ)
	}
	want := []string{
		"ExecutionStarted", "WaitStateEntered", "WaitStateExited", "TaskStateEntered", "TaskScheduled",
		"TaskSucceeded", "TaskStateExited", "ExecutionSucceeded",
	}
	if strings.Join(types, ",") != strings.Join(want, ",") {
		t.Errorf("events = %v\nwant     %v", types, want)
	}
	for i, ev := range v.history {
		if ev.id != int64(i+1) {
			t.Errorf("event %d has id %d", i, ev.id)
		}
	}
}

func TestListExecutionsNewestFirstWithFilter(t *testing.T) {
	f := newFixture(t)
	if err := f.svc.CreateStateMachine("m", longWait); err != nil {
		t.Fatal(err)
	}
	arn := f.svc.StateMachineARN("m")
	var arns []string
	for _, name := range []string{"a", "b", "c"} {
		got, err := f.svc.StartExecution(arn, name, `{}`)
		if err != nil {
			t.Fatal(err)
		}
		arns = append(arns, got)
	}
	if _, err := f.svc.stopExecution(arns[1], "", ""); err != nil {
		t.Fatal(err)
	}
	all, err := f.svc.listExecutions(arn, "")
	if err != nil || len(all) != 3 || all[0].name != "c" || all[2].name != "a" {
		t.Fatalf("all = %v, %v", all, err)
	}
	running, _ := f.svc.listExecutions(arn, statusRunning)
	aborted, _ := f.svc.listExecutions(arn, statusAborted)
	if len(running) != 2 || len(aborted) != 1 || aborted[0].name != "b" {
		t.Errorf("running=%d aborted=%d", len(running), len(aborted))
	}
}

func TestUpdateKeepsRunningExecutionOnItsDefinition(t *testing.T) {
	f := newFixture(t)
	arn := f.start(t, longWait, `{}`)
	f.waitEvent(t, arn, "WaitStateEntered")
	quick := `{"StartAt":"P","States":{"P":{"Type":"Succeed"}}}`
	if _, err := f.svc.updateStateMachine(f.svc.StateMachineARN("m"), quick, ""); err != nil {
		t.Fatal(err)
	}
	if got := f.view(t, arn).status; got != statusRunning {
		t.Errorf("running execution status = %s", got)
	}
	next, err := f.svc.StartExecution(f.svc.StateMachineARN("m"), "", `{}`)
	if err != nil {
		t.Fatal(err)
	}
	f.waitStatus(t, next, statusSucceeded)
	if _, err := f.svc.updateStateMachine(f.svc.StateMachineARN("m"), `{"StartAt":"X"}`, ""); !hasCode(err, "InvalidDefinition") {
		t.Errorf("invalid update: %v", err)
	}
}
