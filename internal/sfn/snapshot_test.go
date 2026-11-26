package sfn

import (
	"testing"
	"time"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

const waitThenPass = `{"StartAt": "Hold", "States": {
	"Hold": {"Type": "Wait", "Seconds": 10, "Next": "Done"},
	"Done": {"Type": "Pass", "End": true}}}`

func TestARestoredExecutionResumesWhereItWas(t *testing.T) {
	f := newFixture(t)
	finished := f.start(t, waitThenPass, `{"n":1}`)
	f.waitEvent(t, finished, "WaitStateEntered")
	f.advance(10 * time.Second)
	f.waitStatus(t, finished, statusSucceeded)

	running, err := f.svc.StartExecution(f.svc.StateMachineARN("m"), "second", `{"n":2}`)
	if err != nil {
		t.Fatal(err)
	}
	f.waitEvent(t, running, "WaitStateEntered")
	f.advance(4 * time.Second)

	data, err := f.svc.Snapshot(nil)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	f.svc.Reset() // the old emulator is gone

	next := New(awsapi.Identity{Account: awsapi.DefaultAccount, Region: awsapi.DefaultRegion}, f.clk)
	next.RegisterIntegration(sendResource, f.rec.fn)
	t.Cleanup(next.Reset)
	if err := next.Restore(data, nil); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	g := &fixture{svc: next, clk: f.clk, rec: f.rec}

	done := g.view(t, finished)
	if done.status != statusSucceeded || done.output != `{"n":1}` || len(done.history) == 0 {
		t.Fatalf("finished execution after Restore: %s %s, %d events", done.status, done.output, len(done.history))
	}
	g.advance(5 * time.Second) // 9 s into the 10 s wait
	settle()
	if v := g.view(t, running); v.status != statusRunning {
		t.Fatalf("the restored execution ended before its wait: %s", v.status)
	}
	g.advance(2 * time.Second)
	v := g.waitStatus(t, running, statusSucceeded)
	if v.output != `{"n":2}` {
		t.Errorf("output of the restored execution = %s", v.output)
	}
	if n := countEvents(v, "WaitStateEntered"); n != 1 {
		t.Errorf("%d WaitStateEntered events, want 1: a resume must not enter the state again", n)
	}
	if _, err := next.StartExecution(next.StateMachineARN("m"), "second", `{"n":3}`); !hasCode(err, "ExecutionAlreadyExists") {
		t.Errorf("reusing a restored execution's name: %v, want ExecutionAlreadyExists", err)
	}
}

func TestARestoredExecutionKeepsTheDefinitionItStartedWith(t *testing.T) {
	f := newFixture(t)
	arn := f.start(t, waitThenPass, `{}`)
	f.waitEvent(t, arn, "WaitStateEntered")
	if _, err := f.svc.updateStateMachine(f.svc.StateMachineARN("m"), `{"StartAt": "X", "States": {"X": {"Type": "Fail", "Error": "New"}}}`, ""); err != nil {
		t.Fatal(err)
	}
	data, err := f.svc.Snapshot(nil)
	if err != nil {
		t.Fatal(err)
	}
	f.svc.Reset()
	next := New(awsapi.Identity{Account: awsapi.DefaultAccount, Region: awsapi.DefaultRegion}, f.clk)
	next.RegisterIntegration(sendResource, f.rec.fn)
	t.Cleanup(next.Reset)
	if err := next.Restore(data, nil); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	g := &fixture{svc: next, clk: f.clk, rec: f.rec}
	g.advance(10 * time.Second)
	if v := g.waitStatus(t, arn, statusSucceeded); v.errName != "" {
		t.Fatalf("the restored execution ran the updated definition: %s %s", v.status, v.errName)
	}
}

func TestSFNRestoreRefusesAnotherFormat(t *testing.T) {
	f := newFixture(t)
	if err := f.svc.Restore([]byte(`{"format":99}`), nil); err == nil {
		t.Fatal("Restore of format 99 succeeded")
	}
}
