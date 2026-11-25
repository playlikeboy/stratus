package sfn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

const scheduledSend = `{
  "StartAt": "WaitUntil",
  "States": {
    "WaitUntil": {"Type": "Wait", "TimestampPath": "$.send_at", "Next": "Send"},
    "Send": {
      "Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage",
      "Parameters": {"QueueUrl.$": "$.queue", "MessageBody.$": "States.JsonToString($.payload)", "Run.$": "$$.Execution.Name"},
      "ResultSelector": {"id.$": "$.MessageId"},
      "ResultPath": "$.sent",
      "Retry": [{"ErrorEquals": ["Boom"], "IntervalSeconds": 10, "MaxAttempts": 2, "BackoffRate": 2}],
      "Catch": [{"ErrorEquals": ["States.ALL"], "ResultPath": "$.error", "Next": "Failed"}],
      "End": true
    },
    "Failed": {"Type": "Fail", "Error": "SendFailed", "Cause": "the message could not be sent"}
  }
}`

func scheduledInput(delay time.Duration) string {
	return `{"send_at":"` + t0.Add(delay).Format(time.RFC3339) + `","queue":"q-url","payload":{"k":1}}`
}

func TestScheduledSendWaitsForTheTimestamp(t *testing.T) {
	f := newFixture(t)
	arn := f.start(t, scheduledSend, scheduledInput(time.Hour))
	f.waitEvent(t, arn, "WaitStateEntered")

	settle()
	f.advance(59 * time.Minute)
	settle()
	if n := f.rec.count(); n != 0 {
		t.Fatalf("%d calls before the timestamp", n)
	}

	f.advance(time.Minute)
	v := f.waitStatus(t, arn, statusSucceeded)
	if n := f.rec.count(); n != 1 {
		t.Fatalf("calls = %d, want exactly 1", n)
	}
	want := `{"MessageBody":"{\"k\":1}","QueueUrl":"q-url","Run":"` + v.name + `"}`
	if got := f.rec.call(0); got != want {
		t.Errorf("parameters = %s, want %s", got, want)
	}
	if !strings.Contains(v.output, `"sent":{"id":"m-1"}`) || !strings.Contains(v.output, `"queue":"q-url"`) {
		t.Errorf("output = %s", v.output)
	}
}

func TestFailingTaskIsRetriedThenCaught(t *testing.T) {
	f := newFixture(t)
	f.rec.failed = func(int) error { return &IntegrationError{Name: "Boom", Cause: "kaput"} }
	arn := f.start(t, scheduledSend, scheduledInput(0))

	steps := []struct {
		advance time.Duration
		calls   int
	}{{0, 1}, {9 * time.Second, 1}, {time.Second, 2}, {19 * time.Second, 2}, {time.Second, 3}}
	for _, st := range steps {
		if st.advance > 0 {
			settle()
			f.advance(st.advance)
		}
		eventually(t, "call count", func() bool { return f.rec.count() == st.calls })
	}
	v := f.waitStatus(t, arn, statusFailed)
	if f.rec.count() != 3 || v.errName != "SendFailed" || v.cause != "the message could not be sent" {
		t.Errorf("calls=%d error=%q cause=%q", f.rec.count(), v.errName, v.cause)
	}
	if got := countEvents(v, "TaskFailed"); got != 3 {
		t.Errorf("TaskFailed events = %d, want 3", got)
	}
}

func TestCatchOutputCarriesErrorAndCause(t *testing.T) {
	f := newFixture(t)
	f.rec.failed = func(int) error { return errors.New("disk on fire") } // not an IntegrationError
	def := `{"StartAt":"Send","States":{
	  "Send":{"Type":"Task","Resource":"arn:aws:states:::sqs:sendMessage","Parameters":{"a":1},
	    "Catch":[{"ErrorEquals":["States.ALL"],"ResultPath":"$.error","Next":"Done"}],"End":true},
	  "Done":{"Type":"Succeed","OutputPath":"$.error"}}}`
	arn := f.start(t, def, `{}`)
	v := f.waitStatus(t, arn, statusSucceeded)
	if v.output != `{"Cause":"disk on fire","Error":"States.TaskFailed"}` {
		t.Errorf("output = %s", v.output)
	}
}

func TestStopExecutionDuringWaitSendsNothing(t *testing.T) {
	f := newFixture(t)
	arn := f.start(t, scheduledSend, scheduledInput(time.Hour))
	f.waitEvent(t, arn, "WaitStateEntered")

	if _, err := f.svc.stopExecution(arn, "Cancelled", "no longer needed"); err != nil {
		t.Fatal(err)
	}
	v := f.view(t, arn)
	if v.status != statusAborted || v.errName != "Cancelled" || v.cause != "no longer needed" {
		t.Errorf("view = %+v", v)
	}
	f.advance(2 * time.Hour)
	settle()
	if n := f.rec.count(); n != 0 {
		t.Errorf("%d calls after the execution was stopped", n)
	}
	if !hasEvent(f.view(t, arn), "ExecutionAborted") {
		t.Error("no ExecutionAborted event")
	}
	if got := f.view(t, arn).status; got != statusAborted {
		t.Errorf("status changed to %s", got)
	}
}

const tokenMachine = `{
  "StartAt": "Ask",
  "States": {
    "Ask": {
      "Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
      "Parameters": {"Token.$": "$$.Task.Token", "Who.$": "$.who"},
      "ResultPath": "$.answer", %s
    }
  }
}`

func tokenDef(extra string) string { return fmt.Sprintf(tokenMachine, extra) }

func TestWaitForTaskTokenSuccess(t *testing.T) {
	f := newFixture(t)
	arn := f.start(t, tokenDef(`"End": true`), `{"who":"ann"}`)
	eventually(t, "the integration call", func() bool { return f.rec.count() == 1 })
	token := tokenOf(t, f.rec.call(0))
	settle()
	if f.view(t, arn).status != statusRunning {
		t.Fatal("the execution must wait for the token")
	}
	if err := f.svc.sendTaskSuccess(token, `{"ok":true}`); err != nil {
		t.Fatal(err)
	}
	v := f.waitStatus(t, arn, statusSucceeded)
	if v.output != `{"answer":{"ok":true},"who":"ann"}` {
		t.Errorf("output = %s", v.output)
	}
	if err := f.svc.sendTaskSuccess(token, `{}`); !hasCode(err, "TaskDoesNotExist") {
		t.Errorf("second completion error = %v", err)
	}
}

func TestWaitForTaskTokenFailure(t *testing.T) {
	f := newFixture(t)
	arn := f.start(t, tokenDef(`"End": true`), `{"who":"ann"}`)
	eventually(t, "the integration call", func() bool { return f.rec.count() == 1 })
	if err := f.svc.sendTaskFailure(tokenOf(t, f.rec.call(0)), "Denied", "not today"); err != nil {
		t.Fatal(err)
	}
	v := f.waitStatus(t, arn, statusFailed)
	if v.errName != "Denied" || v.cause != "not today" {
		t.Errorf("error=%q cause=%q", v.errName, v.cause)
	}
}

func TestWaitForTaskTokenHeartbeatTimeout(t *testing.T) {
	f := newFixture(t)
	arn := f.start(t, tokenDef(`"HeartbeatSeconds": 10, "End": true`), `{"who":"ann"}`)
	eventually(t, "the integration call", func() bool { return f.rec.count() == 1 })
	token := tokenOf(t, f.rec.call(0))

	f.advance(5 * time.Second)
	if err := f.svc.sendTaskHeartbeat(token); err != nil {
		t.Fatal(err)
	}
	f.advance(8 * time.Second) // 13s in, 8s after the heartbeat
	settle()
	if f.view(t, arn).status != statusRunning {
		t.Fatal("a heartbeat must keep the task alive")
	}
	f.advance(3 * time.Second) // 11s after the heartbeat
	v := f.waitStatus(t, arn, statusFailed)
	if v.errName != errTimeout {
		t.Errorf("error = %q", v.errName)
	}
	if err := f.svc.sendTaskSuccess(token, `{}`); !hasCode(err, "TaskTimedOut") {
		t.Errorf("late completion error = %v", err)
	}
	if err := f.svc.sendTaskHeartbeat(token); !hasCode(err, "TaskTimedOut") {
		t.Errorf("late heartbeat error = %v", err)
	}
}

func TestWaitForTaskTokenTimeoutSecondsIsCaught(t *testing.T) {
	f := newFixture(t)
	def := tokenDef(`"TimeoutSeconds": 30, "Catch": [{"ErrorEquals": ["States.Timeout"], "Next": "Late"}], "End": true`)
	def = strings.Replace(def, `"Ask": {`, `"Late": {"Type": "Succeed"}, "Ask": {`, 1)
	arn := f.start(t, def, `{"who":"ann"}`)
	eventually(t, "the integration call", func() bool { return f.rec.count() == 1 })
	f.advance(31 * time.Second)
	v := f.waitStatus(t, arn, statusSucceeded)
	if !strings.Contains(v.output, `"Error":"States.Timeout"`) {
		t.Errorf("output = %s", v.output)
	}
}

func TestTokenErrors(t *testing.T) {
	f := newFixture(t)
	if err := f.svc.sendTaskSuccess("garbage", `{}`); !hasCode(err, "InvalidToken") {
		t.Errorf("garbage token error = %v", err)
	}
	if err := f.svc.sendTaskSuccess(tokenPrefix+"unknown", `{}`); !hasCode(err, "TaskDoesNotExist") {
		t.Errorf("unknown token error = %v", err)
	}
	if err := f.svc.sendTaskSuccess(tokenPrefix+"unknown", `not json`); !hasCode(err, "InvalidOutput") {
		t.Errorf("bad output error = %v", err)
	}
}

func TestSlowTaskHitsTimeoutSeconds(t *testing.T) {
	f := newFixture(t)
	stopped := make(chan struct{})
	f.svc.RegisterIntegration("arn:aws:states:::slow:call", func(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
		<-ctx.Done()
		close(stopped)
		return nil, ctx.Err()
	})
	def := `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::slow:call","TimeoutSeconds":5,"End":true}}}`
	arn := f.start(t, def, `{}`)
	f.waitEvent(t, arn, "TaskScheduled")
	f.advance(6 * time.Second)
	v := f.waitStatus(t, arn, statusFailed)
	if v.errName != errTimeout {
		t.Errorf("error = %q", v.errName)
	}
	<-stopped // the call was cancelled, not abandoned
}

func TestExecutionTimeout(t *testing.T) {
	f := newFixture(t)
	def := `{"StartAt":"W","TimeoutSeconds":10,"States":{"W":{"Type":"Wait","Seconds":3600,"End":true}}}`
	arn := f.start(t, def, `{}`)
	f.waitEvent(t, arn, "WaitStateEntered")
	f.advance(11 * time.Second)
	f.waitStatus(t, arn, statusTimedOut)
}

func TestChoiceFlow(t *testing.T) {
	def := `{
	  "StartAt": "Pick",
	  "States": {
	    "Pick": {"Type": "Choice", "Choices": [
	      {"Variable": "$.n", "NumericGreaterThan": 10, "Next": "Big"},
	      {"Variable": "$.n", "NumericLessThanEquals": 10, "Next": "Small"}], "Default": "Nope"},
	    "Big": {"Type": "Pass", "Result": "big", "ResultPath": "$.size", "End": true},
	    "Small": {"Type": "Pass", "Result": "small", "ResultPath": "$.size", "Next": "Pause"},
	    "Pause": {"Type": "Wait", "Seconds": 1, "End": true},
	    "Nope": {"Type": "Fail", "ErrorPath": "$.err", "CausePath": "$.why"}
	  }}`
	tests := []struct{ name, input, status, output string }{
		{"big", `{"n":11}`, statusSucceeded, `{"n":11,"size":"big"}`},
		{"small waits one second", `{"n":3}`, statusSucceeded, `{"n":3,"size":"small"}`},
		{"no match is an error", `{"n":"x","err":"E","why":"W"}`, statusFailed, ``},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			arn := f.start(t, def, tt.input)
			if tt.name == "small waits one second" {
				f.waitEvent(t, arn, "WaitStateEntered")
				f.advance(time.Second)
			}
			v := f.waitStatus(t, arn, tt.status)
			if tt.output != "" && v.output != tt.output {
				t.Errorf("output = %s, want %s", v.output, tt.output)
			}
			if tt.status == statusFailed && (v.errName != "E" || v.cause != "W") {
				t.Errorf("error=%q cause=%q", v.errName, v.cause)
			}
		})
	}
}

func hasCode(err error, code string) bool {
	var ae interface{ Error() string }
	if !errors.As(err, &ae) || err == nil {
		return false
	}
	return strings.HasPrefix(err.Error(), code+":")
}
