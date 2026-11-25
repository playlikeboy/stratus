package sfn

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/ODudek/stratus/internal/awsapi"
	"github.com/ODudek/stratus/internal/clock"
)

const sendResource = "arn:aws:states:::sqs:sendMessage"

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// recorder is a fake integration that records the parameters it is called
// with and can be told to fail.
type recorder struct {
	mu     sync.Mutex
	calls  []string
	failed func(call int) error // nil means every call succeeds
	result string
}

func (r *recorder) fn(_ context.Context, params json.RawMessage) (json.RawMessage, error) {
	r.mu.Lock()
	r.calls = append(r.calls, string(params))
	n := len(r.calls)
	r.mu.Unlock()
	if r.failed != nil {
		if err := r.failed(n); err != nil {
			return nil, err
		}
	}
	if r.result == "" {
		return json.RawMessage(`{"MessageId":"m-1"}`), nil
	}
	return json.RawMessage(r.result), nil
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *recorder) call(i int) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[i]
}

type fixture struct {
	svc *Service
	clk *clock.Fake
	rec *recorder
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	clk := clock.NewFake(t0)
	svc := New(awsapi.Identity{Account: awsapi.DefaultAccount, Region: awsapi.DefaultRegion}, clk)
	rec := &recorder{}
	svc.RegisterIntegration(sendResource, rec.fn)
	t.Cleanup(svc.Reset)
	return &fixture{svc: svc, clk: clk, rec: rec}
}

// start creates a machine and starts one execution of it.
func (f *fixture) start(t *testing.T, definition, input string) string {
	t.Helper()
	if err := f.svc.CreateStateMachine("m", definition); err != nil {
		t.Fatalf("CreateStateMachine: %v", err)
	}
	arn, err := f.svc.StartExecution(f.svc.StateMachineARN("m"), "", input)
	if err != nil {
		t.Fatalf("StartExecution: %v", err)
	}
	return arn
}

// advance moves the fake clock and wakes the waiting executions.
func (f *fixture) advance(d time.Duration) {
	f.clk.Advance(d)
	f.svc.Wake()
}

func (f *fixture) view(t *testing.T, arn string) execView {
	t.Helper()
	v, err := f.svc.describeExecution(arn)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// eventually polls cond for a few seconds of real time.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (f *fixture) waitStatus(t *testing.T, arn, want string) execView {
	t.Helper()
	eventually(t, "status "+want, func() bool { return f.view(t, arn).status == want })
	return f.view(t, arn)
}

func (f *fixture) waitEvent(t *testing.T, arn, typ string) {
	t.Helper()
	eventually(t, "event "+typ, func() bool { return hasEvent(f.view(t, arn), typ) })
}

func hasEvent(v execView, typ string) bool {
	for _, ev := range v.history {
		if ev.typ == typ {
			return true
		}
	}
	return false
}

func countEvents(v execView, typ string) int {
	n := 0
	for _, ev := range v.history {
		if ev.typ == typ {
			n++
		}
	}
	return n
}

// settle gives the execution goroutines time to react to a Wake before a test
// asserts that nothing happened.
func settle() { time.Sleep(60 * time.Millisecond) }

// tokenOf reads the task token the integration was given.
func tokenOf(t *testing.T, params string) string {
	t.Helper()
	var p struct{ Token string }
	if err := json.Unmarshal([]byte(params), &p); err != nil || p.Token == "" {
		t.Fatalf("no token in %s (%v)", params, err)
	}
	return p.Token
}
