package sfn_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssfn "github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"

	"github.com/stratus-emulator/stratus/internal/awsapi"
	"github.com/stratus-emulator/stratus/internal/clock"
	"github.com/stratus-emulator/stratus/internal/gateway"
	"github.com/stratus-emulator/stratus/internal/sfn"
)

const sendResource = "arn:aws:states:::sqs:sendMessage"

var start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// env is a service behind the gateway with a fake clock and a fake SQS
// integration that records its parameters.
type env struct {
	svc    *sfn.Service
	clk    *clock.Fake
	client *awssfn.Client
	url    string

	mu    sync.Mutex
	calls []string
}

func (e *env) calledWith() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.calls...)
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e := &env{clk: clock.NewFake(start)}
	e.svc = sfn.New(awsapi.Identity{Account: awsapi.DefaultAccount, Region: awsapi.DefaultRegion}, e.clk)
	e.svc.RegisterIntegration(sendResource, func(_ context.Context, p json.RawMessage) (json.RawMessage, error) {
		e.mu.Lock()
		e.calls = append(e.calls, string(p))
		e.mu.Unlock()
		return json.RawMessage(`{"MessageId":"m-1"}`), nil
	})
	srv := httptest.NewServer(gateway.New(e.svc))
	t.Cleanup(func() { srv.Close(); e.svc.Reset() })
	e.url = srv.URL
	e.client = awssfn.New(awssfn.Options{
		Region:       awsapi.DefaultRegion,
		BaseEndpoint: aws.String(srv.URL),
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
	})
	return e
}

func (e *env) advance(d time.Duration) {
	e.clk.Advance(d)
	e.svc.Wake()
}

func (e *env) create(t *testing.T, name, def string) string {
	t.Helper()
	out, err := e.client.CreateStateMachine(t.Context(), &awssfn.CreateStateMachineInput{
		Name: aws.String(name), Definition: aws.String(def), RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
	})
	if err != nil {
		t.Fatalf("CreateStateMachine: %v", err)
	}
	return aws.ToString(out.StateMachineArn)
}

func (e *env) wait(t *testing.T, arn string, want types.ExecutionStatus) *awssfn.DescribeExecutionOutput {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		out, err := e.client.DescribeExecution(t.Context(), &awssfn.DescribeExecutionInput{ExecutionArn: aws.String(arn)})
		if err != nil {
			t.Fatal(err)
		}
		if out.Status == want {
			return out
		}
		if time.Now().After(deadline) {
			t.Fatalf("status = %s, want %s", out.Status, want)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func (e *env) history(t *testing.T, arn string) []types.HistoryEvent {
	t.Helper()
	out, err := e.client.GetExecutionHistory(t.Context(), &awssfn.GetExecutionHistoryInput{ExecutionArn: aws.String(arn)})
	if err != nil {
		t.Fatal(err)
	}
	return out.Events
}

func waitForEvent(t *testing.T, e *env, arn string, typ types.HistoryEventType) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		for _, ev := range e.history(t, arn) {
			if ev.Type == typ {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no %s event", typ)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

const sendLater = `{
  "StartAt": "Hold",
  "States": {
    "Hold": {"Type": "Wait", "TimestampPath": "$.send_at", "Next": "Send"},
    "Send": {"Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage",
      "Parameters": {"QueueUrl.$": "$.queue", "Body.$": "$.body"}, "ResultPath": "$.sent", "End": true}
  }
}`

func sendLaterInput(after time.Duration, body string) *string {
	return aws.String(`{"send_at":"` + start.Add(after).Format(time.RFC3339) + `","queue":"q","body":"` + body + `"}`)
}

func TestSDKStateMachineLifecycle(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	arn := e.create(t, "orders", sendLater)

	again, err := e.client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name: aws.String("orders"), Definition: aws.String(sendLater), RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
	})
	if err != nil || aws.ToString(again.StateMachineArn) != arn {
		t.Fatalf("idempotent create = %v, %v", again, err)
	}

	desc, err := e.client.DescribeStateMachine(ctx, &awssfn.DescribeStateMachineInput{StateMachineArn: aws.String(arn)})
	if err != nil {
		t.Fatal(err)
	}
	if aws.ToString(desc.Name) != "orders" || desc.Status != types.StateMachineStatusActive ||
		desc.Type != types.StateMachineTypeStandard || aws.ToString(desc.Definition) != sendLater ||
		aws.ToString(desc.RoleArn) != "arn:aws:iam::000000000000:role/r" || !desc.CreationDate.Equal(start) {
		t.Errorf("describe = %+v", desc)
	}

	e.create(t, "audit", `{"StartAt":"S","States":{"S":{"Type":"Succeed"}}}`)
	list, err := e.client.ListStateMachines(ctx, &awssfn.ListStateMachinesInput{})
	if err != nil || len(list.StateMachines) != 2 || aws.ToString(list.StateMachines[0].Name) != "audit" {
		t.Fatalf("list = %+v, %v", list, err)
	}
	first, err := e.client.ListStateMachines(ctx, &awssfn.ListStateMachinesInput{MaxResults: 1})
	if err != nil || len(first.StateMachines) != 1 || first.NextToken == nil {
		t.Fatalf("page 1 = %+v, %v", first, err)
	}
	second, err := e.client.ListStateMachines(ctx, &awssfn.ListStateMachinesInput{MaxResults: 1, NextToken: first.NextToken})
	if err != nil || len(second.StateMachines) != 1 || second.NextToken != nil {
		t.Fatalf("page 2 = %+v, %v", second, err)
	}

	quick := `{"StartAt":"S","States":{"S":{"Type":"Succeed"}}}`
	if _, err := e.client.UpdateStateMachine(ctx, &awssfn.UpdateStateMachineInput{
		StateMachineArn: aws.String(arn), Definition: aws.String(quick),
	}); err != nil {
		t.Fatal(err)
	}
	desc, _ = e.client.DescribeStateMachine(ctx, &awssfn.DescribeStateMachineInput{StateMachineArn: aws.String(arn)})
	if aws.ToString(desc.Definition) != quick {
		t.Errorf("definition after update = %s", aws.ToString(desc.Definition))
	}

	if _, err := e.client.DeleteStateMachine(ctx, &awssfn.DeleteStateMachineInput{StateMachineArn: aws.String(arn)}); err != nil {
		t.Fatal(err)
	}
	_, err = e.client.DescribeStateMachine(ctx, &awssfn.DescribeStateMachineInput{StateMachineArn: aws.String(arn)})
	var missing *types.StateMachineDoesNotExist
	if !errors.As(err, &missing) {
		t.Errorf("describe after delete: %v", err)
	}
}

func TestSDKStateMachineErrors(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	e.create(t, "orders", sendLater)

	_, err := e.client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name: aws.String("orders"), Definition: aws.String(`{"StartAt":"S","States":{"S":{"Type":"Succeed"}}}`),
		RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
	})
	var exists *types.StateMachineAlreadyExists
	if !errors.As(err, &exists) {
		t.Errorf("conflicting create: %v", err)
	}

	for name, def := range map[string]string{
		"unreachable":    `{"StartAt":"A","States":{"A":{"Type":"Succeed"},"B":{"Type":"Succeed"}}}`,
		"jsonata":        `{"QueryLanguage":"JSONata","StartAt":"A","States":{"A":{"Type":"Succeed"}}}`,
		"no-integration": `{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::sns:publish","End":true}}}`,
		"parallel":       `{"StartAt":"P","States":{"P":{"Type":"Parallel","Branches":[],"End":true}}}`,
	} {
		_, err := e.client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
			Name: aws.String("bad-" + name), Definition: aws.String(def), RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
		})
		var invalid *types.InvalidDefinition
		if !errors.As(err, &invalid) {
			t.Errorf("%s: error = %v", name, err)
		}
	}
	_, err = e.client.CreateStateMachine(ctx, &awssfn.CreateStateMachineInput{
		Name: aws.String("bad-sns"), Definition: aws.String(`{"StartAt":"T","States":{"T":{"Type":"Task","Resource":"arn:aws:states:::sns:publish","End":true}}}`),
		RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
	})
	if err == nil || !strings.Contains(err.Error(), "arn:aws:states:::sns:publish") {
		t.Errorf("the error must name the resource: %v", err)
	}
}

func TestSDKExecutions(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	arn := e.create(t, "orders", sendLater)

	started, err := e.client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: aws.String(arn), Name: aws.String("run-1"), Input: sendLaterInput(time.Hour, "hello"),
	})
	if err != nil {
		t.Fatal(err)
	}
	execArn := aws.ToString(started.ExecutionArn)
	if !started.StartDate.Equal(start) || !strings.HasSuffix(execArn, ":execution:orders:run-1") {
		t.Errorf("started = %+v", started)
	}
	waitForEvent(t, e, execArn, types.HistoryEventTypeWaitStateEntered)

	desc := e.wait(t, execArn, types.ExecutionStatusRunning)
	if aws.ToString(desc.Name) != "run-1" || aws.ToString(desc.StateMachineArn) != arn || desc.StopDate != nil {
		t.Errorf("running describe = %+v", desc)
	}
	if n := len(e.calledWith()); n != 0 {
		t.Fatalf("%d calls before the time", n)
	}

	e.advance(time.Hour)
	done := e.wait(t, execArn, types.ExecutionStatusSucceeded)
	if done.StopDate == nil || !strings.Contains(aws.ToString(done.Output), `"sent":{"MessageId":"m-1"}`) {
		t.Errorf("finished describe = %+v", done)
	}
	if calls := e.calledWith(); len(calls) != 1 || calls[0] != `{"Body":"hello","QueueUrl":"q"}` {
		t.Errorf("calls = %v", calls)
	}

	hist := e.history(t, execArn)
	var types1 []types.HistoryEventType
	for _, ev := range hist {
		types1 = append(types1, ev.Type)
	}
	if types1[0] != types.HistoryEventTypeExecutionStarted || types1[len(types1)-1] != types.HistoryEventTypeExecutionSucceeded {
		t.Errorf("history = %v", types1)
	}
	if hist[0].ExecutionStartedEventDetails == nil || hist[1].StateEnteredEventDetails == nil ||
		aws.ToString(hist[1].StateEnteredEventDetails.Name) != "Hold" || hist[1].Timestamp == nil {
		t.Errorf("history details = %+v", hist[:2])
	}
	rev, err := e.client.GetExecutionHistory(ctx, &awssfn.GetExecutionHistoryInput{
		ExecutionArn: aws.String(execArn), ReverseOrder: true, MaxResults: 2,
	})
	if err != nil || len(rev.Events) != 2 || rev.Events[0].Type != types.HistoryEventTypeExecutionSucceeded || rev.NextToken == nil {
		t.Errorf("reverse history = %+v, %v", rev, err)
	}
}

func TestSDKStopAndListExecutions(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	arn := e.create(t, "orders", sendLater)
	var arns []string
	for _, name := range []string{"a", "b", "c"} {
		out, err := e.client.StartExecution(ctx, &awssfn.StartExecutionInput{
			StateMachineArn: aws.String(arn), Name: aws.String(name), Input: sendLaterInput(time.Hour, name),
		})
		if err != nil {
			t.Fatal(err)
		}
		arns = append(arns, aws.ToString(out.ExecutionArn))
	}
	waitForEvent(t, e, arns[1], types.HistoryEventTypeWaitStateEntered)
	if _, err := e.client.StopExecution(ctx, &awssfn.StopExecutionInput{
		ExecutionArn: aws.String(arns[1]), Error: aws.String("Nope"), Cause: aws.String("changed my mind"),
	}); err != nil {
		t.Fatal(err)
	}
	stopped := e.wait(t, arns[1], types.ExecutionStatusAborted)
	if aws.ToString(stopped.Error) != "Nope" || aws.ToString(stopped.Cause) != "changed my mind" || stopped.StopDate == nil {
		t.Errorf("stopped = %+v", stopped)
	}

	all, err := e.client.ListExecutions(ctx, &awssfn.ListExecutionsInput{StateMachineArn: aws.String(arn)})
	if err != nil || len(all.Executions) != 3 || aws.ToString(all.Executions[0].Name) != "c" {
		t.Fatalf("all = %+v, %v", all, err)
	}
	aborted, err := e.client.ListExecutions(ctx, &awssfn.ListExecutionsInput{
		StateMachineArn: aws.String(arn), StatusFilter: types.ExecutionStatusAborted,
	})
	if err != nil || len(aborted.Executions) != 1 || aws.ToString(aborted.Executions[0].Name) != "b" {
		t.Errorf("aborted = %+v, %v", aborted, err)
	}

	e.advance(2 * time.Hour)
	e.wait(t, arns[0], types.ExecutionStatusSucceeded)
	e.wait(t, arns[2], types.ExecutionStatusSucceeded)
	if calls := e.calledWith(); len(calls) != 2 {
		t.Errorf("calls = %v; the stopped execution must send nothing", calls)
	}
}

func TestSDKExecutionErrors(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()
	arn := e.create(t, "orders", sendLater)
	if _, err := e.client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: aws.String(arn), Name: aws.String("run"), Input: sendLaterInput(time.Hour, "x"),
	}); err != nil {
		t.Fatal(err)
	}

	_, err := e.client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: aws.String(arn), Name: aws.String("run"), Input: sendLaterInput(time.Hour, "different"),
	})
	var exists *types.ExecutionAlreadyExists
	if !errors.As(err, &exists) {
		t.Errorf("reused name: %v", err)
	}
	_, err = e.client.StartExecution(ctx, &awssfn.StartExecutionInput{StateMachineArn: aws.String(arn), Input: aws.String("{nope")})
	var badInput *types.InvalidExecutionInput
	if !errors.As(err, &badInput) {
		t.Errorf("bad input: %v", err)
	}
	_, err = e.client.StartExecution(ctx, &awssfn.StartExecutionInput{
		StateMachineArn: aws.String(strings.Replace(arn, "orders", "ghost", 1)),
	})
	var noMachine *types.StateMachineDoesNotExist
	if !errors.As(err, &noMachine) {
		t.Errorf("missing machine: %v", err)
	}
	_, err = e.client.DescribeExecution(ctx, &awssfn.DescribeExecutionInput{
		ExecutionArn: aws.String(strings.Replace(arn, "stateMachine:orders", "execution:orders:ghost", 1)),
	})
	var noExec *types.ExecutionDoesNotExist
	if !errors.As(err, &noExec) {
		t.Errorf("missing execution: %v", err)
	}
	_, err = e.client.DescribeExecution(ctx, &awssfn.DescribeExecutionInput{ExecutionArn: aws.String("not-an-arn")})
	var badArn *types.InvalidArn
	if !errors.As(err, &badArn) {
		t.Errorf("bad arn: %v", err)
	}
}

const askMachine = `{
  "StartAt": "Ask",
  "States": {
    "Ask": {"Type": "Task", "Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
      "Parameters": {"Token.$": "$$.Task.Token"}, "HeartbeatSeconds": 20, "TimeoutSeconds": 100,
      "ResultPath": "$.reply", "End": true}
  }
}`

// askToken starts an execution of the token machine and returns the task
// token its integration call received.
func (e *env) askToken(t *testing.T, execName string) (execArn, token string) {
	t.Helper()
	before := len(e.calledWith())
	arn := e.create(t, "ask-"+execName, askMachine)
	out, err := e.client.StartExecution(t.Context(), &awssfn.StartExecutionInput{
		StateMachineArn: aws.String(arn), Name: aws.String(execName), Input: aws.String(`{}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(e.calledWith()) <= before {
		if time.Now().After(deadline) {
			t.Fatal("the integration was not called")
		}
		time.Sleep(2 * time.Millisecond)
	}
	var p struct{ Token string }
	if err := json.Unmarshal([]byte(e.calledWith()[before]), &p); err != nil || p.Token == "" {
		t.Fatalf("no token in %v", e.calledWith()[before])
	}
	return aws.ToString(out.ExecutionArn), p.Token
}

func TestSDKTaskCallbacks(t *testing.T) {
	e := newEnv(t)
	ctx := t.Context()

	okExec, token := e.askToken(t, "ok")
	if _, err := e.client.SendTaskHeartbeat(ctx, &awssfn.SendTaskHeartbeatInput{TaskToken: aws.String(token)}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.client.SendTaskSuccess(ctx, &awssfn.SendTaskSuccessInput{
		TaskToken: aws.String(token), Output: aws.String(`{"answer":42}`),
	}); err != nil {
		t.Fatal(err)
	}
	done := e.wait(t, okExec, types.ExecutionStatusSucceeded)
	if aws.ToString(done.Output) != `{"reply":{"answer":42}}` {
		t.Errorf("output = %s", aws.ToString(done.Output))
	}
	_, err := e.client.SendTaskSuccess(ctx, &awssfn.SendTaskSuccessInput{TaskToken: aws.String(token), Output: aws.String(`{}`)})
	var gone *types.TaskDoesNotExist
	if !errors.As(err, &gone) {
		t.Errorf("completing twice: %v", err)
	}

	badExec, token2 := e.askToken(t, "bad")
	if _, err := e.client.SendTaskFailure(ctx, &awssfn.SendTaskFailureInput{
		TaskToken: aws.String(token2), Error: aws.String("Denied"), Cause: aws.String("by policy"),
	}); err != nil {
		t.Fatal(err)
	}
	failed := e.wait(t, badExec, types.ExecutionStatusFailed)
	if aws.ToString(failed.Error) != "Denied" || aws.ToString(failed.Cause) != "by policy" {
		t.Errorf("failed = %+v", failed)
	}

	lateExec, token3 := e.askToken(t, "late")
	e.advance(25 * time.Second) // past HeartbeatSeconds
	e.wait(t, lateExec, types.ExecutionStatusFailed)
	_, err = e.client.SendTaskHeartbeat(ctx, &awssfn.SendTaskHeartbeatInput{TaskToken: aws.String(token3)})
	var timedOut *types.TaskTimedOut
	if !errors.As(err, &timedOut) {
		t.Errorf("heartbeat after timeout: %v", err)
	}

	_, err = e.client.SendTaskSuccess(ctx, &awssfn.SendTaskSuccessInput{TaskToken: aws.String("garbage"), Output: aws.String(`{}`)})
	var invalid *types.InvalidToken
	if !errors.As(err, &invalid) {
		t.Errorf("garbage token: %v", err)
	}
	_, err = e.client.SendTaskSuccess(ctx, &awssfn.SendTaskSuccessInput{TaskToken: aws.String(token), Output: aws.String(`nope`)})
	var badOut *types.InvalidOutput
	if !errors.As(err, &badOut) {
		t.Errorf("bad output: %v", err)
	}
}

func rawCall(t *testing.T, url, op, body string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AWSStepFunctions."+op)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/eu-central-1/states/aws4_request, SignedHeaders=host, Signature=e2e")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("response %q: %v", raw, err)
	}
	return resp.StatusCode, out
}

func TestRawHTTPStartExecution(t *testing.T) {
	e := newEnv(t)
	arn := e.create(t, "quick", `{"StartAt":"S","States":{"S":{"Type":"Succeed"}}}`)

	status, out := rawCall(t, e.url, "StartExecution", `{"stateMachineArn":"`+arn+`","name":"raw"}`)
	if status != http.StatusOK || !strings.HasSuffix(out["executionArn"].(string), ":execution:quick:raw") {
		t.Fatalf("status %d, body %v", status, out)
	}
	if _, ok := out["startDate"].(float64); !ok {
		t.Errorf("startDate must be a JSON number: %v", out["startDate"])
	}
	e.wait(t, out["executionArn"].(string), types.ExecutionStatusSucceeded)

	status, out = rawCall(t, e.url, "StartExecution", `{"stateMachineArn":"`+strings.Replace(arn, "quick", "ghost", 1)+`"}`)
	if status != http.StatusBadRequest || out["__type"] != "StateMachineDoesNotExist" {
		t.Errorf("missing machine: %d %v", status, out)
	}
	status, out = rawCall(t, e.url, "CreateActivity", `{}`)
	if status != http.StatusBadRequest || out["__type"] != "InvalidAction" || !strings.Contains(out["message"].(string), "CreateActivity") {
		t.Errorf("unsupported operation: %d %v", status, out)
	}
}
