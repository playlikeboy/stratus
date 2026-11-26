package sfn

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

const (
	targetPrefix = "AWSStepFunctions."
	contentType  = "application/x-amz-json-1.0"

	// maxRequestBytes bounds a request body: a definition or an input is at
	// most 1 MiB in AWS, and the JSON framing adds a little.
	maxRequestBytes = 2 << 20
)

type operation func(s *Service, body []byte) (any, error)

var operations = map[string]operation{
	"CreateStateMachine":   op((*Service).opCreateStateMachine),
	"UpdateStateMachine":   op((*Service).opUpdateStateMachine),
	"DescribeStateMachine": op((*Service).opDescribeStateMachine),
	"ListStateMachines":    op((*Service).opListStateMachines),
	"DeleteStateMachine":   op((*Service).opDeleteStateMachine),
	"StartExecution":       op((*Service).opStartExecution),
	"StopExecution":        op((*Service).opStopExecution),
	"DescribeExecution":    op((*Service).opDescribeExecution),
	"ListExecutions":       op((*Service).opListExecutions),
	"GetExecutionHistory":  op((*Service).opGetExecutionHistory),
	"SendTaskSuccess":      op((*Service).opSendTaskSuccess),
	"SendTaskFailure":      op((*Service).opSendTaskFailure),
	"SendTaskHeartbeat":    op((*Service).opSendTaskHeartbeat),
}

// op adapts a typed operation: it decodes the request body into In.
func op[In any](fn func(*Service, In) (any, error)) operation {
	return func(s *Service, body []byte) (any, error) {
		var in In
		if len(body) > 0 {
			if err := json.Unmarshal(body, &in); err != nil {
				return nil, apiError("SerializationException",
					"The request body is not valid JSON for this operation: "+err.Error())
			}
		}
		return fn(s, in)
	}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	out, err := s.serve(w, r)
	if err != nil {
		var apiErr *awsapi.Error
		if !errors.As(err, &apiErr) {
			apiErr = &awsapi.Error{Status: http.StatusInternalServerError, Code: "InternalFailure", Message: err.Error()}
		}
		// The SDK matches the bare shape name, so no namespace is sent.
		awsapi.WriteJSON(w, contentType, apiErr.Status, map[string]string{"__type": apiErr.Code, "message": apiErr.Message})
		return
	}
	awsapi.WriteJSON(w, contentType, http.StatusOK, out)
}

func (s *Service) serve(w http.ResponseWriter, r *http.Request) (any, error) {
	target := r.Header.Get("X-Amz-Target")
	if target == "" {
		return nil, apiError("UnsupportedProtocol",
			"stratus speaks only the Step Functions JSON protocol: the request has no X-Amz-Target header.")
	}
	name := strings.TrimPrefix(target, targetPrefix)
	handle, ok := operations[name]
	if !ok {
		return nil, apiError("InvalidAction", "stratus does not implement the Step Functions operation "+name+".")
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	if err != nil {
		return nil, errValidation("The request body could not be read or is too large.")
	}
	return handle(s, body)
}

type empty struct{}

// page cuts one page out of items. The token is the offset of the page's
// first item.
func page[T any](items []T, maxResults int, token string) ([]T, string, error) {
	if maxResults < 0 || maxResults > 1000 {
		return nil, "", errValidation("maxResults must be between 0 and 1000")
	}
	if maxResults == 0 {
		maxResults = 100
	}
	start := 0
	if token != "" {
		n, err := strconv.Atoi(token)
		if err != nil || n < 0 || n > len(items) {
			return nil, "", apiError("InvalidToken", "Invalid Token: 'Invalid token'")
		}
		start = n
	}
	end := min(start+maxResults, len(items))
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	}
	return items[start:end], next, nil
}

type createStateMachineInput struct {
	Name       string
	Definition string
	RoleArn    string //nolint:revive // the wire name
	Type       string
}

func (s *Service) opCreateStateMachine(in createStateMachineInput) (any, error) {
	if in.Name == "" || in.Definition == "" {
		return nil, apiError("MissingRequiredParameter", "name and definition are required.")
	}
	opts := []CreateOption{WithRoleARN(in.RoleArn)}
	if in.Type != "" {
		opts = append(opts, WithType(in.Type))
	}
	if err := s.CreateStateMachine(in.Name, in.Definition, opts...); err != nil {
		return nil, err
	}
	m, err := s.describeMachine(s.StateMachineARN(in.Name))
	if err != nil {
		return nil, err
	}
	return map[string]any{"stateMachineArn": m.arn, "creationDate": epoch(m.created)}, nil
}

type updateStateMachineInput struct {
	StateMachineArn string //nolint:revive // the wire name
	Definition      string
	RoleArn         string //nolint:revive // the wire name
}

func (s *Service) opUpdateStateMachine(in updateStateMachineInput) (any, error) {
	if in.Definition == "" && in.RoleArn == "" {
		return nil, apiError("MissingRequiredParameter", "Either the definition or the role ARN must be specified.")
	}
	at, err := s.updateStateMachine(in.StateMachineArn, in.Definition, in.RoleArn)
	if err != nil {
		return nil, err
	}
	return map[string]any{"updateDate": epoch(at)}, nil
}

type machineInput struct {
	StateMachineArn string //nolint:revive // the wire name
}

func (s *Service) opDescribeStateMachine(in machineInput) (any, error) {
	m, err := s.describeMachine(in.StateMachineArn)
	if err != nil {
		return nil, err
	}
	role := m.roleARN
	if role == "" {
		role = s.identity.ARN("iam", "role/stratus")
	}
	return map[string]any{
		"stateMachineArn": m.arn, "name": m.name, "status": "ACTIVE", "definition": m.definition,
		"roleArn": role, "type": m.typ, "creationDate": epoch(m.created),
		"loggingConfiguration": map[string]any{"level": "OFF", "includeExecutionData": false},
		"tracingConfiguration": map[string]any{"enabled": false},
	}, nil
}

type listInput struct {
	StateMachineArn string //nolint:revive // the wire name
	StatusFilter    string
	MaxResults      int
	NextToken       string
}

func (s *Service) opListStateMachines(in listInput) (any, error) {
	all := s.listMachines()
	items, next, err := page(all, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, len(items))
	for i, m := range items {
		out[i] = map[string]any{"stateMachineArn": m.arn, "name": m.name, "type": m.typ, "creationDate": epoch(m.created)}
	}
	return withToken(map[string]any{"stateMachines": out}, next), nil
}

func withToken(out map[string]any, next string) map[string]any {
	if next != "" {
		out["nextToken"] = next
	}
	return out
}

func (s *Service) opDeleteStateMachine(in machineInput) (any, error) {
	return empty{}, s.deleteStateMachine(in.StateMachineArn)
}

type startExecutionInput struct {
	StateMachineArn string //nolint:revive // the wire name
	Name            string
	Input           string
}

func (s *Service) opStartExecution(in startExecutionInput) (any, error) {
	arn, err := s.StartExecution(in.StateMachineArn, in.Name, in.Input)
	if err != nil {
		return nil, err
	}
	v, err := s.describeExecution(arn)
	if err != nil {
		return nil, err
	}
	return map[string]any{"executionArn": arn, "startDate": epoch(v.started)}, nil
}

type stopExecutionInput struct {
	ExecutionArn string //nolint:revive // the wire name
	Error        string
	Cause        string
}

func (s *Service) opStopExecution(in stopExecutionInput) (any, error) {
	at, err := s.stopExecution(in.ExecutionArn, in.Error, in.Cause)
	if err != nil {
		return nil, err
	}
	return map[string]any{"stopDate": epoch(at)}, nil
}

type executionInput struct {
	ExecutionArn string //nolint:revive // the wire name
}

func (s *Service) opDescribeExecution(in executionInput) (any, error) {
	v, err := s.describeExecution(in.ExecutionArn)
	if err != nil {
		return nil, err
	}
	out := summary(v)
	out["input"] = v.input
	out["inputDetails"] = map[string]any{"included": true}
	if v.status == statusSucceeded {
		out["output"] = v.output
		out["outputDetails"] = map[string]any{"included": true}
	}
	if v.errName != "" {
		out["error"] = v.errName
	}
	if v.cause != "" {
		out["cause"] = v.cause
	}
	return out, nil
}

func summary(v execView) map[string]any {
	out := map[string]any{
		"executionArn": v.arn, "stateMachineArn": v.machineARN, "name": v.name,
		"status": v.status, "startDate": epoch(v.started),
	}
	if v.status != statusRunning {
		out["stopDate"] = epoch(v.stopped)
	}
	return out
}

var executionStatuses = []string{statusRunning, statusSucceeded, statusFailed, statusAborted, "TIMED_OUT", "PENDING_REDRIVE"}

func (s *Service) opListExecutions(in listInput) (any, error) {
	if in.StatusFilter != "" && !slices.Contains(executionStatuses, in.StatusFilter) {
		return nil, errValidation("Value '" + in.StatusFilter + "' at 'statusFilter' is not a valid execution status")
	}
	all, err := s.listExecutions(in.StateMachineArn, in.StatusFilter)
	if err != nil {
		return nil, err
	}
	items, next, err := page(all, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, len(items))
	for i, v := range items {
		out[i] = summary(v)
	}
	return withToken(map[string]any{"executions": out}, next), nil
}

type historyInput struct {
	ExecutionArn string //nolint:revive // the wire name
	ReverseOrder bool
	MaxResults   int
	NextToken    string
}

func (s *Service) opGetExecutionHistory(in historyInput) (any, error) {
	v, err := s.describeExecution(in.ExecutionArn)
	if err != nil {
		return nil, err
	}
	events := v.history
	if in.ReverseOrder {
		slices.Reverse(events)
	}
	items, next, err := page(events, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, len(items))
	for i, ev := range items {
		out[i] = ev.wire()
	}
	return withToken(map[string]any{"events": out}, next), nil
}

type sendSuccessInput struct {
	TaskToken string
	Output    string
}

func (s *Service) opSendTaskSuccess(in sendSuccessInput) (any, error) {
	return empty{}, s.sendTaskSuccess(in.TaskToken, in.Output)
}

type sendFailureInput struct {
	TaskToken string
	Error     string
	Cause     string
}

func (s *Service) opSendTaskFailure(in sendFailureInput) (any, error) {
	return empty{}, s.sendTaskFailure(in.TaskToken, in.Error, in.Cause)
}

type heartbeatInput struct {
	TaskToken string
}

func (s *Service) opSendTaskHeartbeat(in heartbeatInput) (any, error) {
	return empty{}, s.sendTaskHeartbeat(in.TaskToken)
}
