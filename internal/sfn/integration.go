package sfn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Integration runs one Task state's call; parameters and the result are JSON.
type Integration func(ctx context.Context, parameters json.RawMessage) (json.RawMessage, error)

// IntegrationError is an error a Task can fail with. Name is what a Retry or
// Catch ErrorEquals matches on (the "Error" of the state machine error
// output); Cause is the human-readable detail. The field is called Name, not
// Error, because a type cannot have both a field and a method of that name.
type IntegrationError struct {
	Name  string
	Cause string
}

func (e *IntegrationError) Error() string {
	if e.Cause == "" {
		return e.Name
	}
	return e.Name + ": " + e.Cause
}

// Names of the errors the interpreter itself raises.
const (
	errTaskFailed = "States.TaskFailed"
	errTimeout    = "States.Timeout"
	errRuntime    = "States.Runtime"
	errAll        = "States.ALL"
)

func runtimeError(format string, args ...any) *IntegrationError {
	return &IntegrationError{Name: errRuntime, Cause: fmt.Sprintf(format, args...)}
}

// errCancelled ends an execution goroutine whose execution was stopped, or
// whose service was reset; the stopper has already recorded the outcome.
var errCancelled = errors.New("sfn: execution cancelled")

// errExecTimeout ends an execution that outlived the machine's TimeoutSeconds.
var errExecTimeout = errors.New("sfn: execution timed out")

// RegisterIntegration makes fn the implementation of Task states whose
// Resource is resource, without a ".waitForTaskToken" suffix, for example
// "arn:aws:states:::sqs:sendMessage". Registering again replaces it.
func (s *Service) RegisterIntegration(resource string, fn Integration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.integrations[resource] = fn
}

func (s *Service) integration(resource string) (Integration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn, ok := s.integrations[resource]
	return fn, ok
}

// classify turns what an integration returned into the error a state sees.
func classify(err error) error {
	var ie *IntegrationError
	switch {
	case errors.As(err, &ie):
		return ie
	case errors.Is(err, context.Canceled):
		return errCancelled
	default:
		return &IntegrationError{Name: errTaskFailed, Cause: err.Error()}
	}
}

// splitResource separates a Task's Resource from its integration pattern
// suffix. ".sync" patterns need job polling, which stratus does not emulate.
func splitResource(resource string) (base string, waitToken bool, err error) {
	switch {
	case strings.HasSuffix(resource, ".waitForTaskToken"):
		return strings.TrimSuffix(resource, ".waitForTaskToken"), true, nil
	case strings.HasSuffix(resource, ".sync"), strings.HasSuffix(resource, ".sync:2"):
		return "", false, fmt.Errorf("the .sync integration pattern of resource %q is not supported by stratus", resource)
	}
	return resource, false, nil
}
