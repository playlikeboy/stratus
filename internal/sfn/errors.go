package sfn

import (
	"net/http"

	"github.com/ODudek/stratus/internal/awsapi"
)

func apiError(code, message string) *awsapi.Error {
	return &awsapi.Error{Status: http.StatusBadRequest, Code: code, Message: message}
}

func errMachineNotFound(arn string) *awsapi.Error {
	return apiError("StateMachineDoesNotExist", "State Machine Does Not Exist: '"+arn+"'")
}

func errMachineExists(name string) *awsapi.Error {
	return apiError("StateMachineAlreadyExists",
		"A state machine named '"+name+"' already exists with a different definition or type.")
}

func errExecutionNotFound(arn string) *awsapi.Error {
	return apiError("ExecutionDoesNotExist", "Execution Does Not Exist: '"+arn+"'")
}

func errExecutionExists(name string) *awsapi.Error {
	return apiError("ExecutionAlreadyExists", "Execution Already Exists: '"+name+"'")
}

func errInvalidDefinition(reason string) *awsapi.Error {
	return apiError("InvalidDefinition", "Invalid State Machine Definition: "+reason)
}

func errInvalidArn(arn string) *awsapi.Error {
	return apiError("InvalidArn", "Invalid Arn: '"+arn+"'")
}

func errInvalidName(name, reason string) *awsapi.Error {
	return apiError("InvalidName", "Invalid Name: '"+name+"' ("+reason+")")
}

func errInvalidInput(reason string) *awsapi.Error {
	return apiError("InvalidExecutionInput", "Invalid State Machine Execution Input: "+reason)
}

func errInvalidOutput(reason string) *awsapi.Error {
	return apiError("InvalidOutput", "Invalid State Machine Execution Output: "+reason)
}

func errInvalidToken() *awsapi.Error {
	return apiError("InvalidToken", "Invalid Token: 'Invalid token'")
}

func errTaskNotFound() *awsapi.Error {
	return apiError("TaskDoesNotExist", "Task Does Not Exist")
}

func errTaskTimedOut() *awsapi.Error {
	return apiError("TaskTimedOut", "Task Timed Out")
}

func errValidation(message string) *awsapi.Error {
	return apiError("ValidationException", message)
}
