package sqs

import (
	"net/http"

	"github.com/ODudek/stratus/internal/awsapi"
)

func errQueueNotFound() *awsapi.Error {
	return &awsapi.Error{
		Status:    http.StatusBadRequest,
		Code:      "QueueDoesNotExist",
		QueryCode: "AWS.SimpleQueueService.NonExistentQueue",
		Message:   "The specified queue does not exist.",
	}
}

func errQueueNameExists() *awsapi.Error {
	return &awsapi.Error{
		Status:    http.StatusBadRequest,
		Code:      "QueueNameExists",
		QueryCode: "QueueAlreadyExists",
		Message:   "A queue already exists with the same name and a different value for one of its attributes.",
	}
}

func errInvalidAttributeName(name string) *awsapi.Error {
	return &awsapi.Error{
		Status:  http.StatusBadRequest,
		Code:    "InvalidAttributeName",
		Message: "Unknown or unsupported attribute " + name + ".",
	}
}

func errInvalidAttributeValue(name, reason string) *awsapi.Error {
	return &awsapi.Error{
		Status:  http.StatusBadRequest,
		Code:    "InvalidAttributeValue",
		Message: "Invalid value for the parameter " + name + ". Reason: " + reason,
	}
}

func errInvalidParameter(message string) *awsapi.Error {
	return &awsapi.Error{Status: http.StatusBadRequest, Code: "InvalidParameterValue", Message: message}
}

func errMissingParameter(name string) *awsapi.Error {
	return &awsapi.Error{
		Status:  http.StatusBadRequest,
		Code:    "MissingParameter",
		Message: "The request must contain the parameter " + name + ".",
	}
}

func errReceiptHandleInvalid() *awsapi.Error {
	return &awsapi.Error{
		Status:  http.StatusBadRequest,
		Code:    "ReceiptHandleIsInvalid",
		Message: "The input receipt handle is invalid.",
	}
}

func errMessageNotInflight() *awsapi.Error {
	return &awsapi.Error{
		Status:    http.StatusBadRequest,
		Code:      "MessageNotInflight",
		QueryCode: "AWS.SimpleQueueService.MessageNotInflight",
		Message:   "The message referred to isn't in flight.",
	}
}
