package kinesis

import (
	"net/http"

	"github.com/ODudek/stratus/internal/awsapi"
)

// Every Kinesis error is an HTTP 400 whose "__type" is the bare shape name.

func apiError(code, message string) *awsapi.Error {
	return &awsapi.Error{Status: http.StatusBadRequest, Code: code, Message: message}
}

func errNotFound(message string) *awsapi.Error { return apiError("ResourceNotFoundException", message) }

func errStreamNotFound(name, account string) *awsapi.Error {
	return errNotFound("Stream " + name + " under account " + account + " not found.")
}

func errInUse(message string) *awsapi.Error { return apiError("ResourceInUseException", message) }

func errInvalidArgument(message string) *awsapi.Error {
	return apiError("InvalidArgumentException", message)
}

func errValidation(message string) *awsapi.Error { return apiError("ValidationException", message) }

func errLimitExceeded(message string) *awsapi.Error {
	return apiError("LimitExceededException", message)
}

func errExpiredIterator() *awsapi.Error {
	return apiError("ExpiredIteratorException", "The shard iterator has expired. Shard iterators are only valid for 5 minutes.")
}

// errUnsupported names the operation, so a client knows it is the emulator and
// not its own request that is wrong.
func errUnsupported(operation string) *awsapi.Error {
	return apiError("UnsupportedOperation", "stratus does not implement the Kinesis operation "+operation+".")
}

// errConstraint is the ValidationException the real service sends for a
// parameter outside its documented range.
func errConstraint(field, constraint string) *awsapi.Error {
	return errValidation("1 validation error detected: Value at '" + field + "' failed to satisfy constraint: " + constraint)
}
