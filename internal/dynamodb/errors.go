package dynamodb

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/ODudek/stratus/internal/awsapi"
	"github.com/ODudek/stratus/internal/dynamodb/attr"
)

const (
	namespace         = "com.amazonaws.dynamodb.v20120810"
	validateNamespace = "com.amazon.coral.validate"
)

// apiError is a DynamoDB error. Unlike awsapi.Error it carries the extra
// members some DynamoDB errors have, and its own namespace, because
// validation errors come from a different one.
type apiError struct {
	status    int
	namespace string
	code      string
	message   string
	// item is the ConditionalCheckFailedException's Item, when asked for.
	item attr.Item
	// reasons are the TransactionCanceledException's CancellationReasons.
	reasons []cancellationReason
}

func (e *apiError) Error() string { return e.code + ": " + e.message }

// cancellationReason explains what one action of a cancelled transaction did.
type cancellationReason struct {
	Code    string    `json:"Code"`
	Message string    `json:"Message,omitempty"`
	Item    attr.Item `json:"Item,omitempty"`
}

func errValidation(format string, args ...any) *apiError {
	return &apiError{
		status:    http.StatusBadRequest,
		namespace: validateNamespace,
		code:      "ValidationException",
		message:   fmt.Sprintf(format, args...),
	}
}

func errTableNotFound(name string) *apiError {
	return &apiError{
		status:    http.StatusBadRequest,
		namespace: namespace,
		code:      "ResourceNotFoundException",
		message:   "Requested resource not found: Table: " + name + " not found",
	}
}

func errTableInUse(name string) *apiError {
	return &apiError{
		status:    http.StatusBadRequest,
		namespace: namespace,
		code:      "ResourceInUseException",
		message:   "Table already exists: " + name,
	}
}

// errConditionFailed is a failed ConditionExpression; item is the item as it
// was, sent back only when the request asked for it with
// ReturnValuesOnConditionCheckFailure=ALL_OLD.
func errConditionFailed(item attr.Item) *apiError {
	return &apiError{
		status:    http.StatusBadRequest,
		namespace: namespace,
		code:      "ConditionalCheckFailedException",
		message:   "The conditional request failed",
		item:      item,
	}
}

func errTransactionCanceled(reasons []cancellationReason) *apiError {
	codes := ""
	for i, r := range reasons {
		if i > 0 {
			codes += ", "
		}
		codes += r.Code
	}
	return &apiError{
		status:    http.StatusBadRequest,
		namespace: namespace,
		code:      "TransactionCanceledException",
		message:   "Transaction cancelled, please refer cancellation reasons for specific reasons [" + codes + "]",
		reasons:   reasons,
	}
}

func errSerialization(err error) *apiError {
	return &apiError{
		status:    http.StatusBadRequest,
		namespace: namespace,
		code:      "SerializationException",
		message:   "The request body is not valid JSON for this operation: " + err.Error(),
	}
}

func errUnknownOperation(name string) *apiError {
	return &apiError{
		status:    http.StatusBadRequest,
		namespace: namespace,
		code:      "UnknownOperationException",
		message:   "stratus does not implement the DynamoDB operation " + name + ".",
	}
}

// asAPIError turns any error into the error the client gets: a
// ValidationError from the attr or expr packages is a ValidationException,
// anything unexpected an InternalServerError.
func asAPIError(err error) *apiError {
	var apiErr *apiError
	if errors.As(err, &apiErr) {
		return apiErr
	}
	var invalid *attr.ValidationError
	if errors.As(err, &invalid) {
		return errValidation("%s", invalid.Message)
	}
	return &apiError{
		status:    http.StatusInternalServerError,
		namespace: namespace,
		code:      "InternalServerError",
		message:   err.Error(),
	}
}

// writeError writes e as the awsJson1.0 protocol does. TransactionCanceled
// spells its message member "Message", the other errors "message".
func writeError(w http.ResponseWriter, e *apiError) {
	body := map[string]any{"__type": e.namespace + "#" + e.code}
	if e.code == "TransactionCanceledException" {
		body["Message"] = e.message
		body["CancellationReasons"] = e.reasons
	} else {
		body["message"] = e.message
	}
	if e.item != nil {
		body["Item"] = e.item
	}
	awsapi.WriteJSON(w, contentType, e.status, body)
}
