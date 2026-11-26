package dynamodb

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/stratus-emulator/stratus/internal/awsapi"
	"github.com/stratus-emulator/stratus/internal/dynamodb/attr"
	"github.com/stratus-emulator/stratus/internal/dynamodb/expr"
)

const (
	targetPrefix = "DynamoDB_20120810."
	contentType  = "application/x-amz-json-1.0"

	// maxRequestBytes is DynamoDB's request size limit.
	maxRequestBytes = 16 << 20
)

type operation func(s *Service, body []byte) (any, error)

var operations = map[string]operation{
	"CreateTable":        op((*Service).createTableOp),
	"DeleteTable":        op((*Service).deleteTableOp),
	"DescribeTable":      op((*Service).describeTableOp),
	"ListTables":         op((*Service).listTablesOp),
	"UpdateTimeToLive":   op((*Service).updateTimeToLiveOp),
	"DescribeTimeToLive": op((*Service).describeTimeToLiveOp),
	"PutItem":            op((*Service).putItemOp),
	"GetItem":            op((*Service).getItemOp),
	"UpdateItem":         op((*Service).updateItemOp),
	"DeleteItem":         op((*Service).deleteItemOp),
	"Query":              op((*Service).queryOp),
	"Scan":               op((*Service).scanOp),
	"BatchGetItem":       op((*Service).batchGetItemOp),
	"BatchWriteItem":     op((*Service).batchWriteItemOp),
	"TransactWriteItems": op((*Service).transactWriteItemsOp),
	"TransactGetItems":   op((*Service).transactGetItemsOp),
}

// op adapts a typed operation: it decodes the request body into In. An
// invalid attribute value is a ValidationException, other bad JSON a
// SerializationException.
func op[In any](fn func(*Service, In) (any, error)) operation {
	return func(s *Service, body []byte) (any, error) {
		var in In
		if len(body) > 0 {
			if err := json.Unmarshal(body, &in); err != nil {
				var invalid *attr.ValidationError
				if errors.As(err, &invalid) {
					return nil, errValidation("%s", invalid.Message)
				}
				return nil, errSerialization(err)
			}
		}
		return fn(s, in)
	}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	out, err := s.serve(w, r)
	if err != nil {
		writeError(w, asAPIError(err))
		return
	}
	awsapi.WriteJSON(w, contentType, http.StatusOK, out)
}

func (s *Service) serve(w http.ResponseWriter, r *http.Request) (any, error) {
	target := r.Header.Get("X-Amz-Target")
	if target == "" {
		return nil, &apiError{
			status:    http.StatusBadRequest,
			namespace: namespace,
			code:      "UnknownOperationException",
			message:   "stratus speaks only the DynamoDB JSON protocol: the request has no X-Amz-Target header.",
		}
	}
	name := strings.TrimPrefix(target, targetPrefix)
	handle, ok := operations[name]
	if !ok {
		return nil, errUnknownOperation(name)
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	if err != nil {
		return nil, errValidation("The request body could not be read or is too large.")
	}
	return handle(s, body)
}

// expressionInput holds the placeholders every expression-taking request has.
type expressionInput struct {
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]attr.Value
}

// context checks the placeholders and returns their context. used tells
// whether the request has any expression at all, as DynamoDB rejects
// placeholders without one.
func (in expressionInput) context(used bool) (*expr.Context, error) {
	names, values := in.ExpressionAttributeNames, in.ExpressionAttributeValues
	switch {
	case names != nil && len(names) == 0:
		return nil, errValidation("ExpressionAttributeNames must not be empty")
	case values != nil && len(values) == 0:
		return nil, errValidation("ExpressionAttributeValues must not be empty")
	case names != nil && !used:
		return nil, errValidation("ExpressionAttributeNames can only be specified when using expressions")
	case values != nil && !used:
		return nil, errValidation("ExpressionAttributeValues can only be specified when using expressions")
	}
	return expr.NewContext(names, values), nil
}

// legacyInput holds the pre-expression parameters, which stratus rejects
// rather than ignore.
type legacyInput struct {
	Expected            json.RawMessage
	AttributesToGet     json.RawMessage
	KeyConditions       json.RawMessage
	QueryFilter         json.RawMessage
	ScanFilter          json.RawMessage
	AttributeUpdates    json.RawMessage
	ConditionalOperator json.RawMessage
}

func (in legacyInput) check() error {
	for name, v := range map[string]json.RawMessage{
		"Expected": in.Expected, "AttributesToGet": in.AttributesToGet, "KeyConditions": in.KeyConditions,
		"QueryFilter": in.QueryFilter, "ScanFilter": in.ScanFilter, "AttributeUpdates": in.AttributeUpdates,
		"ConditionalOperator": in.ConditionalOperator,
	} {
		if v != nil {
			return errValidation("stratus does not support the legacy parameter %s; use expressions "+
				"(ConditionExpression, KeyConditionExpression, FilterExpression, ProjectionExpression, "+
				"UpdateExpression) instead", name)
		}
	}
	return nil
}

// parseProjection parses an optional ProjectionExpression.
func parseProjection(s string, ctx *expr.Context) ([]expr.Path, error) {
	if s == "" {
		return nil, nil
	}
	return expr.ParseProjection(s, ctx)
}

// parseCondition parses an optional condition or filter expression.
func parseCondition(kind, s string, ctx *expr.Context) (*expr.Condition, error) {
	if s == "" {
		return nil, nil
	}
	return expr.ParseCondition(kind, s, ctx)
}

// project applies a parsed projection, or returns the item when there is none.
func project(item attr.Item, paths []expr.Path) attr.Item {
	if paths == nil || item == nil {
		return item
	}
	return expr.Project(item, paths)
}

type empty struct{}
