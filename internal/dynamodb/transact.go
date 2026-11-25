package dynamodb

import (
	"errors"

	"github.com/ODudek/stratus/internal/dynamodb/attr"
	"github.com/ODudek/stratus/internal/dynamodb/expr"
)

const maxTransactItems = 100

// transactCommon holds the parameters every transaction action has.
type transactCommon struct {
	expressionInput
	TableName                           string
	ConditionExpression                 string
	ReturnValuesOnConditionCheckFailure string
}

type transactAction struct {
	transactCommon
	Item             attr.Item // Put
	Key              attr.Item // Update, Delete, ConditionCheck
	UpdateExpression string    // Update
}

type transactWriteItem struct {
	ConditionCheck *transactAction
	Put            *transactAction
	Delete         *transactAction
	Update         *transactAction
}

type transactWriteItemsInput struct {
	TransactItems      []transactWriteItem
	ClientRequestToken string // accepted; idempotency is not emulated
}

// txAction is a parsed and statically checked transaction action.
type txAction struct {
	kind string
	t    *table
	in   *transactAction
	cond *expr.Condition
	upd  *expr.Update
}

// transactWriteItemsOp checks every action, then applies all of them or, when
// a condition fails or a change is invalid, none, cancelling the transaction
// with one reason per action.
func (s *Service) transactWriteItemsOp(in transactWriteItemsInput) (any, error) {
	if err := checkTransactSize(len(in.TransactItems)); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	actions := make([]txAction, len(in.TransactItems))
	seen := map[string]bool{}
	for i, item := range in.TransactItems {
		a, err := s.parseTransactAction(item)
		if err != nil {
			return nil, err
		}
		id := a.t.spec.Name + "\x00" + a.t.keyID(a.key())
		if seen[id] {
			return nil, errValidation("Transaction request cannot include multiple operations on one item")
		}
		seen[id] = true
		actions[i] = a
	}
	writes := make([]write, len(actions))
	reasons := make([]cancellationReason, len(actions))
	cancelled := false
	for i, a := range actions {
		w, err := a.prepare()
		reasons[i] = reasonFor(err, a.in.ReturnValuesOnConditionCheckFailure)
		if err != nil {
			cancelled = true
		}
		writes[i] = w
	}
	if cancelled {
		return nil, errTransactionCanceled(reasons)
	}
	for _, w := range writes {
		w.apply()
	}
	return empty{}, nil
}

func checkTransactSize(n int) error {
	if n < 1 || n > maxTransactItems {
		return errValidation("1 validation error detected: Value at 'transactItems' failed to satisfy "+
			"constraint: Member must have length less than or equal to %d and greater than or equal to 1",
			maxTransactItems)
	}
	return nil
}

func (s *Service) parseTransactAction(item transactWriteItem) (txAction, error) {
	var a txAction
	count := 0
	for kind, in := range map[string]*transactAction{
		"ConditionCheck": item.ConditionCheck, "Put": item.Put, "Delete": item.Delete, "Update": item.Update,
	} {
		if in != nil {
			a.kind, a.in = kind, in
			count++
		}
	}
	if count != 1 {
		return a, errValidation("TransactItems can only contain one of Check, Put, Update or Delete")
	}
	if a.kind == "ConditionCheck" && a.in.ConditionExpression == "" {
		return a, errValidation("The ConditionExpression parameter is required for a ConditionCheck")
	}
	if err := checkReturnOnFailure(a.in.ReturnValuesOnConditionCheckFailure); err != nil {
		return a, err
	}
	t, err := s.table(a.in.TableName)
	if err != nil {
		return a, err
	}
	a.t = t
	ctx, err := a.in.context(a.in.ConditionExpression != "" || a.in.UpdateExpression != "")
	if err != nil {
		return a, err
	}
	if a.cond, err = parseCondition("ConditionExpression", a.in.ConditionExpression, ctx); err != nil {
		return a, err
	}
	if a.kind == "Update" {
		if a.upd, err = t.parseUpdate(a.in.UpdateExpression, ctx); err != nil {
			return a, err
		}
	}
	if err := ctx.CheckUnused(); err != nil {
		return a, err
	}
	if a.kind == "Put" {
		return a, t.checkItem(a.in.Item)
	}
	return a, t.checkKey(a.in.Key)
}

func (a txAction) key() attr.Item {
	if a.kind == "Put" {
		return a.t.keyOf(a.in.Item)
	}
	return a.in.Key
}

func (a txAction) prepare() (write, error) {
	switch a.kind {
	case "Put":
		return a.t.preparePut(a.in.Item, a.cond)
	case "Update":
		return a.t.prepareUpdate(a.in.Key, a.upd, a.cond)
	case "Delete":
		return a.t.prepareDelete(a.in.Key, a.cond)
	default:
		return a.t.prepareConditionCheck(a.in.Key, a.cond)
	}
}

// reasonFor is the cancellation reason of one action: None when it would have
// succeeded, ConditionalCheckFailed, or ValidationError for a change that
// cannot be made.
func reasonFor(err error, returnOnFailure string) cancellationReason {
	if err == nil {
		return cancellationReason{Code: "None"}
	}
	var failed *conditionFailed
	if errors.As(err, &failed) {
		r := cancellationReason{Code: "ConditionalCheckFailed", Message: "The conditional request failed"}
		if returnOnFailure == "ALL_OLD" {
			r.Item = failed.old
		}
		return r
	}
	return cancellationReason{Code: "ValidationError", Message: asAPIError(err).message}
}

type transactGetItem struct {
	Get *struct {
		expressionInput
		TableName            string
		Key                  attr.Item
		ProjectionExpression string
	}
}

type itemResponse struct {
	Item attr.Item `json:",omitempty"`
}

// transactGetItemsOp reads every item at one point in time, under the lock.
func (s *Service) transactGetItemsOp(in struct{ TransactItems []transactGetItem }) (any, error) {
	if err := checkTransactSize(len(in.TransactItems)); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	responses := make([]itemResponse, len(in.TransactItems))
	seen := map[string]bool{}
	for i, item := range in.TransactItems {
		g := item.Get
		if g == nil {
			return nil, errValidation("TransactItems can only contain Get")
		}
		ctx, err := g.context(g.ProjectionExpression != "")
		if err != nil {
			return nil, err
		}
		paths, err := parseProjection(g.ProjectionExpression, ctx)
		if err != nil {
			return nil, err
		}
		if err := ctx.CheckUnused(); err != nil {
			return nil, err
		}
		t, err := s.table(g.TableName)
		if err != nil {
			return nil, err
		}
		if err := t.checkKey(g.Key); err != nil {
			return nil, err
		}
		id := g.TableName + "\x00" + t.keyID(g.Key)
		if seen[id] {
			return nil, errValidation("Transaction request cannot include multiple operations on one item")
		}
		seen[id] = true
		responses[i] = itemResponse{project(t.get(g.Key), paths)}
	}
	return map[string][]itemResponse{"Responses": responses}, nil
}
