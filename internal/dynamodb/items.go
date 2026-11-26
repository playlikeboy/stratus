package dynamodb

import (
	"slices"

	"github.com/stratus-emulator/stratus/internal/dynamodb/attr"
	"github.com/stratus-emulator/stratus/internal/dynamodb/expr"
)

// attributesOutput is the response of the single-item writes.
type attributesOutput struct {
	Attributes attr.Item `json:",omitempty"`
}

// writeOptions are the parameters PutItem, UpdateItem and DeleteItem share.
type writeOptions struct {
	expressionInput
	legacyInput
	TableName                           string
	ConditionExpression                 string
	ReturnValues                        string
	ReturnValuesOnConditionCheckFailure string
}

// parse checks the shared parameters and parses the condition. allowed are
// the ReturnValues the operation accepts; more is true when the request has
// expressions besides the condition.
func (o writeOptions) parse(allowed []string, more bool) (*expr.Context, *expr.Condition, error) {
	if err := o.check(); err != nil {
		return nil, nil, err
	}
	if o.ReturnValues != "" && !slices.Contains(allowed, o.ReturnValues) {
		return nil, nil, errValidation("Return values set to invalid value")
	}
	if err := checkReturnOnFailure(o.ReturnValuesOnConditionCheckFailure); err != nil {
		return nil, nil, err
	}
	ctx, err := o.context(more || o.ConditionExpression != "")
	if err != nil {
		return nil, nil, err
	}
	cond, err := parseCondition("ConditionExpression", o.ConditionExpression, ctx)
	return ctx, cond, err
}

type putItemInput struct {
	writeOptions
	Item attr.Item
}

func (s *Service) putItemOp(in putItemInput) (any, error) {
	ctx, cond, err := in.parse([]string{"NONE", "ALL_OLD"}, false)
	if err != nil {
		return nil, err
	}
	if err := ctx.CheckUnused(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(in.TableName)
	if err != nil {
		return nil, err
	}
	if in.Item == nil {
		return nil, errValidation("1 validation error detected: Value null at 'item' failed to satisfy " +
			"constraint: Member must not be null")
	}
	w, err := t.preparePut(in.Item, cond)
	if err != nil {
		return nil, asConditionalCheckFailed(err, in.ReturnValuesOnConditionCheckFailure)
	}
	w.apply()
	if in.ReturnValues == "ALL_OLD" {
		return attributesOutput{w.old}, nil
	}
	return attributesOutput{}, nil
}

type deleteItemInput struct {
	writeOptions
	Key attr.Item
}

func (s *Service) deleteItemOp(in deleteItemInput) (any, error) {
	ctx, cond, err := in.parse([]string{"NONE", "ALL_OLD"}, false)
	if err != nil {
		return nil, err
	}
	if err := ctx.CheckUnused(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(in.TableName)
	if err != nil {
		return nil, err
	}
	w, err := t.prepareDelete(in.Key, cond)
	if err != nil {
		return nil, asConditionalCheckFailed(err, in.ReturnValuesOnConditionCheckFailure)
	}
	w.apply()
	if in.ReturnValues == "ALL_OLD" {
		return attributesOutput{w.old}, nil
	}
	return attributesOutput{}, nil
}

type updateItemInput struct {
	writeOptions
	Key              attr.Item
	UpdateExpression string
}

func (s *Service) updateItemOp(in updateItemInput) (any, error) {
	ctx, cond, err := in.parse([]string{"NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", "UPDATED_NEW"},
		in.UpdateExpression != "")
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(in.TableName)
	if err != nil {
		return nil, err
	}
	upd, err := t.parseUpdate(in.UpdateExpression, ctx)
	if err != nil {
		return nil, err
	}
	if err := ctx.CheckUnused(); err != nil {
		return nil, err
	}
	w, err := t.prepareUpdate(in.Key, upd, cond)
	if err != nil {
		return nil, asConditionalCheckFailed(err, in.ReturnValuesOnConditionCheckFailure)
	}
	w.apply()
	return attributesOutput{updateReturnValues(in.ReturnValues, w, upd)}, nil
}

// updateReturnValues picks what UpdateItem returns. UPDATED_OLD and
// UPDATED_NEW return only the paths the update wrote.
func updateReturnValues(rv string, w write, upd *expr.Update) attr.Item {
	var item attr.Item
	switch rv {
	case "ALL_OLD":
		item = w.old
	case "ALL_NEW":
		item = w.new
	case "UPDATED_OLD":
		if upd != nil && w.old != nil {
			item = expr.Project(w.old, upd.Paths())
		}
	case "UPDATED_NEW":
		if upd != nil {
			item = expr.Project(w.new, upd.Paths())
		}
	}
	if len(item) == 0 {
		return nil
	}
	return item
}

type getItemInput struct {
	expressionInput
	legacyInput
	TableName            string
	Key                  attr.Item
	ProjectionExpression string
	ConsistentRead       bool
}

func (s *Service) getItemOp(in getItemInput) (any, error) {
	if err := in.check(); err != nil {
		return nil, err
	}
	ctx, err := in.context(in.ProjectionExpression != "")
	if err != nil {
		return nil, err
	}
	paths, err := parseProjection(in.ProjectionExpression, ctx)
	if err != nil {
		return nil, err
	}
	if err := ctx.CheckUnused(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(in.TableName)
	if err != nil {
		return nil, err
	}
	if err := t.checkKey(in.Key); err != nil {
		return nil, err
	}
	item := t.get(in.Key)
	if item == nil {
		return empty{}, nil
	}
	// A found item is returned even when the projection leaves it empty.
	return map[string]attr.Item{"Item": project(item, paths)}, nil
}
