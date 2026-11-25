package dynamodb

import (
	"errors"
	"maps"

	"github.com/ODudek/stratus/internal/dynamodb/attr"
	"github.com/ODudek/stratus/internal/dynamodb/expr"
)

// write is a change worked out, and checked, under the lock but not yet
// applied, so that a batch or a transaction can check every change before it
// applies any.
type write struct {
	t   *table
	key attr.Item
	old attr.Item // the item before the change; nil when there was none
	new attr.Item // the item after it; nil when the item is deleted
	// checkOnly marks a transaction's ConditionCheck, which changes nothing.
	checkOnly bool
}

func (w write) apply() {
	switch {
	case w.checkOnly:
	case w.new == nil:
		w.t.remove(w.key)
	default:
		w.t.put(w.new)
	}
}

// conditionFailed is a ConditionExpression that does not hold; old is the
// item it was evaluated on.
type conditionFailed struct {
	old attr.Item
}

func (e *conditionFailed) Error() string { return "The conditional request failed" }

// checkCondition evaluates cond on old, the current item or nil.
func checkCondition(cond *expr.Condition, old attr.Item) error {
	item := old
	if item == nil {
		item = attr.Item{}
	}
	ok, err := cond.Eval(item)
	if err != nil {
		return err
	}
	if !ok {
		return &conditionFailed{old: old}
	}
	return nil
}

// asConditionalCheckFailed turns a failed condition into the client's error,
// with the old item when the request asked for ALL_OLD.
func asConditionalCheckFailed(err error, returnOnFailure string) error {
	var failed *conditionFailed
	if !errors.As(err, &failed) {
		return err
	}
	if returnOnFailure == "ALL_OLD" {
		return errConditionFailed(failed.old)
	}
	return errConditionFailed(nil)
}

func (t *table) preparePut(item attr.Item, cond *expr.Condition) (write, error) {
	if err := t.checkItem(item); err != nil {
		return write{}, err
	}
	key := t.keyOf(item)
	old := t.get(key)
	if err := checkCondition(cond, old); err != nil {
		return write{}, err
	}
	return write{t: t, key: key, old: old, new: item}, nil
}

func (t *table) prepareDelete(key attr.Item, cond *expr.Condition) (write, error) {
	if err := t.checkKey(key); err != nil {
		return write{}, err
	}
	old := t.get(key)
	if err := checkCondition(cond, old); err != nil {
		return write{}, err
	}
	return write{t: t, key: key, old: old}, nil
}

func (t *table) prepareConditionCheck(key attr.Item, cond *expr.Condition) (write, error) {
	w, err := t.prepareDelete(key, cond)
	w.checkOnly = true
	return w, err
}

// prepareUpdate applies upd (nil: no change) to the item of key, or to a new
// item made of the key when there is none, as UpdateItem upserts.
func (t *table) prepareUpdate(key attr.Item, upd *expr.Update, cond *expr.Condition) (write, error) {
	if err := t.checkKey(key); err != nil {
		return write{}, err
	}
	old := t.get(key)
	if err := checkCondition(cond, old); err != nil {
		return write{}, err
	}
	base := old
	if base == nil {
		base = maps.Clone(key)
	}
	updated := base
	if upd != nil {
		var err error
		if updated, err = upd.Apply(base); err != nil {
			return write{}, err
		}
	}
	if err := t.checkItem(updated); err != nil {
		return write{}, err
	}
	return write{t: t, key: key, old: old, new: updated}, nil
}

// parseUpdate parses an UpdateExpression and rejects one that writes a key
// attribute.
func (t *table) parseUpdate(s string, ctx *expr.Context) (*expr.Update, error) {
	if s == "" {
		return nil, nil
	}
	upd, err := expr.ParseUpdate(s, ctx)
	if err != nil {
		return nil, err
	}
	tops := make([]string, 0, len(upd.Paths()))
	for _, p := range upd.Paths() {
		tops = append(tops, p.Top())
	}
	return upd, t.checkUpdatedKey(tops)
}

func checkReturnOnFailure(v string) error {
	if v != "" && v != "NONE" && v != "ALL_OLD" {
		return errValidation("1 validation error detected: Value '%s' at 'returnValuesOnConditionCheckFailure' "+
			"failed to satisfy constraint: Member must satisfy enum value set: [ALL_OLD, NONE]", v)
	}
	return nil
}
