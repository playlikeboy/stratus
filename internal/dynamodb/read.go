package dynamodb

import (
	"slices"

	"github.com/stratus-emulator/stratus/internal/dynamodb/attr"
	"github.com/stratus-emulator/stratus/internal/dynamodb/expr"
)

// maxPageBytes is how much item data one Query or Scan page reads at most.
const maxPageBytes = 1 << 20

// Select values.
const (
	selectAll       = "ALL_ATTRIBUTES"
	selectProjected = "ALL_PROJECTED_ATTRIBUTES"
	selectSpecific  = "SPECIFIC_ATTRIBUTES"
	selectCount     = "COUNT"
)

// readOptions are the parameters Query and Scan share.
type readOptions struct {
	expressionInput
	legacyInput
	TableName            string
	IndexName            string
	FilterExpression     string
	ProjectionExpression string
	Select               string
	Limit                *int
	ExclusiveStartKey    attr.Item
	ConsistentRead       bool
}

// reader is a parsed Query or Scan, ready to page through items.
type reader struct {
	t       *table
	ix      *index
	filter  *expr.Condition
	paths   []expr.Path
	sel     string
	limit   int // 0: no limit
	forward bool
}

// page is the response of Query and Scan.
type page struct {
	Items            *[]attr.Item `json:",omitempty"` // nil only for Select COUNT
	Count            int
	ScannedCount     int
	LastEvaluatedKey attr.Item `json:",omitempty"`
}

// prepare resolves the table and index and parses what Query and Scan share;
// the caller holds the lock and calls ctx.CheckUnused once it has parsed the
// rest.
func (s *Service) prepare(o readOptions, ctx *expr.Context) (*reader, error) {
	t, err := s.table(o.TableName)
	if err != nil {
		return nil, err
	}
	r := &reader{t: t, ix: t.base(), forward: true}
	if o.IndexName != "" {
		if r.ix = t.index(o.IndexName); r.ix == nil {
			return nil, errValidation("The table does not have the specified index: %s", o.IndexName)
		}
		if r.ix.global && o.ConsistentRead {
			return nil, errValidation("Consistent reads are not supported on global secondary indexes")
		}
	}
	if o.Limit != nil {
		if *o.Limit < 1 {
			return nil, errValidation("1 validation error detected: Value '%d' at 'limit' failed to satisfy "+
				"constraint: Member must have value greater than or equal to 1", *o.Limit)
		}
		r.limit = *o.Limit
	}
	if o.ExclusiveStartKey != nil {
		if err := t.checkStartKey(r.ix, o.ExclusiveStartKey); err != nil {
			return nil, err
		}
	}
	if r.filter, err = parseCondition("FilterExpression", o.FilterExpression, ctx); err != nil {
		return nil, err
	}
	if r.paths, err = parseProjection(o.ProjectionExpression, ctx); err != nil {
		return nil, err
	}
	r.sel, err = resolveSelect(o.Select, r.paths != nil, r.ix)
	return r, err
}

// resolveSelect applies DynamoDB's default Select and its rules.
func resolveSelect(sel string, projection bool, ix *index) (string, error) {
	switch {
	case sel == "" && projection:
		return selectSpecific, nil
	case sel == "" && ix.name != "":
		return selectProjected, nil
	case sel == "":
		return selectAll, nil
	case !slices.Contains([]string{selectAll, selectProjected, selectSpecific, selectCount}, sel):
		return "", errValidation("1 validation error detected: Value '%s' at 'select' failed to satisfy "+
			"constraint: Member must satisfy enum value set: [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, "+
			"ALL_PROJECTED_ATTRIBUTES]", sel)
	case sel == selectSpecific && !projection:
		return "", errValidation("Select type SPECIFIC_ATTRIBUTES requires a ProjectionExpression")
	case sel != selectSpecific && projection:
		return "", errValidation("Cannot specify a ProjectionExpression with Select type %s", sel)
	case sel == selectProjected && ix.name == "":
		return "", errValidation("ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName")
	case sel == selectAll && ix.global && ix.projection.Type != ProjectAll:
		return "", errValidation("One or more parameter values were invalid: Select type ALL_ATTRIBUTES is "+
			"not supported for global secondary index %s because its projection type is not ALL", ix.name)
	}
	return sel, nil
}

// read pages through items, in index order, from just after start. Limit
// counts items read before the filter, and a page stops after about 1 MB of
// item data, as in DynamoDB.
func (r *reader) read(candidates []attr.Item, start attr.Item) (page, error) {
	if !r.forward {
		candidates = slices.Clone(candidates)
		slices.Reverse(candidates)
	}
	from := 0
	if start != nil {
		from = len(candidates)
		for i, item := range candidates {
			c := r.t.compareBy(r.ix, item, start)
			if (r.forward && c > 0) || (!r.forward && c < 0) {
				from = i
				break
			}
		}
	}
	out := page{}
	items := []attr.Item{}
	if r.sel != selectCount {
		out.Items = &items
	}
	size := 0
	for i := from; i < len(candidates); i++ {
		item := candidates[i]
		out.ScannedCount++
		size += attr.ItemSize(item)
		ok, err := r.filter.Eval(item)
		if err != nil {
			return page{}, err
		}
		if ok {
			out.Count++
			if r.sel != selectCount {
				items = append(items, r.output(item))
			}
		}
		more := i+1 < len(candidates)
		if out.ScannedCount == r.limit || (size >= maxPageBytes && more) {
			out.LastEvaluatedKey = r.t.keyOfIndex(r.ix, item)
			break
		}
	}
	return out, nil
}

// output shapes an item for the response, according to Select.
func (r *reader) output(item attr.Item) attr.Item {
	switch r.sel {
	case selectProjected:
		return r.t.projectIndex(r.ix, item)
	case selectSpecific:
		if r.ix.global {
			item = r.t.projectIndex(r.ix, item)
		}
		return expr.Project(item, r.paths)
	}
	return item
}
