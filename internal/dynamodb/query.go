package dynamodb

import (
	"hash/fnv"

	"github.com/ODudek/stratus/internal/dynamodb/attr"
	"github.com/ODudek/stratus/internal/dynamodb/expr"
)

type queryInput struct {
	readOptions
	KeyConditionExpression string
	ScanIndexForward       *bool
}

func (s *Service) queryOp(in queryInput) (any, error) {
	if err := in.check(); err != nil {
		return nil, err
	}
	if in.KeyConditionExpression == "" {
		return nil, errValidation("Either the KeyConditions or KeyConditionExpression parameter must be " +
			"specified in the request.")
	}
	ctx, err := in.context(true)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.prepare(in.readOptions, ctx)
	if err != nil {
		return nil, err
	}
	kc, err := expr.ParseKeyCondition(in.KeyConditionExpression, ctx, r.ix.pk, r.ix.sk)
	if err != nil {
		return nil, err
	}
	if err := r.checkKeyCondition(kc); err != nil {
		return nil, err
	}
	if err := ctx.CheckUnused(); err != nil {
		return nil, err
	}
	if in.ScanIndexForward != nil {
		r.forward = *in.ScanIndexForward
	}
	var candidates []attr.Item
	for _, item := range r.t.partitionItems(r.ix, kc.Partition.Value) {
		if kc.Sort == nil || kc.Sort.Match(item[kc.Sort.Name]) {
			candidates = append(candidates, item)
		}
	}
	return r.read(candidates, in.ExclusiveStartKey)
}

// checkKeyCondition checks the value types of a key condition against the
// key schema, and that the filter leaves the keys alone, as DynamoDB wants
// key attributes only in the key condition.
func (r *reader) checkKeyCondition(kc expr.KeyCondition) error {
	parts := []expr.KeyPart{kc.Partition}
	if kc.Sort != nil {
		parts = append(parts, *kc.Sort)
	}
	for _, p := range parts {
		want := r.t.types[p.Name]
		if p.Value.Type != want || (p.Op == "BETWEEN" && p.Upper.Type != want) {
			return errValidation("One or more parameter values were invalid: Condition parameter type does " +
				"not match schema type")
		}
		if p.Op == "begins_with" && want == attr.N {
			return errValidation("Invalid KeyConditionExpression: Incorrect operand type for operator or "+
				"function; operator or function: begins_with, operand type: %s", want)
		}
		if p.Op == "BETWEEN" {
			if c, _ := attr.Compare(p.Value, p.Upper); c > 0 {
				return errValidation("Invalid KeyConditionExpression: The BETWEEN operator requires upper " +
					"bound to be greater than or equal to lower bound")
			}
		}
	}
	if r.filter == nil {
		return nil
	}
	for _, p := range r.filter.Paths() {
		if p.Top() == r.ix.pk || p.Top() == r.ix.sk {
			return errValidation("Filter Expression can only contain non-primary key attributes: Primary key "+
				"attribute: %s", p.Top())
		}
	}
	return nil
}

type scanInput struct {
	readOptions
	Segment       *int
	TotalSegments *int
}

func (s *Service) scanOp(in scanInput) (any, error) {
	if err := in.check(); err != nil {
		return nil, err
	}
	if err := in.checkSegments(); err != nil {
		return nil, err
	}
	ctx, err := in.context(in.FilterExpression != "" || in.ProjectionExpression != "")
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.prepare(in.readOptions, ctx)
	if err != nil {
		return nil, err
	}
	if err := ctx.CheckUnused(); err != nil {
		return nil, err
	}
	candidates := r.t.scanItems(r.ix)
	if in.TotalSegments != nil {
		candidates = segment(candidates, r.ix.pk, *in.Segment, *in.TotalSegments)
	}
	return r.read(candidates, in.ExclusiveStartKey)
}

func (in scanInput) checkSegments() error {
	switch {
	case in.Segment != nil && in.TotalSegments == nil:
		return errValidation("The TotalSegments parameter is required but was not present in the request " +
			"when parameter Segment is present")
	case in.Segment == nil && in.TotalSegments != nil:
		return errValidation("The Segment parameter is required but was not present in the request when " +
			"parameter TotalSegments is present")
	case in.Segment == nil:
		return nil
	case *in.TotalSegments < 1 || *in.TotalSegments > 1000000:
		return errValidation("1 validation error detected: Value '%d' at 'totalSegments' failed to satisfy "+
			"constraint: Member must have value less than or equal to 1000000 and greater than or equal to 1",
			*in.TotalSegments)
	case *in.Segment < 0 || *in.Segment >= *in.TotalSegments:
		return errValidation("The Segment parameter is zero-based and must be less than parameter "+
			"TotalSegments: Segment: %d is not less than TotalSegments: %d", *in.Segment, *in.TotalSegments)
	}
	return nil
}

// segment keeps the items of one parallel-scan segment: a partition belongs
// to the segment its key hashes to, so the segments split the table.
func segment(items []attr.Item, pk string, seg, total int) []attr.Item {
	var out []attr.Item
	for _, item := range items {
		h := fnv.New32a()
		_, _ = h.Write([]byte(encodeKey(item[pk])))
		if int(h.Sum32()%uint32(total)) == seg {
			out = append(out, item)
		}
	}
	return out
}
