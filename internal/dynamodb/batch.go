package dynamodb

import (
	"github.com/ODudek/stratus/internal/dynamodb/attr"
)

const (
	maxBatchGetKeys    = 100
	maxBatchWriteItems = 25
)

// keyID identifies the item of key within its table.
func (t *table) keyID(key attr.Item) string {
	id := encodeKey(key[t.spec.PartitionKey.Name])
	if sk := t.spec.SortKey; sk != nil {
		id += "\x00" + encodeKey(key[sk.Name])
	}
	return id
}

type keysAndAttributes struct {
	expressionInput
	legacyInput
	Keys                 []attr.Item
	ProjectionExpression string
	ConsistentRead       bool
}

type batchGetItemInput struct {
	RequestItems map[string]keysAndAttributes
}

type batchGetItemOutput struct {
	Responses       map[string][]attr.Item
	UnprocessedKeys map[string]keysAndAttributes
}

// batchGetItemOp reads every key; nothing is ever left unprocessed.
func (s *Service) batchGetItemOp(in batchGetItemInput) (any, error) {
	if err := checkBatchGetSize(in.RequestItems); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := batchGetItemOutput{Responses: map[string][]attr.Item{}, UnprocessedKeys: map[string]keysAndAttributes{}}
	for name, req := range in.RequestItems {
		items, err := s.batchGetTable(name, req)
		if err != nil {
			return nil, err
		}
		out.Responses[name] = items
	}
	return out, nil
}

func checkBatchGetSize(requests map[string]keysAndAttributes) error {
	if len(requests) == 0 {
		return errValidation("1 validation error detected: Value null at 'requestItems' failed to satisfy " +
			"constraint: Member must have length greater than or equal to 1")
	}
	total := 0
	for _, req := range requests {
		if len(req.Keys) == 0 {
			return errValidation("1 validation error detected: Value null at 'requestItems.member.keys' " +
				"failed to satisfy constraint: Member must have length greater than or equal to 1")
		}
		total += len(req.Keys)
	}
	if total > maxBatchGetKeys {
		return errValidation("Too many items requested for the BatchGetItem call")
	}
	return nil
}

func (s *Service) batchGetTable(name string, req keysAndAttributes) ([]attr.Item, error) {
	if err := req.check(); err != nil {
		return nil, err
	}
	ctx, err := req.context(req.ProjectionExpression != "")
	if err != nil {
		return nil, err
	}
	paths, err := parseProjection(req.ProjectionExpression, ctx)
	if err != nil {
		return nil, err
	}
	if err := ctx.CheckUnused(); err != nil {
		return nil, err
	}
	t, err := s.table(name)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	items := []attr.Item{}
	for _, key := range req.Keys {
		if err := t.checkKey(key); err != nil {
			return nil, err
		}
		id := t.keyID(key)
		if seen[id] {
			return nil, errValidation("Provided list of item keys contains duplicates")
		}
		seen[id] = true
		if item := t.get(key); item != nil {
			items = append(items, project(item, paths))
		}
	}
	return items, nil
}

type putRequest struct {
	Item attr.Item
}

type deleteRequest struct {
	Key attr.Item
}

type writeRequest struct {
	PutRequest    *putRequest    `json:",omitempty"`
	DeleteRequest *deleteRequest `json:",omitempty"`
}

type batchWriteItemInput struct {
	RequestItems map[string][]writeRequest
}

type batchWriteItemOutput struct {
	UnprocessedItems map[string][]writeRequest
}

// batchWriteItemOp checks every request before applying any, so the batch is
// applied whole or not at all; nothing is ever left unprocessed.
func (s *Service) batchWriteItemOp(in batchWriteItemInput) (any, error) {
	total := 0
	for _, reqs := range in.RequestItems {
		total += len(reqs)
	}
	if total < 1 || total > maxBatchWriteItems {
		return nil, errValidation("1 validation error detected: Value at 'requestItems' failed to satisfy "+
			"constraint: Map value must satisfy constraint: [Member must have length less than or equal to %d, "+
			"Member must have length greater than or equal to 1]", maxBatchWriteItems)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var writes []write
	for name, reqs := range in.RequestItems {
		t, err := s.table(name)
		if err != nil {
			return nil, err
		}
		seen := map[string]bool{}
		for _, req := range reqs {
			w, err := t.prepareBatchWrite(req)
			if err != nil {
				return nil, err
			}
			id := t.keyID(w.key)
			if seen[id] {
				return nil, errValidation("Provided list of item keys contains duplicates")
			}
			seen[id] = true
			writes = append(writes, w)
		}
	}
	for _, w := range writes {
		w.apply()
	}
	return batchWriteItemOutput{UnprocessedItems: map[string][]writeRequest{}}, nil
}

func (t *table) prepareBatchWrite(req writeRequest) (write, error) {
	switch {
	case (req.PutRequest == nil) == (req.DeleteRequest == nil):
		return write{}, errValidation("A WriteRequest must contain exactly one of PutRequest and DeleteRequest")
	case req.PutRequest != nil:
		return t.preparePut(req.PutRequest.Item, nil)
	default:
		return t.prepareDelete(req.DeleteRequest.Key, nil)
	}
}
