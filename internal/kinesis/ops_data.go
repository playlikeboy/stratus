package kinesis

import (
	"unicode/utf8"
)

const (
	maxRecordBytes     = 1 << 20 // Data alone, and Data plus the partition key
	maxPutRecordsBytes = 5 << 20
	maxPutRecords      = 500
	maxPartitionKey    = 256
)

// newEntry validates one record of a put and turns it into an entry.
func newEntry(data []byte, pk, explicitHashKey, ordering string) (entry, error) {
	if n := utf8.RuneCountInString(pk); n < 1 || n > maxPartitionKey {
		return entry{}, errConstraint("partitionKey", "Member must have length between 1 and 256")
	}
	if len(data) > maxRecordBytes {
		return entry{}, errConstraint("data", "Member must have length less than or equal to 1048576")
	}
	if len(data)+len(pk) > maxRecordBytes {
		return entry{}, errInvalidArgument("Records must not exceed 1 MiB, partition key included.")
	}
	if ordering != "" && !isDecimal(ordering) {
		return entry{}, errConstraint("sequenceNumberForOrdering", "Member must satisfy regular expression pattern: 0|([1-9]\\d{0,128})")
	}
	e := entry{data: data, pk: pk}
	if explicitHashKey != "" {
		h, err := parseHashKey(explicitHashKey)
		if err != nil {
			return entry{}, err
		}
		e.hash = h
	}
	return e, nil
}

type putRecordInput struct {
	streamRef
	Data                      []byte
	PartitionKey              string
	ExplicitHashKey           string
	SequenceNumberForOrdering string
}

func (s *Service) putRecord(in putRecordInput) (any, error) {
	e, err := newEntry(in.Data, in.PartitionKey, in.ExplicitHashKey, in.SequenceNumberForOrdering)
	if err != nil {
		return nil, err
	}
	st, err := s.resolve(in.streamRef)
	if err != nil {
		return nil, err
	}
	placed, err := s.put(st, []entry{e})
	if err != nil {
		return nil, err
	}
	return map[string]string{"ShardId": placed[0].shard, "SequenceNumber": formatSeq(placed[0].seq)}, nil
}

type putRecordsEntry struct {
	Data            []byte
	PartitionKey    string
	ExplicitHashKey string
}

type putRecordsInput struct {
	streamRef
	Records []putRecordsEntry
}

type putRecordsResult struct {
	SequenceNumber string
	ShardId        string
}

// putRecords validates the whole batch before it stores any of it, so a bad
// record fails the request and no record is half-written. The emulator never
// throttles, so FailedRecordCount is always 0.
func (s *Service) putRecords(in putRecordsInput) (any, error) {
	if len(in.Records) < 1 || len(in.Records) > maxPutRecords {
		return nil, errConstraint("records", "Member must have length between 1 and 500")
	}
	entries := make([]entry, len(in.Records))
	total := 0
	for i, r := range in.Records {
		e, err := newEntry(r.Data, r.PartitionKey, r.ExplicitHashKey, "")
		if err != nil {
			return nil, err
		}
		entries[i] = e
		total += len(r.Data) + len(r.PartitionKey)
	}
	if total > maxPutRecordsBytes {
		return nil, errInvalidArgument("Records size exceeds the 5 MiB limit of one PutRecords request.")
	}
	st, err := s.resolve(in.streamRef)
	if err != nil {
		return nil, err
	}
	placed, err := s.put(st, entries)
	if err != nil {
		return nil, err
	}
	results := make([]putRecordsResult, len(placed))
	for i, p := range placed {
		results[i] = putRecordsResult{SequenceNumber: formatSeq(p.seq), ShardId: p.shard}
	}
	return map[string]any{"FailedRecordCount": 0, "Records": results}, nil
}

type getShardIteratorInput struct {
	streamRef
	ShardId                string
	ShardIteratorType      string
	StartingSequenceNumber string
	Timestamp              *float64
}

func (s *Service) getShardIterator(in getShardIteratorInput) (any, error) {
	spec := iteratorSpec{kind: in.ShardIteratorType}
	switch in.ShardIteratorType {
	case "TRIM_HORIZON", "LATEST":
	case "AT_SEQUENCE_NUMBER", "AFTER_SEQUENCE_NUMBER":
		if in.StartingSequenceNumber == "" {
			return nil, errInvalidArgument("StartingSequenceNumber is required for " + in.ShardIteratorType + ".")
		}
		seq, err := parseSeq(in.StartingSequenceNumber)
		if err != nil {
			return nil, err
		}
		spec.seq = seq
	case "AT_TIMESTAMP":
		if in.Timestamp == nil {
			return nil, errInvalidArgument("Timestamp is required for AT_TIMESTAMP.")
		}
		spec.timestamp = fromEpoch(*in.Timestamp)
	default:
		return nil, errConstraint("shardIteratorType",
			"Member must satisfy enum value set: [AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER, TRIM_HORIZON, LATEST, AT_TIMESTAMP]")
	}
	if in.ShardId == "" {
		return nil, errConstraint("shardId", "Member must have length between 1 and 128")
	}
	st, err := s.resolve(in.streamRef)
	if err != nil {
		return nil, err
	}
	token, err := s.shardIterator(st, in.ShardId, spec)
	if err != nil {
		return nil, err
	}
	return map[string]string{"ShardIterator": token}, nil
}

type getRecordsInput struct {
	ShardIterator string
	Limit         *int
	StreamARN     string
}

type recordView struct {
	SequenceNumber              string
	ApproximateArrivalTimestamp float64
	Data                        []byte
	PartitionKey                string
}

func (s *Service) getRecordsOp(in getRecordsInput) (any, error) {
	limit := defaultLimit
	if in.Limit != nil {
		limit = *in.Limit
	}
	if limit < 1 || limit > defaultLimit {
		return nil, errConstraint("limit", "Member must have value between 1 and 10000")
	}
	if in.ShardIterator == "" {
		return nil, errConstraint("shardIterator", "Member must have length between 1 and 512")
	}
	res, err := s.getRecords(in.ShardIterator, limit)
	if err != nil {
		return nil, err
	}
	views := make([]recordView, len(res.records))
	for i, r := range res.records {
		views[i] = recordView{
			SequenceNumber:              formatSeq(r.seq),
			ApproximateArrivalTimestamp: epoch(r.arrival),
			Data:                        r.data,
			PartitionKey:                r.pk,
		}
	}
	return map[string]any{
		"Records":            views,
		"NextShardIterator":  res.nextIterator,
		"MillisBehindLatest": res.millisBehind,
	}, nil
}
