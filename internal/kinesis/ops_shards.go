package kinesis

import (
	"encoding/base64"
	"strings"
)

const defaultMaxResults = 1000

type hashKeyRange struct {
	StartingHashKey string
	EndingHashKey   string
}

type sequenceNumberRange struct {
	StartingSequenceNumber string
}

type shardView struct {
	ShardId             string
	HashKeyRange        hashKeyRange
	SequenceNumberRange sequenceNumberRange
}

func viewOf(st *stream, sh *shard) shardView {
	return shardView{
		ShardId:             sh.id,
		HashKeyRange:        hashKeyRange{StartingHashKey: sh.start.String(), EndingHashKey: sh.end.String()},
		SequenceNumberRange: sequenceNumberRange{StartingSequenceNumber: formatSeq(st.firstSeq)},
	}
}

// pageShards lists up to limit shards after the shard exclusiveStart, and
// whether more follow. The caller holds st.mu.
func pageShards(st *stream, exclusiveStart string, limit int) ([]shardView, bool, error) {
	from := 0
	if exclusiveStart != "" {
		idx, ok := parseShardID(exclusiveStart)
		if !ok {
			return nil, false, errInvalidArgument("Invalid ExclusiveStartShardId: " + exclusiveStart)
		}
		from = idx + 1
	}
	if from > len(st.shards) {
		from = len(st.shards)
	}
	end := min(from+limit, len(st.shards))
	page := make([]shardView, 0, end-from)
	for _, sh := range st.shards[from:end] {
		page = append(page, viewOf(st, sh))
	}
	return page, end < len(st.shards), nil
}

type listShardsInput struct {
	streamRef
	NextToken             string
	MaxResults            *int
	ExclusiveStartShardId string
}

// listShards pages the shards. The next token carries the stream name and the
// last shard id returned, so a continuation needs no other parameter.
func (s *Service) listShards(in listShardsInput) (any, error) {
	ref, after := in.streamRef, in.ExclusiveStartShardId
	if in.NextToken != "" {
		if ref.StreamName != "" || ref.StreamARN != "" || after != "" {
			return nil, errInvalidArgument("NextToken cannot be combined with StreamName, StreamARN or ExclusiveStartShardId.")
		}
		var err error
		if ref.StreamName, after, err = decodeShardsToken(in.NextToken); err != nil {
			return nil, err
		}
	}
	limit := defaultMaxResults
	if in.MaxResults != nil {
		limit = *in.MaxResults
	}
	if limit < 1 || limit > maxListLimit {
		return nil, errConstraint("maxResults", "Member must have value between 1 and 10000")
	}
	st, err := s.resolve(ref)
	if err != nil {
		return nil, err
	}
	st.mu.RLock()
	defer st.mu.RUnlock()
	if st.deleted {
		return nil, s.gone(st)
	}
	page, more, err := pageShards(st, after, limit)
	if err != nil {
		return nil, err
	}
	out := map[string]any{"Shards": page}
	if more {
		raw := st.name + "|" + page[len(page)-1].ShardId
		out["NextToken"] = base64.RawURLEncoding.EncodeToString([]byte(raw))
	}
	return out, nil
}

func decodeShardsToken(token string) (name, after string, err error) {
	raw, derr := base64.RawURLEncoding.DecodeString(token)
	name, after, ok := strings.Cut(string(raw), "|")
	if derr != nil || !ok || name == "" {
		return "", "", errInvalidArgument("Invalid NextToken.")
	}
	return name, after, nil
}
