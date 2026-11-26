package kinesis

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	// iteratorTTL is how long a shard iterator stays valid after it is issued.
	iteratorTTL = 5 * time.Minute

	defaultLimit     = 10000
	maxResponseBytes = 10 << 20
)

// iterator is the decoded state behind a shard iterator token: a position in
// one shard of one incarnation of a stream, and when it was issued.
type iterator struct {
	gen    uint64
	stream string
	shard  int
	next   uint64 // the first sequence number it will return
	issued time.Time
}

// encode renders the iterator as an opaque token. The stream name cannot
// contain "|", so the fields split unambiguously.
func (it iterator) encode() string {
	raw := fmt.Sprintf("v1|%d|%s|%d|%d|%d", it.gen, it.stream, it.shard, it.next, it.issued.UnixMilli())
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodeIterator(token string) (iterator, error) {
	invalid := errInvalidArgument("Invalid ShardIterator.")
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return iterator{}, invalid
	}
	f := strings.Split(string(raw), "|")
	if len(f) != 6 || f[0] != "v1" {
		return iterator{}, invalid
	}
	gen, err1 := strconv.ParseUint(f[1], 10, 64)
	idx, err2 := strconv.Atoi(f[3])
	next, err3 := strconv.ParseUint(f[4], 10, 64)
	ms, err4 := strconv.ParseInt(f[5], 10, 64)
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil || idx < 0 {
		return iterator{}, invalid
	}
	return iterator{gen: gen, stream: f[2], shard: idx, next: next, issued: time.UnixMilli(ms)}, nil
}

// iteratorSpec is a GetShardIterator request after validation.
type iteratorSpec struct {
	kind      string
	seq       uint64
	timestamp time.Time
}

// shardIterator issues an iterator over shard id of st.
func (s *Service) shardIterator(st *stream, id string, spec iteratorSpec) (string, error) {
	idx, ok := parseShardID(id)
	st.mu.RLock()
	defer st.mu.RUnlock()
	if st.deleted {
		return "", s.gone(st)
	}
	if !ok || idx >= len(st.shards) {
		return "", errNotFound("Shard " + id + " in stream " + st.name + " under account " + s.identity.Account + " does not exist.")
	}
	sh := st.shards[idx]
	if (spec.kind == "AT_SEQUENCE_NUMBER" || spec.kind == "AFTER_SEQUENCE_NUMBER") &&
		(spec.seq < st.firstSeq || spec.seq > st.lastSeq) {
		return "", errInvalidArgument("StartingSequenceNumber " + formatSeq(spec.seq) + " used in GetShardIterator on shard " +
			sh.id + " in stream " + st.name + " under account " + s.identity.Account + " is invalid.")
	}
	var next uint64
	switch spec.kind {
	case "TRIM_HORIZON":
		next = 0
	case "LATEST":
		next = st.lastSeq + 1
	case "AT_SEQUENCE_NUMBER":
		next = spec.seq
	case "AFTER_SEQUENCE_NUMBER":
		next = spec.seq
		if next < ^uint64(0) {
			next++
		}
	case "AT_TIMESTAMP":
		next = st.lastSeq + 1 // no record at or after the timestamp: the tip
		if i := sh.indexAt(spec.timestamp); i < len(sh.records) {
			next = sh.records[i].seq
		}
	}
	it := iterator{gen: st.gen, stream: st.name, shard: idx, next: next, issued: s.clock.Now()}
	return it.encode(), nil
}

// recordsResult is what one GetRecords call read.
type recordsResult struct {
	records      []record
	nextIterator string
	millisBehind int64
}

// getRecords reads from the iterator's position. It returns at once, never
// waits for data, and always hands back a next iterator for the open shard.
func (s *Service) getRecords(token string, limit int) (recordsResult, error) {
	it, err := decodeIterator(token)
	if err != nil {
		return recordsResult{}, err
	}
	st, err := s.lookup(it.stream, "")
	if err != nil {
		return recordsResult{}, errNotFound("The shard iterator refers to a stream that does not exist.")
	}
	now := s.clock.Now()

	st.mu.RLock()
	defer st.mu.RUnlock()
	if st.deleted || st.gen != it.gen || it.shard >= len(st.shards) {
		return recordsResult{}, errNotFound("The shard iterator refers to a stream or shard that does not exist.")
	}
	if now.Sub(it.issued) > iteratorTTL {
		return recordsResult{}, errExpiredIterator()
	}
	sh := st.shards[it.shard]
	out := recordsResult{records: collect(sh, it.next, limit)}
	next := it.next
	if n := len(out.records); n > 0 {
		next = out.records[n-1].seq + 1
		tip := sh.records[len(sh.records)-1]
		out.millisBehind = tip.arrival.Sub(out.records[n-1].arrival).Milliseconds()
	}
	out.nextIterator = iterator{gen: st.gen, stream: st.name, shard: it.shard, next: next, issued: now}.encode()
	return out, nil
}

// collect returns up to limit records from the first one at or after next,
// stopping before the response would pass 10 MiB, but never empty-handed when
// a record is there. The slice shares the stored records' payloads, which are
// immutable, and is detached from the shard's own backing array.
func collect(sh *shard, next uint64, limit int) []record {
	from := sh.indexFrom(next)
	size, end := 0, from
	for end < len(sh.records) && end-from < limit {
		size += len(sh.records[end].data) + len(sh.records[end].pk)
		if size > maxResponseBytes && end > from {
			break
		}
		end++
	}
	return append([]record(nil), sh.records[from:end]...)
}
