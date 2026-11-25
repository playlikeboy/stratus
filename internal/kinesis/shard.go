package kinesis

import (
	"crypto/md5"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"
)

// hashSpaceBits is the width of the partition key hash space: MD5, 128 bits.
const hashSpaceBits = 128

var (
	// maxHashKey is the last key of the hash space, 2^128-1.
	maxHashKey = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), hashSpaceBits), big.NewInt(1))

	// seqBase is 49 * 10^54: every sequence number is seqBase + a counter, so
	// they are 56 digits long like the real ones and compare as numbers or as
	// strings alike.
	seqBase = new(big.Int).Mul(big.NewInt(49), new(big.Int).Exp(big.NewInt(10), big.NewInt(54), nil))
)

// record is one stored data record. Its fields never change after the append,
// so readers can share them without copying.
type record struct {
	seq     uint64
	arrival time.Time
	data    []byte
	pk      string
}

// shard is an append-only log of the records whose hash key falls in its range.
// It has no lock of its own: the owning stream's lock guards it.
type shard struct {
	id      string
	start   *big.Int
	end     *big.Int
	records []record
}

func shardID(index int) string { return fmt.Sprintf("shardId-%012d", index) }

// parseShardID is the inverse of shardID; ok is false for any other text.
func parseShardID(id string) (index int, ok bool) {
	rest, found := strings.CutPrefix(id, "shardId-")
	if !found || len(rest) != 12 {
		return 0, false
	}
	n, err := strconv.Atoi(rest)
	return n, err == nil && n >= 0
}

// newShards splits the hash space evenly into n shards.
func newShards(n int) []*shard {
	space := new(big.Int).Add(maxHashKey, big.NewInt(1))
	starts := make([]*big.Int, n+1)
	for i := range starts {
		starts[i] = new(big.Int).Div(new(big.Int).Mul(space, big.NewInt(int64(i))), big.NewInt(int64(n)))
	}
	shards := make([]*shard, n)
	for i := range shards {
		shards[i] = &shard{
			id:    shardID(i),
			start: starts[i],
			end:   new(big.Int).Sub(starts[i+1], big.NewInt(1)),
		}
	}
	return shards
}

// partitionHash is the hash key of a partition key: its MD5 as a big-endian
// 128-bit integer.
func partitionHash(partitionKey string) *big.Int {
	sum := md5.Sum([]byte(partitionKey))
	return new(big.Int).SetBytes(sum[:])
}

// parseHashKey reads an ExplicitHashKey: a decimal inside the hash space.
func parseHashKey(s string) (*big.Int, error) {
	if !isDecimal(s) {
		return nil, errInvalidArgument("Invalid ExplicitHashKey. ExplicitHashKey must be a decimal number.")
	}
	h, _ := new(big.Int).SetString(s, 10)
	if h.Cmp(maxHashKey) > 0 {
		return nil, errInvalidArgument("Invalid ExplicitHashKey. ExplicitHashKey must be in the range: [0, 2^128-1].")
	}
	return h, nil
}

func isDecimal(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func formatSeq(n uint64) string { return fmt.Sprintf("49%054d", n) }

// parseSeq maps a client's sequence number onto the counter space: anything at
// or below the base is 0, anything beyond a uint64 counter saturates.
func parseSeq(s string) (uint64, error) {
	if !isDecimal(s) || len(s) > 129 {
		return 0, errInvalidArgument("Invalid SequenceNumber: " + s)
	}
	x, _ := new(big.Int).SetString(s, 10)
	x.Sub(x, seqBase)
	switch {
	case x.Sign() <= 0:
		return 0, nil
	case !x.IsUint64():
		return math.MaxUint64, nil
	}
	return x.Uint64(), nil
}

// append stores r at the shard's tip. Arrival times never go backwards, so the
// time search of AT_TIMESTAMP stays valid even if the wall clock steps back.
func (sh *shard) append(r record) {
	if n := len(sh.records); n > 0 && r.arrival.Before(sh.records[n-1].arrival) {
		r.arrival = sh.records[n-1].arrival
	}
	sh.records = append(sh.records, r)
}

// indexFrom is the position of the first record whose sequence number is at
// least next; len(records) when there is none.
func (sh *shard) indexFrom(next uint64) int {
	return sort.Search(len(sh.records), func(i int) bool { return sh.records[i].seq >= next })
}

// indexAt is the position of the first record that arrived at or after t.
func (sh *shard) indexAt(t time.Time) int {
	return sort.Search(len(sh.records), func(i int) bool { return !sh.records[i].arrival.Before(t) })
}

// trimBefore drops the records that arrived before cutoff and returns how many.
func (sh *shard) trimBefore(cutoff time.Time) int {
	k := sort.Search(len(sh.records), func(i int) bool { return !sh.records[i].arrival.Before(cutoff) })
	if k == 0 {
		return 0
	}
	clear(sh.records[:k]) // let the payloads be collected
	rest := sh.records[k:]
	if k > len(rest) { // most of the backing array is dead: start a new one
		rest = append([]record(nil), rest...)
	}
	sh.records = rest
	return k
}
