package kinesis

import (
	"errors"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ODudek/stratus/internal/awsapi"
	"github.com/ODudek/stratus/internal/clock"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func newTestService(t *testing.T, shards int) (*Service, *clock.Fake) {
	t.Helper()
	clk := clock.NewFake(t0)
	s := New(awsapi.Identity{Account: awsapi.DefaultAccount, Region: awsapi.DefaultRegion}, clk)
	if err := s.CreateStream("orders", shards); err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	return s, clk
}

func code(err error) string {
	var apiErr *awsapi.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code
	}
	if err == nil {
		return ""
	}
	return err.Error()
}

func mustStream(t *testing.T, s *Service) *stream {
	t.Helper()
	st, err := s.lookup("orders", "")
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func mustPut(t *testing.T, s *Service, pk, data string) placement {
	t.Helper()
	p, err := s.put(mustStream(t, s), []entry{{pk: pk, data: []byte(data)}})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	return p[0]
}

func iterate(t *testing.T, s *Service, shard string, spec iteratorSpec) string {
	t.Helper()
	token, err := s.shardIterator(mustStream(t, s), shard, spec)
	if err != nil {
		t.Fatalf("shardIterator(%v): %v", spec.kind, err)
	}
	return token
}

func read(t *testing.T, s *Service, token string, limit int) recordsResult {
	t.Helper()
	res, err := s.getRecords(token, limit)
	if err != nil {
		t.Fatalf("getRecords: %v", err)
	}
	return res
}

func datas(rs []record) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = string(r.data)
	}
	return out
}

func TestShardRoutingByHashKey(t *testing.T) {
	s, _ := newTestService(t, 4)
	st := mustStream(t, s)

	// The ranges tile the hash space with no gap and no overlap.
	if st.shards[0].start.Sign() != 0 || st.shards[3].end.Cmp(maxHashKey) != 0 {
		t.Fatalf("ranges do not cover the space: %v..%v", st.shards[0].start, st.shards[3].end)
	}
	for i := 1; i < 4; i++ {
		if new(big.Int).Add(st.shards[i-1].end, big.NewInt(1)).Cmp(st.shards[i].start) != 0 {
			t.Fatalf("shard %d does not follow shard %d", i, i-1)
		}
	}

	for i := 0; i < 200; i++ {
		pk := "key-" + strconv.Itoa(i)
		h := partitionHash(pk)
		sh := st.route(h)
		if h.Cmp(sh.start) < 0 || h.Cmp(sh.end) > 0 {
			t.Fatalf("%q routed to %s outside its range", pk, sh.id)
		}
		if got := mustPut(t, s, pk, "x").shard; got != sh.id {
			t.Fatalf("%q stored in %s, want %s", pk, got, sh.id)
		}
	}
}

func TestExplicitHashKeyOverridesPartitionKey(t *testing.T) {
	s, _ := newTestService(t, 4)
	st := mustStream(t, s)
	for i, sh := range st.shards {
		for _, h := range []string{sh.start.String(), sh.end.String()} {
			e, err := newEntry([]byte("x"), "same-key", h, "")
			if err != nil {
				t.Fatal(err)
			}
			p, err := s.put(st, []entry{e})
			if err != nil {
				t.Fatal(err)
			}
			if p[0].shard != shardID(i) {
				t.Fatalf("hash key %s went to %s, want %s", h, p[0].shard, shardID(i))
			}
		}
	}
	for _, bad := range []string{"-1", "abc", "340282366920938463463374607431768211456"} {
		if _, err := newEntry(nil, "k", bad, ""); code(err) != "InvalidArgumentException" {
			t.Errorf("ExplicitHashKey %q: %v, want InvalidArgumentException", bad, err)
		}
	}
}

func TestSequenceNumbersIncreaseAndCompareAsNumbers(t *testing.T) {
	s, _ := newTestService(t, 1)
	var prev string
	for i := 0; i < 1200; i++ {
		seq := formatSeq(mustPut(t, s, "k", "x").seq)
		if len(seq) != 56 {
			t.Fatalf("sequence number %q is %d digits, want 56", seq, len(seq))
		}
		if prev != "" && seq <= prev {
			t.Fatalf("%s is not after %s", seq, prev)
		}
		prev = seq
	}
	if n, err := parseSeq(formatSeq(42)); err != nil || n != 42 {
		t.Fatalf("parseSeq round trip = %d, %v", n, err)
	}
	if _, err := parseSeq("12ab"); code(err) != "InvalidArgumentException" {
		t.Fatalf("malformed sequence number: %v", err)
	}
}

func TestIteratorTypes(t *testing.T) {
	s, clk := newTestService(t, 1)
	const shard = "shardId-000000000000"
	var seqs []uint64
	for _, d := range []string{"a", "b", "c", "d"} {
		seqs = append(seqs, mustPut(t, s, "k", d).seq)
		clk.Advance(time.Second)
	}

	tests := []struct {
		name string
		spec iteratorSpec
		want []string
	}{
		{"trim horizon", iteratorSpec{kind: "TRIM_HORIZON"}, []string{"a", "b", "c", "d"}},
		{"latest", iteratorSpec{kind: "LATEST"}, nil},
		{"at sequence", iteratorSpec{kind: "AT_SEQUENCE_NUMBER", seq: seqs[1]}, []string{"b", "c", "d"}},
		{"after sequence", iteratorSpec{kind: "AFTER_SEQUENCE_NUMBER", seq: seqs[1]}, []string{"c", "d"}},
		{"after the last", iteratorSpec{kind: "AFTER_SEQUENCE_NUMBER", seq: seqs[3]}, nil},
		{"at timestamp", iteratorSpec{kind: "AT_TIMESTAMP", timestamp: t0.Add(2 * time.Second)}, []string{"c", "d"}},
		{"timestamp between records", iteratorSpec{kind: "AT_TIMESTAMP", timestamp: t0.Add(1500 * time.Millisecond)}, []string{"c", "d"}},
		{"timestamp before all", iteratorSpec{kind: "AT_TIMESTAMP", timestamp: t0.Add(-time.Hour)}, []string{"a", "b", "c", "d"}},
		{"timestamp after all", iteratorSpec{kind: "AT_TIMESTAMP", timestamp: t0.Add(time.Hour)}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := read(t, s, iterate(t, s, shard, tc.spec), 100)
			got := datas(res.records)
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("read %v, want %v", got, tc.want)
			}
			if res.nextIterator == "" {
				t.Fatal("no NextShardIterator for an open shard")
			}
		})
	}
}

func TestNextIteratorContinuesWhereTheReadStopped(t *testing.T) {
	s, _ := newTestService(t, 1)
	for _, d := range []string{"a", "b", "c", "d", "e"} {
		mustPut(t, s, "k", d)
	}
	token := iterate(t, s, "shardId-000000000000", iteratorSpec{kind: "TRIM_HORIZON"})
	var got []string
	for i := 0; i < 5; i++ {
		res := read(t, s, token, 2)
		got = append(got, datas(res.records)...)
		token = res.nextIterator
	}
	if strings.Join(got, "") != "abcde" {
		t.Fatalf("paged read = %v", got)
	}
	// The tip's iterator still sees a record put afterwards.
	mustPut(t, s, "k", "f")
	if got := datas(read(t, s, token, 10).records); strings.Join(got, "") != "f" {
		t.Fatalf("after the tip: %v", got)
	}
}

func TestIteratorExpiry(t *testing.T) {
	s, clk := newTestService(t, 1)
	mustPut(t, s, "k", "a")
	token := iterate(t, s, "shardId-000000000000", iteratorSpec{kind: "TRIM_HORIZON"})

	clk.Advance(iteratorTTL) // exactly five minutes: still valid
	res := read(t, s, token, 10)
	clk.Advance(time.Millisecond)
	if _, err := s.getRecords(token, 10); code(err) != "ExpiredIteratorException" {
		t.Fatalf("old iterator: %v, want ExpiredIteratorException", err)
	}
	// The iterator a read hands back is fresh, and expires on its own schedule.
	clk.Advance(iteratorTTL - 2*time.Millisecond)
	if _, err := s.getRecords(res.nextIterator, 10); err != nil {
		t.Fatalf("next iterator expired too early: %v", err)
	}
}

func TestIteratorErrors(t *testing.T) {
	s, _ := newTestService(t, 2)
	st := mustStream(t, s)
	if _, err := s.shardIterator(st, "shardId-000000000002", iteratorSpec{kind: "LATEST"}); code(err) != "ResourceNotFoundException" {
		t.Errorf("unknown shard: %v", err)
	}
	if _, err := s.shardIterator(st, "nonsense", iteratorSpec{kind: "LATEST"}); code(err) != "ResourceNotFoundException" {
		t.Errorf("malformed shard id: %v", err)
	}
	if _, err := s.getRecords("not an iterator", 10); code(err) != "InvalidArgumentException" {
		t.Errorf("malformed iterator: %v", err)
	}
	token := iterate(t, s, "shardId-000000000001", iteratorSpec{kind: "LATEST"})
	if err := s.DeleteStream("orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.getRecords(token, 10); code(err) != "ResourceNotFoundException" {
		t.Errorf("iterator of a deleted stream: %v", err)
	}
	// A re-created stream of the same name is not the old one.
	if err := s.CreateStream("orders", 2); err != nil {
		t.Fatal(err)
	}
	if _, err := s.getRecords(token, 10); code(err) != "ResourceNotFoundException" {
		t.Errorf("iterator of a previous incarnation: %v", err)
	}
}

func TestMillisBehindLatest(t *testing.T) {
	s, clk := newTestService(t, 1)
	for _, d := range []string{"a", "b", "c"} {
		mustPut(t, s, "k", d)
		clk.Advance(1500 * time.Millisecond)
	}
	token := iterate(t, s, "shardId-000000000000", iteratorSpec{kind: "TRIM_HORIZON"})

	first := read(t, s, token, 1)
	if first.millisBehind != 3000 {
		t.Fatalf("behind after the first record = %d ms, want 3000", first.millisBehind)
	}
	rest := read(t, s, first.nextIterator, 10)
	if rest.millisBehind != 0 {
		t.Fatalf("behind at the tip = %d ms, want 0", rest.millisBehind)
	}
	if empty := read(t, s, rest.nextIterator, 10); empty.millisBehind != 0 || len(empty.records) != 0 {
		t.Fatalf("empty read at the tip: %+v", empty)
	}
}

func TestGetRecordsLimits(t *testing.T) {
	s, _ := newTestService(t, 1)
	big := make([]byte, 600<<10)
	st := mustStream(t, s)
	for i := 0; i < 30; i++ {
		if _, err := s.put(st, []entry{{pk: "k", data: big}}); err != nil {
			t.Fatal(err)
		}
	}
	token := iterate(t, s, "shardId-000000000000", iteratorSpec{kind: "TRIM_HORIZON"})
	// 600 KiB records: 10 MiB holds seventeen of them.
	if n := len(read(t, s, token, 10000).records); n != 17 {
		t.Fatalf("a 10 MiB read returned %d records of 600 KiB", n)
	}
	if n := len(read(t, s, token, 3).records); n != 3 {
		t.Fatalf("Limit 3 returned %d records", n)
	}
}

func TestTrimExpired(t *testing.T) {
	s, clk := newTestService(t, 2)
	for i := 0; i < 6; i++ {
		mustPut(t, s, "k"+strconv.Itoa(i), "old")
	}
	clk.Advance(23 * time.Hour)
	for i := 0; i < 4; i++ {
		mustPut(t, s, "k"+strconv.Itoa(i), "new")
	}
	if n := s.TrimExpired(); n != 0 {
		t.Fatalf("trimmed %d records inside the retention period", n)
	}
	clk.Advance(2 * time.Hour) // the old ones are 25 h old, the new ones 2 h
	if n := s.TrimExpired(); n != 6 {
		t.Fatalf("trimmed %d records, want 6", n)
	}
	var left []string
	for _, id := range []string{"shardId-000000000000", "shardId-000000000001"} {
		left = append(left, datas(read(t, s, iterate(t, s, id, iteratorSpec{kind: "TRIM_HORIZON"}), 100).records)...)
	}
	if len(left) != 4 || strings.Join(left, ",") != strings.Repeat("new,", 3)+"new" {
		t.Fatalf("after trimming TRIM_HORIZON reads %v, want four new records", left)
	}
	if n := s.TrimExpired(); n != 0 {
		t.Fatalf("second trim dropped %d", n)
	}
}

func TestPutRecordsLimits(t *testing.T) {
	s, _ := newTestService(t, 1)
	rec := func(n int) putRecordsEntry { return putRecordsEntry{Data: make([]byte, n), PartitionKey: "k"} }
	batch := func(n, size int) putRecordsInput {
		in := putRecordsInput{streamRef: streamRef{StreamName: "orders"}}
		for i := 0; i < n; i++ {
			in.Records = append(in.Records, rec(size))
		}
		return in
	}
	tests := []struct {
		name string
		in   putRecordsInput
		want string
	}{
		{"500 records", batch(500, 1), ""},
		{"501 records", batch(501, 1), "ValidationException"},
		{"no records", batch(0, 1), "ValidationException"},
		{"5 MiB in five records", batch(5, 1<<20-1), ""},
		{"over 5 MiB", batch(6, 1<<20-1), "InvalidArgumentException"},
		{"data over 1 MiB", batch(1, 1<<20+1), "ValidationException"},
		{"data and key over 1 MiB", batch(1, 1<<20), "InvalidArgumentException"},
		{"empty partition key", putRecordsInput{streamRef: streamRef{StreamName: "orders"}, Records: []putRecordsEntry{{Data: []byte("x")}}}, "ValidationException"},
		{"unknown stream", putRecordsInput{streamRef: streamRef{StreamName: "nope"}, Records: []putRecordsEntry{{PartitionKey: "k"}}}, "ResourceNotFoundException"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.putRecords(tc.in)
			if got := code(err); got != tc.want {
				t.Fatalf("error %q (%v), want %q", got, err, tc.want)
			}
		})
	}
}

func TestPutRecordsIsAllOrNothing(t *testing.T) {
	s, _ := newTestService(t, 1)
	_, err := s.putRecords(putRecordsInput{streamRef: streamRef{StreamName: "orders"}, Records: []putRecordsEntry{
		{Data: []byte("good"), PartitionKey: "k"},
		{Data: []byte("bad"), PartitionKey: "k", ExplicitHashKey: "-5"},
	}})
	if code(err) != "InvalidArgumentException" {
		t.Fatalf("error = %v", err)
	}
	token := iterate(t, s, "shardId-000000000000", iteratorSpec{kind: "TRIM_HORIZON"})
	if n := len(read(t, s, token, 10).records); n != 0 {
		t.Fatalf("a failed batch left %d records behind", n)
	}
}

func TestStreamLifecycle(t *testing.T) {
	s, _ := newTestService(t, 1)
	if err := s.CreateStream("orders", 1); code(err) != "ResourceInUseException" {
		t.Errorf("duplicate: %v", err)
	}
	for _, bad := range []struct {
		name   string
		shards int
	}{{"", 1}, {"has space", 1}, {"ok", 0}, {"ok", 10001}} {
		if err := s.CreateStream(bad.name, bad.shards); code(err) != "ValidationException" {
			t.Errorf("CreateStream(%q, %d): %v", bad.name, bad.shards, err)
		}
	}
	if err := s.DeleteStream("orders"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteStream("orders"); code(err) != "ResourceNotFoundException" {
		t.Errorf("second delete: %v", err)
	}
	if err := s.CreateStream("orders", 1); err != nil {
		t.Errorf("re-create: %v", err)
	}
	s.Reset()
	if _, err := s.lookup("orders", ""); code(err) != "ResourceNotFoundException" {
		t.Errorf("after Reset: %v", err)
	}
}

func TestLookupByARN(t *testing.T) {
	s, _ := newTestService(t, 1)
	arn := "arn:aws:kinesis:eu-central-1:000000000000:stream/orders"
	if st, err := s.lookup("", arn); err != nil || st.name != "orders" {
		t.Fatalf("lookup by ARN: %v, %v", st, err)
	}
	for arn, want := range map[string]string{
		"arn:aws:kinesis:eu-central-1:000000000000:stream/other": "ResourceNotFoundException",
		"arn:aws:kinesis:us-east-1:999999999999:stream/orders":   "ResourceNotFoundException",
		"garbage": "InvalidArgumentException",
	} {
		if _, err := s.lookup("", arn); code(err) != want {
			t.Errorf("lookup(%q): %v, want %s", arn, err, want)
		}
	}
	if _, err := s.lookup("", ""); code(err) != "ValidationException" {
		t.Errorf("no name and no ARN: %v", err)
	}
}
