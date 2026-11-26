package server

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"encoding/binary"
	"strings"
	"testing"
)

// kplAggregate builds a record as the Kinesis Producer Library writes it:
// the magic, a protobuf AggregatedRecord and its MD5.
func kplAggregate(keys []string, records []struct {
	key  int
	data []byte
}) []byte {
	field := func(num int, wire int) []byte { return binary.AppendUvarint(nil, uint64(num<<3|wire)) }
	bytesField := func(num int, b []byte) []byte {
		out := append(field(num, wireBytes), binary.AppendUvarint(nil, uint64(len(b)))...)
		return append(out, b...)
	}
	var body []byte
	for _, k := range keys {
		body = append(body, bytesField(1, []byte(k))...)
	}
	for _, r := range records {
		rec := append(field(1, wireVarint), binary.AppendUvarint(nil, uint64(r.key))...)
		rec = append(rec, bytesField(3, r.data)...)
		body = append(body, bytesField(3, rec)...)
	}
	sum := md5.Sum(body)
	out := append(append([]byte(nil), kplMagic...), body...)
	return append(out, sum[:]...)
}

func gzipped(t *testing.T, s string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, _ = w.Write([]byte(s))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestDecodeKPLAggregateOfGzippedJSON(t *testing.T) {
	agg := kplAggregate([]string{"user-1", "user-2"}, []struct {
		key  int
		data []byte
	}{
		{0, gzipped(t, `{"event":"mail_accepted","id":1}`)},
		{1, gzipped(t, `{"event":"mail_accepted","id":2}`)},
		{0, []byte("plain text")},
	})
	got := decodeRecord(agg)
	if len(got) != 3 {
		t.Fatalf("decoded %d records, want 3: %+v", len(got), got)
	}
	if got[0].PartitionKey != "user-1" || got[1].PartitionKey != "user-2" || got[2].PartitionKey != "user-1" {
		t.Errorf("partition keys = %q, %q, %q", got[0].PartitionKey, got[1].PartitionKey, got[2].PartitionKey)
	}
	if strings.Join(got[0].Layers, ">") != "KPL>gzip>JSON" || !strings.Contains(got[0].Text, `"id": 1`) {
		t.Errorf("first record = %+v, want KPL>gzip>JSON with indented JSON", got[0])
	}
	if strings.Join(got[2].Layers, ">") != "KPL" || got[2].Text != "plain text" {
		t.Errorf("third record = %+v", got[2])
	}
}

func TestDecodeRejectsABrokenAggregate(t *testing.T) {
	agg := kplAggregate([]string{"k"}, []struct {
		key  int
		data []byte
	}{{0, []byte("x")}})
	agg[len(agg)-1] ^= 0xff // the MD5 no longer matches
	got := decodeRecord(agg)
	if len(got) != 1 || !strings.Contains(got[0].Note, "MD5") || !got[0].Binary {
		t.Fatalf("broken aggregate decoded as %+v", got)
	}
}

func TestDecodePlainRecords(t *testing.T) {
	cases := []struct {
		data    []byte
		layers  string
		binary  bool
		textHas string
		noteHas string
	}{
		{[]byte(`{"a":1}`), "JSON", false, `"a": 1`, ""},
		{[]byte("hello"), "", false, "hello", ""},
		{gzipped(t, "hi there"), "gzip", false, "hi there", ""},
		{[]byte{0, 1, 2, 3}, "", true, "AAECAw==", ""},
		{[]byte{0x28, 0xb5, 0x2f, 0xfd, 1, 2}, "zstd", true, "", "not decompressed"},
		{[]byte{0x1f, 0x8b, 9, 9}, "", true, "", "does not decompress"},
	}
	for _, c := range cases {
		got := decodeRecord(c.data)
		if len(got) != 1 {
			t.Fatalf("%x: %d records", c.data, len(got))
		}
		d := got[0]
		if strings.Join(d.Layers, ">") != c.layers || d.Binary != c.binary ||
			!strings.Contains(d.Text, c.textHas) || !strings.Contains(d.Note, c.noteHas) {
			t.Errorf("%x decoded as %+v", c.data, d)
		}
	}
}

// A small record that would decompress to gigabytes is stopped at 1 MiB.
func TestDecodeBoundsDecompression(t *testing.T) {
	bomb := gzipped(t, strings.Repeat("a", maxDecoded+10))
	got := decodeRecord(bomb)
	if len(got) != 1 || !strings.Contains(got[0].Note, "1 MiB") {
		t.Fatalf("bomb decoded as layers %v, note %q", got[0].Layers, got[0].Note)
	}
}
