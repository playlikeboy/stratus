package server

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

// The UI shows what a Kinesis record carries, not its raw bytes: a record
// written by the Kinesis Producer Library holds many user records, each of
// which may be compressed JSON.

// kplMagic starts every record the Kinesis Producer Library aggregates.
var kplMagic = []byte{0xF3, 0x89, 0x9A, 0xC2}

// maxDecoded bounds what decompressing one record may produce, so that a
// small compressed record cannot fill the emulator's memory.
const maxDecoded = 1 << 20

// decodedRecord is one user record, as shown.
type decodedRecord struct {
	PartitionKey string   // set for a record from a KPL aggregate
	Layers       []string // how it was unwrapped, outermost first: "KPL", "gzip", "JSON"
	Text         string   // the content as text, JSON indented
	Binary       bool     // Text is base64: the content is not text
	Note         string   // why decoding stopped early, if it did
}

// decodeRecord unwraps a record's data: a KPL aggregate into its user
// records, then each one's compression and JSON.
func decodeRecord(data []byte) []decodedRecord {
	if !bytes.HasPrefix(data, kplMagic) {
		return []decodedRecord{decodePayload(data, nil)}
	}
	records, err := deaggregate(data)
	if err != nil {
		r := decodePayload(data, nil)
		r.Note = "Starts like a KPL aggregate but is not one: " + err.Error()
		return []decodedRecord{r}
	}
	out := make([]decodedRecord, 0, len(records))
	for _, rec := range records {
		d := decodePayload(rec.data, []string{"KPL"})
		d.PartitionKey = rec.partitionKey
		out = append(out, d)
	}
	return out
}

// decodePayload undoes the compressions it recognises, then shows the
// result as JSON, text or base64.
func decodePayload(data []byte, layers []string) decodedRecord {
	d := decodedRecord{Layers: append([]string(nil), layers...)}
	for range 3 { // compressed twice is unusual; three times is a loop
		name, open := compression(data)
		if name == "" {
			break
		}
		if open == nil {
			d.Layers = append(d.Layers, name)
			d.Note = name + " is recognised but not decompressed here."
			d.Text, d.Binary = base64.StdEncoding.EncodeToString(data), true
			return d
		}
		plain, err := readAtMost(open, data)
		if err != nil {
			d.Note = "Looks " + name + "-compressed but does not decompress: " + err.Error()
			break
		}
		d.Layers = append(d.Layers, name)
		data = plain
	}
	if json.Valid(data) {
		var buf bytes.Buffer
		if json.Indent(&buf, data, "", "  ") == nil {
			d.Layers = append(d.Layers, "JSON")
			d.Text = buf.String()
			return d
		}
	}
	if isText(data) {
		d.Text = string(data)
		return d
	}
	d.Text, d.Binary = base64.StdEncoding.EncodeToString(data), true
	return d
}

// compression names the format data starts with and how to open it; a nil
// opener means the format is known but not decompressed here.
func compression(data []byte) (string, func(io.Reader) (io.Reader, error)) {
	switch {
	case len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b:
		return "gzip", func(r io.Reader) (io.Reader, error) { return gzip.NewReader(r) }
	case len(data) >= 2 && data[0] == 0x78 && (uint16(data[0])<<8|uint16(data[1]))%31 == 0:
		return "zlib", func(r io.Reader) (io.Reader, error) { return zlib.NewReader(r) }
	case bytes.HasPrefix(data, []byte{0x28, 0xb5, 0x2f, 0xfd}):
		return "zstd", nil
	}
	return "", nil
}

func readAtMost(open func(io.Reader) (io.Reader, error), data []byte) ([]byte, error) {
	r, err := open(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	out, err := io.ReadAll(io.LimitReader(r, maxDecoded+1))
	if err != nil {
		return nil, err
	}
	if len(out) > maxDecoded {
		return nil, errors.New("more than 1 MiB once decompressed")
	}
	return out, nil
}

// isText tells whether data reads as text: UTF-8 with no control characters
// but white space.
func isText(data []byte) bool {
	return utf8.Valid(data) && strings.IndexFunc(string(data), func(r rune) bool {
		return !unicode.IsPrint(r) && !unicode.IsSpace(r)
	}) < 0
}

type userRecord struct {
	partitionKey string
	data         []byte
}

// deaggregate splits a KPL aggregate: the magic, a protobuf AggregatedRecord
// and the MD5 of that protobuf.
//
//	message AggregatedRecord { repeated string partition_key_table = 1;
//	  repeated string explicit_hash_key_table = 2; repeated Record records = 3; }
//	message Record { uint64 partition_key_index = 1; uint64 explicit_hash_key_index = 2;
//	  bytes data = 3; repeated Tag tags = 4; }
func deaggregate(data []byte) ([]userRecord, error) {
	if len(data) < len(kplMagic)+md5.Size {
		return nil, errors.New("too short")
	}
	body := data[len(kplMagic) : len(data)-md5.Size]
	sum := md5.Sum(body)
	if !bytes.Equal(sum[:], data[len(data)-md5.Size:]) {
		return nil, errors.New("its MD5 does not match")
	}
	var keys []string
	type raw struct {
		keyIndex uint64
		data     []byte
	}
	var records []raw
	err := eachField(body, func(field uint64, wire int, v uint64, b []byte) error {
		switch {
		case field == 1 && wire == wireBytes:
			keys = append(keys, string(b))
		case field == 3 && wire == wireBytes:
			var r raw
			if err := eachField(b, func(f uint64, w int, v uint64, b []byte) error {
				switch {
				case f == 1 && w == wireVarint:
					r.keyIndex = v
				case f == 3 && w == wireBytes:
					r.data = b
				}
				return nil
			}); err != nil {
				return err
			}
			records = append(records, r)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := make([]userRecord, 0, len(records))
	for _, r := range records {
		if r.keyIndex >= uint64(len(keys)) {
			return nil, errors.New("a record names a partition key that is not in the table")
		}
		out = append(out, userRecord{partitionKey: keys[r.keyIndex], data: r.data})
	}
	return out, nil
}

const (
	wireVarint = 0
	wireI64    = 1
	wireBytes  = 2
	wireI32    = 5
)

// eachField walks the fields of a protobuf message, giving a varint's value
// or a length-delimited field's bytes; it skips fixed-width fields.
func eachField(msg []byte, fn func(field uint64, wire int, v uint64, b []byte) error) error {
	for len(msg) > 0 {
		tag, n := binary.Uvarint(msg)
		if n <= 0 {
			return errors.New("a malformed protobuf tag")
		}
		msg = msg[n:]
		field, wire := tag>>3, int(tag&7)
		var v uint64
		var b []byte
		switch wire {
		case wireVarint:
			v, n = binary.Uvarint(msg)
			if n <= 0 {
				return errors.New("a malformed protobuf varint")
			}
			msg = msg[n:]
		case wireBytes:
			l, n := binary.Uvarint(msg)
			if n <= 0 || l > uint64(len(msg)-n) {
				return errors.New("a protobuf field runs past the record")
			}
			b, msg = msg[n:n+int(l)], msg[n+int(l):]
		case wireI64, wireI32:
			size := 8
			if wire == wireI32 {
				size = 4
			}
			if len(msg) < size {
				return errors.New("a protobuf field runs past the record")
			}
			msg = msg[size:]
		default:
			return errors.New("an unknown protobuf wire type")
		}
		if err := fn(field, wire, v, b); err != nil {
			return err
		}
	}
	return nil
}
