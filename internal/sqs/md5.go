package sqs

import (
	"crypto/md5" //nolint:gosec // MD5 is what the SQS wire format prescribes
	"encoding/binary"
	"encoding/hex"
	"slices"
	"strings"
)

func md5Hex(s string) string {
	sum := md5.Sum([]byte(s)) //nolint:gosec
	return hex.EncodeToString(sum[:])
}

// md5OfAttributes is the digest SQS returns as MD5OfMessageAttributes: over the
// attributes sorted by name, each as length-prefixed name, data type and value,
// with a transport-type byte (1 string, 2 binary) before the value. It is
// empty when there are no attributes.
func md5OfAttributes(attrs map[string]MessageAttribute) string {
	if len(attrs) == 0 {
		return ""
	}
	names := make([]string, 0, len(attrs))
	for name := range attrs {
		names = append(names, name)
	}
	slices.Sort(names)

	h := md5.New() //nolint:gosec
	writeField := func(b []byte) {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(b))) //nolint:gosec // bounded by the message size limit
		_, _ = h.Write(size[:])
		_, _ = h.Write(b)
	}
	for _, name := range names {
		attr := attrs[name]
		writeField([]byte(name))
		writeField([]byte(attr.DataType))
		if strings.HasPrefix(attr.DataType, "Binary") {
			_, _ = h.Write([]byte{2})
			writeField(attr.BinaryValue)
			continue
		}
		_, _ = h.Write([]byte{1})
		value := ""
		if attr.StringValue != nil {
			value = *attr.StringValue
		}
		writeField([]byte(value))
	}
	return hex.EncodeToString(h.Sum(nil))
}
