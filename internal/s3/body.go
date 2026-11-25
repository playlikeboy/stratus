package s3

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// maxObjectSize is the largest object a single PUT, or a single part, may carry.
const maxObjectSize = 5 << 30

// maxXMLBody bounds the XML documents of configuration and multipart calls.
const maxXMLBody = 4 << 20

// payload is a request body read into memory and verified.
type payload struct {
	data     []byte
	checksum checksum // algorithm empty when the request named none
}

// readPayload reads the request body in whatever framing the client used,
// then verifies Content-MD5 and the announced checksum. It is always called
// without a lock held: a slow upload must not stall anyone else.
func readPayload(r *http.Request, limit int64) (payload, error) {
	data, trailers, err := readBody(r, limit)
	if err != nil {
		return payload{}, err
	}
	if err := verifyContentMD5(r.Header, data); err != nil {
		return payload{}, err
	}
	want, err := checksumFromHeaders(r.Header, trailers)
	if err != nil {
		return payload{}, err
	}
	value, err := want.verify(data)
	if err != nil {
		return payload{}, err
	}
	sum := checksum{algorithm: want.algorithm, value: value}
	if value != "" {
		sum.typ = checksumFullObject
	}
	return payload{data: data, checksum: sum}, nil
}

// isAWSChunked tells whether the body uses the aws-chunked framing: every
// STREAMING-* payload hash announces it, and Content-Encoding names it.
func isAWSChunked(h http.Header) bool {
	return strings.HasPrefix(h.Get("X-Amz-Content-Sha256"), "STREAMING-") ||
		strings.Contains(h.Get("Content-Encoding"), "aws-chunked")
}

// maxPrealloc is the most memory reserved on a client's word about a size
// before its bytes arrive; beyond it a buffer grows with the data.
const maxPrealloc = 64 << 20

// readExactly reads n bytes of r, into a slice of exactly that size when n is
// small enough to reserve up front.
func readExactly(r io.Reader, n int64) ([]byte, error) {
	if n <= maxPrealloc {
		data := make([]byte, n)
		_, err := io.ReadFull(r, data)
		return data, err
	}
	var buf bytes.Buffer
	buf.Grow(maxPrealloc)
	if _, err := io.CopyN(&buf, r, n); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// readBody returns the decoded body and the aws-chunked trailers, if any. A
// body with a known length is read straight into a slice of that size.
func readBody(r *http.Request, limit int64) ([]byte, map[string]string, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, nil, nil
	}
	if isAWSChunked(r.Header) {
		return readAWSChunked(r, limit)
	}
	if r.ContentLength > limit {
		return nil, nil, errEntityTooLarge()
	}
	if r.ContentLength >= 0 {
		data, err := readExactly(r.Body, r.ContentLength)
		if err != nil {
			return nil, nil, errIncompleteBody("You did not provide the number of bytes specified by the Content-Length HTTP header.")
		}
		return data, nil, nil
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return nil, nil, errIncompleteBody("Reading the request body failed: " + err.Error())
	}
	if int64(len(data)) > limit {
		return nil, nil, errEntityTooLarge()
	}
	return data, nil, nil
}

// readAWSChunked decodes an aws-chunked body and checks its size against
// x-amz-decoded-content-length, which is the object's size.
func readAWSChunked(r *http.Request, limit int64) ([]byte, map[string]string, error) {
	decoded := int64(-1)
	if v := r.Header.Get("X-Amz-Decoded-Content-Length"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			return nil, nil, errInvalidArgument("x-amz-decoded-content-length is not a valid length.")
		}
		if n > limit {
			return nil, nil, errEntityTooLarge()
		}
		decoded = n
	}
	data, trailers, err := decodeAWSChunked(r.Body, decoded, limit)
	if err != nil {
		return nil, nil, err
	}
	if decoded >= 0 && int64(len(data)) != decoded {
		return nil, nil, errIncompleteBody("The decoded body is " + strconv.Itoa(len(data)) +
			" bytes, x-amz-decoded-content-length announced " + strconv.FormatInt(decoded, 10) + ".")
	}
	return data, trailers, nil
}

// storedContentEncoding is the Content-Encoding to keep with an object: the
// request's, without the aws-chunked transfer framing.
func storedContentEncoding(h http.Header) string {
	var kept []string
	for _, enc := range strings.Split(h.Get("Content-Encoding"), ",") {
		if enc = strings.TrimSpace(enc); enc != "" && enc != "aws-chunked" {
			kept = append(kept, enc)
		}
	}
	return strings.Join(kept, ",")
}
