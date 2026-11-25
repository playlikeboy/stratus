package s3

import (
	"strconv"
	"strings"
)

// byteRange is an inclusive range of an object's bytes.
type byteRange struct {
	start, end int64
}

func (b byteRange) length() int64 { return b.end - b.start + 1 }

// contentRange is the Content-Range value of a partial response.
func (b byteRange) contentRange(size int64) string {
	return "bytes " + strconv.FormatInt(b.start, 10) + "-" + strconv.FormatInt(b.end, 10) +
		"/" + strconv.FormatInt(size, 10)
}

// parseRange interprets a Range header for an object of size bytes. It
// reports ok=false when the whole object is to be served: no header, or a
// header S3 ignores (not bytes=, several ranges, malformed numbers, a first
// position after the last). A well-formed range that selects no byte of the
// object is InvalidRange.
func parseRange(header string, size int64) (byteRange, bool, error) {
	spec, found := strings.CutPrefix(strings.TrimSpace(header), "bytes=")
	if !found || strings.Contains(spec, ",") {
		return byteRange{}, false, nil
	}
	first, last, found := strings.Cut(strings.TrimSpace(spec), "-")
	if !found {
		return byteRange{}, false, nil
	}
	if first == "" {
		return suffixRange(last, size)
	}
	start, err := strconv.ParseInt(first, 10, 64)
	if err != nil || start < 0 {
		return byteRange{}, false, nil
	}
	end := size - 1
	if last != "" {
		end, err = strconv.ParseInt(last, 10, 64)
		if err != nil || end < start {
			return byteRange{}, false, nil
		}
	}
	if start >= size {
		return byteRange{}, false, errInvalidRange()
	}
	return byteRange{start: start, end: min(end, size-1)}, true, nil
}

// suffixRange is "bytes=-n": the last n bytes, or all of them when the object
// is shorter.
func suffixRange(last string, size int64) (byteRange, bool, error) {
	n, err := strconv.ParseInt(last, 10, 64)
	if err != nil || n < 0 {
		return byteRange{}, false, nil
	}
	if n == 0 || size == 0 {
		return byteRange{}, false, errInvalidRange()
	}
	return byteRange{start: max(0, size-n), end: size - 1}, true, nil
}
