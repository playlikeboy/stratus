package s3

import (
	"net/http"
	"time"
)

// readConditions are the preconditions of a GET or HEAD, or, under their
// x-amz-copy-source-if-* names, of the source of a copy.
type readConditions struct {
	ifMatch           string
	ifNoneMatch       string
	ifModifiedSince   string
	ifUnmodifiedSince string
}

func readConditionsOf(h http.Header) readConditions {
	return readConditions{
		ifMatch:           h.Get("If-Match"),
		ifNoneMatch:       h.Get("If-None-Match"),
		ifModifiedSince:   h.Get("If-Modified-Since"),
		ifUnmodifiedSince: h.Get("If-Unmodified-Since"),
	}
}

func copySourceConditionsOf(h http.Header) readConditions {
	return readConditions{
		ifMatch:           h.Get("x-amz-copy-source-if-match"),
		ifNoneMatch:       h.Get("x-amz-copy-source-if-none-match"),
		ifModifiedSince:   h.Get("x-amz-copy-source-if-modified-since"),
		ifUnmodifiedSince: h.Get("x-amz-copy-source-if-unmodified-since"),
	}
}

// evaluate returns 0 when the request goes ahead, 412 or 304 otherwise.
// As in S3 (and RFC 9110): a passing If-Match overrides a failing
// If-Unmodified-Since, and a failing If-None-Match makes If-Modified-Since
// irrelevant. Times compare at second precision, as Last-Modified has.
func (c readConditions) evaluate(o *object) int {
	modified := o.lastModified.Truncate(time.Second)
	switch {
	case c.ifMatch != "":
		if !etagMatches(c.ifMatch, o.etag) {
			return http.StatusPreconditionFailed
		}
	case c.ifUnmodifiedSince != "":
		if t, err := http.ParseTime(c.ifUnmodifiedSince); err == nil && modified.After(t) {
			return http.StatusPreconditionFailed
		}
	}
	switch {
	case c.ifNoneMatch != "":
		if etagMatches(c.ifNoneMatch, o.etag) {
			return http.StatusNotModified
		}
	case c.ifModifiedSince != "":
		if t, err := http.ParseTime(c.ifModifiedSince); err == nil && !modified.After(t) {
			return http.StatusNotModified
		}
	}
	return 0
}
