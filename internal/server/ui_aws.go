package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

// The UI changes resources through the emulator's own AWS API, in process,
// so that what it does is validated and has the effects a client's call
// would have: S3 notifications, DynamoDB conditions, SQS limits.

// recorder captures a response from the gateway.
type recorder struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (r *recorder) Header() http.Header         { return r.header }
func (r *recorder) Write(b []byte) (int, error) { return r.body.Write(b) }
func (r *recorder) WriteHeader(status int) {
	if r.status == 0 {
		r.status = status
	}
}

func (s *Server) serve(req *http.Request) error {
	rec := &recorder{header: http.Header{}}
	s.gateway.ServeHTTP(rec, req)
	if rec.status == 0 || rec.status < 300 {
		return nil
	}
	return awsError(rec.body.Bytes())
}

// callJSON makes a call of a JSON-protocol service: target is the full
// X-Amz-Target, for example "DynamoDB_20120810.PutItem".
func (s *Server) callJSON(target, contentType string, in any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("X-Amz-Target", target)
	req.Header.Set("Content-Type", contentType)
	return s.serve(req)
}

// callS3 makes an S3 REST call on a key of a bucket; size is the body's
// length, which S3 needs and a plain reader does not tell.
func (s *Server) callS3(method, bucket, key string, body io.Reader, size int64, contentType string) error {
	u := url.URL{Path: "/" + bucket + "/" + key}
	req, err := http.NewRequest(method, u.EscapedPath(), body)
	if err != nil {
		return err
	}
	req.ContentLength = size
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return s.serve(req)
}

var xmlMessage = regexp.MustCompile(`<Message>([^<]*)</Message>`)

// awsError reads the message of an AWS error answer, JSON or XML.
func awsError(body []byte) error {
	var j struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
		Upper   string `json:"Message"`
	}
	if json.Unmarshal(body, &j) == nil && (j.Message != "" || j.Upper != "" || j.Type != "") {
		msg := j.Message + j.Upper
		code := j.Type[strings.LastIndex(j.Type, "#")+1:]
		if msg == "" {
			return errors.New(code)
		}
		if code == "" {
			return errors.New(msg)
		}
		return errors.New(code + ": " + msg)
	}
	if m := xmlMessage.FindSubmatch(body); m != nil {
		return errors.New(html.UnescapeString(string(m[1])))
	}
	return errors.New(strings.TrimSpace(string(body)))
}
