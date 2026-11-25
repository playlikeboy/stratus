package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakeService struct {
	name   string
	prefix string
	hits   int
}

func (f *fakeService) Name() string                                 { return f.name }
func (f *fakeService) TargetPrefix() string                         { return f.prefix }
func (f *fakeService) Reset()                                       { f.hits = 0 }
func (f *fakeService) ServeHTTP(http.ResponseWriter, *http.Request) { f.hits++ }

func TestRouting(t *testing.T) {
	tests := []struct {
		name   string
		target string
		auth   string
		query  string
		want   string // service expected to be hit; empty: none
	}{
		{name: "target header", target: "AmazonSQS.SendMessage", want: "sqs"},
		{name: "target wins over the scope", target: "AmazonSQS.SendMessage", auth: "AWS4-HMAC-SHA256 Credential=k/d/r/s3/aws4_request", want: "sqs"},
		{name: "credential scope", auth: "AWS4-HMAC-SHA256 Credential=test/20260101/eu-central-1/s3/aws4_request, SignedHeaders=host, Signature=e2e", want: "s3"},
		{name: "scope without spaces", auth: "AWS4-HMAC-SHA256 Credential=k/d/r/s3/aws4_request,SignedHeaders=host,Signature=x", want: "s3"},
		{name: "presigned url", query: "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=k%2Fd%2Fr%2Fs3%2Faws4_request", want: "s3"},
		{name: "unknown target and no scope", target: "Unknown.Op"},
		{name: "unknown scope", auth: "AWS4-HMAC-SHA256 Credential=k/d/r/lambda/aws4_request"},
		{name: "nothing to route by"},
		{name: "malformed credential", auth: "AWS4-HMAC-SHA256 Credential=broken"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqs := &fakeService{name: "sqs", prefix: "AmazonSQS."}
			s3 := &fakeService{name: "s3"}
			g := New(sqs, s3)

			req := httptest.NewRequest(http.MethodPost, "/?"+tt.query, nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}
			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}
			rec := httptest.NewRecorder()
			g.ServeHTTP(rec, req)

			hit := ""
			for _, s := range []*fakeService{sqs, s3} {
				if s.hits > 0 {
					hit = s.name
				}
			}
			if hit != tt.want {
				t.Fatalf("routed to %q, want %q", hit, tt.want)
			}
			if tt.want == "" && rec.Code != http.StatusNotImplemented {
				t.Fatalf("status = %d, want 501 for an unroutable request", rec.Code)
			}
			if rec.Header().Get("x-amzn-RequestId") == "" {
				t.Fatal("response has no request id")
			}
		})
	}
}
