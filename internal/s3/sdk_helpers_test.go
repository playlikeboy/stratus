package s3_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"github.com/ODudek/stratus/internal/awsapi"
	"github.com/ODudek/stratus/internal/clock"
	"github.com/ODudek/stratus/internal/gateway"
	"github.com/ODudek/stratus/internal/s3"
)

var identity = awsapi.Identity{Account: awsapi.DefaultAccount, Region: awsapi.DefaultRegion}

// env is one emulator behind an HTTP server and an SDK client.
type env struct {
	svc    *s3.Service
	srv    *httptest.Server
	client *s3sdk.Client
}

// start serves a fresh service over plain HTTP, driven by a client with
// default settings apart from the endpoint, path style and credentials.
func start(t *testing.T, c clock.Clock) *env {
	t.Helper()
	svc := s3.New(identity, c)
	srv := httptest.NewServer(gateway.New(svc))
	t.Cleanup(srv.Close)
	return &env{svc: svc, srv: srv, client: newClient(srv.URL, nil)}
}

// startTLS is start over HTTPS: the client trusts the test certificate and
// is otherwise default. Over HTTPS the SDK streams bodies as aws-chunked with
// a trailing checksum, so this covers the streaming upload path.
func startTLS(t *testing.T) *env {
	t.Helper()
	svc := s3.New(identity, clock.System{})
	srv := httptest.NewTLSServer(gateway.New(svc))
	t.Cleanup(srv.Close)
	return &env{svc: svc, srv: srv, client: newClient(srv.URL, srv.Client())}
}

func newClient(endpoint string, httpClient *http.Client) *s3sdk.Client {
	opts := s3sdk.Options{
		Region:       identity.Region,
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
	}
	if httpClient != nil {
		opts.HTTPClient = httpClient
	}
	return s3sdk.New(opts)
}

func (e *env) mustCreateBucket(t *testing.T, name string, opts s3.BucketOptions) {
	t.Helper()
	if err := e.svc.CreateBucket(name, opts); err != nil {
		t.Fatalf("CreateBucket(%s): %v", name, err)
	}
}

func (e *env) mustPut(t *testing.T, bucket, key, body string) *s3sdk.PutObjectOutput {
	t.Helper()
	out, err := e.client.PutObject(t.Context(), &s3sdk.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader(body),
	})
	if err != nil {
		t.Fatalf("PutObject(%s/%s): %v", bucket, key, err)
	}
	return out
}

func (e *env) mustGet(t *testing.T, in *s3sdk.GetObjectInput) (*s3sdk.GetObjectOutput, string) {
	t.Helper()
	out, err := e.client.GetObject(t.Context(), in)
	if err != nil {
		t.Fatalf("GetObject(%s): %v", aws.ToString(in.Key), err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("reading GetObject body: %v", err)
	}
	return out, string(body)
}

// wantAPIError asserts an SDK error carries an S3 error code.
func wantAPIError(t *testing.T, err error, code string) {
	t.Helper()
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode() != code {
		t.Fatalf("err = %v, want %s", err, code)
	}
}

// wantStatus asserts the HTTP status of an SDK error.
func wantStatus(t *testing.T, err error, status int) {
	t.Helper()
	var respErr interface{ HTTPStatusCode() int }
	if !errors.As(err, &respErr) || respErr.HTTPStatusCode() != status {
		t.Fatalf("err = %v, want HTTP %d", err, status)
	}
}

// onlyReader hides every method but Read, so that the SDK cannot seek it
// nor learn its length.
type onlyReader struct{ r io.Reader }

func (o onlyReader) Read(p []byte) (int, error) { return o.r.Read(p) }

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
