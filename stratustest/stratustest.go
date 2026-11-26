// Package stratustest runs a stratus emulator inside a Go test, on a loopback
// port, with no container and no external process.
package stratustest

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ODudek/stratus/internal/awsapi"
	"github.com/ODudek/stratus/internal/manifest"
	"github.com/ODudek/stratus/internal/server"
)

// Fixed identity of every emulator; clients may use any credentials.
const (
	Account = awsapi.DefaultAccount
	Region  = awsapi.DefaultRegion
)

// Server is a running emulator.
type Server struct {
	// URL is the endpoint to point AWS clients at.
	URL string

	tb     testing.TB
	inner  *server.Server
	client *http.Client
}

// Client is an HTTP client that trusts the emulator's certificate when it
// was started WithTLS.
func (s *Server) Client() *http.Client { return s.client }

type options struct {
	manifest    manifest.Manifest
	tls         bool
	ttlInterval time.Duration
}

// Option configures Start.
type Option func(testing.TB, *options)

// WithManifest starts the emulator with the resources of the YAML manifest.
func WithManifest(yaml string) Option {
	return func(tb testing.TB, o *options) {
		tb.Helper()
		m, err := manifest.Parse([]byte(yaml))
		if err != nil {
			tb.Fatalf("stratustest: manifest: %v", err)
		}
		o.manifest = m
	}
}

// WithManifestFile starts the emulator with the resources of the manifest file.
func WithManifestFile(path string) Option {
	return func(tb testing.TB, o *options) {
		tb.Helper()
		m, err := manifest.Load(path)
		if err != nil {
			tb.Fatalf("stratustest: %v", err)
		}
		o.manifest = m
	}
}

// WithTTLInterval deletes expired DynamoDB items in the background every d,
// as DynamoDB does. Without it they are deleted only by Server.ExpireTTL.
func WithTTLInterval(d time.Duration) Option {
	return func(_ testing.TB, o *options) { o.ttlInterval = d }
}

// WithTLS serves the emulator over HTTPS; AWS clients must then use
// Server.Client, which trusts its certificate. The AWS SDK streams a request
// body it cannot rewind only over HTTPS.
func WithTLS() Option {
	return func(_ testing.TB, o *options) { o.tls = true }
}

// Start runs an emulator until the test ends.
func Start(tb testing.TB, opts ...Option) *Server {
	tb.Helper()
	var o options
	for _, opt := range opts {
		opt(tb, &o)
	}
	inner, err := server.New(server.Config{Manifest: o.manifest, DataDir: tb.TempDir(), TTLInterval: o.ttlInterval})
	if err != nil {
		tb.Fatalf("stratustest: %v", err)
	}
	// Registered first, so it runs after the HTTP server has stopped.
	tb.Cleanup(func() { _ = inner.Close() })
	ts := httptest.NewUnstartedServer(inner)
	if o.tls {
		ts.StartTLS()
	} else {
		ts.Start()
	}
	tb.Cleanup(func() {
		// Wake long-polling receivers first: Close waits for open requests.
		ts.CloseClientConnections()
		ts.Close()
	})
	return &Server{URL: ts.URL, tb: tb, inner: inner, client: ts.Client()}
}

// Reset returns the emulator to the state it started in: all data is dropped
// and the manifest's resources are created again.
func (s *Server) Reset() {
	s.tb.Helper()
	if err := s.inner.Reset(); err != nil {
		s.tb.Fatalf("stratustest: reset: %v", err)
	}
}

// ExpireTTL deletes the DynamoDB items whose TTL attribute is in the past, as
// DynamoDB does in the background, and returns how many it deleted.
func (s *Server) ExpireTTL() int { return s.inner.ExpireTTL() }
