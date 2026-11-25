// Package stratustest runs a stratus emulator inside a Go test, on a loopback
// port, with no container and no external process.
package stratustest

import (
	"net/http/httptest"
	"testing"

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

	tb    testing.TB
	inner *server.Server
}

type options struct {
	manifest manifest.Manifest
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

// Start runs an emulator until the test ends.
func Start(tb testing.TB, opts ...Option) *Server {
	tb.Helper()
	var o options
	for _, opt := range opts {
		opt(tb, &o)
	}
	inner, err := server.New(server.Config{Manifest: o.manifest})
	if err != nil {
		tb.Fatalf("stratustest: %v", err)
	}
	ts := httptest.NewServer(inner)
	tb.Cleanup(func() {
		// Wake long-polling receivers first: Close waits for open requests.
		ts.CloseClientConnections()
		ts.Close()
	})
	return &Server{URL: ts.URL, tb: tb, inner: inner}
}

// Reset returns the emulator to the state it started in: all data is dropped
// and the manifest's resources are created again.
func (s *Server) Reset() {
	s.tb.Helper()
	if err := s.inner.Reset(); err != nil {
		s.tb.Fatalf("stratustest: reset: %v", err)
	}
}
