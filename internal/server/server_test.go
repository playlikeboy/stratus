package server

import (
	"sync"
	"testing"

	"github.com/ODudek/stratus/internal/manifest"
)

func TestConcurrentResets(t *testing.T) {
	m, err := manifest.Parse([]byte("kinesis:\n  streams:\n    - name: events\ns3:\n  buckets:\n    - name: files\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	srv, err := New(Config{Manifest: m})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 40)
	for range 40 {
		wg.Go(func() { errs <- srv.Reset() })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("a concurrent reset failed: %v", err)
		}
	}
}
