package server

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stratus-emulator/stratus/internal/manifest"
)

func TestDefaultDataDirIsOnDiskNotTmp(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_CACHE_HOME", filepath.Join(home, "cache"))
	if got := DefaultDataDir(); filepath.Dir(got) == home || !isUnder(got, home) {
		t.Fatalf("DefaultDataDir() = %s, want the cache directory under %s", got, home)
	}

	t.Setenv("HOME", "")
	t.Setenv("XDG_CACHE_HOME", "")
	if _, err := os.Stat("/var/tmp"); err != nil {
		t.Skip("no /var/tmp here")
	}
	if got := DefaultDataDir(); got != "/var/tmp" {
		t.Fatalf("without a home directory DefaultDataDir() = %s, want /var/tmp", got)
	}
}

func isUnder(path, dir string) bool {
	rel, err := filepath.Rel(dir, path)
	return err == nil && rel != "." && !filepath.IsAbs(rel) && rel[0] != '.'
}

func TestCloseRemovesOnlyItsOwnDirectory(t *testing.T) {
	parent := t.TempDir()
	keep := filepath.Join(parent, "keep")
	if err := os.WriteFile(keep, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	srv, err := New(Config{DataDir: parent})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if entries, _ := os.ReadDir(parent); len(entries) != 2 {
		t.Fatalf("data directory has %d entries, want the file and the emulator's directory", len(entries))
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	entries, _ := os.ReadDir(parent)
	if len(entries) != 1 || entries[0].Name() != "keep" {
		t.Fatalf("after Close the data directory has %v, want only keep", entries)
	}
}

func TestConcurrentResets(t *testing.T) {
	m, err := manifest.Parse([]byte("kinesis:\n  streams:\n    - name: events\ns3:\n  buckets:\n    - name: files\n"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	srv, err := New(Config{Manifest: m, DataDir: t.TempDir()})
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
