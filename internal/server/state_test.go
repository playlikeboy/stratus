package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stratus-emulator/stratus/internal/manifest"
	"github.com/stratus-emulator/stratus/internal/s3"
	"github.com/stratus-emulator/stratus/internal/sqs"
)

const stateManifest = `
sqs:
  queues:
    - name: jobs
    - name: scratch
      persistence: false
s3:
  buckets:
    - name: docs
`

func newStateServer(t *testing.T, dir, text string) *Server {
	t.Helper()
	m, err := manifest.Parse([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{Manifest: m, StateDir: dir, StateInterval: time.Hour})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func receiveBodies(t *testing.T, s *Server, queue string) []string {
	t.Helper()
	noWait := time.Duration(0)
	got, err := s.targets.SQS.Receive(context.Background(), queue, sqs.ReceiveInput{Max: 10, Wait: &noWait})
	if err != nil {
		t.Fatalf("Receive(%s): %v", queue, err)
	}
	var bodies []string
	for _, m := range got {
		bodies = append(bodies, m.Body)
	}
	return bodies
}

func TestAServerWithAStateDirectoryKeepsItsDataAcrossARestart(t *testing.T) {
	dir := t.TempDir()
	first := newStateServer(t, dir, stateManifest)
	for _, q := range []string{"jobs", "scratch"} {
		if _, err := first.targets.SQS.Send(q, sqs.SendInput{Body: "hello " + q}); err != nil {
			t.Fatal(err)
		}
	}
	if err := first.targets.S3.PutObject("docs", "a.txt", []byte("on disk"), s3.ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	second := newStateServer(t, dir, stateManifest)
	defer func() { _ = second.Close() }()
	if got := receiveBodies(t, second, "jobs"); len(got) != 1 || got[0] != "hello jobs" {
		t.Errorf("jobs after a restart: %v, want its message", got)
	}
	if got := receiveBodies(t, second, "scratch"); len(got) != 0 {
		t.Errorf("scratch, not kept, after a restart: %v, want empty", got)
	}
	body, _, found, err := second.targets.S3.ObjectHead("docs", "a.txt", 100)
	if err != nil || !found || string(body) != "on disk" {
		t.Errorf("docs/a.txt after a restart: %q, %v, %v", body, found, err)
	}
}

func TestWithoutAStateDirectoryNothingIsKept(t *testing.T) {
	m, err := manifest.Parse([]byte("persistence: true\nsqs:\n  queues:\n    - name: jobs\n"))
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(Config{Manifest: m, DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Save(); err != nil {
		t.Errorf("Save without a state directory: %v", err)
	}
	_ = s.Close()
}

func TestTheSaveEndpointWritesTheState(t *testing.T) {
	dir := t.TempDir()
	s := newStateServer(t, dir, stateManifest)
	defer func() { _ = s.Close() }()
	if _, err := s.targets.SQS.Send("jobs", sqs.SendInput{Body: "x"}); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/_stratus/save", nil))
	if rec.Code != http.StatusNoContent {
		t.Fatalf("POST /_stratus/save = %d %s", rec.Code, rec.Body)
	}
	data, err := os.ReadFile(filepath.Join(dir, "sqs.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 || !strings.Contains(string(data), `"x"`) {
		t.Errorf("sqs.json after a save: %s", data)
	}
}
