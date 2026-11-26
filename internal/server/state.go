package server

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/stratus-emulator/stratus/internal/manifest"
)

// DefaultStateInterval is how often the state is saved when
// Config.StateInterval is zero.
const DefaultStateInterval = 10 * time.Second

// persistentService is a service whose state can be saved and brought back.
type persistentService interface {
	Snapshot(keep func(name string) bool) ([]byte, error)
	Restore(data []byte, keep func(name string) bool) error
}

// stateFile is one service's state in the state directory.
type stateFile struct {
	service string // as the manifest's persistence names it
	file    string
	svc     persistentService
}

func (s *Server) stateFiles() []stateFile {
	// Step Functions last: a restored execution may resume at a Task that
	// calls another service.
	return []stateFile{
		{manifest.ServiceSQS, "sqs.json", s.targets.SQS},
		{manifest.ServiceKinesis, "kinesis.json", s.targets.Kinesis},
		{manifest.ServiceS3, "s3.json", s.targets.S3},
		{manifest.ServiceDynamoDB, "dynamodb.json", s.targets.DynamoDB},
		{manifest.ServiceStepFunctions, "stepfunctions.json", s.targets.SFN},
	}
}

// loadState brings back the state saved in the state directory, the
// resources the manifest keeps only. A service without a saved state starts
// empty; S3's directory then loses the bytes of an earlier run.
func (s *Server) loadState() error {
	for _, f := range s.stateFiles() {
		data, err := os.ReadFile(filepath.Join(s.stateDir, f.file))
		if errors.Is(err, fs.ErrNotExist) {
			if f.service == manifest.ServiceS3 {
				data = []byte(`{"format":1}`) // no buckets: every file of the directory goes
			} else {
				continue
			}
		} else if err != nil {
			return fmt.Errorf("reading the saved state: %w", err)
		}
		if err := f.svc.Restore(data, s.persistence.Filter(f.service)); err != nil {
			return fmt.Errorf("%s: %w; delete it to start that service empty", filepath.Join(s.stateDir, f.file), err)
		}
	}
	return nil
}

// Save writes the state of the resources the manifest keeps to the state
// directory, one file per service, each replaced at once: a crash leaves
// either the earlier state or this one. Without a state directory it does
// nothing.
func (s *Server) Save() error {
	if s.stateDir == "" {
		return nil
	}
	s.resetMu.Lock()
	defer s.resetMu.Unlock()
	var errs []error
	for _, f := range s.stateFiles() {
		data, err := f.svc.Snapshot(s.persistence.Filter(f.service))
		if err == nil {
			err = writeFileAtomic(filepath.Join(s.stateDir, f.file), data)
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("saving the %s state: %w", f.service, err))
		}
	}
	return errors.Join(errs...)
}

// writeFileAtomic replaces path with data through a synced temporary file
// renamed over it.
func writeFileAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// saveEvery saves the state every interval until Close.
func (s *Server) saveEvery(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.closed:
			return
		case <-ticker.C:
			if err := s.Save(); err != nil {
				slog.Error("saving the state failed", "error", err)
			}
		}
	}
}
