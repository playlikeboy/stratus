// Package server composes the emulator: the services, the gateway in front of
// them, the manifest they start from and the control endpoints.
package server

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ODudek/stratus/internal/awsapi"
	"github.com/ODudek/stratus/internal/clock"
	"github.com/ODudek/stratus/internal/dynamodb"
	"github.com/ODudek/stratus/internal/gateway"
	"github.com/ODudek/stratus/internal/kinesis"
	"github.com/ODudek/stratus/internal/manifest"
	"github.com/ODudek/stratus/internal/s3"
	"github.com/ODudek/stratus/internal/sfn"
	"github.com/ODudek/stratus/internal/sqs"
)

// Config is what an emulator is built from. Zero fields take the defaults.
type Config struct {
	Identity awsapi.Identity
	Manifest manifest.Manifest
	Clock    clock.Clock
	// DataDir is the existing directory in which S3 keeps object bytes, in a
	// directory of their own that Close removes. Empty: DefaultDataDir.
	DataDir string
	// TTLInterval is how often expired DynamoDB items are deleted in the
	// background, as DynamoDB does. Zero: never by itself, only through
	// ExpireTTL or POST /_stratus/expire-ttl, so tests stay deterministic.
	TTLInterval time.Duration
}

// Server is one emulator. It is ready as soon as New returns: the manifest's
// resources already exist.
type Server struct {
	// resetMu makes a reset one step: two interleaved resets would each find
	// the other's resources in the way.
	resetMu  sync.Mutex
	identity awsapi.Identity
	manifest manifest.Manifest
	targets  manifest.Targets
	gateway  *gateway.Gateway
	control  *http.ServeMux
	dataDir  string // this emulator's own directory of object bytes, removed by Close

	closeOnce sync.Once
	closed    chan struct{} // closed by Close; stops the background TTL expiry
}

func New(cfg Config) (*Server, error) {
	if cfg.Identity.Account == "" {
		cfg.Identity.Account = awsapi.DefaultAccount
	}
	if cfg.Identity.Region == "" {
		cfg.Identity.Region = awsapi.DefaultRegion
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.System{}
	}
	if cfg.DataDir == "" {
		cfg.DataDir = DefaultDataDir()
	}
	// A directory of its own: nothing left by an earlier run is in the way.
	dataDir, err := os.MkdirTemp(cfg.DataDir, "stratus-")
	if err != nil {
		return nil, fmt.Errorf("creating the data directory: %w", err)
	}
	queues := sqs.New(cfg.Identity, cfg.Clock)
	streams := kinesis.New(cfg.Identity, cfg.Clock)
	buckets := s3.New(cfg.Identity, cfg.Clock, dataDir)
	buckets.SetQueueNotifier(notifyQueue(queues))
	tables := dynamodb.New(cfg.Identity, cfg.Clock)
	machines := sfn.New(cfg.Identity, cfg.Clock)
	machines.RegisterIntegration(sqsSendMessageResource, sqsSendMessage(queues))
	s := &Server{
		identity: cfg.Identity,
		manifest: cfg.Manifest,
		targets:  manifest.Targets{Identity: cfg.Identity, SQS: queues, Kinesis: streams, S3: buckets, DynamoDB: tables, SFN: machines},
		gateway:  gateway.New(queues, streams, buckets, tables, machines).WithFallback(buckets),
		control:  http.NewServeMux(),
		dataDir:  dataDir,
		closed:   make(chan struct{}),
	}
	s.control.HandleFunc("GET /_stratus/ready", s.handleReady)
	s.control.HandleFunc("POST /_stratus/reset", s.handleReset)
	s.control.HandleFunc("POST /_stratus/expire-ttl", s.handleExpireTTL)
	s.registerUI()
	// Kept so that health checks written for LocalStack keep working.
	s.control.HandleFunc("GET /_localstack/health", s.handleLocalStackHealth)

	if err := s.applyManifest(); err != nil {
		_ = s.Close()
		return nil, err
	}
	if cfg.TTLInterval > 0 {
		go s.expireTTLEvery(cfg.TTLInterval)
	}
	return s, nil
}

// expireTTLEvery deletes the expired DynamoDB items every interval, as
// DynamoDB does in the background, until Close.
func (s *Server) expireTTLEvery(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.closed:
			return
		case <-ticker.C:
			s.targets.DynamoDB.ExpireTTL()
		}
	}
}

// ExpireTTL deletes the DynamoDB items whose TTL has passed and returns how
// many it deleted.
func (s *Server) ExpireTTL() int { return s.targets.DynamoDB.ExpireTTL() }

// DefaultDataDir is where object bytes go when Config.DataDir is empty: a
// directory on disk, never /tmp, which is a tmpfs held in memory on many
// systems. It is the user's cache directory, else /var/tmp, which outlives a
// reboot and so is not a tmpfs, else the system's temporary directory.
func DefaultDataDir() string {
	if cache, err := os.UserCacheDir(); err == nil {
		dir := filepath.Join(cache, "stratus")
		if os.MkdirAll(dir, 0o700) == nil {
			return dir
		}
	}
	if info, err := os.Stat("/var/tmp"); err == nil && info.IsDir() {
		return "/var/tmp"
	}
	return os.TempDir()
}

// Close stops the background TTL expiry and removes the object bytes the
// emulator kept on disk. The emulator must not serve requests after it.
func (s *Server) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return os.RemoveAll(s.dataDir)
}

// Reset returns the emulator to the state it started in: everything is
// dropped and the manifest's resources are created again.
func (s *Server) Reset() error {
	s.resetMu.Lock()
	defer s.resetMu.Unlock()
	s.gateway.Reset()
	return s.applyManifest()
}

func (s *Server) applyManifest() error {
	return s.manifest.Apply(s.targets)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Control paths are matched by hand: a ServeMux in front of the services
	// would clean and redirect paths that are valid object keys.
	if strings.HasPrefix(r.URL.Path, "/_stratus/") || strings.HasPrefix(r.URL.Path, "/_localstack/") {
		s.control.ServeHTTP(w, r)
		return
	}
	s.gateway.ServeHTTP(w, r)
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	awsapi.WriteJSON(w, "application/json", http.StatusOK, map[string]any{
		"ready":    true,
		"services": s.gateway.Services(),
	})
}

func (s *Server) handleReset(w http.ResponseWriter, _ *http.Request) {
	if err := s.Reset(); err != nil {
		awsapi.WriteJSON(w, "application/json", http.StatusInternalServerError,
			map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleExpireTTL(w http.ResponseWriter, _ *http.Request) {
	awsapi.WriteJSON(w, "application/json", http.StatusOK, map[string]int{"deleted": s.ExpireTTL()})
}

func (s *Server) handleLocalStackHealth(w http.ResponseWriter, _ *http.Request) {
	services := map[string]string{}
	for _, name := range s.gateway.Services() {
		services[name] = "running"
	}
	awsapi.WriteJSON(w, "application/json", http.StatusOK, map[string]any{
		"services": services,
		"edition":  "stratus",
	})
}
