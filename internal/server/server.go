// Package server composes the emulator: the services, the gateway in front of
// them, the manifest they start from and the control endpoints.
package server

import (
	"net/http"
	"strings"

	"github.com/ODudek/stratus/internal/awsapi"
	"github.com/ODudek/stratus/internal/clock"
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
}

// Server is one emulator. It is ready as soon as New returns: the manifest's
// resources already exist.
type Server struct {
	identity awsapi.Identity
	manifest manifest.Manifest
	targets  manifest.Targets
	gateway  *gateway.Gateway
	control  *http.ServeMux
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
	queues := sqs.New(cfg.Identity, cfg.Clock)
	streams := kinesis.New(cfg.Identity, cfg.Clock)
	buckets := s3.New(cfg.Identity, cfg.Clock)
	buckets.SetQueueNotifier(notifyQueue(queues))
	machines := sfn.New(cfg.Identity, cfg.Clock)
	machines.RegisterIntegration(sqsSendMessageResource, sqsSendMessage(queues))
	s := &Server{
		identity: cfg.Identity,
		manifest: cfg.Manifest,
		targets:  manifest.Targets{Identity: cfg.Identity, SQS: queues, Kinesis: streams, S3: buckets, SFN: machines},
		gateway:  gateway.New(queues, streams, buckets, machines).WithFallback(buckets),
		control:  http.NewServeMux(),
	}
	s.control.HandleFunc("GET /_stratus/ready", s.handleReady)
	s.control.HandleFunc("POST /_stratus/reset", s.handleReset)
	// Kept so that health checks written for LocalStack keep working.
	s.control.HandleFunc("GET /_localstack/health", s.handleLocalStackHealth)

	if err := s.applyManifest(); err != nil {
		return nil, err
	}
	return s, nil
}

// Reset returns the emulator to the state it started in: everything is
// dropped and the manifest's resources are created again.
func (s *Server) Reset() error {
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
