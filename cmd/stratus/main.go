// Command stratus is a local emulator of the AWS services an application
// needs in development and end-to-end tests.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ODudek/stratus/internal/awsapi"
	"github.com/ODudek/stratus/internal/manifest"
	"github.com/ODudek/stratus/internal/server"
)

const shutdownTimeout = 5 * time.Second

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("stratus failed", "error", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("stratus", flag.ContinueOnError)
	listen := flags.String("listen", ":4566", "address to listen on")
	manifestPath := flags.String("manifest", "", "YAML file with the resources to create at start")
	region := flags.String("region", awsapi.DefaultRegion, "region of every resource")
	account := flags.String("account", awsapi.DefaultAccount, "account id of every resource")
	healthcheck := flags.Bool("healthcheck", false, "check that a running stratus on -listen is ready, then exit")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *healthcheck {
		return checkReady(*listen)
	}

	var resources manifest.Manifest
	if *manifestPath != "" {
		loaded, err := manifest.Load(*manifestPath)
		if err != nil {
			return err
		}
		resources = loaded
	}
	emulator, err := server.New(server.Config{
		Identity: awsapi.Identity{Account: *account, Region: *region},
		Manifest: resources,
	})
	if err != nil {
		return fmt.Errorf("creating the manifest's resources: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", *listen)
	if err != nil {
		return err
	}
	// No read or write timeout: a long-polling receive is a slow request by design.
	httpServer := &http.Server{Handler: emulator, ReadHeaderTimeout: 10 * time.Second}
	served := make(chan error, 1)
	go func() { served <- httpServer.Serve(listener) }()
	slog.Info("stratus is ready", "listen", listener.Addr().String(), "region", *region, "manifest", *manifestPath)

	select {
	case err := <-served:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		// Long-polling receivers are still open: cut them.
		return httpServer.Close()
	}
	return nil
}

// checkReady is the container health check: the image has no shell or curl.
func checkReady(listen string) error {
	addr := listen
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/_stratus/ready", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("stratus is not ready: HTTP %d", resp.StatusCode)
	}
	return nil
}
