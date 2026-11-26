// Command stratus is a local emulator of the AWS services an application
// needs in development and end-to-end tests.
package main

import (
	"context"
	"crypto/tls"
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

	"github.com/stratus-emulator/stratus/internal/awsapi"
	"github.com/stratus-emulator/stratus/internal/manifest"
	"github.com/stratus-emulator/stratus/internal/server"
)

const shutdownTimeout = 5 * time.Second

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("stratus failed", "error", err)
		os.Exit(1)
	}
}

// version is the release, set when the binary is built: -ldflags "-X main.version=v1.2.3".
var version = "dev"

func run(args []string) error {
	flags := flag.NewFlagSet("stratus", flag.ContinueOnError)
	showVersion := flags.Bool("version", false, "print the version and exit")
	listen := flags.String("listen", ":4566", "address to listen on")
	manifestPath := flags.String("manifest", "", "YAML file with the resources to create at start")
	region := flags.String("region", awsapi.DefaultRegion, "region of every resource")
	account := flags.String("account", awsapi.DefaultAccount, "account id of every resource")
	healthcheck := flags.Bool("healthcheck", false, "check that a running stratus on -listen is ready, then exit")
	ttlInterval := flags.Duration("ttl-interval", 0, "how often to delete expired DynamoDB items in the background, for example 1m; 0 only on POST /_stratus/expire-ttl")
	dataDir := flags.String("data-dir", "", "existing directory to keep S3 object bytes in, under a directory removed on exit; when empty the user's cache directory, else /var/tmp (never /tmp, often a tmpfs held in memory)")
	var https tlsOptions
	tlsListen := flags.String("tls-listen", "", "address to also serve HTTPS on, for example :4567")
	flags.StringVar(&https.certFile, "tls-cert", "", "certificate file for -tls-listen; a self-signed one is generated when empty")
	flags.StringVar(&https.keyFile, "tls-key", "", "private key file for -tls-cert")
	flags.StringVar(&https.hosts, "tls-hosts", "localhost,127.0.0.1,::1", "names and addresses a generated certificate is valid for")
	flags.StringVar(&https.certOut, "tls-cert-out", "", "file to write a generated certificate to, for clients to trust")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *showVersion {
		fmt.Println("stratus", version)
		return nil
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
		Identity:    awsapi.Identity{Account: *account, Region: *region},
		Manifest:    resources,
		DataDir:     *dataDir,
		TTLInterval: *ttlInterval,
	})
	if err != nil {
		return fmt.Errorf("creating the manifest's resources: %w", err)
	}
	defer func() { _ = emulator.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", *listen)
	if err != nil {
		return err
	}
	// No read or write timeout: a long-polling receive is a slow request by design.
	httpServer := &http.Server{Handler: emulator, ReadHeaderTimeout: 10 * time.Second}
	served := make(chan error, 2)
	go func() { served <- httpServer.Serve(listener) }()
	if *tlsListen != "" {
		cert, err := https.certificate()
		if err != nil {
			return err
		}
		secure, err := (&net.ListenConfig{}).Listen(ctx, "tcp", *tlsListen)
		if err != nil {
			return err
		}
		httpServer.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
		go func() { served <- httpServer.ServeTLS(secure, "", "") }()
		slog.Info("stratus serves HTTPS", "listen", secure.Addr().String())
	}
	slog.Info("stratus is ready", "version", version, "listen", listener.Addr().String(), "region", *region, "manifest", *manifestPath)

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
