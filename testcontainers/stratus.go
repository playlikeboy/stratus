// Package stratus runs the stratus AWS emulator in a container for tests,
// with Testcontainers for Go.
//
//	ctr, err := stratus.Run(ctx, stratus.DefaultImage, stratus.WithManifestYAML(`
//	sqs:
//	  queues:
//	    - name: jobs
//	`))
//	defer testcontainers.TerminateContainer(ctr)
//	endpoint, err := ctr.BaseEndpoint(ctx) // http://127.0.0.1:<port>, for any AWS SDK
//
// The container is ready once every resource of the manifest exists.
package stratus

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// DefaultImage is the stratus release this module is tested with.
const DefaultImage = "stratusemulator/stratus:0.1.0"

const (
	port         = "4566/tcp"
	manifestPath = "/manifest.yaml"
)

// Container is a running stratus.
type Container struct {
	testcontainers.Container
}

// Run starts stratus from img and waits until the resources of its manifest,
// if it has one, exist.
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Container, error) {
	moduleOpts := []testcontainers.ContainerCustomizer{
		testcontainers.WithExposedPorts(port),
		testcontainers.WithWaitStrategy(wait.ForHTTP("/_stratus/ready").WithPort(port).WithStartupTimeout(time.Minute)),
	}
	moduleOpts = append(moduleOpts, opts...)

	ctr, err := testcontainers.Run(ctx, img, moduleOpts...)
	var c *Container
	if ctr != nil {
		c = &Container{Container: ctr}
	}
	if err != nil {
		return c, fmt.Errorf("run stratus: %w", err)
	}
	return c, nil
}

// WithManifest starts stratus with the resources of the manifest file at
// path, on the host.
func WithManifest(path string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Files = append(req.Files, testcontainers.ContainerFile{
			HostFilePath: path, ContainerFilePath: manifestPath, FileMode: 0o644,
		})
		req.Cmd = append(req.Cmd, "-manifest", manifestPath)
		return nil
	}
}

// WithManifestYAML starts stratus with the resources of a manifest given as
// YAML, for a manifest written in the test itself.
func WithManifestYAML(yaml string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Files = append(req.Files, testcontainers.ContainerFile{
			Reader: strings.NewReader(yaml), ContainerFilePath: manifestPath, FileMode: 0o644,
		})
		req.Cmd = append(req.Cmd, "-manifest", manifestPath)
		return nil
	}
}

// WithTTLInterval makes stratus delete expired DynamoDB items every interval,
// as DynamoDB does in the background.
func WithTTLInterval(interval time.Duration) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Cmd = append(req.Cmd, "-ttl-interval", interval.String())
		return nil
	}
}

// BaseEndpoint is the URL to give an AWS SDK as its endpoint (BaseEndpoint in
// the AWS SDK for Go v2), or the AWS CLI as --endpoint-url.
func (c *Container) BaseEndpoint(ctx context.Context) (string, error) {
	return c.PortEndpoint(ctx, port, "http")
}

// Reset drops every resource and all data, and creates the manifest's
// resources again: a clean emulator for the next test, without a restart.
func (c *Container) Reset(ctx context.Context) error {
	_, err := c.post(ctx, "/_stratus/reset")
	return err
}

// ExpireTTL deletes the DynamoDB items whose TTL has passed and returns how
// many it deleted.
func (c *Container) ExpireTTL(ctx context.Context) (int, error) {
	resp, err := c.post(ctx, "/_stratus/expire-ttl")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var body struct{ Deleted int }
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, fmt.Errorf("expire TTL: %w", err)
	}
	return body.Deleted, nil
}

func (c *Container) post(ctx context.Context, path string) (*http.Response, error) {
	endpoint, err := c.BaseEndpoint(ctx)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("POST %s: %w", path, err)
	}
	if resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("POST %s: %s", path, resp.Status)
	}
	return resp, nil
}
