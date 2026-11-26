# Go packages

## `stratustest`

```go
import "github.com/stratus-emulator/stratus/stratustest"
```

Runs stratus inside a Go test, on a loopback port, with no container and no external process.
[pkg.go.dev](https://pkg.go.dev/github.com/stratus-emulator/stratus/stratustest) has the full API.

| | |
|---|---|
| `Start(tb, opts...) *Server` | runs an emulator until the test ends; fails the test if the manifest is invalid |
| `WithManifest(yaml)` | the resources of a manifest given as YAML |
| `WithManifestFile(path)` | the resources of a manifest file |
| `WithTTLInterval(d)` | deletes expired DynamoDB items every `d` |
| `WithTLS()` | serves HTTPS |
| `Server.URL` | the endpoint |
| `Server.Client()` | an HTTP client trusting the emulator's certificate |
| `Server.Reset()` | back to the manifest's state |
| `Server.ExpireTTL() int` | deletes expired DynamoDB items now |
| `Account`, `Region` | the fixed identity: `000000000000`, `eu-central-1` |

Guide: [Go tests](../guides/go-tests.md).

## `testcontainers`

```go
import stratus "github.com/stratus-emulator/stratus/testcontainers"
```

A [Testcontainers for Go](https://golang.testcontainers.org) module, released separately, with
tags `testcontainers/vX.Y.Z`.
[pkg.go.dev](https://pkg.go.dev/github.com/stratus-emulator/stratus/testcontainers) has the full API.

| | |
|---|---|
| `DefaultImage` | the release the module is tested with |
| `Run(ctx, image, opts...) (*Container, error)` | starts stratus and waits until the manifest's resources exist |
| `WithManifest(path)` | a manifest file on the host |
| `WithManifestYAML(yaml)` | a manifest written in the test |
| `WithTTLInterval(d)` | deletes expired DynamoDB items every `d` |
| `Container.BaseEndpoint(ctx)` | the endpoint, `http://<host>:<port>` |
| `Container.Reset(ctx)` | back to the manifest's state |
| `Container.ExpireTTL(ctx) (int, error)` | deletes expired DynamoDB items now |

Guide: [Testcontainers](../guides/testcontainers.md).
