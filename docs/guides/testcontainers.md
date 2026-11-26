# Testcontainers

The `testcontainers` module runs the stratus image from a Go test with
[Testcontainers for Go](https://golang.testcontainers.org).

```sh
go get github.com/stratus-emulator/stratus/testcontainers
```

## A test

```go
import (
    "github.com/aws/aws-sdk-go-v2/service/sqs"
    "github.com/testcontainers/testcontainers-go"

    stratus "github.com/stratus-emulator/stratus/testcontainers"
)

func TestJobs(t *testing.T) {
    ctx := context.Background()
    ctr, err := stratus.Run(ctx, stratus.DefaultImage, stratus.WithManifestYAML(`
sqs:
  queues:
    - name: jobs
`))
    testcontainers.CleanupContainer(t, ctr)
    if err != nil {
        t.Fatal(err)
    }
    endpoint, err := ctr.BaseEndpoint(ctx)
    if err != nil {
        t.Fatal(err)
    }
    client := sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = &endpoint })
    // The queue "jobs" exists: Run returns once the manifest's resources do.
}
```

## Options and methods

| | |
|---|---|
| `Run(ctx, image, opts...)` | starts stratus and waits until the manifest's resources exist |
| `WithManifest(path)` | the resources of a manifest file on the host |
| `WithManifestYAML(yaml)` | the resources of a manifest written in the test |
| `WithTTLInterval(d)` | deletes expired DynamoDB items in the background every `d` |
| `ctr.BaseEndpoint(ctx)` | the endpoint for an AWS SDK (`BaseEndpoint`) or the AWS CLI (`--endpoint-url`) |
| `ctr.Reset(ctx)` | drops all data and recreates the manifest's resources, without a restart |
| `ctr.ExpireTTL(ctx)` | deletes expired DynamoDB items now; returns how many |

Every Testcontainers option works as well, such as `testcontainers.WithEnv`,
`testcontainers.WithCmdArgs` or a network.

## HTTPS in a container

Pass the stratus flags and expose the port, then copy the certificate stratus wrote:

```go
ctr, err := stratus.Run(ctx, stratus.DefaultImage,
    testcontainers.WithCmdArgs("-tls-listen", ":4567", "-tls-cert-out", "/data/tls.pem"),
    testcontainers.WithExposedPorts("4567/tcp"),
)
pem, err := ctr.CopyFileFromContainer(ctx, "/data/tls.pem")
```

## Coming from the LocalStack module

| LocalStack module | stratus |
|---|---|
| `localstack.Run(ctx, "localstack/localstack:…")` | `stratus.Run(ctx, stratus.DefaultImage)` |
| an auth token for current images | none |
| resources created by init scripts or in the test | declared in a manifest, ready when `Run` returns |
| `ctr.PortEndpoint(ctx, "4566/tcp", "http")` | `ctr.BaseEndpoint(ctx)` |
| restart, or delete what the test made | `ctr.Reset(ctx)` |

## Docker on macOS

With Colima, or any Docker that is not at `/var/run/docker.sock`, tell Testcontainers where it is:

```sh
export DOCKER_HOST="unix://$HOME/.colima/default/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
```

!!! note "Without the module"
    You do not need the module to run stratus with Testcontainers. A plain
    `testcontainers.Run(ctx, "stratusemulator/stratus:0.2.0", …)` that waits on
    `wait.ForHTTP("/_stratus/ready")` does the same, and adds no dependency to your module.
