# stratus

A local emulator of the AWS services an application needs in development and
end-to-end tests. One static Go binary, state in memory, ready in milliseconds.

- **One port, no signature checks.** Every service is served on `:4566`. A request is routed by
  its `X-Amz-Target` header or, failing that, by the service in its SigV4 credential scope. The
  signature itself is never verified, so any credentials work, including a placeholder written by
  hand in a test tool.
- **Resources are declared, not scripted.** A YAML manifest lists what exists at start. The
  emulator is ready only once all of it exists, and a key it does not understand stops the start.
- **Built for tests.** `POST /_stratus/reset` returns to the manifest's state between suites, and
  the `stratustest` package runs the emulator inside a Go test with no container.

## Status

| Service | State |
|---|---|
| SQS (standard queues, JSON protocol) | available |
| Kinesis, S3, DynamoDB, Step Functions | planned, in that order |

SQS covers `CreateQueue`, `DeleteQueue`, `GetQueueUrl`, `ListQueues`, `GetQueueAttributes`,
`SetQueueAttributes`, `PurgeQueue`, `SendMessage`, `ReceiveMessage`, `DeleteMessage` and
`ChangeMessageVisibility`, with delays, visibility timeouts, long polling, message attributes and
dead-letter queues. Not emulated: FIFO queues, the batch operations, the legacy query protocol and
message retention (a message stays until it is deleted or purged).

## Run

```sh
make build
./bin/stratus -manifest examples/manifest.yaml
```

```sh
docker build -t stratus .
docker run --rm -p 4566:4566 -v "$PWD/examples/manifest.yaml:/manifest.yaml:ro" stratus -manifest /manifest.yaml
```

Point a client at it with any credentials:

```sh
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=eu-central-1
aws --endpoint-url http://localhost:4566 sqs list-queues
```

| Flag | Default | Meaning |
|---|---|---|
| `-listen` | `:4566` | address to listen on |
| `-manifest` | none | YAML file with the resources to create at start |
| `-region` | `eu-central-1` | region of every resource |
| `-account` | `000000000000` | account id of every resource |
| `-healthcheck` | | check that a running stratus on `-listen` is ready, then exit |

The image has no shell, so a container health check calls the binary:

```yaml
healthcheck:
  test: ["CMD", "/stratus", "-healthcheck"]
  interval: 1s
```

## Manifest

```yaml
sqs:
  queues:
    - name: orders
      visibilityTimeoutSeconds: 60
      deadLetter:
        queue: orders-dlq
        maxReceiveCount: 5
    - name: orders-dlq
```

Queues may be listed in any order. An unknown key, a duplicate name or a dead-letter queue that is
not declared is an error at start.

## Control endpoints

| Endpoint | Meaning |
|---|---|
| `GET /_stratus/ready` | `200` once the manifest's resources exist, with the list of services |
| `POST /_stratus/reset` | drop all state and create the manifest's resources again |
| `GET /_localstack/health` | the same answer in LocalStack's shape, for existing health checks |

## In a Go test

```go
srv := stratustest.Start(t, stratustest.WithManifestFile("testdata/manifest.yaml"))

client := sqs.New(sqs.Options{
    Region:       stratustest.Region,
    BaseEndpoint: aws.String(srv.URL),
    Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
})
```

The emulator stops with the test. `srv.Reset()` returns it to the manifest's state.

## Develop

```sh
make test   # go test -race ./...
make lint   # gofmt and go vet
```

The tests in `stratustest/` drive the emulator through the real AWS SDK for Go: a behaviour is
considered emulated when the SDK accepts it, including its typed errors.

Layout: `cmd/stratus` is the binary, `internal/gateway` routes requests, each service lives in
`internal/<service>`, `internal/manifest` reads the resource manifest and `internal/server` puts
them together.
