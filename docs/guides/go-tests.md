# Go tests

The `stratustest` package runs stratus inside the test process on a loopback port. It needs no
container, no Docker and no external process, and it starts in milliseconds.

```sh
go get github.com/stratus-emulator/stratus/stratustest
```

## A test

```go
package orders_test

import (
    "context"
    "testing"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/credentials"
    "github.com/aws/aws-sdk-go-v2/service/sqs"

    "github.com/stratus-emulator/stratus/stratustest"
)

func TestPlaceOrder(t *testing.T) {
    srv := stratustest.Start(t, stratustest.WithManifest(`
sqs:
  queues:
    - name: orders
`))
    client := sqs.New(sqs.Options{
        Region:       stratustest.Region,
        BaseEndpoint: aws.String(srv.URL),
        Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
    })

    // The queue exists: Start returns once the manifest's resources do.
    _, err := client.SendMessage(context.Background(), &sqs.SendMessageInput{
        QueueUrl:    aws.String(srv.URL + "/" + stratustest.Account + "/orders"),
        MessageBody: aws.String("hello"),
    })
    if err != nil {
        t.Fatal(err)
    }
}
```

The emulator stops when the test ends. Its S3 bytes live in the test's temporary directory.

## Sharing one emulator between tests

Starting an emulator is cheap, but in a large package you may start one in `TestMain`, or in the
first test of a group, and call `srv.Reset()` between tests. Reset drops all data and creates the
manifest's resources again, without a restart.

## Options

| Option | Effect |
|---|---|
| `WithManifest(yaml)` | the resources of a manifest written in the test |
| `WithManifestFile(path)` | the resources of a manifest file |
| `WithTTLInterval(d)` | deletes expired DynamoDB items in the background every `d` |
| `WithTLS()` | serves HTTPS; give the AWS client `srv.Client()` as its `HTTPClient` |

| Method | Effect |
|---|---|
| `srv.URL` | the endpoint for the AWS SDK (`BaseEndpoint`) |
| `srv.Reset()` | back to the manifest's state |
| `srv.ExpireTTL()` | deletes expired DynamoDB items now, returning how many |
| `srv.Client()` | an HTTP client that trusts the emulator's certificate (with `WithTLS`) |

## Deterministic time

Nothing time-driven happens on its own unless you ask for it:

- DynamoDB TTL deletes items only on `srv.ExpireTTL()` or with `WithTTLInterval`.
- S3 lifecycle rules and Kinesis retention trimming do not run by themselves.
- SQS drops a message past its retention when its queue is next used, as SQS does.

A test therefore sees exactly the data it wrote.

## When to use Testcontainers instead

`stratustest` runs the code of the version in your `go.mod`, in your process. Use the
[Testcontainers module](testcontainers.md) when tests must run the released image, when they are
not in Go, or when the code under test runs in other containers that need to reach the emulator.
