# stratus for Testcontainers

Run [stratus](https://github.com/stratus-emulator/stratus) in your Go tests with
[Testcontainers for Go](https://golang.testcontainers.org): a real AWS-compatible endpoint for SQS, S3,
DynamoDB, Kinesis and Step Functions, with the resources your test declares, ready in well under a second.

```sh
go get github.com/stratus-emulator/stratus/testcontainers
```

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

Any credentials work: stratus does not check signatures.

## Options and methods

| | |
|---|---|
| `Run(ctx, image, opts...)` | starts stratus and waits until the manifest's resources exist |
| `WithManifest(path)` | the resources of a manifest file on the host |
| `WithManifestYAML(yaml)` | the resources of a manifest written in the test |
| `WithTTLInterval(d)` | delete expired DynamoDB items in the background every `d` |
| `ctr.BaseEndpoint(ctx)` | the endpoint for an AWS SDK (`BaseEndpoint`) or the AWS CLI (`--endpoint-url`) |
| `ctr.Reset(ctx)` | drop all data and recreate the manifest's resources, for the next test, without a restart |
| `ctr.ExpireTTL(ctx)` | delete expired DynamoDB items now; returns how many |

Every Testcontainers option works as well, such as `testcontainers.WithEnv` or a network.
The manifest format is in the [stratus README](https://github.com/stratus-emulator/stratus#manifest).

## Coming from the LocalStack module

| LocalStack module | stratus |
|---|---|
| `localstack.Run(ctx, "localstack/localstack:…")` | `stratus.Run(ctx, stratus.DefaultImage)` |
| an auth token for current images | none |
| resources created by init scripts or in the test | declared in a manifest, ready when `Run` returns |
| `ctr.PortEndpoint(ctx, "4566/tcp", "http")` | `ctr.BaseEndpoint(ctx)` |
| restart, or delete what the test made | `ctr.Reset(ctx)` |

stratus emulates SQS, S3, DynamoDB, Kinesis and Step Functions; the
[stratus README](https://github.com/stratus-emulator/stratus#services) says what each covers.

## Docker on macOS

With Colima or another Docker that is not at `/var/run/docker.sock`, tell Testcontainers where it is:

```sh
export DOCKER_HOST="unix://$HOME/.colima/default/docker.sock"
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
```
