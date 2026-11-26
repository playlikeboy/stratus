# stratus

A local emulator of **SQS, S3, DynamoDB, Kinesis and Step Functions** for development and
end-to-end tests: one static Go binary, ready in milliseconds, with the resources you declare in a
manifest.

```sh
docker run --rm -p 4566:4566 stratusemulator/stratus
```

![The stratus web panel: the overview, a Kinesis stream whose KPL-aggregated records are decoded, and a DynamoDB item being edited](assets/panel-tour.gif)

## Features

- **One port, any credentials.** Every service is served on `:4566`. Signatures are never
  checked, so any AWS SDK or the AWS CLI works with placeholder credentials.
- **Resources are declared, not scripted.** A YAML [manifest](reference/manifest.md) lists what
  exists at start, and stratus is ready only once all of it exists.
- **Built for tests.** `POST /_stratus/reset` returns to the manifest between suites. The emulator
  runs [inside a Go test](guides/go-tests.md) with no container, or
  [with Testcontainers](guides/testcontainers.md).
- **Keeps data when you want it to.** With `-state-dir`, all five services keep their data across
  restarts, chosen [per resource](guides/persistence.md).
- **A panel to look inside.** `/_stratus/ui` shows, searches and edits every resource, and decodes
  Kinesis records aggregated by the KPL.

## Next steps

- [**Getting started**](getting-started.md): from nothing to a queue, a table and a bucket in five
  minutes.
- [**Docker and Compose**](guides/docker.md): run stratus next to your services.
- [**Go tests**](guides/go-tests.md) and [**Testcontainers**](guides/testcontainers.md): use it
  from tests.
- [**Moving from LocalStack**](guides/from-localstack.md): replace LocalStack in an existing
  project.
- [**Manifest reference**](reference/manifest.md) and
  [**services and operations**](reference/services.md): look things up.

## How fast

Both in Docker with 1 CPU and 2 GB, from the same 63-resource manifest:

| | LocalStack | stratus |
|---|---|---|
| Ready | 13.7 s | **0.24 s** |
| Memory once ready | 636 MiB | **3 MiB** |
| Image | 1.81 GB | **12.9 MB** |
| SQS SendMessage, 32 clients | 883 ops/s | **13 058 ops/s** |
| Kinesis PutRecord, 32 clients | 64 ops/s | **13 580 ops/s** |

The [full comparison](explanation/localstack.md) gives the method and says what is *not* compared.
