---
hide:
  - navigation
---

# stratus

A local emulator of the AWS services an application needs in development and end-to-end tests:
**SQS, S3, DynamoDB, Kinesis and Step Functions**. It is one static Go binary, ready in
milliseconds, with the resources you declare in a manifest.

```sh
docker run --rm -p 4566:4566 stratusemulator/stratus
```

![The stratus web panel: the overview, a Kinesis stream whose KPL-aggregated records are decoded, and a DynamoDB item being edited](assets/panel-tour.gif)

## Why stratus

**One port, any credentials**
:   Every service is served on `:4566`. Signatures are never checked, so any AWS SDK or the AWS CLI
    works with placeholder credentials.

**Resources are declared, not scripted**
:   A YAML [manifest](reference/manifest.md) lists the queues, streams, buckets, tables and state
    machines that exist at start. stratus reports ready only once all of them exist.

**Built for tests**
:   `POST /_stratus/reset` returns to the manifest between suites. The [`stratustest`](guides/go-tests.md)
    package runs the emulator inside a Go test with no container, and the
    [Testcontainers module](guides/testcontainers.md) runs it in one.

**Keeps data when you want it to**
:   With `-state-dir`, the data of the resources you choose survives restarts. That covers every service,
    in-flight messages, Kinesis checkpoints and running executions included. See
    [Keeping data across restarts](guides/persistence.md).

**A panel to look inside**
:   `/_stratus/ui` shows every resource and its data. You can search it and change it, and Kinesis
    records aggregated by the KPL are decoded.

## How fast

Both in Docker with 1 CPU and 2 GB, from the same 63-resource manifest:

| | LocalStack | stratus |
|---|---|---|
| Ready | 13.7 s | **0.24 s** |
| Memory once ready | 636 MiB | **3 MiB** |
| Image | 1.81 GB | **12.9 MB** |
| SQS SendMessage, 32 clients | 883 ops/s | **13 058 ops/s** |
| Kinesis PutRecord, 32 clients | 64 ops/s | **13 580 ops/s** |

[The full comparison](explanation/localstack.md) explains the method and what is *not* compared.

## Where to go next

- **New to stratus?** [Getting started](getting-started.md) takes five minutes.
- **Running it next to your services?** See [Docker and Compose](guides/docker.md).
- **Writing tests?** See [Go tests](guides/go-tests.md) or [Testcontainers](guides/testcontainers.md).
- **Replacing LocalStack?** See [Moving from LocalStack](guides/from-localstack.md).
- **Looking something up?** The [reference](reference/index.md) lists every manifest key, flag,
  endpoint and operation.
