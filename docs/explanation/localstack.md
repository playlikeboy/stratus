# Compared with LocalStack

stratus and LocalStack solve the same problem in different ways. LocalStack emulates over a hundred
AWS services in a Python process that starts Java and other engines for some of them. stratus
emulates five, in one Go binary. This page compares the two where they overlap.

## Method

`bench/run.sh` in the repository starts each emulator in Docker with the same limits
(`--cpus 1 --memory 2g`) and from the same manifest. It measures how long each takes to be ready
with the manifest's resources, then drives it with the AWS SDK for Go. Ready means every resource
exists: `/_stratus/ready` for stratus, and `/_localstack/init/ready` for LocalStack. LocalStack gets
its resources from an init hook that creates them with boto3 inside the container, eight at a time,
which is the fastest way to give it them.

The full tables, every operation at 1, 8, 32 and 128 clients with p50 and p99, are in
[`bench/README.md`](https://github.com/stratus-emulator/stratus/blob/main/bench/README.md).

## Start-up

Median of five starts, the clock starting at `docker run`.

| Manifest | LocalStack | stratus |
|---|---|---|
| 8 resources | 8.15 s · 563 MiB | **0.15 s · 3 MiB** |
| 63 resources | 13.67 s · 636 MiB | **0.24 s · 3 MiB** |

About 0.15 s of stratus's time is `docker run` itself. LocalStack's start grows with the number of
resources.

## Under load, 32 clients

| Operation | LocalStack | stratus |
|---|---|---|
| DynamoDB PutItem | 194 ops/s, p99 373 ms | **11 106 ops/s, p99 30 ms** |
| DynamoDB GetItem | 227 ops/s, p99 534 ms | **15 517 ops/s, p99 5 ms** |
| S3 PutObject 4 KiB | 777 ops/s, p99 108 ms | **9 043 ops/s, p99 45 ms** |
| S3 PutObject 256 KiB | 329 ops/s, p99 201 ms | **663 ops/s, p99 125 ms** |
| S3 GetObject 4 KiB | 711 ops/s, p99 101 ms | **11 301 ops/s, p99 8 ms** |
| SQS SendMessage | 883 ops/s, p99 98 ms | **13 058 ops/s, p99 6 ms** |
| Kinesis PutRecord | 64 ops/s, p99 2.2 s | **13 580 ops/s, p99 5 ms** |

## In a real project

When a project with about 60 resources and 30 services in Compose moved from LocalStack to
stratus:

| | LocalStack | stratus |
|---|---|---|
| emulator healthy after `docker compose up` | 22–28 s cold, 13–16 s warm | about 2 s |
| emulator memory | 550–710 MiB | about 5 MiB |
| integration suites (36 tests) | 237 s | 85 s |

The move is described in [Moving from LocalStack](../guides/from-localstack.md).

## What this does not compare

- **Behaviour.** That both emulators answer an operation does not mean they answer it the same way.
  [Services and operations](../reference/services.md) lists what stratus leaves out. When the
  project above ran its S3 contract tests against both, LocalStack needed an exception for one case
  and stratus none. That is one data point, not a general claim.
- **Breadth.** LocalStack emulates many services stratus does not, such as SNS, Lambda, IAM and
  API Gateway. If you need them, stratus is not a replacement.
- **Hardware.** One machine and one run. Expect other numbers elsewhere, and differences of 10–20 %
  between runs: compare orders of magnitude.
