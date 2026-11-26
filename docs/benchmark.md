# Benchmark

How fast stratus starts and how much load it takes, next to LocalStack, measured the same way
for both. The harness is in the repository's [`bench/`](https://github.com/stratus-emulator/stratus/tree/main/bench)
directory, and you can [run it yourself](#run-it-yourself).

!!! note "What this measures"
    Speed, memory and start-up only. That two emulators answer an operation does not mean they
    answer it the same way: see [Services and operations](reference/services.md) for what stratus
    leaves out, and [Compared with LocalStack](explanation/localstack.md) for the differences in scope.

## Setup

| | |
|---|---|
| Host | Apple silicon, Colima VM with 6 CPUs (Ubuntu 24.04) |
| Limits per emulator | `--cpus 1 --memory 2g --memory-swap 2g` |
| LocalStack | `localstack/localstack:community-archive`, `PERSISTENCE=0`, services s3, kinesis, dynamodb, sqs, stepfunctions, sts |
| stratus | the image built from the repository, object bytes on a Docker volume at `/data` |
| Client | `bench/load`, AWS SDK for Go v2, on the host, 8 s per measurement |
| Date | one run on 2026-11-26 |

**Same resources for both.** Both emulators start from the same manifest. stratus reads it with
`-manifest`. LocalStack runs an init hook that creates the resources with boto3 inside the
container, eight at a time. That is the fastest way to give LocalStack these resources, because the
AWS CLI is not started once per resource.

| Manifest | Resources |
|---|---|
| `examples/manifest.yaml` | 8 |
| `bench/manifest-large.yaml` | 63, shaped like a real project's: 26 tables with local and global indexes and TTLs, 20 queues (8 with a dead-letter queue), 12 buckets with versioning, lifecycle rules and notifications, 4 streams, a state machine |

**Ready** means every resource exists: `/_stratus/ready` for stratus, and for LocalStack the moment
`/_localstack/init/ready` reports that its init scripts have completed. The clock starts at
`docker run`.

## Start-up

Median of 5 starts. Memory is `docker stats` once ready.

| Manifest | LocalStack | stratus |
|---|---|---|
| 8 resources | 8.15 s · 563 MiB | **0.15 s · 3 MiB** |
| 63 resources | 13.67 s · 636 MiB | **0.24 s · 3 MiB** |
| Image size | 1.81 GB | **12.9 MB** |

About 0.15 s of stratus's time is `docker run` itself; the emulator creates the 63 resources in
milliseconds. LocalStack's start grows with the number of resources, by about 5.5 s from 8 to 63.

## Load

Throughput and p99 latency at 1, 8, 32 and 128 concurrent clients; the better value of each pair is
in bold. Every measurement completed without errors, and neither emulator was killed for memory.

=== "DynamoDB"

    | Operation | Clients | LocalStack ops/s | stratus ops/s | LocalStack p99 | stratus p99 |
    |---|---:|---:|---:|---:|---:|
    | PutItem | 1 | 181 | **1 684** | 14 ms | **1.2 ms** |
    |  | 8 | 225 | **8 383** | 91 ms | **2.0 ms** |
    |  | 32 | 194 | **11 106** | 373 ms | **30 ms** |
    |  | 128 | 138 | **14 837** | 2 704 ms | **57 ms** |
    | GetItem | 1 | 265 | **714** | 15 ms | **7.2 ms** |
    |  | 8 | 165 | **4 121** | 162 ms | **8.2 ms** |
    |  | 32 | 227 | **15 517** | 534 ms | **5.1 ms** |
    |  | 128 | 237 | **17 671** | 967 ms | **56 ms** |
    | Query (50 items) | 1 | 114 | **687** | 55 ms | **4.7 ms** |
    |  | 8 | 147 | **2 395** | 186 ms | **53 ms** |
    |  | 32 | 173 | **2 448** | 313 ms | **82 ms** |
    |  | 128 | 181 | **2 644** | 1 150 ms | **179 ms** |

=== "S3"

    | Operation | Clients | LocalStack ops/s | stratus ops/s | LocalStack p99 | stratus p99 |
    |---|---:|---:|---:|---:|---:|
    | PutObject 4 KiB | 1 | 372 | **1 195** | 4.8 ms | **1.4 ms** |
    |  | 8 | 787 | **6 663** | 46 ms | **2.3 ms** |
    |  | 32 | 777 | **9 043** | 108 ms | **45 ms** |
    |  | 128 | 733 | **9 515** | 460 ms | **70 ms** |
    | PutObject 256 KiB | 1 | 247 | **341** | **6.5 ms** | 7.1 ms |
    |  | 8 | 310 | **576** | **54 ms** | 77 ms |
    |  | 32 | 329 | **663** | 201 ms | **125 ms** |
    |  | 128 | 265 | **691** | 976 ms | **461 ms** |
    | GetObject 4 KiB | 1 | 411 | **756** | 5.7 ms | **2.6 ms** |
    |  | 8 | 684 | **5 514** | 26 ms | **2.8 ms** |
    |  | 32 | 711 | **11 301** | 101 ms | **8.3 ms** |
    |  | 128 | 782 | **15 009** | 341 ms | **60 ms** |
    | GetObject 256 KiB | 1 | 217 | **429** | 13 ms | **3.7 ms** |
    |  | 8 | 416 | **916** | 70 ms | **15 ms** |
    |  | 32 | 467 | **928** | 123 ms | **75 ms** |
    |  | 128 | 441 | **958** | 426 ms | **167 ms** |

=== "SQS"

    | Operation | Clients | LocalStack ops/s | stratus ops/s | LocalStack p99 | stratus p99 |
    |---|---:|---:|---:|---:|---:|
    | SendMessage | 1 | 544 | **828** | 3.8 ms | **2.1 ms** |
    |  | 8 | 827 | **5 434** | 26 ms | **3.1 ms** |
    |  | 32 | 883 | **13 058** | 98 ms | **5.9 ms** |
    |  | 128 | 663 | **19 401** | 435 ms | **53 ms** |
    | Receive 10 + delete each | 1 | 48 | **61** | 59 ms | **44 ms** |
    |  | 8 | 76 | **510** | 209 ms | **87 ms** |
    |  | 32 | 70 | **1 220** | 932 ms | **101 ms** |
    |  | 128 | 71 | **1 705** | 2 000 ms | **268 ms** |

=== "Kinesis"

    | Operation | Clients | LocalStack ops/s | stratus ops/s | LocalStack p99 | stratus p99 |
    |---|---:|---:|---:|---:|---:|
    | PutRecord | 1 | 81 | **655** | 58 ms | **5.3 ms** |
    |  | 8 | 73 | **6 026** | 423 ms | **2.6 ms** |
    |  | 32 | 64 | **13 580** | 2 197 ms | **5.3 ms** |
    |  | 128 | 64 | **19 057** | 2 690 ms | **53 ms** |

`bench/results/load.csv` also has p50 for every row.

## Reading the numbers

- **One client** measures latency more than throughput: a call's round trip includes the SDK and
  the host's loopback into the VM. Concurrency shows how an emulator holds up when a test suite, or
  several services, use it at once, as end-to-end runs do.
- **S3 bytes on a volume.** stratus writes object bytes to files. With them on the container's
  overlay layer instead of a volume, large writes were slower than LocalStack's: 191 against 265
  ops/s at 256 KiB, and p99 over 2 s at 128 clients. A 2 GB memory limit throttles writing dirty
  pages through overlayfs. LocalStack keeps objects under 512 KiB in memory, so it does not meet
  that. Mount a volume at `/data`.
- **SQS receive** counts one receive of up to 10 messages plus a delete of each as one operation,
  on a queue filled by the send measurement before it.
- **Memory.** `docker stats` peaked at 1.4 GiB for LocalStack and 1.3 GiB for stratus under load.
  stratus's figure includes the page cache of the object files it wrote, which the kernel reclaims,
  and about 300 000 messages the send measurement left in a queue. At rest stratus holds a few MiB
  plus what tests store; LocalStack holds about 550 MiB before any data.

## In a real project

When a project with about 60 resources and 30 services in Compose moved from LocalStack to
stratus ([how](guides/from-localstack.md)):

| | LocalStack | stratus |
|---|---|---|
| emulator healthy after `docker compose up` | 22–28 s cold, 13–16 s warm | about 2 s |
| emulator memory | 550–710 MiB | about 5 MiB |
| integration suites (36 tests) | 237 s | 85 s |

## What this does not show

- **Behaviour.** That an operation is fast says nothing about whether it answers as AWS does.
- **Other machines.** This is one machine and one run. Expect other numbers on other hardware,
  and differences of 10–20 % between runs of the same setup: compare orders of magnitude, not
  single digits.

## Run it yourself

You need Docker, Go and curl.

```sh
bench/run.sh                        # 1 CPU, 2 GB, both phases
CPUS=2 MEMORY=4g REPS=3 bench/run.sh
PHASES=load bench/run.sh            # only the load phase
```

Results go to `bench/results/`: `startup.csv`, `load.csv`, `oom.txt` and `environment.txt`.
