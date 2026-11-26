# Benchmark: stratus and LocalStack

`bench/run.sh` starts each emulator in Docker with the same limits, measures how long it takes
to be ready with a manifest's resources, then drives it with the AWS SDK for Go and records
throughput, latency and memory. The results below come from one run on 2026-11-26.

```sh
bench/run.sh                        # 1 CPU, 2 GB, both phases
CPUS=2 MEMORY=4g REPS=3 bench/run.sh
PHASES=load bench/run.sh            # only the load phase
```

It needs Docker, Go and curl. Results go to `bench/results/` (`startup.csv`, `load.csv`,
`oom.txt`, `environment.txt`).

## Setup

| | |
|---|---|
| Host | Apple silicon, Colima VM with 6 CPUs (Ubuntu 24.04) |
| Limits per emulator | `--cpus 1 --memory 2g --memory-swap 2g` |
| LocalStack | `localstack/localstack:community-archive`, `PERSISTENCE=0`, services s3, kinesis, dynamodb, sqs, stepfunctions, sts |
| stratus | the image built from this repository, object bytes on a Docker volume at `/data` |
| Client | `bench/load`, AWS SDK for Go v2, on the host, 8 s per measurement |

**Same resources for both.** Both emulators start from the same manifest. stratus reads it with
`-manifest`. LocalStack runs `bench/localstack/ready.d/from-manifest.py` as an init hook, which
creates the resources with boto3 inside LocalStack, eight at a time. That avoids starting the AWS
CLI once per resource, so it is the fastest way to give LocalStack these resources.

- `examples/manifest.yaml` has 8 resources.
- `bench/manifest-large.yaml` has 63, shaped like a real project's (names are made up):
  26 tables with local and global indexes and TTLs, 20 queues (8 with a dead-letter queue),
  12 buckets with versioning, lifecycle rules and notifications, 4 streams and a state machine.

**Ready** means every resource exists: `/_stratus/ready` for stratus, and for LocalStack the moment
`/_localstack/init/ready` reports its init scripts completed. The clock starts at `docker run`.

## Start-up

Median of 5 starts. Memory is `docker stats` once ready.

| Manifest | LocalStack | stratus |
|---|---|---|
| example, 8 resources | 8.15 s · 563 MiB | **0.15 s · 3 MiB** |
| large, 63 resources | 13.67 s · 636 MiB | **0.24 s · 3 MiB** |

About 0.15 s of stratus's time is `docker run` itself: the emulator creates the 63 resources in
milliseconds. LocalStack's start grows with the number of resources, by about 5.5 s from 8 to 63.

## Load

Throughput and p99 latency for each emulator, side by side; the better value of each pair is in bold.
Every measurement completed without errors, and neither emulator was killed for memory.

| Operation | Clients | LocalStack ops/s | stratus ops/s | LocalStack p99 ms | stratus p99 ms |
|---|---:|---:|---:|---:|---:|
| DynamoDB PutItem | 1 | 181 | **1 684** | 14 | **1.2** |
|  | 8 | 225 | **8 383** | 91 | **2.0** |
|  | 32 | 194 | **11 106** | 373 | **30** |
|  | 128 | 138 | **14 837** | 2704 | **57** |
| DynamoDB GetItem | 1 | 265 | **714** | 15 | **7.2** |
|  | 8 | 165 | **4 121** | 162 | **8.2** |
|  | 32 | 227 | **15 517** | 534 | **5.1** |
|  | 128 | 237 | **17 671** | 967 | **56** |
| DynamoDB Query (50 items) | 1 | 114 | **687** | 55 | **4.7** |
|  | 8 | 147 | **2 395** | 186 | **53** |
|  | 32 | 173 | **2 448** | 313 | **82** |
|  | 128 | 181 | **2 644** | 1150 | **179** |
| S3 PutObject 4 KiB | 1 | 372 | **1 195** | 4.8 | **1.4** |
|  | 8 | 787 | **6 663** | 46 | **2.3** |
|  | 32 | 777 | **9 043** | 108 | **45** |
|  | 128 | 733 | **9 515** | 460 | **70** |
| S3 PutObject 256 KiB | 1 | 247 | **341** | **6.5** | 7.1 |
|  | 8 | 310 | **576** | **54** | 77 |
|  | 32 | 329 | **663** | 201 | **125** |
|  | 128 | 265 | **691** | 976 | **461** |
| S3 GetObject 4 KiB | 1 | 411 | **756** | 5.7 | **2.6** |
|  | 8 | 684 | **5 514** | 26 | **2.8** |
|  | 32 | 711 | **11 301** | 101 | **8.3** |
|  | 128 | 782 | **15 009** | 341 | **60** |
| S3 GetObject 256 KiB | 1 | 217 | **429** | 13 | **3.7** |
|  | 8 | 416 | **916** | 70 | **15** |
|  | 32 | 467 | **928** | 123 | **75** |
|  | 128 | 441 | **958** | 426 | **167** |
| SQS SendMessage | 1 | 544 | **828** | 3.8 | **2.1** |
|  | 8 | 827 | **5 434** | 26 | **3.1** |
|  | 32 | 883 | **13 058** | 98 | **5.9** |
|  | 128 | 663 | **19 401** | 435 | **53** |
| SQS receive 10 + delete each | 1 | 48 | **61** | 59 | **44** |
|  | 8 | 76 | **510** | 209 | **87** |
|  | 32 | 70 | **1 220** | 932 | **101** |
|  | 128 | 71 | **1 705** | 2000 | **268** |
| Kinesis PutRecord | 1 | 81 | **655** | 58 | **5.3** |
|  | 8 | 73 | **6 026** | 423 | **2.6** |
|  | 32 | 64 | **13 580** | 2197 | **5.3** |
|  | 128 | 64 | **19 057** | 2690 | **53** |

`load.csv` has p50 and p99 for every row.

Notes on reading this:

- **One client** measures latency more than throughput: a call's round trip includes the SDK and
  the host's loopback into the VM. Concurrency shows how an emulator holds up when a test suite or
  several services use it at once, as end-to-end runs do.
- **S3 bytes on a volume.** stratus writes object bytes to files. With them on the container's
  overlay layer instead of a volume, large writes were slower than LocalStack's (191 against 265
  ops/s at 256 KiB, p99 over 2 s at 128 clients), because a 2 GB memory limit throttles writing
  dirty pages through overlayfs. LocalStack keeps objects under 512 KiB in memory, so it does not
  meet that. Mount a volume at `/data`, as the README says.
- **SQS receive** counts one receive of up to 10 messages plus a delete of each as one operation,
  on a queue filled by the send measurement before it.
- **Memory.** `docker stats` peaked at 1.4 GiB for LocalStack and 1.3 GiB for stratus. stratus's
  figure includes the page cache of the object files it wrote, which the kernel reclaims, and the
  roughly 300 000 messages the send measurement left in a queue. At rest both hold their state:
  stratus in a few MiB plus what tests store, LocalStack in about 550 MiB before any data.

## What this does not show

- Only throughput, latency, memory and start-up are compared, not behaviour. That two emulators
  answer an operation does not mean they answer it the same way; the services' README tables
  list what stratus leaves out.
- One machine, one run. Expect other numbers on other hardware, and differences of 10–20% between
  runs of the same setup; compare orders of magnitude, not single digits.
