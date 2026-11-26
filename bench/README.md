# Benchmark: stratus, LocalStack and moto

`bench/run.sh` starts each emulator in Docker with the same limits, measures how long it takes
to be ready with a manifest's resources, then drives it with the AWS SDK for Go and records
throughput, latency and memory. The results below come from one run on 2026-11-26, in which all
three emulators ran one after another on the same machine.

```sh
bench/run.sh                        # 1 CPU, 2 GB, both phases, all three emulators
CPUS=2 MEMORY=4g REPS=3 bench/run.sh
PHASES=load bench/run.sh            # only the load phase
EMULATORS="stratus moto" bench/run.sh
PORT=4599 bench/run.sh              # host port, when 4566 is taken
```

It needs Docker, Go and curl. Results go to `bench/results/` (`startup.csv`, `load.csv`,
`oom.txt`, `verify.txt`, `environment.txt`).

## Setup

| | |
|---|---|
| Host | Apple silicon, Colima VM with 6 CPUs (Ubuntu 24.04) |
| Limits per emulator | `--cpus 1 --memory 2g --memory-swap 2g` |
| LocalStack | `localstack/localstack:community-archive`, `PERSISTENCE=0`, services s3, kinesis, dynamodb, sqs, stepfunctions, sts |
| stratus | the image built from this repository, object bytes on a Docker volume at `/data` |
| moto | `motoserver/moto:5.2.6` (moto in server mode, one Python process), started by `bench/moto/start.sh` |
| Client | `bench/load`, AWS SDK for Go v2, on the host, 8 s per measurement |

**Same resources for all three.** Every emulator starts from the same manifest. stratus reads it
with `-manifest`. LocalStack runs `bench/localstack/ready.d/from-manifest.py` as an init hook, which
creates the resources with boto3 inside LocalStack, eight at a time. That avoids starting the AWS
CLI once per resource, so it is the fastest way to give LocalStack these resources. moto has
neither a manifest nor init hooks: `bench/moto/start.sh` replaces its entrypoint, starts
`moto_server`, runs the same `from-manifest.py` inside the container once moto answers, and
then creates a marker bucket, `bench-moto-ready`. The resources are created inside the
container for moto as for LocalStack, not from the host.

- `examples/manifest.yaml` has 10 resources: 5 queues, a stream, 2 buckets, a table and a
  state machine.
- `bench/manifest-large.yaml` has 63, shaped like a real project's (names are made up):
  26 tables with local and global indexes and TTLs, 20 queues (8 with a dead-letter queue),
  12 buckets with versioning, lifecycle rules and notifications, 4 streams and a state machine.

`run.sh` checks once per manifest that moto holds every resource, with `bench/moto/verify.py`
(names of tables, queues, streams, buckets and state machines against the manifest's); the
counts are in `verify.txt`: 10 of 10 and 63 of 63.

**Ready** means every resource exists: `/_stratus/ready` for stratus, for LocalStack the moment
`/_localstack/init/ready` reports its init scripts completed, and for moto the moment the marker
bucket answers. The clock starts at `docker run`.

## Start-up

Median of 5 starts. Memory is `docker stats` once ready.

| Manifest | LocalStack | stratus | moto |
|---|---|---|---|
| example, 10 resources | 6.18 s · 545 MiB | **0.15 s · 4 MiB** | 1.32 s · 82 MiB |
| large, 63 resources | 10.30 s · 646 MiB | **0.13 s · 4 MiB** | 6.05 s · 90 MiB |

About 0.15 s of stratus's time is `docker run` itself: the emulator creates the 63 resources in
milliseconds. LocalStack's and moto's starts grow with the number of resources, by about 4 s and
4.7 s from 10 to 63. moto's server answers in about half a second; the rest is the Python
script importing boto3 and creating the resources against a server that shares the one CPU.

## Load

Throughput and p99 latency for each emulator, side by side; the best value of each row is in
bold. The last column counts failed calls, which are left out of ops/s and p99.
No emulator was killed for memory (`oom.txt`).

| Operation | Clients | LocalStack ops/s | stratus ops/s | moto ops/s | LocalStack p99 ms | stratus p99 ms | moto p99 ms | Errors |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| DynamoDB PutItem | 1 | 204 | **2 653** | 552 | 13 | **1.2** | 4.1 |  |
|  | 8 | 256 | **12 581** | 766 | 74 | **1.6** | 24 |  |
|  | 32 | 235 | **19 222** | 679 | 1093 | **17** | 72 | moto 1 312 |
|  | 128 | 183 | **22 722** | 138 | 2635 | **54** | 338 | moto 8 662 |
| DynamoDB GetItem | 1 | 451 | **1 946** | 369 | 6.4 | **1.0** | 6.0 |  |
|  | 8 | 539 | **11 840** | 697 | 55 | **1.2** | 24 |  |
|  | 32 | 561 | **21 871** | 314 | 176 | **4.1** | 72 | moto 3 693 |
|  | 128 | 595 | **29 299** | 257 | 365 | **52** | 380 | moto 5 313 |
| DynamoDB Query (50 items) | 1 | 305 | **1 216** | 37 | 14 | **3.1** | 92 |  |
|  | 8 | 379 | **3 223** | 43 | 68 | **51** | 237 |  |
|  | 32 | 392 | **3 489** | 44 | 205 | **75** | 873 |  |
|  | 128 | 401 | **3 672** | 43 | 643 | **114** | 3214 |  |
| S3 PutObject 4 KiB | 1 | 855 | **1 749** | 474 | 2.1 | **1.3** | 5.3 |  |
|  | 8 | 1 443 | **7 669** | 633 | 13 | **3.2** | 25 |  |
|  | 32 | 1 402 | **11 131** | 651 | 66 | **44** | 83 |  |
|  | 128 | 1 449 | **11 082** | 327 | 227 | **77** | 404 | moto 4 093 |
| S3 PutObject 256 KiB | 1 | **372** | 369 | 258 | 7.5 | 13 | **6.5** |  |
|  | 8 | 492 | **723** | 350 | 63 | 77 | **46** |  |
|  | 32 | 539 | **832** | 310 | **106** | 125 | 229 |  |
|  | 128 | 490 | **995** | 327 | 908 | **479** | 518 | LocalStack 23 |
| S3 GetObject 4 KiB | 1 | 804 | **1 541** | 519 | 3.2 | **2.2** | 3.3 |  |
|  | 8 | 1 204 | **9 271** | 655 | 18 | **2.2** | 25 |  |
|  | 32 | 1 300 | **13 490** | 582 | 51 | **17** | 82 | moto 1 562 |
|  | 128 | 1 289 | **23 509** | 355 | 253 | **55** | 391 | moto 5 194 |
| S3 GetObject 256 KiB | 1 | 487 | **695** | 285 | 5.0 | **3.0** | 6.5 |  |
|  | 8 | 732 | **1 426** | 531 | 25 | **9.1** | 29 |  |
|  | 32 | 748 | **1 433** | 393 | 101 | **42** | 112 | moto 1 268 |
|  | 128 | 685 | **1 470** | 432 | 413 | **108** | 443 | moto 1 646 |
| SQS SendMessage | 1 | 814 | **1 900** | 180 | 2.7 | **1.0** | 11 |  |
|  | 8 | 1 277 | **10 962** | 96 | 17 | **1.4** | 174 |  |
|  | 32 | 1 176 | **23 268** | 78 | 92 | **2.9** | 483 |  |
|  | 128 | 1 239 | **24 396** | 66 | 383 | **59** | 2077 |  |
| SQS receive 10 + delete each | 1 | 81 | **195** | 2 | 26 | **7.5** | 513 |  |
|  | 8 | 118 | **1 009** | 2 | 129 | **17** | 4920 |  |
|  | 32 | 107 | **2 107** | 2 | 616 | **68** | 13285 |  |
|  | 128 | 107 | **2 639** | 3 | 1505 | **194** | 45628 |  |
| Kinesis PutRecord | 1 | 124 | **2 095** | 497 | 48 | **0.8** | 3.2 |  |
|  | 8 | 113 | **11 297** | 636 | 338 | **1.3** | 22 |  |
|  | 32 | 119 | **17 823** | 580 | 812 | **6.2** | 87 |  |
|  | 128 | 95 | **30 989** | 300 | 1961 | **46** | 408 | moto 3 422 |

`load.csv` has p50 and p99 for every row.

Notes on reading this:

- **One client** measures latency more than throughput: a call's round trip includes the SDK and
  the host's loopback into the VM. Concurrency shows how an emulator holds up when a test suite or
  several services use it at once, as end-to-end runs do.
- **moto's errors are connections, not answers.** moto in server mode runs on Werkzeug's
  development server: one Python process, a thread per connection, a listen backlog of 128, and
  `Connection: close` on every response, so every call opens a new TCP connection through the
  host's port forwarding into the VM. From 32 clients on, some of those fail with
  `connection refused` or `EOF` (`bench/load` prints the first error of each measurement). Which
  rows fail varies between runs: a second moto run had no errors on PutItem at 32 clients but
  4 679 on GetItem. No call failed with an AWS error. moto's ops/s and p99 count only the calls
  that succeeded, so at 32 and 128 clients they flatter moto.
- **moto's SQS and Query.** On SQS, moto sent 180 messages per second with one client, fewer with
  more clients, and receive-and-delete took about half a second per call even with one client,
  on a queue of a few thousand messages. moto's DynamoDB Query ran at about 40 per second after
  the PutItem measurement had filled the table (about 95 per second on a fresh table in a trial
  run). Both look like work that grows with the data held; they did not fail.
- **LocalStack's 23 errors** on 256 KiB writes at 128 clients were not diagnosed: they happened
  before `bench/load` printed error causes.
- **S3 bytes on a volume.** stratus writes object bytes to files. With them on the container's
  overlay layer instead of a volume, large writes were slower than LocalStack's (191 against 265
  ops/s at 256 KiB, p99 over 2 s at 128 clients, in an earlier run), because a 2 GB memory limit
  throttles writing dirty pages through overlayfs. LocalStack keeps objects under 512 KiB in
  memory, so it does not meet that. Mount a volume at `/data`, as the README says.
- **SQS receive** counts one receive of up to 10 messages plus a delete of each as one operation,
  on a queue filled by the send measurement before it.
- **Memory.** `docker stats` peaked at 2 046 MiB for LocalStack (at its 2 GiB limit during the
  last measurement, without being killed), 1 785 MiB for stratus and 1 062 MiB for moto. stratus's
  figure includes the page cache of the object files it wrote, which the kernel reclaims, and the
  roughly 480 000 messages the send measurement put in a queue; moto held a few thousand, since
  it sent no more. At rest stratus holds its state in a few MiB plus what tests store, moto in
  about 85 MiB and LocalStack in about 550 MiB before any data.

## What this does not show

- Only throughput, latency, memory and start-up are compared, not behaviour. That two emulators
  answer an operation does not mean they answer it the same way; the services' README tables
  list what stratus leaves out.
- moto is built to mock AWS in unit tests, mostly in-process from Python with its decorators;
  server mode is a secondary way to run it, on a development web server. It does not aim at
  being a shared, long-running endpoint for several services, which is what this load test
  asks of it. Read its numbers as speed, not as a verdict on moto.
- One machine, one run. Expect other numbers on other hardware, and differences between runs of
  the same setup larger than one would like: LocalStack started in 8.15 s and 13.67 s in an
  earlier run on the same machine against 6.18 s and 10.30 s here, and several load rows of
  LocalStack and stratus moved by a factor of up to almost three between the two runs (stratus's
  GetItem with one client 714 against 1 946 ops/s, LocalStack's Query with one client 114
  against 305). Compare orders of
  magnitude, not single digits.
