# Benchmark

How fast stratus starts and how much load it takes, next to [LocalStack](https://github.com/localstack/localstack)
and [moto](https://github.com/getmoto/moto), measured the same way for all three. The harness is in
the repository's [`bench/`](https://github.com/stratus-emulator/stratus/tree/main/bench)
directory, and you can [run it yourself](#run-it-yourself).

!!! note "What this measures"
    Speed, memory and start-up only. That emulators answer an operation does not mean they answer
    it the same way: see [Services and operations](reference/services.md) for what stratus leaves
    out. The three are built for different things. LocalStack emulates over a hundred services.
    moto mocks AWS for unit tests, mostly inside the test process, and its server mode is a side use.
    stratus emulates five services for development and end-to-end tests.

## Setup

| | |
|---|---|
| Host | Apple silicon, Colima VM with 6 CPUs (Ubuntu 24.04) |
| Limits per emulator | `--cpus 1 --memory 2g --memory-swap 2g` |
| LocalStack | `localstack/localstack:community-archive`, `PERSISTENCE=0`, services s3, kinesis, dynamodb, sqs, stepfunctions, sts |
| moto | `motoserver/moto:5.2.6`, server mode, `MOTO_ACCOUNT_ID=000000000000` |
| stratus | the image built from the repository, object bytes on a Docker volume at `/data` |
| Client | `bench/load`, AWS SDK for Go v2, on the host, 8 s per measurement |
| Run | the three one after another, in one run on 2026-11-26 |

**Same resources for all three.** Every emulator starts from the same manifest. stratus reads it with
`-manifest`. LocalStack and moto get the same script, which creates the resources with boto3 inside
the container, eight at a time. For LocalStack it runs as an init hook. For moto it runs once the
server answers, and a marker bucket says it has finished. After creation, the resource counts were
checked against the manifests: 10 of 10 and 63 of 63 on each.

| Manifest | Resources |
|---|---|
| `examples/manifest.yaml` | 10 |
| `bench/manifest-large.yaml` | 63, shaped like a real project's: 26 tables with local and global indexes and TTLs, 20 queues (8 with a dead-letter queue), 12 buckets with versioning, lifecycle rules and notifications, 4 streams, a state machine |

**Ready** means every resource exists: `/_stratus/ready` for stratus, `/_localstack/init/ready`
for LocalStack, and the marker bucket for moto. The clock starts at `docker run`.

## Start-up

Median of 5 starts. Memory is `docker stats` once ready.

| Manifest | LocalStack | moto | stratus |
|---|---:|---:|---:|
| 10 resources | 6.18 s · 545 MiB | 1.32 s · 82 MiB | **0.15 s · 4 MiB** |
| 63 resources | 10.30 s · 646 MiB | 6.05 s · 90 MiB | **0.13 s · 4 MiB** |

| Image | LocalStack | moto | stratus |
|---|---:|---:|---:|
| Size | 1.81 GB | 578 MB | **16.1 MB** |

stratus creates the 63 resources in milliseconds; most of its time is `docker run` itself.
LocalStack's and moto's start grows with the number of resources.

## Load

Throughput and p99 latency at 1, 8, 32 and 128 concurrent clients, per service; the best value of
each row is in bold. **Failed calls are left out of both numbers**: a row with failed calls shows only
the calls that succeeded, and the last column says how many failed.

=== "DynamoDB"

    **Throughput, operations per second**

    | Operation | Clients | LocalStack | moto | stratus | Failed calls |
    |---|---:|---:|---:|---:|---|
    | PutItem | 1 | 204 | 552 | **2 653** |  |
    |  | 8 | 256 | 766 | **12 581** |  |
    |  | 32 | 235 | 679 | **19 222** | moto 1 312 |
    |  | 128 | 183 | 138 | **22 722** | moto 8 662 |
    | GetItem | 1 | 451 | 369 | **1 946** |  |
    |  | 8 | 539 | 697 | **11 840** |  |
    |  | 32 | 561 | 314 | **21 871** | moto 3 693 |
    |  | 128 | 595 | 257 | **29 299** | moto 5 313 |
    | Query (50 items) | 1 | 305 | 37 | **1 216** |  |
    |  | 8 | 379 | 43 | **3 223** |  |
    |  | 32 | 392 | 44 | **3 489** |  |
    |  | 128 | 401 | 43 | **3 672** |  |

    **p99 latency**

    | Operation | Clients | LocalStack | moto | stratus |
    |---|---:|---:|---:|---:|
    | PutItem | 1 | 13 ms | 4.1 ms | **1.2 ms** |
    |  | 8 | 74 ms | 24 ms | **1.6 ms** |
    |  | 32 | 1 093 ms | 72 ms | **17 ms** |
    |  | 128 | 2 635 ms | 338 ms | **54 ms** |
    | GetItem | 1 | 6.4 ms | 6.0 ms | **1.0 ms** |
    |  | 8 | 55 ms | 24 ms | **1.2 ms** |
    |  | 32 | 176 ms | 72 ms | **4.1 ms** |
    |  | 128 | 365 ms | 380 ms | **52 ms** |
    | Query (50 items) | 1 | 14 ms | 92 ms | **3.1 ms** |
    |  | 8 | 68 ms | 237 ms | **51 ms** |
    |  | 32 | 205 ms | 873 ms | **75 ms** |
    |  | 128 | 643 ms | 3 214 ms | **114 ms** |

=== "S3"

    **Throughput, operations per second**

    | Operation | Clients | LocalStack | moto | stratus | Failed calls |
    |---|---:|---:|---:|---:|---|
    | PutObject 4 KiB | 1 | 855 | 474 | **1 749** |  |
    |  | 8 | 1 443 | 633 | **7 669** |  |
    |  | 32 | 1 402 | 651 | **11 131** |  |
    |  | 128 | 1 449 | 327 | **11 082** | moto 4 093 |
    | PutObject 256 KiB | 1 | **372** | 258 | 369 |  |
    |  | 8 | 492 | 350 | **723** |  |
    |  | 32 | 539 | 310 | **832** |  |
    |  | 128 | 490 | 327 | **995** | LocalStack 23 |
    | GetObject 4 KiB | 1 | 804 | 519 | **1 541** |  |
    |  | 8 | 1 204 | 655 | **9 271** |  |
    |  | 32 | 1 300 | 582 | **13 490** | moto 1 562 |
    |  | 128 | 1 289 | 355 | **23 509** | moto 5 194 |
    | GetObject 256 KiB | 1 | 487 | 285 | **695** |  |
    |  | 8 | 732 | 531 | **1 426** |  |
    |  | 32 | 748 | 393 | **1 433** | moto 1 268 |
    |  | 128 | 685 | 432 | **1 470** | moto 1 646 |

    **p99 latency**

    | Operation | Clients | LocalStack | moto | stratus |
    |---|---:|---:|---:|---:|
    | PutObject 4 KiB | 1 | 2.1 ms | 5.3 ms | **1.3 ms** |
    |  | 8 | 13 ms | 25 ms | **3.2 ms** |
    |  | 32 | 66 ms | 83 ms | **44 ms** |
    |  | 128 | 227 ms | 404 ms | **77 ms** |
    | PutObject 256 KiB | 1 | 7.5 ms | **6.5 ms** | 13 ms |
    |  | 8 | 63 ms | **46 ms** | 77 ms |
    |  | 32 | **106 ms** | 229 ms | 125 ms |
    |  | 128 | 908 ms | 518 ms | **479 ms** |
    | GetObject 4 KiB | 1 | 3.2 ms | 3.3 ms | **2.2 ms** |
    |  | 8 | 18 ms | 25 ms | **2.2 ms** |
    |  | 32 | 51 ms | 82 ms | **17 ms** |
    |  | 128 | 253 ms | 391 ms | **55 ms** |
    | GetObject 256 KiB | 1 | 5.0 ms | 6.5 ms | **3.0 ms** |
    |  | 8 | 25 ms | 29 ms | **9.1 ms** |
    |  | 32 | 101 ms | 112 ms | **42 ms** |
    |  | 128 | 413 ms | 443 ms | **108 ms** |

=== "SQS"

    **Throughput, operations per second**

    | Operation | Clients | LocalStack | moto | stratus | Failed calls |
    |---|---:|---:|---:|---:|---|
    | SendMessage | 1 | 814 | 180 | **1 900** |  |
    |  | 8 | 1 277 | 96 | **10 962** |  |
    |  | 32 | 1 176 | 78 | **23 268** |  |
    |  | 128 | 1 239 | 66 | **24 396** |  |
    | Receive 10 + delete each | 1 | 81 | 2.0 | **195** |  |
    |  | 8 | 118 | 2.0 | **1 009** |  |
    |  | 32 | 107 | 2.0 | **2 107** |  |
    |  | 128 | 107 | 3.0 | **2 639** |  |

    **p99 latency**

    | Operation | Clients | LocalStack | moto | stratus |
    |---|---:|---:|---:|---:|
    | SendMessage | 1 | 2.7 ms | 11 ms | **1.0 ms** |
    |  | 8 | 17 ms | 174 ms | **1.4 ms** |
    |  | 32 | 92 ms | 483 ms | **2.9 ms** |
    |  | 128 | 383 ms | 2 077 ms | **59 ms** |
    | Receive 10 + delete each | 1 | 26 ms | 513 ms | **7.5 ms** |
    |  | 8 | 129 ms | 4 920 ms | **17 ms** |
    |  | 32 | 616 ms | 13 285 ms | **68 ms** |
    |  | 128 | 1 505 ms | 45 628 ms | **194 ms** |

=== "Kinesis"

    **Throughput, operations per second**

    | Operation | Clients | LocalStack | moto | stratus | Failed calls |
    |---|---:|---:|---:|---:|---|
    | PutRecord | 1 | 124 | 497 | **2 095** |  |
    |  | 8 | 113 | 636 | **11 297** |  |
    |  | 32 | 119 | 580 | **17 823** |  |
    |  | 128 | 95 | 300 | **30 989** | moto 3 422 |

    **p99 latency**

    | Operation | Clients | LocalStack | moto | stratus |
    |---|---:|---:|---:|---:|
    | PutRecord | 1 | 48 ms | 3.2 ms | **0.8 ms** |
    |  | 8 | 338 ms | 22 ms | **1.3 ms** |
    |  | 32 | 812 ms | 87 ms | **6.2 ms** |
    |  | 128 | 1 961 ms | 408 ms | **46 ms** |

`bench/results/load.csv` also has p50 and the peak memory of every row.

## Reading the numbers

- **One client** measures latency more than throughput: a call's round trip includes the SDK and
  the host's loopback into the VM. Concurrency shows how an emulator holds up when a test suite, or
  several services, use it at once, as end-to-end runs do.
- **moto's failures are dropped connections, not AWS errors.** moto's server answers every request
  with `Connection: close` and queues at most 128 waiting connections, so each call opens a new TCP
  connection. Under concurrency some are refused or cut (`connection refused`, `EOF`). Which rows
  fail changes from run to run. stratus had no failed calls. LocalStack had 23, on S3 PutObject
  256 KiB at 128 clients, whose cause this run did not record.
- **moto's SQS is very slow.** Receive-and-delete ran at 2–3 operations per second, with a p50
  around 470 ms, even with one client, on a queue of a few thousand messages. DynamoDB Query on
  moto slowed down after the PutItem measurement had filled the table.
- **moto is faster than LocalStack** at start-up, at single-client DynamoDB PutItem, and at Kinesis
  PutRecord at every concurrency.
- **S3 bytes on a volume.** stratus writes object bytes to files. On the container's overlay layer,
  instead of a volume, large writes under a 2 GB memory limit are throttled. Mount a volume at
  `/data`.
- **SQS receive** counts one receive of up to 10 messages plus a delete of each as one operation, on
  a queue filled by the send measurement before it.
- **Peak memory under load** was 2 046 MiB for LocalStack, at its 2 GiB limit during the last
  Kinesis measurement; 1 785 MiB for stratus; and 1 062 MiB for moto. stratus's figure includes the
  page cache of the object files it wrote, which the kernel reclaims, and the messages the send
  measurement left in a queue. None of the three was killed for memory.

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
- **Other machines, other runs.** This is one machine and one run. Between this run and an earlier
  one on the same machine, single rows moved by up to 3×, such as stratus GetItem with one client
  (714 → 1 946 ops/s) and LocalStack start-up (13.7 → 10.3 s with 63 resources). Compare orders
  of magnitude, not single digits.

## Run it yourself

You need Docker, Go and curl.

```sh
bench/run.sh                                  # all three, 1 CPU, 2 GB, both phases
EMULATORS="stratus moto" bench/run.sh         # some of them
CPUS=2 MEMORY=4g REPS=3 bench/run.sh
PHASES=load PORT=4599 bench/run.sh            # only the load phase, on another port
```

Results go to `bench/results/`: `startup.csv`, `load.csv`, `oom.txt`, `verify.txt` and
`environment.txt`.
