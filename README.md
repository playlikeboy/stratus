# stratus

A local emulator of the AWS services an application needs in development and
end-to-end tests. One static Go binary, ready in milliseconds; S3 object bytes are kept on disk,
all other state in memory.

- **One port, no signature checks.** Every service is served on `:4566`. A request is routed by
  its `X-Amz-Target` header or, failing that, by the service in its SigV4 credential scope. The
  signature itself is never verified, so any credentials work, including a placeholder written by
  hand in a test tool.
- **Resources are declared, not scripted.** A YAML manifest lists what exists at start. The
  emulator is ready only once all of it exists, and a key it does not understand stops the start.
- **Built for tests.** `POST /_stratus/reset` returns to the manifest's state between suites, and
  the `stratustest` package runs the emulator inside a Go test with no container.

## Services

| Service | Covers | Not emulated |
|---|---|---|
| **SQS** | standard and FIFO queues, delays, visibility timeouts, long polling, message attributes, dead-letter queues, batch operations, retention, tags; JSON and query protocols | message move tasks (redrive) |
| **Kinesis** | streams and shards, `PutRecord`/`PutRecords`, every shard iterator type with expiry, `GetRecords` with `MillisBehindLatest`, retention, tags | enhanced fan-out (`SubscribeToShard`), shard split and merge, encryption |
| **S3** | objects, multipart uploads, versioning, range and conditional requests, all checksum algorithms, `aws-chunked` bodies, presigned URLs, lifecycle expiry, event notifications to queues | ACLs, policies, tagging, object lock, replication |
| **DynamoDB** | tables, items, `Query`/`Scan`, the full expression language, local and global secondary indexes, batches, transactions, TTL | streams, `UpdateTable`, consumed-capacity reporting, reserved-word checks |
| **Step Functions** | Amazon States Language with `Pass`, `Task`, `Wait`, `Choice`, `Succeed`, `Fail`, `Retry`/`Catch`, task tokens; the `sqs:sendMessage` integration | `Parallel`, `Map`, `.sync` integrations, JSONata, activities |

An operation that is not emulated answers with an error that names it. Some optional request
settings are accepted and have no effect: server-side encryption, DynamoDB stream and tag settings
on `CreateTable`, and consumed-capacity reporting.

Time-driven clean-up runs in the background only when asked to, so tests stay deterministic. SQS
drops a message past its retention when its queue is next used. DynamoDB TTL deletes expired items
every `-ttl-interval` (`stratustest.WithTTLInterval`), or once on `POST /_stratus/expire-ttl`
(`Server.ExpireTTL`); without either, expired items stay, as in LocalStack by default. Like
DynamoDB it ignores a TTL attribute that is not a number, and one more than five years in the past.
S3 lifecycle expiry and Kinesis trimming do nothing until a test asks for them through the Go API.

S3 keeps each object's bytes in a file of a directory of its own, created at start under
`-data-dir` and removed on exit, so large objects do not hold memory; a file goes once no version
refers to it. Without `-data-dir` that directory is in the user's cache directory, else `/var/tmp`,
never `/tmp`, which is a tmpfs held in memory on many systems. The Docker image uses `/data`;
mount a volume there to keep the bytes off the container's writable layer. Nothing survives a
restart: buckets and object metadata are in memory.

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
| `-ttl-interval` | `0` (off) | how often to delete expired DynamoDB items in the background, for example `1m` |
| `-data-dir` | user cache dir, else `/var/tmp` | existing directory under which S3 object bytes are kept, in a directory removed on exit |
| `-tls-listen` | none | address to also serve HTTPS on, for example `:4567` |
| `-tls-cert`, `-tls-key` | none | certificate and key for HTTPS; a self-signed pair is generated when empty |
| `-tls-hosts` | `localhost,127.0.0.1,::1` | names and addresses a generated certificate is valid for |
| `-tls-cert-out` | none | file to write a generated certificate to, for clients to trust |

HTTPS matters for one case: the AWS SDKs upload a request body they cannot rewind (a pipe, a
network stream) only over HTTPS. Over plain HTTP `PutObject` needs a seekable body, or the
transfer manager.

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
      deadLetter: {queue: orders-dlq, maxReceiveCount: 5}
    - name: orders-dlq
    - name: uploads
    - name: ledger.fifo            # a name ending in .fifo makes a FIFO queue
      contentBasedDeduplication: true

kinesis:
  streams:
    - name: events
      shards: 2                    # default 1

s3:
  buckets:
    - name: packages
      versioning: true
      lifecycle:
        - {prefix: tmp/, expirationDays: 7}
      notifications:               # s3:ObjectCreated:* unless events are listed
        - {queue: uploads, prefix: incoming/, suffix: .zip}

dynamodb:
  tables:
    - name: orders
      partitionKey: {name: customer}            # type S unless given
      sortKey: {name: order_id, type: N}
      ttlAttribute: expires_at
      localIndexes:
        - name: by-status
          sortKey: {name: status}
          projection: INCLUDE                   # ALL (default), KEYS_ONLY or INCLUDE
          nonKeyAttributes: [total]

stepFunctions:
  stateMachines:
    - name: delayed-notification
      definitionFile: delayed-notification.asl.json   # relative to the manifest; or `definition:` inline
      substitutions:                                  # replaces ${QueueUrl} in the definition
        QueueUrl: http://localhost:4566/000000000000/orders
```

Resources may be listed in any order. An unknown key, a duplicate name, or a reference to a queue
that is not declared is an error at start. `examples/manifest.yaml` is a complete one.

## Control endpoints

| Endpoint | Meaning |
|---|---|
| `GET /_stratus/ready` | `200` once the manifest's resources exist, with the list of services |
| `POST /_stratus/reset` | drop all state and create the manifest's resources again |
| `POST /_stratus/expire-ttl` | delete the DynamoDB items past their TTL now; answers `{"deleted": n}` |
| `GET /_localstack/health` | the same answer in LocalStack's shape, for existing health checks |
| `GET /_stratus/ui` | the web panel, below |

## Web panel

Open `http://localhost:4566/_stratus/ui` in a browser. The overview shows a card per queue,
stream, bucket, table and state machine with its counts, refreshed every 2 seconds, and creates
resources of each kind. **Reset** returns to the manifest's state, **Expire TTL** deletes expired
DynamoDB items.

A resource's name opens its page: its settings, a search suited to the service, its operations and
its data, refreshed every 3 seconds (not while you edit it).

| Resource | Settings | Search | Change |
|---|---|---|---|
| Queue | attributes, dead-letter queue and its sources | text in bodies and attributes | send; edit (a new message replaces the old: SQS cannot change one); delete; purge; redrive a DLQ to its source |
| Stream | ARN, mode, retention, shards with hash ranges and record counts | partition key, shard, text in the decoded data | put a record (records cannot be changed or deleted) |
| Bucket | versioning, lifecycle rules, notifications | key prefix and text | upload a file or store text; edit a text object; delete; empty |
| Table | key schema, indexes, TTL, item count and size | partition key value, in the table or an index; text | put, edit and delete items as DynamoDB JSON |
| State machine | ARN, type, role, definition | execution status and name | start an execution; stop a running one |

Every resource can also be deleted from its page. Changes go through the emulator's own AWS API,
so they are validated and have their usual effects, such as S3 notifications to queues.

**Kinesis data is decoded.** A record aggregated by the Kinesis Producer Library is split into its
user records, each with its partition key; gzip and zlib payloads are decompressed (up to 1 MiB)
and JSON is indented, so a record reads `KPL → gzip → JSON`. A search by partition key finds an
aggregate by the keys inside it. zstd is recognised but not decompressed. The same decoding shows
S3 objects, SQS bodies and execution input and output.

The panel is part of the binary (server-rendered with [htmx](https://htmx.org) 2.0.4, embedded),
needs no network, and has no login, like the rest of the endpoint: anyone who can reach the port
can use it. Its changes only accept requests sent by the panel itself (the `HX-Request` header),
so a page on another site cannot make them in a browser that has the panel open. Downloads are
sent as attachments, so an uploaded page never runs in the panel.

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
`stratustest.WithTLS()` serves it over HTTPS; pass `srv.Client()` to the AWS client as its
`HTTPClient`.

## Compared with LocalStack

Both in Docker with 1 CPU and 2 GB each, started from the same manifest, driven by the AWS SDK for
Go. Method, every operation and its caveats: [`bench/README.md`](bench/README.md); rerun with
`bench/run.sh`.

| | LocalStack (`community-archive`) | stratus |
|---|---|---|
| Ready with 63 resources | 13.7 s | **0.24 s** |
| Memory once ready | 636 MiB | **3 MiB** |
| Image | 1.81 GB | **12.9 MB** |
| DynamoDB PutItem, 32 clients | 194 ops/s, p99 373 ms | **11 106 ops/s, p99 30 ms** |
| S3 GetObject 4 KiB, 32 clients | 711 ops/s, p99 101 ms | **11 301 ops/s, p99 8 ms** |
| S3 PutObject 256 KiB, 32 clients | 329 ops/s, p99 201 ms | **663 ops/s, p99 125 ms** |
| SQS SendMessage, 32 clients | 883 ops/s, p99 98 ms | **13 058 ops/s, p99 6 ms** |
| Kinesis PutRecord, 32 clients | 64 ops/s, p99 2.2 s | **13 580 ops/s, p99 5 ms** |

This compares speed, not behaviour: see each service's "Not emulated" column above.

## Develop

```sh
make test   # go test -race ./...
make lint   # gofmt and go vet
```

Every service is tested through the real AWS SDK for Go, in its own package and end to end in
`stratustest/`: a behaviour is considered emulated when the SDK accepts it, including its typed
errors. None of it has been compared against live AWS yet.

Layout: `cmd/stratus` is the binary, `internal/gateway` routes requests, each service lives in
`internal/<service>`, `internal/manifest` reads the resource manifest and `internal/server` puts
them together.
