# Manifest

The manifest is a YAML file, given with `-manifest`, that lists the resources that exist when
stratus starts. stratus reports ready only once all of them exist.

```yaml
persistence: true                 # optional, since 0.2.0

sqs:
  queues: [...]
kinesis:
  streams: [...]
s3:
  buckets: [...]
dynamodb:
  tables: [...]
stepFunctions:
  stateMachines: [...]
```

Every section is optional. Resources may be listed in any order: a queue may name a dead-letter
queue declared after it. `examples/manifest.yaml` in the repository is a complete one.

## What is checked at start

The start fails, naming the key, when the manifest has:

- a key stratus does not know (a typo is not ignored);
- a resource without a `name`, or a name declared twice in a section;
- a dead-letter queue or a notification queue that is not declared;
- `contentBasedDeduplication` on a queue whose name does not end in `.fifo`;
- a bucket notification to a FIFO queue (S3 delivers only to standard queues);
- a lifecycle rule without `expirationDays`;
- a negative shard count;
- a table without `partitionKey`;
- a state machine with both, or neither, of `definition` and `definitionFile`.

The AWS services' own rules apply on top, for example to queue and bucket names, or to an
invalid state machine definition.

## `persistence`

*Since 0.2.0.* Whether a resource keeps its data across restarts of an emulator started with
`-state-dir`. It can be set at three levels, and the nearest one decides:

| Level | Key | Applies to |
|---|---|---|
| manifest | `persistence` at the root | every resource |
| service | `sqs.persistence`, `kinesis.persistence`, `s3.persistence`, `dynamodb.persistence`, `stepFunctions.persistence` | every resource of the service, declared or created through the API |
| resource | `persistence` on a queue, stream, bucket, table or state machine | that resource |

Unset everywhere, everything is kept whenever `-state-dir` is given. Without `-state-dir` nothing
is kept, whatever the manifest says. See [Keeping data across restarts](../guides/persistence.md).

## `sqs.queues[]`

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | required | the queue name; ending in `.fifo` makes a FIFO queue |
| `visibilityTimeoutSeconds` | int | `30` | `VisibilityTimeout` |
| `delaySeconds` | int | `0` | `DelaySeconds` |
| `receiveWaitTimeSeconds` | int | `0` | `ReceiveMessageWaitTimeSeconds`, the long-polling default |
| `deadLetter.queue` | string | none | the dead-letter queue, declared in the manifest |
| `deadLetter.maxReceiveCount` | int | none | receives before a message moves there |
| `contentBasedDeduplication` | bool | `false` | FIFO queues only |
| `persistence` | bool | the service's | see above |

```yaml
sqs:
  queues:
    - name: orders
      visibilityTimeoutSeconds: 60
      deadLetter: {queue: orders-dlq, maxReceiveCount: 5}
    - name: orders-dlq
    - name: ledger.fifo
      contentBasedDeduplication: true
```

The queue URL is `http://<host>:4566/<account>/<name>`, with account `000000000000` unless
`-account` says otherwise.

## `kinesis.streams[]`

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | required | the stream name |
| `shards` | int | `1` | the number of shards, splitting the hash key space evenly |
| `persistence` | bool | the service's | see above |

## `s3.buckets[]`

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | required | the bucket name |
| `versioning` | bool | `false` | versioning enabled |
| `lifecycle[].prefix` | string | `""` (every key) | the keys a rule applies to |
| `lifecycle[].expirationDays` | int | required | days after creation an object expires |
| `notifications[].queue` | string | required | a standard queue of the manifest |
| `notifications[].events` | list | `[s3:ObjectCreated:*]` | S3 event types |
| `notifications[].prefix`, `.suffix` | string | none | filter on the object key |
| `persistence` | bool | the service's | see above |

```yaml
s3:
  buckets:
    - name: packages
      versioning: true
      lifecycle:
        - {prefix: tmp/, expirationDays: 7}
      notifications:
        - {queue: uploads, prefix: incoming/, suffix: .zip}
```

## `dynamodb.tables[]`

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | required | the table name |
| `partitionKey.name` | string | required | the partition (hash) key |
| `partitionKey.type` | `S`, `N` or `B` | `S` | its type |
| `sortKey` | key | none | the sort (range) key, same shape |
| `ttlAttribute` | string | none | the attribute holding each item's expiry, in epoch seconds |
| `localIndexes[]` | index | none | local secondary indexes: `name`, `sortKey`, `projection`, `nonKeyAttributes` |
| `globalIndexes[]` | index | none | global secondary indexes: also `partitionKey` |
| `persistence` | bool | the service's | see above |

An index's `projection` is `ALL` (the default), `KEYS_ONLY` or `INCLUDE`; `INCLUDE` lists the
attributes it copies in `nonKeyAttributes`.

```yaml
dynamodb:
  tables:
    - name: orders
      partitionKey: {name: customer}
      sortKey: {name: order_id, type: N}
      ttlAttribute: expires_at
      localIndexes:
        - name: by-status
          sortKey: {name: status}
          projection: INCLUDE
          nonKeyAttributes: [total]
```

Tables are created `PAY_PER_REQUEST` and `ACTIVE` at once.

## `stepFunctions.stateMachines[]`

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | required | the state machine name |
| `definition` | string | | the Amazon States Language definition, inline |
| `definitionFile` | string | | a file with it, relative to the manifest |
| `substitutions` | map | none | replaces each `${key}` in the definition with its value |
| `type` | `STANDARD` or `EXPRESS` | `STANDARD` | the type; both run the same way |
| `persistence` | bool | the service's | see above |

Exactly one of `definition` and `definitionFile` is required.

```yaml
stepFunctions:
  stateMachines:
    - name: delayed-notification
      definitionFile: delayed-notification.asl.json
      substitutions:
        QueueUrl: http://localhost:4566/000000000000/notifications
```

The state machines are created last, so a definition can refer to the other resources.

## Applied over saved data

With `-state-dir`, the manifest is applied over the data brought back at start. A missing
resource is created. An existing one takes the settings that can change in place, and the start
stops on a change that cannot. The table in [Keeping data across
restarts](../guides/persistence.md#change-the-manifest-of-a-running-stack) lists which is which.
Without `-state-dir` stratus starts empty, and the same rules give the same resources.
