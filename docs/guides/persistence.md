# Keeping data across restarts

*Since 0.2.0.*

By default stratus starts from the manifest every time, which is what tests want. A local stack
that lives for days next to a database on a volume wants more. Its queues, tables, streams and
objects should survive `docker compose down` like the database's rows. Otherwise a user exists in
Postgres but not in DynamoDB, and seed scripts have to run after every restart.

## Turn it on

Give stratus a state directory on a volume:

```yaml
services:
  stratus:
    image: stratusemulator/stratus:0.2.0
    command: ["-manifest", "/manifest/manifest.yaml", "-state-dir", "/data/state"]
    volumes:
      - ./stratus:/manifest:ro
      - stratus-state:/data

volumes:
  stratus-state:
```

That is all. Unless the manifest says otherwise, every resource now keeps its data.

| | Kept |
|---|---|
| SQS | queues, messages (in-flight ones keep their receipt handle and visibility), tags, FIFO deduplication |
| Kinesis | streams, records, and the sequence numbers, so consumers' checkpoints stay valid |
| S3 | buckets, every object version, multipart uploads in progress; the bytes stay on disk |
| DynamoDB | tables, items, the TTL attribute |
| Step Functions | state machines and executions; a running execution resumes where it was |

stratus saves every `-state-interval` (10 s) and on exit. `docker stop` sends SIGTERM, so a
clean stop loses nothing, and a crash loses at most the last interval. `POST /_stratus/save`
saves at once, for example before a snapshot of the volume.

## Choose what is kept

`persistence` can be set at three levels. The nearest one decides:

```yaml
persistence: true              # 1. every resource (the default once -state-dir is given)

sqs:
  persistence: false           # 2. every queue...
  queues:
    - name: orders
      persistence: true        # 3. ...but this one
    - name: scratch            #    not kept: empty after every restart

dynamodb:
  tables:
    - name: sessions
      partitionKey: {name: id}
      persistence: false       # 3. a cache that may start empty
```

- A resource created through the API, not declared in the manifest, follows its service's setting.
- Without `-state-dir`, `persistence` is ignored, so the same manifest serves tests (nothing kept)
  and the local stack (data kept).
- When the manifest stops keeping a resource, its saved data is dropped at the next start.

## Change the manifest of a running stack

At start stratus brings the saved data back, then applies the manifest over it:

| Change in the manifest | What happens |
|---|---|
| a new resource | created |
| a resource removed | stays, with its data; delete it through the API or the panel |
| queue attributes, dead-letter queue | taken from the manifest; an attribute removed from it goes back to SQS's default |
| bucket versioning, lifecycle, notifications | taken from the manifest; versioning removed from a versioned bucket suspends it, as S3 does |
| a table's TTL attribute | taken from the manifest |
| a state machine's definition | a new version; running executions finish on the version they started with |
| a table's keys or indexes, a stream's shard count, a state machine's type | **the start stops** with an error naming the resource |

The last row cannot be applied in place, just as in AWS. Delete that resource through the API or
the panel, or delete the state, and start again.

## Start over

| To | Do |
|---|---|
| empty every resource, keep running | `POST /_stratus/reset` (the next save writes the empty state) |
| forget one service's data | delete `sqs.json`, `dynamodb.json`… from the state directory while stratus is stopped |
| forget everything | `docker compose down -v`, or delete the state directory |

## Kinesis consumers

The Kinesis Client Library keeps its checkpoints outside the emulator, in DynamoDB, Redis or
elsewhere. With persistence, the stream's sequence numbers continue after a restart, so the
checkpoints stay valid and consumers resume where they stopped.

If the stream's data is *not* kept but the checkpoints are, which happens after `down -v` of
stratus alone or with `persistence: false` on the stream, a consumer resuming from an old checkpoint
gets `InvalidArgumentException`, as it would from Kinesis. Drop the checkpoints together with the
stream.

## Step Functions

A running execution is saved with the state it is in, that state's input, and when it entered it.
After a restart it resumes there:

- a `Wait` ends when it would have, not a whole wait after the restart;
- a `Task` that was waiting for its integration, or for a task token, is called again: the
  integration sees the call twice, as it can on a retry in AWS.

More on how this works: [How state is kept](../explanation/state.md).
