# How state is kept

*Since 0.2.0.* This page explains what happens behind `-state-dir`. To use it, see
[Keeping data across restarts](../guides/persistence.md).

## Files, not a database

```
state/
├── sqs.json            queues, messages, FIFO deduplication
├── kinesis.json        streams, records, sequence counters
├── dynamodb.json       tables and items (DynamoDB's own JSON for attribute values)
├── s3.json             buckets and object metadata, naming the files below
├── stepfunctions.json  state machines and executions
└── s3/                 object bytes, one file per version or upload part
```

While stratus runs, the state stays in memory, exactly as without `-state-dir`, so the API is not
slower. Every `-state-interval`, and on exit, each service writes its whole state as JSON.

Each file is written to a temporary file next to it, synced, and renamed over the old one. A
rename is atomic, so a crash at any moment leaves either the previous complete file or the new
one, never half of one. The services are saved one after another, not as one transaction. After a
crash they can be up to one interval apart, which a local emulator can live with.

Every file starts with `"format": 1`. A stratus that reads another format refuses the file and
names it, instead of half reading it. The fix is to delete that file, and that service then
starts empty.

## S3 bytes

Object bytes are already in files, so a snapshot only names them. At start the files no saved
version names are removed: those written after the last save and before a crash, or those of a
bucket the manifest no longer keeps. A version whose file is gone is dropped with a warning,
rather than served with no bytes.

## What each service keeps

| Service | Detail worth knowing |
|---|---|
| SQS | an in-flight message keeps its receipt handle and its visibility deadline, so a consumer can still delete it after a restart; FIFO keeps its sequence and deduplication window |
| Kinesis | the stream's sequence counter goes on where it stopped, so the KCL's checkpoints, kept outside the emulator, stay valid |
| DynamoDB | the items are written in the table's sort order and indexed again on load |
| S3 | every version and delete marker, uploads in progress with their parts |
| Step Functions | each execution with the version of the definition it started with, its history, and, if running, the state it is in |

## Resuming an execution

A running execution records, as it enters each state, the state's name, its input and the moment
it entered. After a restart the execution's goroutine starts again from that state, with that
input and that moment:

- a `Wait` of 10 seconds entered 4 seconds before the save ends 6 seconds after the restart;
- the history does not get a second `StateEntered` event;
- a `Task` is invoked again. A token it was waiting for is not kept, so the integration is called
  anew and gets a new token. This is at-least-once delivery, as on a retry in AWS.

## The manifest over saved data

At start the saved state is brought back first, and then the manifest is applied. Applying a
manifest *ensures* each resource rather than creating it:

- a missing resource is created, with no data;
- an existing one takes every setting that can change in place in AWS. A setting the manifest
  leaves out goes back to the default, so removing a line from the manifest is not silently
  ignored;
- a difference AWS cannot make in place (a table's keys or indexes, a stream's shard count, a
  state machine's type) stops the start and names the resource to delete.

Applying a manifest to an empty emulator is the same operation, so a start with and a start
without saved data cannot disagree about what the manifest means.

## Why not LocalStack's way

LocalStack Community with `PERSISTENCE=1` kept what its external engines write to disk: DynamoDB
Local's SQLite files and kinesis-mock's JSON. Queues, buckets and executions held in its own
process were lost. Its init scripts also ran only against empty state, so the saved state drifted
away from the scripts over time. stratus keeps all five services, and applies the manifest at
every start, so the manifest stays the definition of what exists.

## Limits

- **The size of the state.** Each save writes every service's whole state, not just what changed.
  For a local stack this is kilobytes to megabytes, written in milliseconds. A table with millions
  of items would make each save expensive. A change journal would solve that; ask in
  [Discussions](https://github.com/stratus-emulator/stratus/discussions) if you need it.
- **The window after a crash.** A crash or `kill -9` loses at most one `-state-interval`. A clean
  stop loses nothing.
- **Not shared.** One state directory belongs to one stratus at a time.
