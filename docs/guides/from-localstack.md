# Moving from LocalStack

stratus emulates a smaller set of services than LocalStack: SQS, S3, DynamoDB, Kinesis and Step
Functions. If those are what your project uses, the move is mostly mechanical. This page follows
the order that worked when a large project moved: about 60 resources and 30 services in Compose.

## 1. Check that your operations are covered

List the AWS operations your code calls and compare them with
[Services and operations](../reference/services.md). In Go, the input types name them:

```sh
grep -rhoE '&(sqs|s3|dynamodb|kinesis|sfn)\.[A-Z][A-Za-z]+Input' --include='*.go' . | sort -u
```

Check two things beyond the operations:

- **Step Functions state types.** `Parallel` and `Map` are not emulated.
- **Kinesis enhanced fan-out.** `SubscribeToShard` is not emulated. The Kinesis Client Library's
  polling mode works.

## 2. Turn init scripts into a manifest

LocalStack creates resources with scripts in `/etc/localstack/init/ready.d`. stratus reads them
from a [manifest](../reference/manifest.md) and is ready only once they all exist.

| LocalStack init script | stratus manifest |
|---|---|
| `awslocal sqs create-queue --queue-name jobs --attributes '{"VisibilityTimeout":"60"}'` | `- name: jobs`<br>`  visibilityTimeoutSeconds: 60` |
| `RedrivePolicy` with `deadLetterTargetArn`, `maxReceiveCount` | `deadLetter: {queue: jobs-dlq, maxReceiveCount: 5}` |
| `awslocal kinesis create-stream --shard-count 2` | `shards: 2` |
| `awslocal s3 mb` + `put-bucket-versioning` | `versioning: true` |
| `put-bucket-lifecycle-configuration` with `Expiration.Days` | `lifecycle: [{prefix: tmp/, expirationDays: 7}]` |
| `put-bucket-notification-configuration` to a queue | `notifications: [{queue: uploads, prefix: …, suffix: …}]` |
| `dynamodb create-table` + `update-time-to-live` | `partitionKey`, `sortKey`, `ttlAttribute` |
| `--local-secondary-indexes` | `localIndexes` with `projection` and `nonKeyAttributes` |
| `stepfunctions create-state-machine --definition "$(sed …)"` | `definitionFile` + `substitutions` |

Objects (`awslocal s3 cp`) are data, not resources. Put them with a one-shot `curl` container,
[as shown here](docker.md#seeding-objects). Comments in the scripts are worth keeping in the
manifest; YAML takes them.

!!! tip "Check the result"
    Start stratus with the new manifest and compare counts with LocalStack:
    `aws dynamodb list-tables`, `aws sqs list-queues`, `aws kinesis list-streams`,
    `aws s3api list-buckets`. In one migration this showed the persisted LocalStack state had
    drifted from the scripts: a stream was missing and an old table still existed.

## 3. Swap the Compose service

```yaml
  stratus:
    image: stratusemulator/stratus:0.2.0
    command: ["-manifest", "/manifest/manifest.yaml"]
    ports: ["4566:4566"]
    volumes: ["./stratus:/manifest:ro"]
    healthcheck:
      test: ["CMD", "/stratus", "-healthcheck"]
      interval: 1s
      retries: 30
    networks:
      default:
        aliases: [localstack]   # every config that says http://localstack:4566 keeps working
```

Rename `depends_on: localstack` to `stratus` and drop `SERVICES`, `PERSISTENCE`, `DEBUG` and the
Docker socket mount: stratus needs none of them.

## 4. Replace what was LocalStack-specific

| LocalStack | stratus |
|---|---|
| `/_localstack/health` | `/_stratus/ready` (`/_localstack/health` is also answered, for old checks) |
| `/_localstack/init/ready` | not needed: `/_stratus/ready` is ready only once the resources exist |
| `awslocal …` | `aws --endpoint-url http://localhost:4566 …`, or `AWS_ENDPOINT_URL` |
| `docker exec localstack awslocal …` | run the AWS CLI on the host: the image has no shell |
| `pip install awscli-local` in CI | the runner's AWS CLI v2 |
| an HTTPS certificate under `/var/lib/localstack/cache` | `-tls-listen :4567 -tls-cert-out /data/tls.pem` |
| Testcontainers LocalStack module | [the stratus module](testcontainers.md), or a plain `testcontainers.Run` |
| `PERSISTENCE=1` | `-state-dir` ([Keeping data across restarts](persistence.md)) |

## 5. Decide about state

LocalStack Community with `PERSISTENCE=1` kept DynamoDB and Kinesis data across restarts, the
files of its external engines. S3, SQS and Step Functions data was lost.

- **For tests**, run stratus without a state directory: every start is clean.
- **For a long-lived local stack**, add `-state-dir` on a volume: all five services keep their
  data, and the manifest is applied over it.

!!! warning "Kinesis checkpoints"
    Consumers keep their checkpoints outside the emulator, often in a Redis or DynamoDB with a
    volume. If stratus starts empty while those checkpoints survive, consumers resume from
    sequence numbers the new streams do not have. stratus refuses those with
    `InvalidArgumentException`, as Kinesis does. Keep the streams with `-state-dir`, or drop the
    checkpoints whenever the streams are new.

## 6. Measure

Worth recording before and after, for your team:

- the time from `docker compose up` until everything is healthy;
- the emulator's memory (`docker stats`);
- the duration of the integration suites.

In the migration this page is based on, the emulator was healthy in about 2 s instead of 22–28 s.
It used about 5 MiB instead of 550–700 MiB, and the integration suites ran in 85 s instead of 237 s.
