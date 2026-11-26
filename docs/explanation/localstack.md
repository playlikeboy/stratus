# Compared with LocalStack

stratus and LocalStack solve the same problem in different ways. LocalStack emulates over a hundred
AWS services in a Python process that starts Java and other engines for some of them. stratus
emulates five, in one Go binary. This page compares their scope and design. The
[Benchmark](../benchmark.md) compares their speed.

| | LocalStack (Community) | stratus |
|---|---|---|
| Services | over a hundred | SQS, S3, DynamoDB, Kinesis, Step Functions |
| Process | Python, with DynamoDB Local (Java), kinesis-mock and others | one Go binary |
| Image | 1.81 GB | 12.9 MB |
| Resources at start | init scripts (`ready.d`) | a declarative [manifest](../reference/manifest.md), checked at start |
| Ready signal | `/_localstack/init/ready` once the scripts ran | `/_stratus/ready` once every resource exists |
| Keeping data | `PERSISTENCE=1` kept what DynamoDB Local and kinesis-mock write; S3, SQS and Step Functions were lost | `-state-dir` keeps all five services, [per resource](../guides/persistence.md) |
| Auth token | needed by current images | none |
| Tests in Go | the Testcontainers module | [`stratustest`](../guides/go-tests.md) in-process, or the [Testcontainers module](../guides/testcontainers.md) |

## When LocalStack is the better choice

- Your application uses services stratus does not emulate, such as SNS, Lambda, IAM or API Gateway.
- You rely on behaviour stratus leaves out, such as `Parallel` and `Map` states, DynamoDB
  streams or Kinesis enhanced fan-out. See [Services and operations](../reference/services.md).

## When stratus is

- Your application uses some of the five services, and you want the stack, or each test, to start
  in under a second with a few MiB of memory.
- You want resources declared and checked, rather than created by scripts that run in the
  background.
- You want a local stack that keeps its data across restarts, all of it.

## Behaviour

Speed says nothing about whether an operation answers as AWS does. Both emulators approximate
AWS. stratus tests every behaviour through the real AWS SDK for Go, but has not yet been compared
against live AWS. In one project that ran the same S3 contract tests against both, LocalStack 4.14
needed an exception for one case (a completion without parts) and stratus none. That is one data
point, not a general claim.
