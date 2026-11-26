# Getting started

This page takes you from nothing to a queue, a table and a bucket you can use from the AWS CLI.

## 1. Write a manifest

Save this as `manifest.yaml`:

```yaml
sqs:
  queues:
    - name: orders
      deadLetter: {queue: orders-dlq, maxReceiveCount: 5}
    - name: orders-dlq

dynamodb:
  tables:
    - name: customers
      partitionKey: {name: id}

s3:
  buckets:
    - name: invoices
```

The [manifest reference](reference/manifest.md) lists every key.

## 2. Start stratus

=== "Docker"

    ```sh
    docker run --rm -p 4566:4566 \
      -v "$PWD/manifest.yaml:/manifest.yaml:ro" \
      stratusemulator/stratus -manifest /manifest.yaml
    ```

=== "Go"

    ```sh
    go install github.com/stratus-emulator/stratus/cmd/stratus@latest
    stratus -manifest manifest.yaml
    ```

stratus logs `stratus is ready` once every resource of the manifest exists. It usually takes well
under a second. You can also ask it:

```sh
curl localhost:4566/_stratus/ready
# {"ready":true,"services":["sqs","kinesis","s3","dynamodb","states"]}
```

## 3. Use it

Any credentials work; stratus does not check signatures.

```sh
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=eu-central-1
export AWS_ENDPOINT_URL=http://localhost:4566   # the AWS CLI v2 and recent SDKs read it

aws sqs send-message --queue-url http://localhost:4566/000000000000/orders --message-body hello
aws dynamodb put-item --table-name customers --item '{"id":{"S":"c-1"},"name":{"S":"Ada"}}'
echo invoice > a.txt && aws s3 cp a.txt s3://invoices/2026/a.txt
```

From code, point the SDK's endpoint at stratus. In Go:

```go
cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("eu-central-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
)
queues := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
    o.BaseEndpoint = aws.String("http://localhost:4566")
})
```

!!! tip "S3 addressing"
    stratus serves both path-style (`localhost:4566/invoices/a.txt`) and virtual-hosted-style
    (`invoices.localhost:4566/a.txt`) requests. With a custom endpoint, the AWS SDKs use path style.

## 4. Look inside

Open [http://localhost:4566/_stratus/ui](http://localhost:4566/_stratus/ui). The overview shows
every resource with its counts. A resource's name opens its data, which you can search and edit.
See [Web panel](guides/web-panel.md).

## 5. Start over

```sh
curl -X POST localhost:4566/_stratus/reset
```

This drops all data and creates the manifest's resources again, without a restart.

## Next

- Run stratus next to your services: [Docker and Compose](guides/docker.md).
- Use it from tests: [Go tests](guides/go-tests.md), [Testcontainers](guides/testcontainers.md).
- Keep data across restarts: [Keeping data across restarts](guides/persistence.md).
