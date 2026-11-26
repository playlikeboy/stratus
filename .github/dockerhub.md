<p align="center"><img src="https://raw.githubusercontent.com/stratus-emulator/stratus/main/assets/logo/stratus-mark.svg" alt="stratus" width="96"></p>

# stratus

A fast local emulator of the AWS services your application needs in development and tests:
**SQS, S3, DynamoDB, Kinesis and Step Functions** on one port, ready in milliseconds, with the resources you declare
in a manifest and a web panel to watch and change them.

```sh
docker run --rm -p 4566:4566 stratusemulator/stratus
```

Point any AWS SDK or the AWS CLI at it, with any credentials:

```sh
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=eu-central-1
aws --endpoint-url http://localhost:4566 sqs list-queues
```

Open the panel at http://localhost:4566/_stratus/ui.

## Resources from a manifest

```sh
docker run --rm -p 4566:4566 \
  -v "$PWD/manifest.yaml:/manifest.yaml:ro" \
  stratusemulator/stratus -manifest /manifest.yaml
```

## Tags

| Tag | Image |
|---|---|
| `latest` | the newest release |
| `1.2.3`, `1.2`, `1` | a release |
| `edge` | the `main` branch |

Images are built for `linux/amd64` and `linux/arm64`, run as an unprivileged user (uid 65532) and keep S3 object
bytes in `/data`; mount a volume there for large objects. The same images are on
`ghcr.io/stratus-emulator/stratus`.

## More

Documentation, the manifest format, every emulated operation and the source:
**https://github.com/stratus-emulator/stratus** · Apache License 2.0
