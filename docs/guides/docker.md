# Docker and Compose

## The image

| Registry | Name |
|---|---|
| Docker Hub | `stratusemulator/stratus` |
| GitHub Container Registry | `ghcr.io/stratus-emulator/stratus` |

Both are built for `linux/amd64` and `linux/arm64` and carry an SBOM and provenance.

| Tag | Points at |
|---|---|
| `0.2.0`, `0.2`, `0` | a release |
| `latest` | the newest release |
| `edge` | the head of `main` |

Pin a release in anything that should not change under you.

The image is built `FROM scratch`. It has no shell, curl or AWS CLI, only the binary. It runs as
an unprivileged user (uid 65532), and its working directory is `/data`, where S3 object bytes are kept.

## A Compose service

```yaml
services:
  stratus:
    image: stratusemulator/stratus:0.2.0
    command: ["-manifest", "/manifest/manifest.yaml"]
    ports:
      - "4566:4566"
    volumes:
      - ./stratus:/manifest:ro
    healthcheck:
      test: ["CMD", "/stratus", "-healthcheck"]
      interval: 1s
      timeout: 2s
      retries: 30

  app:
    build: .
    environment:
      AWS_ENDPOINT_URL: http://stratus:4566
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_REGION: eu-central-1
    depends_on:
      stratus:
        condition: service_healthy
```

`-healthcheck` asks the running stratus whether it is ready. stratus is ready only once every
resource of the manifest exists, so `condition: service_healthy` means "the tables, queues,
streams and buckets are there". Nothing needs to poll for them.

A manifest can name a Step Functions definition file relative to itself (`definitionFile`), so mount
the directory that holds both.

### Keeping an existing endpoint name

If your services' configuration already names another host, such as `http://localstack:4566`,
give stratus that name on the network instead of changing every config:

```yaml
  stratus:
    networks:
      default:
        aliases: [localstack]
```

## Volumes and file ownership

S3 object bytes live in `/data`. A named or anonymous volume there works as it is:

```sh
docker run -p 4566:4566 -v stratus-data:/data stratusemulator/stratus
```

A directory of the host must be writable by uid 65532. Otherwise, run the container as your own
user with `--user "$(id -u)"`.

To keep data across restarts, add `-state-dir` on a volume: see
[Keeping data across restarts](persistence.md).

## HTTPS

```sh
docker run -p 4566:4566 -p 4567:4567 stratusemulator/stratus \
  -tls-listen :4567 -tls-cert-out /data/tls.pem
docker cp <container>:/data/tls.pem .
```

stratus generates a self-signed certificate for `localhost`, `127.0.0.1` and `::1` (`-tls-hosts`
changes the list) and writes it where clients can trust it. HTTPS matters in one case: the AWS
SDKs upload a body they cannot rewind (a pipe, a stream), or send `aws-chunked` checksum
trailers, only over HTTPS.

## Seeding objects

A manifest declares resources, not their data. Put objects with a one-shot container once stratus
is healthy. stratus does not check signatures, so plain `curl` is enough:

```yaml
  stratus-seed:
    image: curlimages/curl:8.11.1
    entrypoint: ["/bin/sh", "-c", "curl -sSf -T /seed/GeoLite2-ASN.mmdb http://stratus:4566/geoip/GeoLite2-ASN.mmdb"]
    volumes:
      - ./seed:/seed:ro
    depends_on:
      stratus:
        condition: service_healthy

  app:
    depends_on:
      stratus-seed:
        condition: service_completed_successfully
```

!!! note
    `docker compose up --wait` fails when you name a one-shot service in the command, because it
    exits. Name only the long-running services; their dependencies still run.
