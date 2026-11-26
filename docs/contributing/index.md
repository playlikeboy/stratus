# Contributing

Questions and ideas are welcome in
[Discussions](https://github.com/stratus-emulator/stratus/discussions), and issues and pull
requests in the [repository](https://github.com/stratus-emulator/stratus). Read the
[contributing guide](https://github.com/stratus-emulator/.github/blob/main/CONTRIBUTING.md) first.
Report security problems privately, as the
[security policy](https://github.com/stratus-emulator/.github/blob/main/SECURITY.md) says.

## Build and test

You need Go (the version in `go.mod`) and, for the Testcontainers module and the benchmark, Docker.

```sh
make build   # bin/stratus, with the version from git
make test    # go test -race -count=1 ./...
make lint    # gofmt and go vet
make run     # build, then run with examples/manifest.yaml
```

The documentation is built with [MkDocs](https://www.mkdocs.org) and
[Material for MkDocs](https://squidfunk.github.io/mkdocs-material/):

```sh
python3 -m venv .venv && .venv/bin/pip install -r docs/requirements.txt
.venv/bin/mkdocs serve          # http://127.0.0.1:8000, reloads on save
.venv/bin/mkdocs build --strict # what CI runs: a broken link fails it
```

The site's colours are the web panel's (`internal/server/ui/style.css`). When you change one, change
`docs/stylesheets/stratus.css` too.

## Layout

| Path | What |
|---|---|
| `cmd/stratus` | the binary: flags, signals, HTTPS |
| `internal/server` | puts the services, the manifest, the state and the control endpoints together; the web panel |
| `internal/gateway` | routes a request to its service |
| `internal/<service>` | one AWS service: `sqs`, `kinesis`, `s3`, `dynamodb` (with `attr` and `expr`), `sfn` |
| `internal/manifest` | reads, checks and applies the manifest; resolves `persistence` |
| `internal/awsapi` | what the services share: identities, ARNs, errors, JSON |
| `internal/clock` | the clock services read, faked in tests |
| `stratustest` | the in-process emulator for Go tests, and end-to-end tests through the AWS SDK |
| `testcontainers` | the Testcontainers module, a Go module of its own |
| `bench` | the benchmark against LocalStack |
| `docs` | this site |

## How behaviour is tested

A behaviour counts as emulated when the real AWS SDK for Go accepts it, typed errors included.
Every service has `sdk_test.go` files that start the service behind an HTTP server and call it
with the SDK. Unit tests next to them cover what is hard to reach through the SDK. None of it has
been compared against live AWS yet, so a pull request that checks a behaviour against AWS and fixes
a difference is especially welcome.

Tests must be deterministic. Read time from the injected `clock.Clock`, move a `clock.Fake` by hand,
and never sleep to wait for something time-driven.

## Next

- [Adding an operation](adding-an-operation.md): the steps, with an example.
- [Releases](releases.md): how a version gets to Docker Hub and GHCR.
