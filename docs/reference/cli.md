# Command line

```sh
stratus [flags]
```

| Flag | Default | Meaning |
|---|---|---|
| `-listen` | `:4566` | address to listen on |
| `-manifest` | none | YAML file with the resources to create at start ([Manifest](manifest.md)) |
| `-region` | `eu-central-1` | region of every resource |
| `-account` | `000000000000` | account id of every resource |
| `-ttl-interval` | `0` (off) | how often to delete expired DynamoDB items in the background, for example `1m` |
| `-data-dir` | user cache dir, else `/var/tmp` | existing directory under which S3 object bytes are kept, in a directory removed on exit |
| `-state-dir` | none | directory to keep data in across restarts, S3 object bytes included; the manifest's `persistence` says which resources. *Since 0.2.0* |
| `-state-interval` | `10s` | how often to save the state to `-state-dir`, besides on exit. *Since 0.2.0* |
| `-tls-listen` | none | address to also serve HTTPS on, for example `:4567` |
| `-tls-cert`, `-tls-key` | none | certificate and key for HTTPS; a self-signed pair is generated when empty |
| `-tls-hosts` | `localhost,127.0.0.1,::1` | names and addresses a generated certificate is valid for |
| `-tls-cert-out` | none | file to write a generated certificate to, for clients to trust |
| `-healthcheck` | | check that a running stratus on `-listen` is ready, then exit `0`, or `1` when it is not |
| `-version` | | print the version and exit |

## Where data goes

| Flags | S3 object bytes | Everything else |
|---|---|---|
| neither | a new directory under the user's cache directory, else `/var/tmp`, removed on exit | memory |
| `-data-dir D` | a new directory under `D`, removed on exit | memory |
| `-state-dir S` | `S/s3`, kept | memory, saved to `S/*.json` |

stratus never puts data under `/tmp`, which is a tmpfs held in memory on many systems.

## Signals

On SIGINT or SIGTERM stratus stops accepting requests and waits up to 5 seconds for open ones;
long-polling receives are cut. With `-state-dir` it then saves the state a last time.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | stopped by a signal, or `-healthcheck` found it ready |
| `1` | a start failure (an invalid manifest, a state that cannot be read, a port in use), or `-healthcheck` found it not ready |
