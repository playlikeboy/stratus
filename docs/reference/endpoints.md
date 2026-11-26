# Control endpoints

Next to the AWS APIs, stratus answers a few endpoints of its own on the same port.

| Endpoint | Answer |
|---|---|
| `GET /_stratus/ready` | `200` once the manifest's resources exist: `{"ready":true,"services":["sqs","kinesis","s3","dynamodb","states"]}` |
| `POST /_stratus/reset` | `204`: every resource and all data dropped, the manifest's resources created again |
| `POST /_stratus/expire-ttl` | `200` `{"deleted": n}`: the DynamoDB items past their TTL deleted now |
| `POST /_stratus/save` | `204`: the state saved to `-state-dir` now; does nothing without one. *Since 0.2.0* |
| `GET /_stratus/ui` | the [web panel](../guides/web-panel.md) |
| `GET /_localstack/health` | `200` in LocalStack's shape, for health checks written for it |

A failed reset or save answers `500` with `{"error": "…"}`.

```sh
curl -sf localhost:4566/_stratus/ready
curl -sf -X POST localhost:4566/_stratus/reset
curl -s  -X POST localhost:4566/_stratus/expire-ttl
```
