# Adding an operation

This page adds an operation to a JSON-protocol service. Kinesis is the example; SQS, DynamoDB and
Step Functions are built the same way. S3 routes by method and query parameters instead (see
`internal/s3/handler.go`).

## 1. Write the SDK test first

In `internal/kinesis/sdk_test.go`, call the operation the way an application would, and assert what
AWS documents, the errors included:

```go
func TestSDKUpdateStreamMode(t *testing.T) {
    e := newEnv(t) // the service behind httptest, with the SDK pointed at it
    ctx := context.Background()
    e.createStream(t, "events", 1)

    _, err := e.client.UpdateStreamMode(ctx, &sdk.UpdateStreamModeInput{
        StreamARN:         aws.String("arn:aws:kinesis:eu-central-1:000000000000:stream/events"),
        StreamModeDetails: &types.StreamModeDetails{StreamMode: types.StreamModeOnDemand},
    })
    if err != nil {
        t.Fatalf("UpdateStreamMode: %v", err)
    }
    // ...assert DescribeStreamSummary shows ON_DEMAND, and that an unknown
    // stream fails with *types.ResourceNotFoundException.
}
```

Run it and watch it fail: today the operation answers `UnknownOperationException`.

## 2. Implement it

Each JSON service has a table from the operation name, the part after the `X-Amz-Target` prefix,
to a method:

```go
var operations = map[string]operation{
    "CreateStream":     op((*Service).createStreamOp),
    // ...
    "UpdateStreamMode": op((*Service).updateStreamMode),
}
```

`op` decodes the request body into the method's input struct and encodes its result as the
response. Write the method next to the operations it belongs with, in `ops_stream.go`:

```go
type updateStreamModeInput struct {
    StreamARN         string
    StreamModeDetails struct{ StreamMode string }
}

func (s *Service) updateStreamMode(in updateStreamModeInput) (any, error) {
    st, err := s.resolve(streamRef{StreamARN: in.StreamARN})
    if err != nil {
        return nil, err
    }
    // validate as AWS does, then change st under st.mu
    return struct{}{}, nil
}
```

Errors are `*awsapi.Error` values built by the helpers in the service's `errors.go`. Each carries
the error code the SDK maps to its typed error.

## 3. Keep the rest in step

- **State that changes.** If the operation adds state to a resource, add it to the service's
  `snapshot.go`, so it survives a restart with `-state-dir`, and to its round-trip test.
- **The manifest.** If the setting is one a manifest should declare, add the key in
  `internal/manifest`, apply it in `Apply`, and say whether a change of it can be made in place
  (see `ensure.go`).
- **The web panel.** Show the new setting on the resource's page, if it is something people look
  for.
- **The documentation.** List the operation in `docs/reference/services.md` and in the README's
  services table.

## 4. Check

```sh
make test lint
```

Then open a pull request. Its description says what AWS does that the emulator now does, and
what it still does not.
