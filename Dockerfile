# syntax=docker/dockerfile:1
# The build runs on the builder's own platform and cross-compiles for each
# target, so a multi-platform image needs no emulation to compile.
FROM --platform=$BUILDPLATFORM golang:1.27.4 AS build
ARG TARGETOS TARGETARCH VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w -X main.version=$VERSION" -o /out/stratus ./cmd/stratus \
 && mkdir -p /out/data

FROM scratch
LABEL org.opencontainers.image.title="stratus" \
      org.opencontainers.image.description="Fast local emulator of SQS, S3, DynamoDB, Kinesis and Step Functions" \
      org.opencontainers.image.source="https://github.com/stratus-emulator/stratus" \
      org.opencontainers.image.licenses="Apache-2.0"
# S3 object bytes go to /data, on the container's disk or a mounted volume,
# owned by the unprivileged user stratus runs as.
COPY --from=build --chown=65532:65532 /out/data /data
COPY --from=build /out/stratus /stratus
USER 65532:65532
WORKDIR /data
EXPOSE 4566
HEALTHCHECK --interval=5s --timeout=2s --retries=3 CMD ["/stratus", "-healthcheck"]
ENTRYPOINT ["/stratus", "-data-dir", "/data"]
