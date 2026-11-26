FROM golang:1.27.4 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /stratus ./cmd/stratus

FROM scratch
# S3 object bytes go to /data, on the container's disk or a mounted volume;
# WORKDIR creates it, as scratch has no directories.
WORKDIR /data
COPY --from=build /stratus /stratus
EXPOSE 4566
ENTRYPOINT ["/stratus", "-data-dir", "/data"]
