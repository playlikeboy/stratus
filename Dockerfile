FROM golang:1.27.4 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /stratus ./cmd/stratus

FROM scratch
COPY --from=build /stratus /stratus
EXPOSE 4566
ENTRYPOINT ["/stratus"]
