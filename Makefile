.PHONY: build test lint run

build:
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=$$(git describe --tags --always --dirty 2>/dev/null || echo dev)" -o bin/stratus ./cmd/stratus

test:
	go test -race -count=1 ./...

lint:
	@unformatted="$$(gofmt -l .)"; if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi
	go vet ./...

run: build
	./bin/stratus -manifest examples/manifest.yaml
