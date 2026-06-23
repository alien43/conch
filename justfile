default:
    @just --list

# Build the conch binary
build:
    CGO_ENABLED=0 go build -ldflags="-s -w" -o conch cmd/conch/main.go

# Run unit and integration tests
test:
    go test -v -race ./...

# Run the cluster chaos test suite
test-chaos: build
    ./test-cluster-chaos.sh

# Format the Go source code
fmt:
    go fmt ./...

# Run basic static analysis
lint:
    go vet ./...
