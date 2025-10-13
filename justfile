# Development commands for ProbeLab go-commons

# Run all tests
test:
    go test ./...

# Build all packages
build:
    go build ./...

# Check for compile errors without building
check:
    go vet ./...

# Run static analysis
lint:
    golangci-lint run
