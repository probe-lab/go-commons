# Task runner. `just` lists the recipes; CI runs `just ci`.

# List the recipes
default:
    @just --list

# Build every package
build:
    go build ./...

# Run the tests
test *args:
    go test ./... {{ args }}

# Check lint without changing files
lint:
    golangci-lint run

# Format the code in place
format:
    gofumpt -w .

# Check for compile errors and suspicious constructs
check:
    go vet ./...

# Run everything CI runs
ci: lint build test
