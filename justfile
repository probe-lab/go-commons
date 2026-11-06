# Task runner. `just` lists the recipes; CI runs `just ci`.

# List the recipes
default:
    @just --list

# Build every package and command
build:
    go build ./...

# Run the tests
test *args:
    go test ./... {{ args }}

# Check formatting, go fix, vet, and staticcheck without changing files
lint:
    @test -z "$(gofumpt -l .)" || { echo "not formatted, run just format:"; gofumpt -l .; exit 1; }
    go fix -diff ./...
    go vet ./...
    staticcheck ./...

# Format the code and apply go fix in place
format:
    gofumpt -w .
    go fix ./...

# Run everything CI runs
ci: lint build test
