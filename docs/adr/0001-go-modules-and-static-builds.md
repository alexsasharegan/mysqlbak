---
date: 2026-10-10
status: ACCEPTED
id: "0001"
---

# 1. Go Modules and Portable Static Builds

## Context

The repository predated standard Go module management and relied on `dep` (`Gopkg.toml`, `Gopkg.lock`). The build script used legacy compiler flags (`-tags netgo -ldflags '-extldflags "-static"'`) to produce static binaries.

## Decision

1. Initialize standard Go modules (`go mod init github.com/alexsasharegan/mysqlbak`) and delete `Gopkg.toml` and `Gopkg.lock`.
2. Modernize `build.sh` to enforce `CGO_ENABLED=0` alongside target environment defaults (`GOOS=${GOOS:-linux} GOARCH=${GOARCH:-amd64}`).

## Consequences

- Simplifies dependency management using standard Go toolchain primitives (`go.mod`, `go.sum`).
- Guarantees Cgo-free, statically linked binaries that run reliably on minimal Linux server distributions without dynamic `libc` dependencies.
