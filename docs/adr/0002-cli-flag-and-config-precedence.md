---
date: 2026-10-10
status: ACCEPTED
id: "0002"
---

# 2. CLI Flag and Configuration Precedence Refactor

## Status

Accepted

## Context

`main.go` executed flag parsing, directory validation, and TOML resolution inside `init()`, triggering `os.Exit`. This prevented `go test` from running due to unrecognized test flags (`-test.*`). Additionally, flag default resolution conflicted with TOML `BaseDir` overrides.

## Decision

1. Remove all process setup and flag parsing from `init()`, delegating execution to a parameterizable `run(args []string, stdout, stderr io.Writer) int` function in `main.go`.
2. Use `flag.FlagSet.Visit` to track explicitly set CLI arguments.
3. Enforce layered configuration precedence: **Explicit CLI Flags > TOML Config Options > Hardcoded Defaults**.

## Consequences

- Eliminates side-effects during package initialization, making `main.go` fully unit-testable.
- Resolves CLI flag override bugs when a TOML configuration file is present.
