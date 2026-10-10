---
date: 2026-10-10
status: ACCEPTED
id: "0003"
---

# 3. Standard Library Upgrades and Rotation Cascade Fix

## Status

Accepted

## Context

1. `archive.go` relied on deprecated packages (`io/ioutil`, `github.com/pkg/errors`).
2. `strings.IndexRune(name, '.')` truncated filenames at the first dot, breaking timestamp parsing if dots appeared prior to `.sql.gz`.
3. `sortAsArchives` pushed duplicate backups within a timeframe tape into subsequent timeframe tapes instead of purging them immediately.

## Decision

1. Replace `ioutil` with `os.ReadDir` and `io.Discard`.
2. Replace `github.com/pkg/errors` with Go standard `%w` error wrapping and `errors.Join` for dual-error cleanup handling.
3. Change filename parsing to target `.sql.gz` suffixes via `strings.TrimSuffix`.
4. Truncate each timeframe tape to hold only its single newest snapshot (`tapes[i] = t[:1]`) and move all duplicate entries (`t[1:]`) directly to `a.purge`.

## Consequences

- Removes all third-party error wrapping and deprecated I/O dependencies.
- Prevents file parsing errors on complex database names or dot-separated timestamp formats.
- Enforces strict compliance with the 12-backup quota retention policy.
