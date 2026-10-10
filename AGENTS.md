# Agent Development Guide

## Overview
`mysqlbak` streams gzipped MySQL database backups to disk, enforces an exponential decay rotation schedule, and logs results via Mailgun.

## Source of Truth
- System Specification: `docs/spec.md`
- Architecture Decisions: `docs/adr/`
- Active Development Tasks: `docs/tasks/`

## Operating Guidelines
1. Always reference `docs/spec.md` before changing core behavior.
2. Ensure test coverage exists or is added in `archive_test.go` or `main_test.go` prior to refactoring.
3. Keep changes incremental and record architectural shifts in `docs/adr/`.
