# Task 01: Test Suite & Bug Reproduction

## Goal

Construct unit tests that fail under current code to prove bug identification, establishing a regression baseline.

## Steps

1. **Mock Execution**: Abstract `mysqldump` invocation in `Archive()` to allow test assertions without needing a live MySQL server.
2. **Reproduce Config Precedence Bug**: Write a test in `main_test.go` asserting that specifying `-bak /custom/path` alongside a TOML config respects the CLI flag.
3. **Reproduce Filename Parsing Bug**: Create test cases in `archive_test.go` using filenames containing dots (e.g., `db.name_2026-01-01_00-00-00_UTC.sql.gz`) to trigger parsing failures in `parseFileInfoList`.
4. **Reproduce Cascade Bug**: Write a test for `sortAsArchives` with multiple colliding backups to verify single-bucket shift behavior instead of dumping all excess entries.
