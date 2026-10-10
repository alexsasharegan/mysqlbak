# mysqlbak Specification

## Core Requirements

1. **Zero-Spooling Stream**: Database dumps must execute via `mysqldump <db>` and pipe `stdout` directly into `gzip.Writer` without uncompressed temporary disk storage. On failure, partial backup files must be unlinked.
2. **Exponential Rotation**: Keep a maximum quota of 12 backups per database (`quota = 12`). Time windows double starting from 1 hour, with a 5-minute initial window.
3. **Execution Logging & Delivery**: All operational output must be recorded and emailed via Mailgun upon exit if Mailgun credentials are configured.

## CLI & Configuration Contract

- **Default Backup Path**: `/var/bak/mysql`
- **Flags**:
  - `-bak <path>`: Directory where backup subfolders reside.
  - `-c <path>`: TOML configuration file location.
  - `-p`: Automatically create parent directories if missing.
  - `-h`, `-help`: Display usage text.
- **Precedence Rule**: Explicit CLI flags override TOML config file options; TOML config options override hardcoded defaults.
