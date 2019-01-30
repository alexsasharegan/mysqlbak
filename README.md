# mysqlbak

A cli tool to perform mysql dumps and rotate backup files.

## Goal

I want a simple, reusable tool to backup all my VPS mysql databases. I want them
to be gzipped to keep storage down, but also rotated to keep a few recent
backups and some older ones at increasing periods.

## Usage

```sh
Usage of /tmp/go-build546230299/b001/exe/main:

/tmp/go-build546230299/b001/exe/main [...args] database1 database2 [...]

Options
  -bak string
        path to the backup directory (default "/var/bak/mysql")
  -h    show help
  -help
        show help (long form)
```