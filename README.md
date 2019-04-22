# mysqlbak

A cli tool to perform mysql dumps and rotate backup files.

## Goal

I want a simple, reusable tool to backup all my VPS mysql databases. I want them
to be gzipped to keep storage down, but also rotated to keep a few recent
backups and some older ones at increasing periods.

## Usage

```sh
Usage of mysqlbak:

mysqlbak [...args] database1 database2 [...]

Options
  -bak string
    	path to the backup directory (default "/var/bak/mysql")
  -c string
    	path to a config file
  -h	show help
  -help
    	show help (long form)
  -p	make parent directories as needed

```
