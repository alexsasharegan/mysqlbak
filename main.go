package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/alexsasharegan/mysqlbak/bak"
)

// Program holds the cli arguments
type Program struct {
	help       bool
	backupPath string
	timeFormat string
	databases  []string
}

var (
	program = Program{
		timeFormat: "2006-01-02_15-04-05",
	}
	logger = log.New(os.Stderr, "", 0)
)

// Usage prints the command help to stderr and exits.
func (p *Program) Usage() {
	logger.Printf("Usage of %s:\n", os.Args[0])
	logger.Printf("\n%s [...args] database1 database2 [...]\n\n", os.Args[0])
	logger.Println("Options")
	flag.PrintDefaults()
	logger.Println()

	os.Exit(1)
}

func init() {
	h := flag.Bool("h", false, "show help")
	help := flag.Bool("help", false, "show help (long form)")
	backupPath := flag.String("bak", "/var/bak/mysql", "path to the backup directory")
	parents := flag.Bool("p", false, "no error if existing, make parent directories as needed")

	flag.Parse()

	program.help = *h || *help
	program.databases = flag.Args()
	program.backupPath = *backupPath

	if program.help {
		program.Usage()
	}

	if *parents {
		if err := os.MkdirAll(program.backupPath, 0755); err != nil {
			logger.Fatalf("failed to initialize base backup directory: %v", err)
		}
	}

	fi, err := os.Stat(program.backupPath)
	if err != nil {
		logger.Fatalf("error verifying backup path: %v", err)
	}
	if !fi.IsDir() {
		logger.Fatalf("backup path must be a directory: %q", program.backupPath)
	}
	if len(program.databases) == 0 {
		program.Usage()
	}

	for _, dbname := range program.databases {
		if fi, err := os.Stat(filepath.Join(program.backupPath, dbname)); err == nil && fi.IsDir() {
			continue
		}

		if err := os.Mkdir(filepath.Join(program.backupPath, dbname), 0775); err != nil {
			logger.Fatalf("error while initializing archive directories for %q: %v", dbname, err)
		}
	}
}

func main() {
	arch := bak.Archiver{
		BackupPath: program.backupPath,
		TimeFormat: program.timeFormat,
	}

	for _, dbname := range program.databases {
		logger.Printf("Archiving %q.\n", dbname)
		start := time.Now()
		if err := arch.Archive(dbname); err != nil {
			logger.Fatal(err)
		}
		logger.Printf("Done in %s.\n", time.Now().Sub(start).String())
	}
}
