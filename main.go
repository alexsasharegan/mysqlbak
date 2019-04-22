package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/alexsasharegan/mysqlbak/bak"
	"github.com/alexsasharegan/mysqlbak/xmail"
)

// Program holds the cli arguments
type Program struct {
	BackupPath string
	Databases  []string
	Mailgun    *xmail.MailConfig
}

// Config is the toml configuration.
type Config struct {
	BaseDir   string
	Databases []string
	Mailgun   *xmail.MailConfig
}

const (
	timeFormat = "2006-01-02_15-04-05_MST"
	defBak     = "/var/bak/mysql"
)

var (
	program = &Program{}
	output  = &bytes.Buffer{}
	logger  = log.New(io.MultiWriter(os.Stderr, output), "", 0)
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

	BackupPath := flag.String("bak", defBak, "path to the backup directory")
	conf := flag.String("c", "", "path to a config file")
	parents := flag.Bool("p", false, "make parent directories as needed")

	flag.Parse()

	if err := resolveConf(program, *conf); err != nil {
		logger.Fatalln(err)
	}

	dbs := flag.Args()
	if len(dbs) > 0 {
		program.Databases = dbs
	}

	if program.BackupPath == "" {
		program.BackupPath = *BackupPath
	}

	if *BackupPath != defBak {
		program.BackupPath = *BackupPath
	}

	if *h || *help {
		program.Usage()
	}

	if *parents {
		if err := os.MkdirAll(program.BackupPath, 0755); err != nil {
			logger.Fatalf("failed to initialize root backup directory: %v", err)
		}
	}

	finfo, err := os.Stat(program.BackupPath)
	if err != nil {
		logger.Fatalf("could not verify backup path: %v", err)
	}
	if !finfo.IsDir() {
		logger.Fatalf("backup path must be a directory: %q", program.BackupPath)
	}
	if len(program.Databases) == 0 {
		program.Usage()
	}

	for _, dbname := range program.Databases {
		if fi, err := os.Stat(filepath.Join(program.BackupPath, dbname)); err == nil && fi.IsDir() {
			continue
		}

		if err := os.Mkdir(filepath.Join(program.BackupPath, dbname), 0775); err != nil {
			logger.Fatalf("error initializing archive directory for %q: %v", dbname, err)
		}
	}
}

func main() {
	arch := bak.Archiver{
		BackupPath: program.BackupPath,
		TimeFormat: timeFormat,
	}
	arch.SetLogger(logger)

	var exit int
	defer func() {
		logger.Printf("Exit code: %d\n", exit)

		if program.Mailgun == nil {
			os.Exit(exit)
		}

		m := xmail.Message{
			Body:      output.String(),
			Recipient: program.Mailgun.MailTo,
			Subject:   "MySQL Backup",
		}
		m.SetConfig(program.Mailgun)

		if _, _, err := xmail.Send(nil, &m); err != nil {
			logger.Println(err)
		}

		os.Exit(exit)
	}()

	for _, dbname := range program.Databases {
		logger.Printf("Database: %q\n", dbname)
		logger.Print(strings.Repeat("#", 32) + "\n\n")
		logger.Println("Archiving...")
		start := time.Now()
		if err := arch.Archive(dbname); err != nil {
			logger.Println(err)
			exit = 1
			continue
		}
		logger.Printf("Done in %s.\n\n", time.Now().Sub(start).String())
		logger.Printf("Preparing to rotate logs for %q.\n", dbname)

		if err := arch.Rotate(dbname); err != nil {
			exit = 1
			logger.Printf("Log rotation for %q encountered errors: %v", dbname, err)
		}

		logger.Println()
	}
}

func resolveConf(program *Program, path string) error {
	if path == "" {
		return nil
	}

	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("no config file found: %v", err)
	}

	var conf Config
	f, err := os.Open(path)
	if err != nil {
		return err
	}

	_, err = toml.DecodeReader(f, &conf)
	if err != nil {
		return err
	}

	if conf.BaseDir != "" {
		program.BackupPath = conf.BaseDir
	}

	if len(conf.Databases) > 0 {
		program.Databases = conf.Databases
	}

	if conf.Mailgun != nil {
		program.Mailgun = conf.Mailgun
	}

	return nil
}
