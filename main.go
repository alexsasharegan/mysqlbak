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

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, _, stderr io.Writer) int {
	output := &bytes.Buffer{}
	logger := log.New(io.MultiWriter(stderr, output), "", 0)

	fs := flag.NewFlagSet("mysqlbak", flag.ContinueOnError)
	fs.SetOutput(stderr)

	h := fs.Bool("h", false, "show help")
	help := fs.Bool("help", false, "show help (long form)")
	bakPathFlag := fs.String("bak", defBak, "path to the backup directory")
	confFlag := fs.String("c", "", "path to a config file")
	parentsFlag := fs.Bool("p", false, "make parent directories as needed")

	if err := fs.Parse(args); err != nil {
		return 1
	}

	if *h || *help {
		fs.Usage()
		return 0
	}

	program := &Program{
		BackupPath: defBak,
	}

	// Track explicitly set flags on the CLI invocation
	explicitFlags := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) {
		explicitFlags[f.Name] = true
	})

	// 1. Resolve configuration file if provided
	if *confFlag != "" {
		if err := resolveConf(program, *confFlag); err != nil {
			logger.Println(err)
			return 1
		}
	}

	// 2. Explicit CLI -bak flag overrides config BaseDir
	if explicitFlags["bak"] {
		program.BackupPath = *bakPathFlag
	}

	// 3. CLI database args override config database list
	dbs := fs.Args()
	if len(dbs) > 0 {
		program.Databases = dbs
	}

	if len(program.Databases) == 0 {
		fs.Usage()
		return 1
	}

	if *parentsFlag {
		if err := os.MkdirAll(program.BackupPath, 0755); err != nil {
			logger.Printf("failed to initialize root backup directory: %v\n", err)
			return 1
		}
	}

	finfo, err := os.Stat(program.BackupPath)
	if err != nil {
		logger.Printf("could not verify backup path: %v\n", err)
		return 1
	}
	if !finfo.IsDir() {
		logger.Printf("backup path must be a directory: %q\n", program.BackupPath)
		return 1
	}

	for _, dbname := range program.Databases {
		if fi, err := os.Stat(filepath.Join(program.BackupPath, dbname)); err == nil && fi.IsDir() {
			continue
		}

		if err := os.Mkdir(filepath.Join(program.BackupPath, dbname), 0775); err != nil {
			logger.Printf("error initializing archive directory for %q: %v\n", dbname, err)
			return 1
		}
	}

	arch := bak.Archiver{
		BackupPath: program.BackupPath,
		TimeFormat: timeFormat,
	}
	arch.SetLogger(logger)

	var exitCode int
	defer func() {
		logger.Printf("Exit code: %d\n", exitCode)

		if program.Mailgun == nil {
			return
		}
		if program.Mailgun.Subject == "" {
			program.Mailgun.Subject = "MySQL Backup"
		}

		m := xmail.Message{
			Body:      output.String(),
			Recipient: program.Mailgun.MailTo,
			Subject:   program.Mailgun.Subject,
		}
		m.SetConfig(program.Mailgun)

		if _, _, err := xmail.Send(&m); err != nil {
			logger.Println(err)
		}
	}()

	for _, dbname := range program.Databases {
		logger.Printf("Database: %q\n", dbname)
		logger.Print(strings.Repeat("=", 32) + "\n\n")
		logger.Println("Archiving...")

		start := time.Now()
		if err := arch.Archive(dbname); err != nil {
			logger.Println(err)
			exitCode = 1
			continue
		}

		logger.Printf("Done in %s.\n\n", time.Since(start).String())
		logger.Printf("Preparing to rotate backups for %q.\n", dbname)

		if err := arch.Rotate(dbname); err != nil {
			exitCode = 1
			logger.Printf("Log rotation for %q encountered errors: %v\n", dbname, err)
		}

		logger.Println()
	}

	return exitCode
}

func resolveConf(program *Program, path string) error {
	if path == "" {
		return nil
	}

	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("no config file found: %w", err)
	}

	var conf Config
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := toml.NewDecoder(f).Decode(&conf); err != nil {
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
