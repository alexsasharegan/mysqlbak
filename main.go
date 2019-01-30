package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// Program holds the cli arguments
type Program struct {
	help       bool
	backupPath string
	timeFormat string
	databases  []string
}

// Usage prints the command help to stderr and exits.
func (p *Program) Usage() {
	logger.Printf("Usage of %s:\n", os.Args[0])
	logger.Printf("\n%s [...args] database1 database2 [...]\n\n", os.Args[0])
	logger.Println("Options")
	flag.PrintDefaults()
	logger.Println()

	os.Exit(1)
}

var (
	program = Program{
		timeFormat: "2006-01-02_15-04-05",
	}
	logger = log.New(os.Stderr, "", 0)
)

func init() {
	backupPath := flag.String("bak", "/var/bak/mysql", "path to the backup directory")
	h := flag.Bool("h", false, "show help")
	help := flag.Bool("help", false, "show help (long form)")

	flag.Parse()

	program.backupPath = *backupPath
	program.help = *h || *help
	program.databases = flag.Args()

	if program.help {
		program.Usage()
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
}

func main() {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		archiveDB()
		wg.Done()
	}()

	wg.Wait()
}

func archiveDB() {
	now := time.Now().Format(program.timeFormat)
	f, err := os.OpenFile(
		filepath.Join(program.backupPath, "db", fmt.Sprintf("%s.sql.gz", now)),
		os.O_CREATE|os.O_WRONLY,
		0666,
	)
	if err != nil {
		logger.Fatalln(errors.Wrap(err, "failed to open db archive for writing"))
	}
	defer f.Close()

	gzw := gzip.NewWriter(f)
	defer gzw.Close()

	cmd := exec.Command("mysqldump", "sensiblemoney")
	cmd.Stdout = gzw
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		logger.Fatalln(errors.Wrap(err, "mysql dump failed"))
	}

	rotateArchives("db")
}

// Finfo embeds os.FileInfo alongside an easily compared created time (int64)
type Finfo struct {
	os.FileInfo
	created int64
}

// FinfoList implements sort.Interface
type FinfoList struct {
	list []Finfo
}

func (fil FinfoList) Len() int {
	return len(fil.list)
}

func (fil FinfoList) Less(i, j int) bool {
	return fil.list[i].created < fil.list[j].created
}

func (fil FinfoList) Swap(i, j int) {
	fil.list[i], fil.list[j] = fil.list[j], fil.list[i]
}

func rotateArchives(subdir string) {
	fis, err := ioutil.ReadDir(filepath.Join(program.backupPath, subdir))
	if err != nil {
		logger.Fatalln(errors.Wrap(err, "failed to list directory"))
	}

	var (
		today, thisWeek, thisMonth, lastMonth, old FinfoList
	)

	now := time.Now()
	y, m, d := now.Date()
	endOfYesterday := time.Date(y, m, d, 0, 0, 0, 0, now.Location())
	// 0 indexed
	endOfLastWeek := endOfYesterday.AddDate(0, 0, -int(endOfYesterday.Weekday()))
	// 1 indexed
	endOfLastMonth := endOfYesterday.AddDate(0, 0, 1-int(endOfYesterday.Day()))
	EndOfMonthPrior := endOfLastMonth.AddDate(0, -1, 0)

	for _, fi := range fis {
		name := fi.Name()
		ftime, err := time.Parse(program.timeFormat, name[:strings.IndexRune(name, '.')])
		if err != nil {
			logger.Println(
				errors.Wrapf(
					err,
					"failed to parse time of backup from filename %q",
					fi.Name(),
				),
			)
			continue
		}

		switch {
		case ftime.After(endOfYesterday):
			today.list = append(today.list, Finfo{fi, ftime.Unix()})
		case ftime.After(endOfLastWeek):
			thisWeek.list = append(thisWeek.list, Finfo{fi, ftime.Unix()})
		case ftime.After(endOfLastMonth):
			thisMonth.list = append(thisMonth.list, Finfo{fi, ftime.Unix()})
		case ftime.After(EndOfMonthPrior):
			lastMonth.list = append(lastMonth.list, Finfo{fi, ftime.Unix()})
		default:
			old.list = append(old.list, Finfo{fi, ftime.Unix()})
		}
	}

	var del []Finfo
	collection := []struct {
		max  int
		data FinfoList
	}{
		{3, today},
		{3, thisWeek},
		{1, thisMonth},
		{1, lastMonth},
		{1, old},
	}
	for _, col := range collection {
		if col.data.Len() > col.max {
			// Use a reverse sort to keep the newest files at the beginning
			sort.Sort(sort.Reverse(col.data))
			del = append(del, col.data.list[col.max:]...)
		}
	}

	for _, fi := range del {
		err := os.Remove(filepath.Join(program.backupPath, subdir, fi.Name()))
		if err != nil {
			logger.Println(errors.Wrapf(err, "failed to remove old backup file %q", fi.Name()))
		}
	}
}
