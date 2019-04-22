package bak

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/alexsasharegan/mysqlbak/bytefmt"
	"github.com/pkg/errors"
)

const (
	quota    = 12
	timeBase = 30 * time.Minute
)

// An exponential series of durations controlled by the quota and timeBase.
// https://www.tablix.org/~avian/blog/archives/2015/01/exponential_backup_rotation/
var timeSeries []time.Duration

func init() {
	timeSeries = make([]time.Duration, quota+1)

	// add a special case for recent backups.
	timeSeries[0] = 5 * time.Minute

	var n uint
	// Subtract the special case so quota is correct
	for ; n < quota-1; n++ {
		timeSeries[n+1] = timeBase * (1 << n)
	}
}

// Archiver performs, stores and rotates mysql backups.
type Archiver struct {
	TimeFormat string
	BackupPath string
	logger     *log.Logger
}

// SetLogger lets callers configure the internal logger used.
func (arch *Archiver) SetLogger(l *log.Logger) {
	arch.logger = l
}

// Archive performs a backup on the given database.
func (arch *Archiver) Archive(database string) (eArch error) {
	now := time.Now().Format(arch.TimeFormat)

	f, err := os.OpenFile(
		filepath.Join(arch.BackupPath, database, fmt.Sprintf("%s.sql.gz", now)),
		os.O_CREATE|os.O_WRONLY,
		0666,
	)
	if err != nil {
		return errors.Wrapf(err, "failed to open db archive for %q", database)
	}

	defer func() {
		err := f.Close()
		if err != nil {
			eArch = errors.Wrap(err, fmt.Sprintf("failed closing the file archive for %q", database))
			// If our command fails and the file close errors,
			// the error won't be returned, so we'll log here.
			arch.logger.Println(eArch)
		}
	}()

	gzw := gzip.NewWriter(f)
	defer func() {
		err := gzw.Close()
		if err != nil {
			eArch = errors.Wrap(err, fmt.Sprintf("failed closing the gzip archiver for %q", database))
			// If our command fails and the gzip close errors,
			// the error won't be returned, so we'll log here.
			arch.logger.Println(eArch)
		}
	}()

	cmd := exec.Command("mysqldump", database)
	cmd.Stdout = gzw
	cmd.Stderr = os.Stderr

	// Delete orphaned files created by a failed dump.
	if err := cmd.Run(); err != nil {
		os.Remove(f.Name())
		return errors.Wrapf(err, "mysql dump failed on database %q", database)
	}

	return
}

// Rotate purges old backups in a given database subdirectory.
func (arch *Archiver) Rotate(database string) error {
	fis, err := ioutil.ReadDir(filepath.Join(arch.BackupPath, database))
	if err != nil {
		return errors.Wrapf(err, "failed to list directory %q", database)
	}

	arch.logger.Println("Directory listing:")
	arch.logger.Println()

	fiBuf := make([]*Finfo, 0, len(fis))
	now := time.Now()
	nowUnix := now.Unix()

	for _, finfo := range fis {
		name := finfo.Name()

		ftime, err := time.Parse(arch.TimeFormat, name[:strings.IndexRune(name, '.')])
		if err != nil {
			arch.logger.Println(
				errors.Wrapf(err, "failed to parse time of backup from filename %q", name),
			)
			continue
		}

		arch.logger.Printf(
			"- [%s %6.1fhrs] %s\n",
			bytefmt.ByteSize(finfo.Size()), now.Sub(ftime).Hours(), name)

		fiBuf = append(fiBuf, &Finfo{
			FileInfo: finfo,
			time:     ftime,
			created:  ftime.Unix(),
		})
	}

	arch.logger.Println()

	// Sort as recent => oldest
	sort.Sort(FinfoList(fiBuf))

	// Make a set of backup tapes for each time series
	tapes := make([][]*Finfo, quota)
	for i, d := range timeSeries {
		for j, fi := range fiBuf {
			// Guard against the holes we progressively put in this slice
			if fi == nil {
				continue
			}

			// This file fits in this time window
			if float64(nowUnix-fi.created) < d.Seconds() {
				// safely add to the tape time series
				tapes[i] = append(tapes[i], fi)
				// remove it from further iterations
				fiBuf[j] = nil
			}
		}
	}

	var purge []*Finfo
	// Run back through the original buffer looking for leftovers (non-nil files)
	// that are too old to fit inside the defined time series.
	for _, fi := range fiBuf {
		if fi != nil {
			purge = append(purge, fi)
		}
	}
	fiBuf = nil

	// Run through our backups and look for collisions to purge.
	// Collisions are multiple backups in a single timeframe.
	for i, tape := range tapes {
		// If this timeframe has 1 or none, it's good to go.
		if len(tape) < 2 {
			continue
		}

		// Push excess into the next timeframe only if empty (and not at end of list).
		if len(tape)-1 != i && len(tapes[i+1]) == 0 {
			tapes[i+1] = append(tapes[i+1], tape[1:]...)
			continue
		}

		// This timeframe has collisions to be removed. Purge all but the first.
		purge = append(purge, tape[1:]...)
	}

	arch.logger.Println("Backups staged for removal:")
	arch.logger.Println()
	var errors []error
	for _, fi := range purge {
		arch.logger.Printf("- %s\n", fi.Name())
		if err := os.Remove(filepath.Join(arch.BackupPath, database, fi.Name())); err != nil {
			errors = append(errors, err)
			arch.logger.Printf("  - failed (%v)\n", err)
		}
	}

	if len(purge) == 0 {
		arch.logger.Println("- No files to remove")
	}

	if len(errors) > 0 {
		return fmt.Errorf("unable to remove all excess backups")
	}

	return nil
}

// Finfo embeds os.FileInfo alongside an easily compared created time (int64)
type Finfo struct {
	os.FileInfo
	time    time.Time
	created int64
}

// FinfoList implements sort.Interface
type FinfoList []*Finfo

func (fil FinfoList) Len() int {
	return len(fil)
}

// Less prefers the latest time
func (fil FinfoList) Less(i, j int) bool {
	return fil[i].created > fil[j].created
}

func (fil FinfoList) Swap(i, j int) {
	fil[i], fil[j] = fil[j], fil[i]
}
