package bak

import (
	"compress/gzip"
	"fmt"
	"github.com/alexsasharegan/mysqlbak/bytefmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	xtime "github.com/alexsasharegan/mysqlbak/now"
	"github.com/pkg/errors"
)

const (
	quotaHourly = 3
	quotaDaily  = 3
	quotaWeekly = 1
	quotaMonthy = 1
)

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
	for _, finfo := range fis {
		name := finfo.Name()
		arch.logger.Printf("- [%s] %s\n", bytefmt.ByteSize(finfo.Size()), name)

		ftime, err := time.Parse(arch.TimeFormat, name[:strings.IndexRune(name, '.')])
		if err != nil {
			arch.logger.Println(
				errors.Wrapf(err, "failed to parse time of backup from filename %q", name),
			)
			continue
		}
		fiBuf = append(fiBuf, &Finfo{
			FileInfo: finfo,
			time:     ftime,
			created:  ftime.Unix(),
		})
	}

	arch.logger.Println()

	// Sort as recent => oldest
	sort.Sort(FinfoList(fiBuf))

	now := &xtime.Now{Time: time.Now()}
	eod := now.BeginningOfDay()
	eow := now.BeginningOfWeek()
	eom := now.BeginningOfMonth()

	tapes := make(map[string][]*Finfo)

	// First pass: put each backup in its natural time slot.
	for _, fi := range fiBuf {
		switch {
		case fi.created > eod.Unix():
			tapes["hourly"] = append(tapes["hourly"], fi)
		case fi.created > eow.Unix():
			tapes["daily"] = append(tapes["daily"], fi)
		case fi.created > eom.Unix():
			tapes["weekly"] = append(tapes["weekly"], fi)
		default:
			tapes["monthly"] = append(tapes["monthly"], fi)
		}
	}

	// Go through each backup tape from recent => oldest
	// and fulfill each tape's quotas.
	// If a tape series' quota is fulfilled and there are leftovers,
	// check if the next time series is empty.
	// If empty, advance the leftovers into the next time series.
	// Check if backups collide with a time series (e.g. 2 in the same hour).
	// Stage collisions for deletion.

	seen := make(map[int]bool)
	count := 0
	for _, fi := range tapes["hourly"] {
		// Purge same hour collisions
		if seen[fi.time.Hour()] {
			tapes["purge"] = append(tapes["purge"], fi)
			continue
		}

		seen[fi.time.Hour()] = true
		// Handle quota fulfillment
		if count >= quotaHourly {
			if len(tapes["daily"]) == 0 {
				tapes["daily"] = append(tapes["daily"], fi)
			} else {
				tapes["purge"] = append(tapes["purge"], fi)
			}
			continue
		}

		count++
	}

	seen = make(map[int]bool)
	count = 0
	for _, fi := range tapes["daily"] {
		// Purge same day collisions
		if seen[fi.time.Day()] {
			tapes["purge"] = append(tapes["purge"], fi)
			continue
		}

		seen[fi.time.Day()] = true
		// Handle quota fulfillment
		if count >= quotaDaily {
			if len(tapes["weekly"]) == 0 {
				tapes["weekly"] = append(tapes["weekly"], fi)
			} else {
				tapes["purge"] = append(tapes["purge"], fi)
			}
			continue
		}

		count++
	}

	seen = make(map[int]bool)
	count = 0
	for _, fi := range tapes["weekly"] {
		// TODO: Purge same week collisions
		if seen[fi.time.Day()] {
			tapes["purge"] = append(tapes["purge"], fi)
			continue
		}

		seen[fi.time.Day()] = true
		// Handle quota fulfillment
		if count >= quotaWeekly {
			if len(tapes["monthly"]) == 0 {
				tapes["monthly"] = append(tapes["monthly"], fi)
			} else {
				tapes["purge"] = append(tapes["purge"], fi)
			}
			continue
		}

		count++
	}

	seen = make(map[int]bool)
	count = 0
	for _, fi := range tapes["monthly"] {
		// Purge same month collisions
		if seen[int(fi.time.Month())] {
			tapes["purge"] = append(tapes["purge"], fi)
			continue
		}

		seen[int(fi.time.Month())] = true
		// Handle quota fulfillment
		if count >= quotaMonthy {
			tapes["purge"] = append(tapes["purge"], fi)
			continue
		}

		count++
	}

	arch.logger.Println("Backups staged for removal:")
	arch.logger.Println()
	var errors []error
	for _, fi := range tapes["purge"] {
		arch.logger.Printf("- %s\n", fi.Name())
		if err := os.Remove(filepath.Join(arch.BackupPath, database, fi.Name())); err != nil {
			errors = append(errors, err)
			arch.logger.Printf("  - failed (%v)\n", err)
		}
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
