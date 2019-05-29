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
	quota            = 12
	rotationInterval = time.Hour
)

// An exponential series of durations controlled by the quota and rotationInterval.
// https://www.tablix.org/~avian/blog/archives/2015/01/exponential_backup_rotation/
var timeSeries []time.Duration

func init() {
	// add a special case for recent backups.
	timeSeries = make([]time.Duration, quota)
	timeSeries[0] = 5 * time.Minute

	for n := 1; n < quota; n++ {
		timeSeries[n] = rotationInterval * 1 << uint(n-1)
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
		0664,
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

	now := time.Now()
	fiBuf := arch.parseFileInfoList(fis, now)
	a := arch.sortAsArchives(fiBuf, now)

	arch.logger.Println()
	arch.logger.Println("Backups staged for removal:")
	arch.logger.Println()

	var errors []error
	for _, fi := range a.purge {
		arch.logger.Printf("- %s\n", fi.Name())
		err := os.Remove(
			filepath.Join(arch.BackupPath, database, fi.Name()),
		)
		if err != nil {
			errors = append(errors, err)
			arch.logger.Printf("  - failed (%v)\n", err)
		}
	}

	if len(a.purge) == 0 {
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

func (arch *Archiver) parseFileInfoList(fis []os.FileInfo, now time.Time) []*Finfo {
	fiBuf := make([]*Finfo, 0, len(fis))

	for _, finfo := range fis {
		name := finfo.Name()

		dotIdx := strings.IndexRune(name, '.')
		if dotIdx == -1 {
			arch.logger.Printf("no file extension found: %q", name)
			continue
		}

		ftime, err := time.Parse(arch.TimeFormat, name[:dotIdx])
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

	sortFileInfo(fiBuf)

	return fiBuf
}

func sortFileInfo(fis []*Finfo) {
	// Sort as recent => oldest
	sort.Sort(FinfoList(fis))
}

type archives struct {
	tapes [][]*Finfo
	purge []*Finfo
}

func (arch *Archiver) sortAsArchives(files FinfoList, now time.Time) archives {
	nowUnix := now.Unix()
	a := archives{
		tapes: make([][]*Finfo, quota),
	}

	// Make a set of backup tapes for each time series
	tapes := make([][]*Finfo, quota)
	for i, d := range timeSeries {
		for j, f := range files {
			// Guard against the holes we progressively put in this slice
			if f == nil {
				continue
			}

			// This file fits in this time window
			if float64(nowUnix-f.created) < d.Seconds() {
				// safely add to the tape time series
				tapes[i] = append(tapes[i], f)
				// remove it from further iterations
				files[j] = nil
			}
		}
	}

	// Run back through the original buffer looking for leftovers (non-nil files)
	// that are too old to fit inside the defined time series.
	for _, f := range files {
		if f != nil {
			a.purge = append(a.purge, f)
		}
	}

	// Run through our backups and look for collisions to purge.
	// Collisions are multiple backups in a single timeframe.
	for i, t := range tapes {
		// If this timeframe has 1 or none, it's good to go.
		if len(t) < 2 {
			continue
		}

		// Push excess into the next timeframe only if empty (and not at end of list).
		if len(tapes)-1 != i && len(tapes[i+1]) == 0 {
			tapes[i+1] = append(tapes[i+1], t[1:]...)
			continue
		}

		// This timeframe has collisions to be removed. Purge all but the first.
		a.purge = append(a.purge, t[1:]...)
	}

	return a
}
