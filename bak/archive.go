package bak

import (
	"compress/gzip"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/alexsasharegan/mysqlbak/bytefmt"
)

const (
	quota            = 12
	rotationInterval = time.Hour
)

// An exponential series of durations controlled by quota and rotationInterval.
// https://www.tablix.org/~avian/blog/archives/2015/01/exponential_backup_rotation/
var timeSeries []time.Duration

func init() {
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
		return fmt.Errorf("failed to open db archive for %q: %w", database, err)
	}

	defer func() {
		if err := f.Close(); err != nil {
			eArch = fmt.Errorf("failed closing the file archive for %q: %w", database, err)
			arch.logger.Println(eArch)
		}
	}()

	gzw := gzip.NewWriter(f)
	defer func() {
		if err := gzw.Close(); err != nil {
			eArch = fmt.Errorf("failed closing the gzip archiver for %q: %w", database, err)
			arch.logger.Println(eArch)
		}
	}()

	cmd := exec.Command("mysqldump", database)
	cmd.Stdout = gzw
	cmd.Stderr = os.Stderr

	// Delete orphaned files created by a failed dump.
	if err := cmd.Run(); err != nil {
		rmErr := os.Remove(f.Name())
		runErr := fmt.Errorf("mysql dump failed on database %q: %w", database, err)
		return errors.Join(runErr, rmErr)
	}

	return nil
}

// Rotate purges old backups in a given database subdirectory.
func (arch *Archiver) Rotate(database string) error {
	entries, err := os.ReadDir(filepath.Join(arch.BackupPath, database))
	if err != nil {
		return fmt.Errorf("failed to list directory %q: %w", database, err)
	}

	arch.logger.Println("Directory listing:")
	arch.logger.Println()

	var fis []os.FileInfo
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		fis = append(fis, info)
	}

	now := time.Now()
	fiBuf := arch.parseFileInfoList(fis, now)
	a := arch.sortAsArchives(fiBuf, now)

	arch.logger.Println()
	arch.logger.Println("Backups staged for removal:")
	arch.logger.Println()

	var removeErrors []error
	for _, fi := range a.purge {
		arch.logger.Printf("- %s\n", fi.Name())
		if err := os.Remove(filepath.Join(arch.BackupPath, database, fi.Name())); err != nil {
			removeErrors = append(removeErrors, err)
			arch.logger.Printf("  - failed (%v)\n", err)
		}
	}

	if len(a.purge) == 0 {
		arch.logger.Println("- No files to remove")
	}

	if len(removeErrors) > 0 {
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

func (fil FinfoList) Len() int           { return len(fil) }
func (fil FinfoList) Less(i, j int) bool { return fil[i].created > fil[j].created }
func (fil FinfoList) Swap(i, j int)      { fil[i], fil[j] = fil[j], fil[i] }

func (arch *Archiver) parseFileInfoList(fis []os.FileInfo, now time.Time) []*Finfo {
	fiBuf := make([]*Finfo, 0, len(fis))

	for _, finfo := range fis {
		name := finfo.Name()

		if !strings.HasSuffix(name, ".sql.gz") {
			continue
		}

		timeStr := strings.TrimSuffix(name, ".sql.gz")
		ftime, err := time.Parse(arch.TimeFormat, timeStr)
		if err != nil {
			arch.logger.Printf("failed to parse time of backup from filename %q: %v\n", name, err)
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

	tapes := make([][]*Finfo, quota)
	for i, d := range timeSeries {
		for j, f := range files {
			if f == nil {
				continue
			}

			if float64(nowUnix-f.created) < d.Seconds() {
				tapes[i] = append(tapes[i], f)
				files[j] = nil
			}
		}
	}

	// Unclaimed files older than max timeframe are purged
	for _, f := range files {
		if f != nil {
			a.purge = append(a.purge, f)
		}
	}

	// Purge collisions within each timeframe tape and retain only the first snapshot
	for i, t := range tapes {
		if len(t) > 1 {
			a.purge = append(a.purge, t[1:]...)
			tapes[i] = t[:1]
		}
	}

	a.tapes = tapes
	return a
}
