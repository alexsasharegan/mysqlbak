package bak

import (
	"compress/gzip"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
)

// Archiver performs, stores and rotates mysql backups.
type Archiver struct {
	TimeFormat string
	BackupPath string
}

// Archive performs a backup on the given database.
func (a *Archiver) Archive(database string) (eArch error) {
	now := time.Now().Format(a.TimeFormat)

	f, err := os.OpenFile(
		filepath.Join(a.BackupPath, database, fmt.Sprintf("%s.sql.gz", now)),
		os.O_CREATE|os.O_WRONLY,
		0666,
	)
	if err != nil {
		return errors.Wrapf(err, "failed to open db archive for writing for %q", database)
	}

	defer func() {
		err := f.Close()
		if err != nil {
			eArch = errors.Wrap(err, "failed closing the file archive")
		}
	}()

	gzw := gzip.NewWriter(f)
	defer func() {
		err := gzw.Close()
		if err != nil {
			eArch = errors.Wrap(err, "failed closing the gzip archiver")
		}
	}()

	cmd := exec.Command("mysqldump", database)
	cmd.Stdout = gzw
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		os.Remove(f.Name())
		return errors.Wrapf(err, "mysql dump failed on database %q", database)
	}

	return
}

// // Finfo embeds os.FileInfo alongside an easily compared created time (int64)
// type Finfo struct {
// 	os.FileInfo
// 	created int64
// }

// // FinfoList implements sort.Interface
// type FinfoList struct {
// 	list []Finfo
// }

// func (fil FinfoList) Len() int {
// 	return len(fil.list)
// }

// func (fil FinfoList) Less(i, j int) bool {
// 	return fil.list[i].created < fil.list[j].created
// }

// func (fil FinfoList) Swap(i, j int) {
// 	fil.list[i], fil.list[j] = fil.list[j], fil.list[i]
// }

// func archiveDB(database string) {
// 	now := time.Now().Format(program.timeFormat)
// 	f, err := os.OpenFile(
// 		filepath.Join(program.backupPath, "db", fmt.Sprintf("%s.sql.gz", now)),
// 		os.O_CREATE|os.O_WRONLY,
// 		0666,
// 	)
// 	if err != nil {
// 		logger.Fatalln(errors.Wrap(err, "failed to open db archive for writing"))
// 	}
// 	defer f.Close()

// 	gzw := gzip.NewWriter(f)
// 	defer gzw.Close()

// 	cmd := exec.Command("mysqldump", database)
// 	cmd.Stdout = gzw
// 	cmd.Stderr = os.Stderr

// 	if err := cmd.Run(); err != nil {
// 		logger.Fatalln(errors.Wrap(err, "mysql dump failed"))
// 	}

// 	rotateArchives("db")
// }

// func rotateArchives(subdir string) {
// 	fis, err := ioutil.ReadDir(filepath.Join(program.backupPath, subdir))
// 	if err != nil {
// 		logger.Fatalln(errors.Wrap(err, "failed to list directory"))
// 	}

// 	var (
// 		today, thisWeek, thisMonth, lastMonth, old FinfoList
// 	)

// 	now := time.Now()
// 	y, m, d := now.Date()
// 	endOfYesterday := time.Date(y, m, d, 0, 0, 0, 0, now.Location())
// 	// 0 indexed
// 	endOfLastWeek := endOfYesterday.AddDate(0, 0, -int(endOfYesterday.Weekday()))
// 	// 1 indexed
// 	endOfLastMonth := endOfYesterday.AddDate(0, 0, 1-int(endOfYesterday.Day()))
// 	EndOfMonthPrior := endOfLastMonth.AddDate(0, -1, 0)

// 	for _, fi := range fis {
// 		name := fi.Name()
// 		ftime, err := time.Parse(program.timeFormat, name[:strings.IndexRune(name, '.')])
// 		if err != nil {
// 			logger.Println(
// 				errors.Wrapf(
// 					err,
// 					"failed to parse time of backup from filename %q",
// 					fi.Name(),
// 				),
// 			)
// 			continue
// 		}

// 		switch {
// 		case ftime.After(endOfYesterday):
// 			today.list = append(today.list, Finfo{fi, ftime.Unix()})
// 		case ftime.After(endOfLastWeek):
// 			thisWeek.list = append(thisWeek.list, Finfo{fi, ftime.Unix()})
// 		case ftime.After(endOfLastMonth):
// 			thisMonth.list = append(thisMonth.list, Finfo{fi, ftime.Unix()})
// 		case ftime.After(EndOfMonthPrior):
// 			lastMonth.list = append(lastMonth.list, Finfo{fi, ftime.Unix()})
// 		default:
// 			old.list = append(old.list, Finfo{fi, ftime.Unix()})
// 		}
// 	}

// 	var del []Finfo
// 	collection := []struct {
// 		max  int
// 		data FinfoList
// 	}{
// 		{3, today},
// 		{3, thisWeek},
// 		{1, thisMonth},
// 		{1, lastMonth},
// 		{1, old},
// 	}
// 	for _, col := range collection {
// 		if col.data.Len() > col.max {
// 			// Use a reverse sort to keep the newest files at the beginning
// 			sort.Sort(sort.Reverse(col.data))
// 			del = append(del, col.data.list[col.max:]...)
// 		}
// 	}

// 	for _, fi := range del {
// 		err := os.Remove(filepath.Join(program.backupPath, subdir, fi.Name()))
// 		if err != nil {
// 			logger.Println(errors.Wrapf(err, "failed to remove old backup file %q", fi.Name()))
// 		}
// 	}
// }
