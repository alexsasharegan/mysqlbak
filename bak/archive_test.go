package bak

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

const timeFormat = "2006-01-02_15-04-05_MST"

var (
	files []os.FileInfo
	// 2000/01/01 00:00:00 UTC
	start = time.Date(
		2000, time.January, 1, 0, 0, 0, 0, time.UTC,
	)
)

func init() {
	// Create a list of mock files. They should emulate a backup routine
	// that runs at 2x the frequency of the rotation interval.
	step := rotationInterval / 2
	var d time.Duration
	for ; d < 100; d++ {
		shift := step * d
		files = append(files, fileInfoMock{
			time: start.Add(-shift),
			mode: 0666,
		})
	}
}

func newArchiver() Archiver {
	return Archiver{
		TimeFormat: timeFormat,
		logger:     log.New(ioutil.Discard, "", 0),
	}
}

func TestParseFileInfoList(t *testing.T) {
	arch := newArchiver()

	reverseFiles := make([]os.FileInfo, 0, len(files))
	for i := len(files) - 1; i >= 0; i-- {
		reverseFiles = append(reverseFiles, files[i])
	}

	n := start.Unix() + 1
	for i, fi := range arch.parseFileInfoList(reverseFiles, start) {
		if fi.created != files[i].ModTime().Unix() {
			t.Errorf("the parsed time does not match the source file")
		}
		if fi.created > n {
			t.Errorf(
				"the file is not chronological order: (%d > %d) %s",
				fi.created, n, fi.time.Format(timeFormat))
		}
		n = fi.created
	}
}

func TestArchivalSort(t *testing.T) {
	arch := newArchiver()
	fis := arch.parseFileInfoList(files, start)
	a := arch.sortAsArchives(fis, start.Add(time.Second))

	var count int
	for _, list := range a.tapes {
		for range list {
			count++
		}
	}

	if count > quota {
		t.Errorf("quota exceeded: %d", count)
	}

	// ioutil.WriteFile()
}

type fileInfoMock struct {
	time time.Time
	size int64
	mode os.FileMode
}

func (fi fileInfoMock) Name() string {
	return fmt.Sprintf("%s.sql.gz", fi.time.Format(timeFormat))
}

func (fi fileInfoMock) Size() int64 {
	return fi.size
}

func (fi fileInfoMock) Mode() os.FileMode {
	return 0664
}

func (fi fileInfoMock) ModTime() time.Time {
	return fi.time
}

func (fi fileInfoMock) IsDir() bool {
	return false
}

func (fi fileInfoMock) Sys() interface{} {
	return nil
}
