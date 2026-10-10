package bak

import (
	"fmt"
	"io"
	"log"
	"os"
	"testing"
	"time"
)

const timeFormat = "2006-01-02_15-04-05_MST"

var (
	files []os.FileInfo
	start = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
)

func init() {
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
		logger:     log.New(io.Discard, "", 0),
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
			t.Errorf("the file is not chronological order: (%d > %d) %s",
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
}

func TestParseFileInfoList_DotsInFilename(t *testing.T) {
	dotTimeFormat := "2006.01.02_15-04-05_MST"
	arch := Archiver{
		TimeFormat: dotTimeFormat,
		logger:     log.New(io.Discard, "", 0),
	}
	now := time.Now()

	dotFilename := fmt.Sprintf("%s.sql.gz", now.Format(dotTimeFormat))
	mockFiles := []fileInfoMock{{time: now, mode: 0666}}

	customMock := customNameMock{
		fileInfoMock: mockFiles[0],
		customName:   dotFilename,
	}

	parsed := arch.parseFileInfoList([]os.FileInfo{customMock}, now)

	if len(parsed) == 0 {
		t.Errorf("parseFileInfoList failed to parse filename with dots in TimeFormat: %q", dotFilename)
	}
}

func TestSortAsArchives_DuplicateCascade(t *testing.T) {
	arch := newArchiver()
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	f1 := &Finfo{time: now.Add(-1 * time.Minute), created: now.Add(-1 * time.Minute).Unix()}
	f2 := &Finfo{time: now.Add(-2 * time.Minute), created: now.Add(-2 * time.Minute).Unix()}
	f3 := &Finfo{time: now.Add(-3 * time.Minute), created: now.Add(-3 * time.Minute).Unix()}

	files := FinfoList{f1, f2, f3}
	res := arch.sortAsArchives(files, now)

	if len(res.purge) != 2 {
		t.Errorf("Expected 2 excess backups in purge list, got %d", len(res.purge))
	}
}

type customNameMock struct {
	fileInfoMock
	customName string
}

func (m customNameMock) Name() string {
	return m.customName
}

type fileInfoMock struct {
	time time.Time
	size int64
	mode os.FileMode
}

func (fi fileInfoMock) Name() string {
	return fmt.Sprintf("%s.sql.gz", fi.time.Format(timeFormat))
}

func (fi fileInfoMock) Size() int64        { return fi.size }
func (fi fileInfoMock) Mode() os.FileMode  { return 0664 }
func (fi fileInfoMock) ModTime() time.Time { return fi.time }
func (fi fileInfoMock) IsDir() bool        { return false }
func (fi fileInfoMock) Sys() interface{}   { return nil }
