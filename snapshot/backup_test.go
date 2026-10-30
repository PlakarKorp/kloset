package snapshot_test

import (
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

func TestSimpleBackup(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	files := []ptesting.MockFile{
		ptesting.NewMockFile("hello.txt", 0644, "hello world!\n"),
		ptesting.NewMockFile("unreadable", 0, "wooo\n"),
	}
	snap := ptesting.GenerateSnapshot(t, repo, files)

	summary := snap.Header.GetSource(0).Summary
	require.Equal(t, summary.Directory.Errors+summary.Below.Errors, uint64(1))

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	fp, err := fs.Open("hello.txt")
	require.NoError(t, err, "can't open expected file")
	require.NotNil(t, fp)

	fp, err = fs.Open("unreadable")
	require.NotNil(t, err, "can open file unexpectedly")
	require.Nil(t, fp)
}

func errorGenerator(ch chan<- *connectors.Record) {
	ch <- &connectors.Record{
		Pathname: "/",
		FileInfo: objects.FileInfo{
			Lname:      "/",
			Lnlink:     1,
			Lmode:      os.ModeDir,
			Lusername:  "flan",
			Lgroupname: "hacker",
		},
	}

	for i := 'a'; i < 'g'; i++ {
		ch <- &connectors.Record{
			Pathname: fmt.Sprintf("/%v", i),
			FileInfo: objects.FileInfo{
				Lname:      fmt.Sprint(i),
				Lnlink:     1,
				Lmode:      os.ModeDir,
				Lusername:  "flan",
				Lgroupname: "hacker",
			},
		}
		for j := 'a'; j < 'g'; j++ {
			ch <- &connectors.Record{
				Pathname: fmt.Sprintf("/%v/%v", i, j),
				FileInfo: objects.FileInfo{
					Lname:      fmt.Sprint(j),
					Lnlink:     1,
					Lmode:      os.ModeDir,
					Lusername:  "flan",
					Lgroupname: "hacker",
				},
			}

			for k := range 10 {
				if k%2 == 0 {
					ch <- &connectors.Record{
						Pathname: fmt.Sprintf("/%v/%v/%v", i, j, k),
						FileInfo: objects.FileInfo{
							Lname:      fmt.Sprint(k),
							Lsize:      int64(len("hello world")),
							Lnlink:     1,
							Lusername:  "flan",
							Lgroupname: "hacker",
						},
						Reader: connectors.NewLazyReader(func() (io.ReadCloser, error) {
							return io.NopCloser(strings.NewReader("hello world")), nil
						}),
					}
				} else {
					ch <- &connectors.Record{
						Pathname: fmt.Sprintf("/%v/%v/%v", i, j, k),
						Err:      os.ErrPermission,
					}
				}
			}
		}
	}
}

func TestBackupManyError(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	snap := ptesting.GenerateSnapshot(t, repo, nil, ptesting.WithGenerator(errorGenerator))

	summary := snap.Header.GetSource(0).Summary
	require.Equal(t, summary.Below.Files, uint64(180))
	require.Equal(t, summary.Below.Directories, uint64(36))
	require.Equal(t, summary.Below.Errors, uint64(180))
}

func emptyGenerator(ch chan<- *connectors.Record) {
	// send no files
}

func TestBackupEmptyScan(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	snap := ptesting.GenerateSnapshot(t, repo, nil, ptesting.WithGenerator(emptyGenerator))

	summary := snap.Header.GetSource(0).Summary
	require.Equal(t, summary.Below.Directories, uint64(0))
}
