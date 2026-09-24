package snapshot_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot"
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

func TestBackupWithExcludes(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	files := []ptesting.MockFile{
		ptesting.NewMockFile("hello0", 0644, "hello world!\n"),
		ptesting.NewMockFile("hello1", 0644, "hello world!\n"),
		ptesting.NewMockFile("hello2", 0644, "hello world!\n"),
		ptesting.NewMockFile("hello3", 0644, "hello world!\n"),
		ptesting.NewMockFile("hello4", 0644, "hello world!\n"),
		ptesting.NewMockFile("hello5", 0644, "hello world!\n"),
		ptesting.NewMockFile("hello6", 0644, "hello world!\n"),
		ptesting.NewMockFile("hello7", 0644, "hello world!\n"),
		ptesting.NewMockFile("hello8", 0644, "hello world!\n"),
		ptesting.NewMockFile("hello9", 0644, "hello world!\n"),
	}

	snap := ptesting.GenerateSnapshot(t, repo, files, ptesting.WithExcludes([]string{
		"/hello0", "/hello2", "/hello4", "/hello8",
	}))

	summary := &snap.Header.GetSource(0).Summary
	require.Equal(t, uint64(6), summary.Directory.Files)
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

// rudeImporter fails without closing the records channel, leaving records
// buffered behind it. Closing records is only a convention that in-tree
// importers happen to follow, so the backup has to cancel its workers
// rather than wait on a close that may never come.
type rudeImporter struct{ *ptesting.MockImporter }

func (rudeImporter) Import(ctx context.Context, records chan<- *connectors.Record, results <-chan *connectors.Result) error {
	// Leave more records queued than the workers can retire before we
	// return, so anything waiting on a close of records stays parked.
	for i := range 256 {
		f := ptesting.NewMockFile(fmt.Sprintf("f%d", i), 0644, "hello world!\n")
		select {
		case records <- f.ScanResult():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errors.New("importer gave up")
}

func TestBackupImporterErrorWithoutClose(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	builder, err := snapshot.Create(repo, repository.DefaultType, "", objects.NilMac,
		&snapshot.BuilderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { builder.Close() })

	base, err := ptesting.NewMockImporter(repo.AppContext(), &connectors.Options{},
		"mock", map[string]string{"location": "mock://place"})
	require.NoError(t, err)

	src, err := snapshot.NewSource(repo.AppContext(),
		rudeImporter{base.(*ptesting.MockImporter)})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- builder.Backup(src) }()

	select {
	case err := <-done:
		require.ErrorContains(t, err, "importer gave up")
	case <-time.After(30 * time.Second):
		t.Fatal("Backup hung after the importer failed without closing records")
	}
}
