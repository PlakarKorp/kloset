package snapshot_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

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

// failingImporter emits a batch of records, closes the channel as the
// contract requires, then fails. The workers are still chewing on the
// records when Import returns, so the backup has to wait for them before
// reporting the scan counters.
type failingImporter struct{ *ptesting.MockImporter }

func (failingImporter) Import(ctx context.Context, records chan<- *connectors.Record, results <-chan *connectors.Result) (map[string]string, error) {
	defer close(records)

	for i := range 256 {
		f := ptesting.NewMockFile(fmt.Sprintf("f%d", i), 0644, "hello world!\n")
		select {
		case records <- f.ScanResult():
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, errors.New("importer gave up")
}

func TestBackupImporterError(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	builder, err := snapshot.Create(repo, repository.DefaultType, "", objects.NilMac,
		&snapshot.BuilderOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { builder.Close() })

	base, err := ptesting.NewMockImporter(repo.AppContext(), &connectors.Options{},
		"mock", map[string]string{"location": "mock://place"})
	require.NoError(t, err)

	src, err := snapshot.NewSource(repo.AppContext(),
		failingImporter{base.(*ptesting.MockImporter)})
	require.NoError(t, err)

	err = builder.Backup(src)
	require.ErrorContains(t, err, "importer gave up")
}
