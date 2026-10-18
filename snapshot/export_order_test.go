package snapshot_test

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// orderExporter records the pathnames in the order Export hands them over.
type orderExporter struct {
	flags location.Flags
	paths []string
}

func (e *orderExporter) Origin() string        { return "order" }
func (e *orderExporter) Type() string          { return "order" }
func (e *orderExporter) Root() string          { return "/" }
func (e *orderExporter) Flags() location.Flags { return e.flags }

func (e *orderExporter) Ping(ctx context.Context) error  { return nil }
func (e *orderExporter) Close(ctx context.Context) error { return nil }

func (e *orderExporter) Export(ctx context.Context, records <-chan *connectors.Record, results chan<- *connectors.Result) error {
	defer close(results)

	for record := range records {
		e.paths = append(e.paths, record.Pathname)
		results <- record.Ok()
	}
	return nil
}

// generateSnapshotTwoDirs builds a tree where a depth-first walk and a
// per-directory walk disagree: both directories must be listed before either
// of their children for the latter.
func generateSnapshotTwoDirs(t *testing.T) *snapshot.Snapshot {
	t.Helper()
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	dir := func(pathname string) *connectors.Record {
		name := pathname[strings.LastIndex(pathname, "/")+1:]
		return connectors.NewRecord(pathname, "", objects.FileInfo{
			Lname: name,
			Lmode: os.ModeDir | 0755,
		}, nil, nil)
	}
	file := func(pathname, content string) *connectors.Record {
		name := pathname[strings.LastIndex(pathname, "/")+1:]
		return connectors.NewRecord(pathname, "", objects.FileInfo{
			Lname: name,
			Lmode: 0644,
			Lsize: int64(len(content)),
		}, nil, func() (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(content)), nil
		})
	}

	return ptesting.GenerateSnapshot(t, repo, nil,
		ptesting.WithGenerator(func(ch chan<- *connectors.Record) {
			ch <- connectors.NewRecord("/", "", objects.FileInfo{
				Lname: "/",
				Lmode: os.ModeDir | 0755,
			}, nil, nil)
			ch <- dir("/a")
			ch <- file("/a/x", "x")
			ch <- dir("/b")
			ch <- file("/b/y", "y")
		}),
	)
}

func TestExportWalkDirRecordsOrder(t *testing.T) {
	snap := generateSnapshotTwoDirs(t)
	defer snap.Close()

	exp := &orderExporter{flags: location.FLAG_WALKDIRORDER}
	require.NoError(t, snap.Export(exp, "/", &snapshot.ExportOptions{}))
	require.Equal(t, []string{"/", "/a", "/a/x", "/b", "/b/y"}, exp.paths)
}

// Without the flag the dirpack walk lists a directory at a time. Asserting it
// keeps the test above honest: should both walks ever agree on this tree, it
// no longer proves the flag picks the depth-first one.
func TestExportDefaultRecordsOrder(t *testing.T) {
	snap := generateSnapshotTwoDirs(t)
	defer snap.Close()

	exp := &orderExporter{}
	require.NoError(t, snap.Export(exp, "/", &snapshot.ExportOptions{}))
	require.Equal(t, []string{"/", "/a", "/b", "/a/x", "/b/y"}, exp.paths)
}
