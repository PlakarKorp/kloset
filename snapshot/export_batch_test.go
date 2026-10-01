package snapshot_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

func batchTree() map[string][]byte {
	rnd := rand.New(rand.NewSource(1))
	large := make([]byte, 3<<20)
	rnd.Read(large)

	files := map[string][]byte{
		"/empty":      {},
		"/dup/a":      []byte("same content"),
		"/dup/b":      []byte("same content"),
		"/large/blob": large,
	}
	for d := range 3 {
		for f := range 20 {
			files[fmt.Sprintf("/dir%d/file%02d", d, f)] = fmt.Appendf(nil, "file %d in dir %d\n", f, d)
		}
	}
	return files
}

func generateBatchSnapshot(t *testing.T, files map[string][]byte) (*repository.Repository, *snapshot.Snapshot) {
	t.Helper()
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	dirs := map[string]bool{"/": true}
	for p := range files {
		for d := path.Dir(p); !dirs[d]; d = path.Dir(d) {
			dirs[d] = true
		}
	}

	return repo, ptesting.GenerateSnapshot(t, repo, nil, ptesting.WithGenerator(func(ch chan<- *connectors.Record) {
		for d := range dirs {
			ch <- connectors.NewRecord(d, "", objects.FileInfo{Lname: path.Base(d), Lmode: os.ModeDir | 0755}, nil, nil)
		}
		for p, content := range files {
			ch <- connectors.NewRecord(p, "", objects.FileInfo{
				Lname: path.Base(p),
				Lmode: 0644,
				Lsize: int64(len(content)),
			}, nil, func() (io.ReadCloser, error) {
				return io.NopCloser(bytes.NewReader(content)), nil
			})
		}
	}))
}

func exportTo(t *testing.T, snap *snapshot.Snapshot, pathname string) (map[string][]byte, error) {
	t.Helper()
	exp, err := ptesting.NewMockExporter(context.Background(), nil, "mock", map[string]string{"location": "mock://" + t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = exp.Close(context.Background()) })

	err = snap.Export(exp, pathname, &snapshot.ExportOptions{Strip: pathname})
	return exp.(*ptesting.MockExporter).Files(), err
}

func TestExportBatched(t *testing.T) {
	files := batchTree()
	_, snap := generateBatchSnapshot(t, files)
	defer func() { _ = snap.Close() }()

	for _, tc := range []struct {
		name                string
		batchFiles, smallFB int
	}{
		{"small batches, large file streamed", 7, 1 << 20},
		{"small batches, large file in batch", 7, 8 << 20},
		{"every file streamed", 7, 0},
		{"one batch", 1024, 1 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot.SetExportBatchLimits(t, tc.batchFiles, 32<<20, tc.smallFB)

			got, err := exportTo(t, snap, "/")
			require.NoError(t, err)
			require.Len(t, got, len(files))
			for p, want := range files {
				require.Equal(t, want, got[p], p)
			}
		})
	}
}

func TestExportBatchedSubtree(t *testing.T) {
	files := batchTree()
	_, snap := generateBatchSnapshot(t, files)
	defer func() { _ = snap.Close() }()
	snapshot.SetExportBatchLimits(t, 7, 32<<20, 1<<20)

	got, err := exportTo(t, snap, "/dir1")
	require.NoError(t, err)
	require.Len(t, got, 20)
	for f := range 20 {
		require.Equal(t, files[fmt.Sprintf("/dir1/file%02d", f)], got[fmt.Sprintf("/file%02d", f)])
	}
}

func TestExportBatchFetchErrors(t *testing.T) {
	_, snap := generateBatchSnapshot(t, map[string][]byte{"/a": []byte("content of a")})
	defer func() { _ = snap.Close() }()

	pvfs, err := snap.Filesystem()
	require.NoError(t, err)
	a, err := pvfs.GetEntry("/a")
	require.NoError(t, err)

	missing := *a
	missing.ResolvedObject = nil
	missing.Object = objects.MAC{1, 2, 3}

	data, errs := snapshot.FetchExportFiles(snap, []*vfs.Entry{a, &missing})
	require.NoError(t, errs[0])
	require.Equal(t, "content of a", string(data[0]))
	require.ErrorIs(t, errs[1], repository.ErrBlobNotFound)
}

func TestExportBatchedCancelled(t *testing.T) {
	files := batchTree()
	_, snap := generateBatchSnapshot(t, files)
	defer func() { _ = snap.Close() }()
	snapshot.SetExportBatchLimits(t, 7, 32<<20, 1<<20)

	snap.AppContext().Cancel(errors.New("stop"))

	done := make(chan map[string][]byte, 1)
	go func() {
		got, _ := exportTo(t, snap, "/")
		done <- got
	}()
	select {
	case got := <-done:
		for p, content := range got {
			require.Equal(t, files[p], content, p)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("export did not return after cancellation")
	}
}
