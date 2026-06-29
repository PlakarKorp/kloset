package vfs_test

import (
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// dirRec / fileRec build importer records for a directory / regular file.
func dirRec(p string) *connectors.Record {
	return connectors.NewRecord(p, "", objects.FileInfo{
		Lname: path.Base(p), Lmode: os.ModeDir | 0755,
	}, nil, nil)
}

func fileRec(p, content string) *connectors.Record {
	return connectors.NewRecord(p, "", objects.FileInfo{
		Lname: path.Base(p), Lmode: 0644, Lsize: int64(len(content)),
	}, nil, func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(content)), nil
	})
}

// prefetchTree emits a multi-directory tree (10 dirs of 3 files each) so the
// dirpack spans more directories than a small prefetch window, exercising the
// feeder's consume-driven advance rather than just the initial priming.
const (
	prefetchTreeDirs      = 10
	prefetchTreeFiles     = 3
	prefetchTreeFileSlots = prefetchTreeDirs * prefetchTreeFiles
)

func prefetchTree(ch chan<- *connectors.Record) {
	ch <- dirRec("/")
	for d := 0; d < prefetchTreeDirs; d++ {
		dir := fmt.Sprintf("/dir%02d", d)
		ch <- dirRec(dir)
		for f := 0; f < prefetchTreeFiles; f++ {
			p := fmt.Sprintf("%s/file%02d.txt", dir, f)
			ch <- fileRec(p, fmt.Sprintf("content of %s", p))
		}
	}
}

func prefetchTreeFilePaths() []string {
	files := make([]string, 0, prefetchTreeFileSlots)
	for d := 0; d < prefetchTreeDirs; d++ {
		for f := 0; f < prefetchTreeFiles; f++ {
			files = append(files, fmt.Sprintf("/dir%02d/file%02d.txt", d, f))
		}
	}
	return files
}

// freshCacheFS loads the snapshot anew and returns a cache-backed filesystem,
// so each caller gets an independent (cold) dirpack cache and prefetcher slot.
func freshCacheFS(t *testing.T, repo *repository.Repository, id objects.MAC) *vfs.Filesystem {
	t.Helper()
	snap, err := snapshot.Load(repo, id)
	require.NoError(t, err)
	t.Cleanup(func() { snap.Close() })
	fs, err := snap.FilesystemWithCache()
	require.NoError(t, err)
	return fs
}

func walkForBackup(t *testing.T, fs *vfs.Filesystem, files []string) map[string]*vfs.Entry {
	t.Helper()
	out := make(map[string]*vfs.Entry, len(files))
	for _, p := range files {
		e, err := fs.GetEntryForBackup(p)
		require.NoError(t, err, p)
		out[p] = e
	}
	return out
}

// TestDirpackPrefetchSameResultAsCold is the core safety net: resolving every
// entry through GetEntryForBackup with the prefetcher running must produce the
// exact same entries as resolving them on a cold cache with no prefetcher. This
// guards the loadDirpackMap/loadDirpackMapByMAC split, the restored pre-
// singleflight cache fast-path, and the prefetch/on-demand singleflight
// coalescing.
func TestDirpackPrefetchSameResultAsCold(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	base := ptesting.GenerateSnapshot(t, repo, nil, ptesting.WithGenerator(prefetchTree))
	defer base.Close()
	id := base.Header.Identifier
	files := prefetchTreeFilePaths()

	coldFS := freshCacheFS(t, repo, id)
	cold := walkForBackup(t, coldFS, files)

	warmFS := freshCacheFS(t, repo, id)
	warmFS.StartDirpackPrefetch(8, 4)
	defer warmFS.StopDirpackPrefetch()
	warm := walkForBackup(t, warmFS, files)

	for _, p := range files {
		c, w := cold[p], warm[p]
		require.Equal(t, c.MAC, w.MAC, p)
		require.Equal(t, c.Object, w.Object, p)
		require.Equal(t, c.FileInfo, w.FileInfo, p)
		require.Equal(t, c.GetContentType(), w.GetContentType(), p)
		require.Equal(t, c.GetChunks(), w.GetChunks(), p)
		require.Equal(t, c.GetEntropy(), w.GetEntropy(), p)
	}
}

// TestDirpackPrefetchLifecycle exercises the start/stop contract: stopping with
// nothing running, starting twice (second is a no-op), and restarting after stop.
func TestDirpackPrefetchLifecycle(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	base := ptesting.GenerateSnapshot(t, repo, nil, ptesting.WithGenerator(prefetchTree))
	defer base.Close()
	files := prefetchTreeFilePaths()

	fs := freshCacheFS(t, repo, base.Header.Identifier)

	// Stop without start must be a safe no-op.
	require.NotPanics(t, func() { fs.StopDirpackPrefetch() })

	// Double start: the second call must be a no-op, not leak a prefetcher.
	fs.StartDirpackPrefetch(8, 4)
	fs.StartDirpackPrefetch(8, 4)
	walkForBackup(t, fs, files)
	fs.StopDirpackPrefetch()

	// Restart on the same filesystem must work and still resolve entries.
	fs2 := freshCacheFS(t, repo, base.Header.Identifier)
	fs2.StartDirpackPrefetch(4, 2)
	fs2.StopDirpackPrefetch()
	fs2.StartDirpackPrefetch(4, 2)
	defer fs2.StopDirpackPrefetch()
	got := walkForBackup(t, fs2, files)
	require.Len(t, got, len(files))
}

// TestDirpackPrefetchNoCacheNoop verifies the prefetcher is inert on a
// filesystem built without a dirpack cache (the non-backup NewFilesystem path):
// Start is a no-op and GetEntryForBackup still resolves via the fallback path.
func TestDirpackPrefetchNoCacheNoop(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	base := ptesting.GenerateSnapshot(t, repo, nil, ptesting.WithGenerator(prefetchTree))
	defer base.Close()

	// Snapshot.Filesystem() uses NewFilesystem (no dirpack cache).
	fs, err := base.Filesystem()
	require.NoError(t, err)

	require.NotPanics(t, func() { fs.StartDirpackPrefetch(8, 4) })

	for _, p := range prefetchTreeFilePaths() {
		e, err := fs.GetEntryForBackup(p)
		require.NoError(t, err, p)
		require.Equal(t, path.Base(p), e.FileInfo.Lname, p)
	}
}
