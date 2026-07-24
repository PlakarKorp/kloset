package vfs_test

import (
	"context"
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

// warmTree emits a multi-directory tree (10 dirs of 3 files each) so a
// PrefetchDirs batch spans several dirpack directories.
const (
	warmTreeDirs  = 10
	warmTreeFiles = 3
)

func warmTree(ch chan<- *connectors.Record) {
	ch <- dirRec("/")
	for d := range warmTreeDirs {
		dir := fmt.Sprintf("/dir%02d", d)
		ch <- dirRec(dir)
		for f := range warmTreeFiles {
			p := fmt.Sprintf("%s/file%02d.txt", dir, f)
			ch <- fileRec(p, fmt.Sprintf("content of %s", p))
		}
	}
}

func warmTreeFilePaths() []string {
	files := make([]string, 0, warmTreeDirs*warmTreeFiles)
	for d := range warmTreeDirs {
		for f := range warmTreeFiles {
			files = append(files, fmt.Sprintf("/dir%02d/file%02d.txt", d, f))
		}
	}
	return files
}

// warmTreeParentDirs is what the backup's warm stage would extract from a
// batch covering the whole tree: every distinct parent directory.
func warmTreeParentDirs() []string {
	dirs := []string{"/"}
	for d := range warmTreeDirs {
		dirs = append(dirs, fmt.Sprintf("/dir%02d", d))
	}
	return dirs
}

// freshCacheFS loads the snapshot anew and returns a cache-backed filesystem,
// so each caller gets an independent (cold) dirpack cache.
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

// TestPrefetchDirsSameResultAsCold is the core safety net: entries resolved
// through a PrefetchDirs-warmed cache must be identical to entries resolved
// on-demand on a cold cache. This guards the whole warm pipeline — Find,
// the GetBlobs rounds, chunk assembly, and decodeDirpackMap — against the
// on-demand loadDirpackMap path.
func TestPrefetchDirsSameResultAsCold(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	base := ptesting.GenerateSnapshot(t, repo, nil, ptesting.WithGenerator(warmTree))
	defer base.Close()
	id := base.Header.Identifier
	files := warmTreeFilePaths()

	coldFS := freshCacheFS(t, repo, id)
	cold := walkForBackup(t, coldFS, files)

	warmFS := freshCacheFS(t, repo, id)
	require.NoError(t, warmFS.PrefetchDirs(context.Background(), warmTreeParentDirs()))
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

// TestPrefetchDirsActuallyWarms proves warming is not a silent no-op, with no
// reach into internals: warm the cache, then destroy the backend entirely —
// every lookup must still resolve from the warmed cache alone. A regression
// that quietly stops warming (say, an inverted error check) fails this
// immediately.
func TestPrefetchDirsActuallyWarms(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	base := ptesting.GenerateSnapshot(t, repo, nil, ptesting.WithGenerator(warmTree))
	defer base.Close()

	fs := freshCacheFS(t, repo, base.Header.Identifier)
	require.NoError(t, fs.PrefetchDirs(context.Background(), warmTreeParentDirs()))

	for mac := range repo.ListPackfiles() {
		require.NoError(t, repo.DeletePackfile(mac))
	}

	got := walkForBackup(t, fs, warmTreeFilePaths())
	require.Len(t, got, warmTreeDirs*warmTreeFiles)
}

// TestPrefetchDirsUnknownDirsAreSoft: directories that do not exist in the
// snapshot (new dirs, from the backup's perspective) are silently skipped,
// and known dirs in the same batch still warm.
func TestPrefetchDirsUnknownDirsAreSoft(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	base := ptesting.GenerateSnapshot(t, repo, nil, ptesting.WithGenerator(warmTree))
	defer base.Close()

	fs := freshCacheFS(t, repo, base.Header.Identifier)

	dirs := append(warmTreeParentDirs(), "/does/not/exist", "/neither/does/this")
	require.NoError(t, fs.PrefetchDirs(context.Background(), dirs))

	// The known dirs must have warmed regardless: destroy the backend and walk.
	for mac := range repo.ListPackfiles() {
		require.NoError(t, repo.DeletePackfile(mac))
	}
	walkForBackup(t, fs, warmTreeFilePaths())
}

// TestPrefetchDirsBackendGoneIsSoft: if the store reads fail, PrefetchDirs
// must not error or panic — dirs stay cold and the (walk-side) on-demand
// load is the one that reports the problem.
func TestPrefetchDirsBackendGoneIsSoft(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	base := ptesting.GenerateSnapshot(t, repo, nil, ptesting.WithGenerator(warmTree))
	defer base.Close()

	fs := freshCacheFS(t, repo, base.Header.Identifier)

	for mac := range repo.ListPackfiles() {
		require.NoError(t, repo.DeletePackfile(mac))
	}

	require.NotPanics(t, func() {
		require.NoError(t, fs.PrefetchDirs(context.Background(), warmTreeParentDirs()))
	})

	// Cold dir + dead backend: the on-demand path is the one that errors.
	_, err := fs.GetEntryForBackup(warmTreeFilePaths()[0])
	require.Error(t, err)
}

// TestPrefetchDirsAlreadyWarm: warming the same dirs twice is a cheap no-op
// (the already-cached skip) and never disturbs previously warmed entries.
func TestPrefetchDirsAlreadyWarm(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	base := ptesting.GenerateSnapshot(t, repo, nil, ptesting.WithGenerator(warmTree))
	defer base.Close()

	fs := freshCacheFS(t, repo, base.Header.Identifier)
	dirs := warmTreeParentDirs()
	require.NoError(t, fs.PrefetchDirs(context.Background(), dirs))

	// Second warm runs against a dead backend: it must not need it (all
	// dirs cached) and must not damage the cache.
	for mac := range repo.ListPackfiles() {
		require.NoError(t, repo.DeletePackfile(mac))
	}
	require.NoError(t, fs.PrefetchDirs(context.Background(), dirs))

	walkForBackup(t, fs, warmTreeFilePaths())
}

// TestPrefetchDirsNoCacheNoop: inert on a filesystem built without a dirpack
// cache (the non-backup Filesystem() path) — no panic, lookups unaffected.
func TestPrefetchDirsNoCacheNoop(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	base := ptesting.GenerateSnapshot(t, repo, nil, ptesting.WithGenerator(warmTree))
	defer base.Close()

	fs, err := base.Filesystem()
	require.NoError(t, err)

	require.NotPanics(t, func() {
		require.NoError(t, fs.PrefetchDirs(context.Background(), warmTreeParentDirs()))
	})

	for _, p := range warmTreeFilePaths() {
		e, err := fs.GetEntryForBackup(p)
		require.NoError(t, err, p)
		require.Equal(t, path.Base(p), e.FileInfo.Lname, p)
	}
}
