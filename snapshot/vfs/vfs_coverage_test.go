package vfs_test

import (
	"io"
	"io/fs"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/snapshot/vfs"
	"github.com/stretchr/testify/require"
)

// TestVdirentMethods opens a directory entry via the standard fs.File API and
// reads its children — this drives vdirent.IsDir / Type / Info / Name, the
// thin fs.DirEntry wrapper that's otherwise unreachable from external tests.
func TestVdirentMethods(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	dirFile, err := fsc.Open("/subdir")
	require.NoError(t, err)
	defer dirFile.Close()

	dr, ok := dirFile.(fs.ReadDirFile)
	require.True(t, ok, "directory should expose ReadDirFile")

	entries, err := dr.ReadDir(-1)
	if err != nil && err != io.EOF {
		require.NoError(t, err)
	}
	require.NotEmpty(t, entries)

	for _, e := range entries {
		// These all touch vdirent's methods.
		_ = e.Name()
		_ = e.IsDir()
		_ = e.Type()
		info, err := e.Info()
		require.NoError(t, err)
		require.NotNil(t, info)
	}
}

// TestLookupAndGetEntry exercises the unexported Filesystem.lookup via the
// public GetEntry / GetEntryNoFollow paths.
func TestLookupAndGetEntry(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	// GetEntry on a real path drives lookup + getEntryFollow.
	entry, err := fsc.GetEntry("/subdir/dummy.txt")
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.False(t, entry.IsDir())

	// GetEntryNoFollow on a real path drives lookup + getEntry without
	// the symlink-follow branch.
	entryNF, err := fsc.GetEntryNoFollow("/subdir/dummy.txt")
	require.NoError(t, err)
	require.NotNil(t, entryNF)

	// GetEntry on a missing path returns an error.
	_, err = fsc.GetEntry("/does/not/exist")
	require.Error(t, err)
}

// TestFilesBelowViaWalkDir uses WalkDir-backed iteration (which goes through
// filesbelow) to exercise the unexported helper.
func TestFilesBelowViaWalkDir(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	// Errors() iterates over error nodes — drives Errors / IterErrorNodes.
	count := 0
	for range fsc.Errors("/") {
		count++
	}

	// Walking "/" via WalkDir triggers filesbelow indirectly.
	require.NoError(t, fsc.WalkDir("/", func(path string, entry *vfs.Entry, err error) error {
		return nil
	}))
}

// TestStatMixed drives Filesystem.Stat which wraps GetEntry on both a file
// and a directory, plus the not-found error path.
func TestStatMixed(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	info, err := fsc.Stat("/subdir/dummy.txt")
	require.NoError(t, err)
	require.NotNil(t, info)
	require.False(t, info.IsDir())

	infoDir, err := fsc.Stat("/subdir")
	require.NoError(t, err)
	require.True(t, infoDir.IsDir())

	_, err = fsc.Stat("/missing")
	require.Error(t, err)
}

// TestGetChunksOnDirAndFile exercises Entry.GetChunks: zero on directories,
// non-zero on regular files (after the resolver hydrates ResolvedObject).
func TestGetChunksOnDirAndFile(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	dir, err := fsc.GetEntry("/subdir")
	require.NoError(t, err)
	require.Equal(t, uint64(0), dir.GetChunks())

	file, err := fsc.GetEntry("/subdir/dummy.txt")
	require.NoError(t, err)
	// File chunk count comes from ResolvedObject when it has been hydrated.
	if file.ResolvedObject != nil {
		require.Equal(t, uint64(len(file.ResolvedObject.Chunks)), file.GetChunks())
	} else {
		require.Equal(t, uint64(0), file.GetChunks())
	}
}

// TestGetContentTypeAndEntropyOnDir covers GetContentType / GetEntropy
// branches when ResolvedObject is nil (directory entry).
func TestGetContentTypeAndEntropyOnDir(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	dir, err := fsc.GetEntry("/subdir")
	require.NoError(t, err)
	require.Equal(t, "", dir.GetContentType())
	require.Equal(t, float64(0), dir.GetEntropy())
}

// TestOpenOnMissingObject exercises Open on a file entry that does not yet
// have its ResolvedObject populated — this drives the GetBlob + decode path
// inside Entry.Open.
func TestOpenLoadsObjectIfNeeded(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	entry, err := fsc.GetEntry("/subdir/dummy.txt")
	require.NoError(t, err)

	// Clear the cached resolved object so Open() must fetch the blob.
	entry.ResolvedObject = nil

	f, err := entry.Open(fsc)
	require.NoError(t, err)
	defer f.Close()

	buf := make([]byte, 5)
	n, err := f.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "hello", string(buf))
}

// TestVdirRead covers the *vdir.Read error path: trying to read from a
// directory file should fail.
func TestVdirReadFails(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	dirFile, err := fsc.Open("/subdir")
	require.NoError(t, err)
	defer dirFile.Close()

	buf := make([]byte, 16)
	_, err = dirFile.Read(buf)
	require.Error(t, err)
}

// TestVdirSeekFails covers the *vdir.Seek error path.
func TestVdirSeekFails(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	dirFile, err := fsc.Open("/subdir")
	require.NoError(t, err)
	defer dirFile.Close()

	seeker, ok := dirFile.(io.Seeker)
	require.True(t, ok)

	_, err = seeker.Seek(0, io.SeekStart)
	require.Error(t, err)
}

// TestFileMacsIterate exercises FileMacs which walks the filesystem and
// yields the MAC of every regular file.
func TestFileMacsIterate(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	it, err := fsc.FileMacs()
	require.NoError(t, err)

	count := 0
	for range it {
		count++
	}
	require.Greater(t, count, 0)
}

// TestReadDirEntryPath ensures fs.DirEntry entries returned from ReadDir
// expose the expected names.
func TestReadDirEntryPath(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	entries, err := fsc.ReadDir("/subdir")
	require.NoError(t, err)
	require.NotEmpty(t, entries)

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	// At least dummy.txt should be there.
	require.Contains(t, strings.Join(names, ","), "dummy.txt")
}
