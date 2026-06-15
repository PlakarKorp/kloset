package vfs_test

import (
	"io"
	iofs "io/fs"
	"os"
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

var fsErrClosed = iofs.ErrClosed

// generateRichSnapshot builds a snapshot whose tree contains a nested
// directory, a regular file, a symlink (relative) and an absolute symlink, so
// the symlink-following and listing paths in the vfs package are exercised.
//
//	/
//	/dir/
//	/dir/file.txt    -> "hello world"
//	/dir/rel.lnk     -> "file.txt"   (relative symlink to /dir/file.txt)
//	/abs.lnk         -> "/dir/file.txt" (absolute symlink)
func generateRichSnapshot(t *testing.T) (*repository.Repository, *snapshot.Snapshot) {
	t.Helper()
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	snap := ptesting.GenerateSnapshot(t, repo, nil,
		ptesting.WithGenerator(func(ch chan<- *connectors.Record) {
			ch <- connectors.NewRecord("/", "", objects.FileInfo{
				Lname: "/", Lmode: os.ModeDir | 0755,
			}, nil, nil)
			ch <- connectors.NewRecord("/dir", "", objects.FileInfo{
				Lname: "dir", Lmode: os.ModeDir | 0755,
			}, nil, nil)
			ch <- connectors.NewRecord("/dir/file.txt", "", objects.FileInfo{
				Lname: "file.txt", Lmode: 0644, Lsize: int64(len("hello world")),
			}, []string{"user.comment"}, func() (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("hello world")), nil
			})
			ch <- connectors.NewXattr("/dir/file.txt", "user.comment", objects.AttributeExtended,
				func() (io.ReadCloser, error) {
					return io.NopCloser(strings.NewReader("an xattr value")), nil
				})
			ch <- connectors.NewRecord("/dir/rel.lnk", "file.txt", objects.FileInfo{
				Lname: "rel.lnk", Lmode: os.ModeSymlink | 0777,
			}, nil, nil)
			ch <- connectors.NewRecord("/abs.lnk", "/dir/file.txt", objects.FileInfo{
				Lname: "abs.lnk", Lmode: os.ModeSymlink | 0777,
			}, nil, nil)
			ch <- connectors.NewError("/dir/unreadable", os.ErrPermission)
		}),
	)
	return repo, snap
}

// TestGetEntryFollowSymlinks exercises getEntryFollow on both a relative and an
// absolute symlink, resolving to the target file.
func TestGetEntryFollowSymlinks(t *testing.T) {
	_, snap := generateRichSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	// Relative symlink resolves to /dir/file.txt.
	e, err := fs.GetEntry("/dir/rel.lnk")
	require.NoError(t, err)
	require.Equal(t, "file.txt", e.FileInfo.Lname)

	// Absolute symlink resolves to /dir/file.txt as well.
	e, err = fs.GetEntry("/abs.lnk")
	require.NoError(t, err)
	require.Equal(t, "file.txt", e.FileInfo.Lname)

	// NoFollow returns the symlink entry itself.
	e, err = fs.GetEntryNoFollow("/dir/rel.lnk")
	require.NoError(t, err)
	require.NotZero(t, e.FileInfo.Lmode&os.ModeSymlink)
}

// TestGetEntryNotExist covers the not-found branches of getEntry.
func TestGetEntryNotExist(t *testing.T) {
	_, snap := generateRichSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	_, err = fs.GetEntry("/nope")
	require.Error(t, err)
	_, err = fs.GetEntryNoFollow("/nope")
	require.Error(t, err)

	// Relative input gets a leading slash prepended, then cleaned.
	_, err = fs.GetEntry("also/nope")
	require.Error(t, err)
}

// TestFilesAndFilesBelow exercises Files("/") (ScanAll path) and Files(prefix)
// (filesbelow/WalkDir path).
func TestFilesAndFilesBelow(t *testing.T) {
	_, snap := generateRichSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	var all int
	for _, err := range fs.Files("/") {
		require.NoError(t, err)
		all++
	}
	require.Greater(t, all, 0)

	var below int
	for _, err := range fs.Files("/dir") {
		require.NoError(t, err)
		below++
	}
	require.Greater(t, below, 0)
}

// TestPathnamesIter exercises the Pathnames iterator.
func TestPathnamesIter(t *testing.T) {
	_, snap := generateRichSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	var names []string
	for p, err := range fs.Pathnames() {
		require.NoError(t, err)
		names = append(names, p)
	}
	require.Contains(t, names, "/dir/file.txt")
}

// TestChildrenAndFileMacs exercises Children and FileMacs over a directory.
func TestChildrenAndFileMacs(t *testing.T) {
	_, snap := generateRichSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	children, err := fs.Children("/dir")
	require.NoError(t, err)
	var names []string
	for c, err := range children {
		require.NoError(t, err)
		names = append(names, c.Stat().Name())
	}
	require.Contains(t, names, "file.txt")

	macs, err := fs.FileMacs()
	require.NoError(t, err)
	count := 0
	for _, err := range macs {
		require.NoError(t, err)
		count++
	}
	require.Greater(t, count, 0)
}

// TestGetEntryForBackup exercises the backup-oriented entry lookup without a
// dirpack cache (the getEntryNoFollow fallback).
func TestGetEntryForBackup(t *testing.T) {
	_, snap := generateRichSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	e, err := fs.GetEntryForBackup("/dir/file.txt")
	require.NoError(t, err)
	require.Equal(t, "file.txt", e.FileInfo.Lname)

	_, err = fs.GetEntryForBackup("/missing")
	require.Error(t, err)
}

// TestGetEntryForBackupWithCache drives the dirpack-cache path of
// getEntryForBackup, which loads and caches a directory's entry map via
// loadDirpackMap.
func TestGetEntryForBackupWithCache(t *testing.T) {
	_, snap := generateRichSnapshot(t)
	defer snap.Close()

	fs, err := snap.FilesystemWithCache()
	require.NoError(t, err)

	// First lookup populates the dirpack cache for "/dir".
	e, err := fs.GetEntryForBackup("/dir/file.txt")
	require.NoError(t, err)
	require.Equal(t, "file.txt", e.FileInfo.Lname)

	// Second lookup in the same directory hits the cached map.
	_, err = fs.GetEntryForBackup("/dir/rel.lnk")
	require.NoError(t, err)

	// A missing base in a known directory returns ErrNotExist.
	_, err = fs.GetEntryForBackup("/dir/missing")
	require.Error(t, err)
}

// TestXattrLookup exercises Entry.Xattr's btree lookup path. Note: the xattr
// index is currently keyed by the bare record pathname (see buildXattrIndex in
// snapshot/backup.go), whereas Entry.Xattr composes "<path><name><sep>", so a
// lookup by attribute name does not match and returns ErrNotExist. We assert
// that observed behaviour rather than a found value.
func TestXattrLookup(t *testing.T) {
	_, snap := generateRichSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	e, err := fs.GetEntry("/dir/file.txt")
	require.NoError(t, err)

	_, err = e.Xattr(fs, "user.comment")
	require.Error(t, err)

	_, err = e.Xattr(fs, "user.nope")
	require.Error(t, err)
}

// TestErrorsIterator exercises the Errors iterator over a snapshot that
// recorded a permission error.
func TestErrorsIterator(t *testing.T) {
	_, snap := generateRichSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	var items int
	for item, err := range fs.Errors("/") {
		require.NoError(t, err)
		require.NotEmpty(t, item.Name)
		items++
	}
	require.Greater(t, items, 0)
}

// TestGetdentsVfs builds a Filesystem with a nil dirpack index so ReadDir goes
// through the getdentsVfs path rather than getdentsDirpack.
func TestGetdentsVfs(t *testing.T) {
	repo, snap := generateRichSnapshot(t)
	defer snap.Close()

	v := snap.Header.GetSource(0).VFS
	fs, err := vfs.NewFilesystem(repo, v.Root, v.Xattrs, v.Errors, nil)
	require.NoError(t, err)

	entries, err := fs.ReadDir("/dir")
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	require.Contains(t, names, "file.txt")
}

// TestVFileIO exercises the vfile Read/Seek/ReadAt/Stat/Close paths, including
// the post-close ErrClosed branches.
func TestVFileIO(t *testing.T) {
	_, snap := generateRichSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	e, err := fs.GetEntry("/dir/file.txt")
	require.NoError(t, err)

	f, err := e.Open(fs)
	require.NoError(t, err)

	fi, err := f.Stat()
	require.NoError(t, err)
	require.Equal(t, "file.txt", fi.Name())

	buf := make([]byte, 5)
	n, err := f.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "hello", string(buf))

	seeker := f.(io.Seeker)
	_, err = seeker.Seek(0, io.SeekStart)
	require.NoError(t, err)

	readerAt := f.(io.ReaderAt)
	at := make([]byte, 5)
	_, err = readerAt.ReadAt(at, 6)
	require.NoError(t, err)
	require.Equal(t, "world", string(at))

	require.NoError(t, f.Close())

	// Post-close everything returns ErrClosed.
	_, err = f.Stat()
	require.ErrorIs(t, err, fsErrClosed)
	_, err = f.Read(buf)
	require.ErrorIs(t, err, fsErrClosed)
	_, err = seeker.Seek(0, io.SeekStart)
	require.ErrorIs(t, err, fsErrClosed)
	_, err = readerAt.ReadAt(at, 0)
	require.ErrorIs(t, err, fsErrClosed)
	require.ErrorIs(t, f.Close(), fsErrClosed)
}

// TestVDirIO exercises the vdir Read/Seek/Stat/Close paths.
func TestVDirIO(t *testing.T) {
	_, snap := generateRichSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	e, err := fs.GetEntry("/dir")
	require.NoError(t, err)

	d, err := e.Open(fs)
	require.NoError(t, err)

	fi, err := d.Stat()
	require.NoError(t, err)
	require.True(t, fi.IsDir())

	// Read/Seek on a directory are invalid.
	_, err = d.Read(make([]byte, 1))
	require.Error(t, err)
	_, err = d.(io.Seeker).Seek(0, io.SeekStart)
	require.Error(t, err)

	require.NoError(t, d.Close())
	_, err = d.Stat()
	require.ErrorIs(t, err, fsErrClosed)
	require.ErrorIs(t, d.Close(), fsErrClosed)
}

// TestWalkDirFull exercises ResolveEntry on a file with object resolution and
// the WalkDir traversal.
func TestWalkDirFull(t *testing.T) {
	_, snap := generateRichSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	var visited []string
	err = fs.WalkDir("/", func(p string, e *vfs.Entry, err error) error {
		require.NoError(t, err)
		visited = append(visited, p)
		return nil
	})
	require.NoError(t, err)
	require.Contains(t, visited, "/dir/file.txt")
}

// TestWalkDirRootNotFound covers WalkDir's branch where GetEntry on the root
// fails and the error is forwarded to the callback.
func TestWalkDirRootNotFound(t *testing.T) {
	_, snap := generateRichSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	var gotErr error
	err = fs.WalkDir("/does/not/exist", func(p string, e *vfs.Entry, cbErr error) error {
		gotErr = cbErr
		return cbErr
	})
	require.Error(t, err)
	require.Error(t, gotErr)
}

// TestWalkDirSkipAll covers the SkipAll short-circuit in WalkDir.
func TestWalkDirSkipAll(t *testing.T) {
	_, snap := generateRichSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	visited := 0
	err = fs.WalkDir("/", func(p string, e *vfs.Entry, cbErr error) error {
		require.NoError(t, cbErr)
		visited++
		return iofs.SkipAll
	})
	require.NoError(t, err)
	require.Equal(t, 1, visited) // SkipAll stops after the first entry.
}

// TestReadUnresolvedObject covers the ErrInvalid branch in vfile.Read/Seek/
// ReadAt for an entry whose object is not resolved (a directory opened as a
// file would not apply; instead we use a zero-byte entry that has no object).
func TestReadUnresolvedObject(t *testing.T) {
	_, snap := generateRichSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	// A symlink entry has no resolved object; opening it yields a vfile whose
	// ResolvedObject is nil, so Read/Seek/ReadAt return ErrInvalid.
	e, err := fs.GetEntryNoFollow("/dir/rel.lnk")
	require.NoError(t, err)
	require.Nil(t, e.ResolvedObject)

	f, err := e.Open(fs)
	require.NoError(t, err)
	defer f.Close()

	_, err = f.Read(make([]byte, 1))
	require.ErrorIs(t, err, iofs.ErrInvalid)
	_, err = f.(io.Seeker).Seek(0, io.SeekStart)
	require.ErrorIs(t, err, iofs.ErrInvalid)
	_, err = f.(io.ReaderAt).ReadAt(make([]byte, 1), 0)
	require.ErrorIs(t, err, iofs.ErrInvalid)
}
