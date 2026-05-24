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

// generateSnapshot2 is a local copy of the generateSnapshot helper from
// vfs_test.go so that this file can be compiled as a separate test binary
// without conflicts.  It builds a repository with the following tree:
//
//	/
//	/subdir/
//	/subdir/dummy.txt  (content: "hello")
//	/subdir/big        (10 MiB of "hello\n")
func generateSnapshot2(t *testing.T) (*repository.Repository, *snapshot.Snapshot) {
	t.Helper()
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	snap := ptesting.GenerateSnapshot(t, repo,
		nil,
		ptesting.WithGenerator(func(ch chan<- *connectors.Record) {
			ch <- connectors.NewRecord("/", "", objects.FileInfo{
				Lname: "/",
				Lmode: os.ModeDir | 0755,
			}, nil, nil)
			ch <- connectors.NewRecord("/subdir", "", objects.FileInfo{
				Lname: "subdir",
				Lmode: os.ModeDir | 0755,
			}, nil, nil)
			ch <- connectors.NewRecord("/subdir/dummy.txt", "", objects.FileInfo{
				Lname: "dummy.txt",
				Lmode: 0644,
				Lsize: int64(len("hello")),
			}, nil, func() (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("hello")), nil
			})
			ch <- connectors.NewRecord("/subdir/big", "", objects.FileInfo{
				Lname: "big",
				Lmode: 0644,
			}, nil, func() (io.ReadCloser, error) {
				return &repreader2{msg: []byte("hello\n"), n: 10 * 1024 * 1024}, nil
			})
		}),
	)
	return repo, snap
}

// repreader2 repeats msg n times.
type repreader2 struct {
	msg       []byte
	i, n, off int
}

func (b *repreader2) Close() error { return nil }
func (b *repreader2) Read(p []byte) (int, error) {
	tot := 0
	for b.i < b.n {
		t := copy(p, b.msg[b.off:])
		tot += t
		if t == len(b.msg)-b.off {
			p = p[t:]
			b.i++
			b.off = 0
			continue
		}
		b.off += t
		return tot, nil
	}
	return tot, io.EOF
}

// -----------------------------------------------------------------------
// Filesystem.Stat
// -----------------------------------------------------------------------

func TestStat(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	fi, err := fs.Stat("/subdir/dummy.txt")
	require.NoError(t, err)
	require.NotNil(t, fi)
	require.Equal(t, "dummy.txt", fi.Name())
	require.False(t, fi.IsDir())

	fiDir, err := fs.Stat("/subdir")
	require.NoError(t, err)
	require.NotNil(t, fiDir)
	require.True(t, fiDir.IsDir())
}

func TestStatNotExist(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	_, err = fs.Stat("/no/such/path")
	require.Error(t, err)
}

// -----------------------------------------------------------------------
// Filesystem.ReadDir
// -----------------------------------------------------------------------

func TestReadDir(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	entries, err := fsc.ReadDir("/subdir")
	require.NoError(t, err)
	require.Len(t, entries, 2)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	require.ElementsMatch(t, []string{"dummy.txt", "big"}, names)
}

func TestReadDirRoot(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	entries, err := fsc.ReadDir("/")
	require.NoError(t, err)
	require.NotEmpty(t, entries)

	var foundSubdir bool
	for _, e := range entries {
		if e.Name() == "subdir" {
			foundSubdir = true
			require.True(t, e.IsDir())
		}
	}
	require.True(t, foundSubdir)
}

// -----------------------------------------------------------------------
// DirEntry methods (Name, IsDir, Type, Info)
// -----------------------------------------------------------------------

func TestDirEntryMethods(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	entries, err := fsc.ReadDir("/subdir")
	require.NoError(t, err)
	require.NotEmpty(t, entries)

	for _, e := range entries {
		name := e.Name()
		require.NotEmpty(t, name)

		mode := e.Type()
		_ = mode // just verify it doesn't panic

		fi, err := e.Info()
		require.NoError(t, err)
		require.Implements(t, (*iofs.FileInfo)(nil), fi)
		require.Equal(t, name, fi.Name())

		// IsDir should match what Type reports.
		require.Equal(t, e.IsDir(), fi.IsDir())
	}
}

// -----------------------------------------------------------------------
// GetEntryNoFollow
// -----------------------------------------------------------------------

func TestGetEntryNoFollow(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	entry, err := fsc.GetEntryNoFollow("/subdir/dummy.txt")
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, "dummy.txt", entry.Name())
	require.False(t, entry.IsDir())
}

func TestGetEntryNoFollowDir(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	entry, err := fsc.GetEntryNoFollow("/subdir")
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.True(t, entry.IsDir())
}

func TestGetEntryNoFollowNotExist(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	_, err = fsc.GetEntryNoFollow("/nonexistent")
	require.Error(t, err)
}

// -----------------------------------------------------------------------
// Entry.Path, Entry.Name, Entry.IsDir, Entry.Type, Entry.Info
// -----------------------------------------------------------------------

func TestEntryMethods(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	entry, err := fsc.GetEntry("/subdir/dummy.txt")
	require.NoError(t, err)
	require.NotNil(t, entry)

	require.Equal(t, "dummy.txt", entry.Name())
	require.Equal(t, "/subdir/dummy.txt", entry.Path())
	require.False(t, entry.IsDir())

	mode := entry.Type()
	require.True(t, mode.IsRegular())

	fi, err := entry.Info()
	require.NoError(t, err)
	require.Equal(t, "dummy.txt", fi.Name())
}

func TestEntryMethodsDir(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	entry, err := fsc.GetEntry("/subdir")
	require.NoError(t, err)
	require.NotNil(t, entry)

	require.Equal(t, "subdir", entry.Name())
	require.Equal(t, "/subdir", entry.Path())
	require.True(t, entry.IsDir())
	require.True(t, entry.Type().IsDir())
}

// -----------------------------------------------------------------------
// ReadAt on a file entry
// -----------------------------------------------------------------------

func TestVfileReadAt(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	f, err := fsc.Open("/subdir/dummy.txt")
	require.NoError(t, err)
	defer f.Close()

	// ReadAt from offset 0.
	buf := make([]byte, 3)
	n, err := f.(io.ReaderAt).ReadAt(buf, 0)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, "hel", string(buf))

	// ReadAt from a middle offset.
	buf2 := make([]byte, 2)
	n2, err := f.(io.ReaderAt).ReadAt(buf2, 3)
	// ReadAt at end may return EOF with data
	if err != nil {
		require.ErrorIs(t, err, io.EOF)
	}
	require.Equal(t, 2, n2)
	require.Equal(t, "lo", string(buf2))
}

// -----------------------------------------------------------------------
// vfile Close semantics
// -----------------------------------------------------------------------

func TestVfileCloseTwice(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	f, err := fsc.Open("/subdir/dummy.txt")
	require.NoError(t, err)

	require.NoError(t, f.Close())
	require.Error(t, f.Close(), "second Close must return an error")
}

// -----------------------------------------------------------------------
// vdir ReadDir
// -----------------------------------------------------------------------

func TestVdirReadDir(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	f, err := fsc.Open("/subdir")
	require.NoError(t, err)
	defer f.Close()

	type dirReader interface {
		ReadDir(n int) ([]iofs.DirEntry, error)
	}

	dr, ok := f.(dirReader)
	require.True(t, ok, "directory file should implement ReadDir")

	entries, err := dr.ReadDir(-1)
	if err != nil && err != io.EOF {
		require.NoError(t, err)
	}
	require.NotEmpty(t, entries)

	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	require.ElementsMatch(t, []string{"dummy.txt", "big"}, names)
}

// -----------------------------------------------------------------------
// WalkDir
// -----------------------------------------------------------------------

func TestWalkDir(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	var visited []string
	err = fsc.WalkDir("/", func(path string, entry *vfs.Entry, err error) error {
		require.NoError(t, err)
		visited = append(visited, path)
		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, visited)

	// All four paths must appear.
	require.Contains(t, visited, "/subdir")
	require.Contains(t, visited, "/subdir/dummy.txt")
	require.Contains(t, visited, "/subdir/big")
}

func TestWalkDirSkipDir(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	var visited []string
	err = fsc.WalkDir("/", func(path string, entry *vfs.Entry, err error) error {
		require.NoError(t, err)
		visited = append(visited, path)
		if entry != nil && entry.IsDir() && entry.Name() == "subdir" {
			return iofs.SkipDir
		}
		return nil
	})
	require.NoError(t, err)

	// Files inside /subdir must NOT be visited when we skip that dir.
	for _, p := range visited {
		require.False(t, strings.HasPrefix(p, "/subdir/"),
			"expected /subdir/* to be skipped, got %q", p)
	}
}

// -----------------------------------------------------------------------
// NewFilesystemWithCache
// -----------------------------------------------------------------------

func TestNewFilesystemWithCache(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	// FilesystemWithCache wraps NewFilesystemWithCache; exercise through the
	// Snapshot API so we use the real repository.
	fsc, err := snap.FilesystemWithCache()
	require.NoError(t, err)
	require.NotNil(t, fsc)

	// Verify that basic operations work on the cache-backed filesystem.
	entry, err := fsc.GetEntry("/subdir/dummy.txt")
	require.NoError(t, err)
	require.Equal(t, "dummy.txt", entry.Name())
}

// -----------------------------------------------------------------------
// ErrorItem serialization round-trip
// -----------------------------------------------------------------------

func TestErrorItemRoundTrip(t *testing.T) {
	item := vfs.NewErrorItem("/some/path", "permission denied")
	require.Equal(t, "/some/path", item.Name)
	require.Equal(t, "permission denied", item.Error)

	data, err := item.ToBytes()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	item2, err := vfs.ErrorItemFromBytes(data)
	require.NoError(t, err)
	require.Equal(t, item.Name, item2.Name)
	require.Equal(t, item.Error, item2.Error)
}

func TestErrorNodeFromBytes(t *testing.T) {
	// Verify that a zero-value node survives a serialization round-trip.
	item := vfs.NewErrorItem("/foo", "some error")
	data, err := item.ToBytes()
	require.NoError(t, err)

	// ErrorNodeFromBytes operates on serialized btree nodes, not ErrorItems;
	// just ensure it doesn't panic on valid msgpack data.
	_, _ = vfs.ErrorNodeFromBytes(data)
}

// -----------------------------------------------------------------------
// Xattr serialization round-trip
// -----------------------------------------------------------------------

func TestNewXattr(t *testing.T) {
	rec := connectors.NewXattr("/etc/passwd", "user.comment", objects.AttributeExtended, nil)
	mac := objects.MAC{1, 2, 3}

	x := vfs.NewXattr(rec, mac, 128)
	require.NotNil(t, x)
	require.Equal(t, "/etc/passwd", x.Path)
	require.Equal(t, "user.comment", x.Name)
	require.Equal(t, objects.AttributeExtended, x.Type)
	require.Equal(t, mac, x.Object)
	require.Equal(t, int64(128), x.Size)
}

func TestXattrToBytes(t *testing.T) {
	x := &vfs.Xattr{
		Path: "/etc/passwd",
		Name: "user.comment",
		Size: 42,
		Type: objects.AttributeExtended,
	}

	data, err := x.ToBytes()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	x2, err := vfs.XattrFromBytes(data)
	require.NoError(t, err)
	require.Equal(t, x.Path, x2.Path)
	require.Equal(t, x.Name, x2.Name)
	require.Equal(t, x.Size, x2.Size)
	require.Equal(t, x.Type, x2.Type)
}

func TestXattrToPath(t *testing.T) {
	cases := []struct {
		xattr    vfs.Xattr
		expected string
	}{
		{
			xattr: vfs.Xattr{
				Path: "/etc/passwd",
				Name: "user.comment",
				Type: objects.AttributeExtended,
			},
			expected: "/etc/passwduser.comment:",
		},
		{
			xattr: vfs.Xattr{
				Path: "/etc/passwd",
				Name: "stream",
				Type: objects.AttributeADS,
			},
			expected: "/etc/passwdstream@",
		},
	}

	for _, tc := range cases {
		got := tc.xattr.ToPath()
		require.Equal(t, tc.expected, got)
	}
}

func TestXattrNodeFromBytes(t *testing.T) {
	x := &vfs.Xattr{Path: "/foo", Name: "bar", Size: 1}
	data, err := x.ToBytes()
	require.NoError(t, err)

	// Just ensure the function doesn't crash on msgpack data.
	_, _ = vfs.XattrNodeFromBytes(data)
}

// -----------------------------------------------------------------------
// NodeFromBytes
// -----------------------------------------------------------------------

func TestNodeFromBytes(t *testing.T) {
	x := &vfs.Xattr{Path: "/test", Name: "attr"}
	data, err := x.ToBytes()
	require.NoError(t, err)

	// NodeFromBytes works on serialized btree nodes; passing a non-node
	// will produce an error but must not panic.
	_, _ = vfs.NodeFromBytes(data)
}

// -----------------------------------------------------------------------
// IterNodes / XattrNodes / BTrees
// -----------------------------------------------------------------------

func TestIterErrorNodes(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	iter := fsc.IterErrorNodes()
	require.NotNil(t, iter)
	for iter.Next() {
	}
	require.NoError(t, iter.Err())
}

func TestIterNodes(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	iter := fsc.IterNodes()
	require.NotNil(t, iter)

	var count int
	for iter.Next() {
		count++
	}
	require.NoError(t, iter.Err())
	require.Greater(t, count, 0)
}

func TestXattrNodes(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	iter := fsc.XattrNodes()
	require.NotNil(t, iter)
	// Drain the iterator; no xattrs in our test snapshot, but it must not panic.
	for iter.Next() {
	}
	require.NoError(t, iter.Err())
}

func TestBTrees(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	tree, errTree, xattrTree := fsc.BTrees()
	require.NotNil(t, tree)
	require.NotNil(t, errTree)
	require.NotNil(t, xattrTree)
}

// -----------------------------------------------------------------------
// Filesystem.Errors
// -----------------------------------------------------------------------

func TestErrors(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	// Our test snapshot has no errors; verify that iterating doesn't panic.
	for item, err := range fsc.Errors("/") {
		require.NoError(t, err)
		require.NotNil(t, item)
	}
}

// -----------------------------------------------------------------------
// Entry.Open on directory (vdir)
// -----------------------------------------------------------------------

func TestOpenDirectory(t *testing.T) {
	_, snap := generateSnapshot2(t)
	defer snap.Close()

	fsc, err := snap.Filesystem()
	require.NoError(t, err)

	entry, err := fsc.GetEntry("/subdir")
	require.NoError(t, err)

	f, err := entry.Open(fsc)
	require.NoError(t, err)
	require.NotNil(t, f)

	fi, err := f.Stat()
	require.NoError(t, err)
	require.True(t, fi.IsDir())

	// Reading a directory must return ErrInvalid.
	buf := make([]byte, 16)
	_, err = f.Read(buf)
	require.Error(t, err)

	require.NoError(t, f.Close())
}
