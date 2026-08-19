package snapshot_test

import (
	"io"
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

// backupOnce runs a single backup of the records emitted by gen into repo,
// optionally seeding the builder's VFS cache from a previous snapshot's
// filesystem (to drive the incremental checkVFSCache path).
func backupOnce(t *testing.T, repo *repository.Repository, name string, vfsCache *vfs.Filesystem, gen func(chan<- *connectors.Record)) *snapshot.Snapshot {
	t.Helper()

	builder, err := snapshot.Create(repo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{Name: name})
	require.NoError(t, err)
	if vfsCache != nil {
		builder.WithVFSCache(vfsCache)
	}

	imp, err := ptesting.NewMockImporter(repo.AppContext(), &connectors.Options{}, "mock", map[string]string{"location": "mock://place"})
	require.NoError(t, err)
	imp.(*ptesting.MockImporter).SetGenerator(gen)

	src, err := snapshot.NewSource(repo.AppContext(), imp)
	require.NoError(t, err)
	require.NoError(t, src.SetExcludes(nil))

	require.NoError(t, builder.Backup(src))
	require.NoError(t, builder.Commit())
	require.NoError(t, builder.Close())
	require.NoError(t, builder.Repository().RebuildState())

	snap, err := snapshot.Load(repo, builder.Header.Identifier)
	require.NoError(t, err)

	checkCache, err := repo.AppContext().GetCache().Check()
	require.NoError(t, err)
	snap.SetCheckCache(checkCache)
	return snap
}

// incrementalTree emits a stable tree: a directory, a regular file and a
// symlink. Emitting the same content twice lets the second backup match the
// first via the VFS cache.
func incrementalTree(ch chan<- *connectors.Record) {
	ch <- connectors.NewRecord("/", "", objects.FileInfo{Lname: "/", Lmode: os.ModeDir | 0755}, nil, nil)
	ch <- connectors.NewRecord("/d", "", objects.FileInfo{Lname: "d", Lmode: os.ModeDir | 0755}, nil, nil)
	ch <- connectors.NewRecord("/d/f.txt", "", objects.FileInfo{
		Lname: "f.txt", Lmode: 0644, Lsize: int64(len("stable content")),
	}, nil, func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader("stable content")), nil
	})
	ch <- connectors.NewRecord("/d/lnk", "f.txt", objects.FileInfo{
		Lname: "lnk", Lmode: os.ModeSymlink | 0777,
	}, nil, nil)
}

// TestIncrementalBackupUsesVFSCache backs up the same tree twice, seeding the
// second backup with the first snapshot's filesystem as a VFS cache. The second
// pass drives checkVFSCache's match / file-cached / symlink-cached branches.
func TestIncrementalBackupUsesVFSCache(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	first := backupOnce(t, repo, "inc-1", nil, incrementalTree)
	defer first.Close()

	firstFS, err := first.FilesystemWithCache()
	require.NoError(t, err)

	second := backupOnce(t, repo, "inc-2", firstFS, incrementalTree)
	defer second.Close()

	// The second snapshot must still contain the file and symlink.
	fs, err := second.Filesystem()
	require.NoError(t, err)

	e, err := fs.GetEntry("/d/f.txt")
	require.NoError(t, err)
	require.Equal(t, "f.txt", e.FileInfo.Lname)

	lnk, err := fs.GetEntryNoFollow("/d/lnk")
	require.NoError(t, err)
	require.NotZero(t, lnk.FileInfo.Lmode&os.ModeSymlink)

	// And it should pass a full check.
	require.NoError(t, second.Check("/", &snapshot.CheckOptions{}))
}

// repReader repeats msg until n copies have been emitted.
type repReader struct {
	msg  []byte
	i, n int
	off  int
}

func (b *repReader) Close() error { return nil }
func (b *repReader) Read(p []byte) (int, error) {
	tot := 0
	for b.i < b.n {
		c := copy(p, b.msg[b.off:])
		tot += c
		if c == len(b.msg)-b.off {
			p = p[c:]
			b.i++
			b.off = 0
			continue
		}
		b.off += c
		return tot, nil
	}
	if tot == 0 {
		return 0, io.EOF
	}
	return tot, nil
}

// TestBackupLargeFileMultiChunk backs up a multi-megabyte file so the content
// is split into several chunks, exercising the multi-chunk loops in chunkify,
// computeContent and (via Check) checkObject/checkChunk over many chunks.
func TestBackupLargeFileMultiChunk(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	gen := func(ch chan<- *connectors.Record) {
		ch <- connectors.NewRecord("/", "", objects.FileInfo{Lname: "/", Lmode: os.ModeDir | 0755}, nil, nil)
		ch <- connectors.NewRecord("/big", "", objects.FileInfo{
			Lname: "big", Lmode: 0644,
		}, nil, func() (io.ReadCloser, error) {
			return &repReader{msg: []byte("the quick brown fox jumps\n"), n: 512 * 1024}, nil
		})
	}

	snap := backupOnce(t, repo, "big-1", nil, gen)
	defer snap.Close()

	// The file resolved into more than one chunk.
	fs, err := snap.Filesystem()
	require.NoError(t, err)
	e, err := fs.GetEntry("/big")
	require.NoError(t, err)
	require.Greater(t, e.GetChunks(), uint64(1))

	// First check populates the chunk/object status cache; the second check
	// hits the "seen" fast paths in checkChunk/checkObject.
	require.NoError(t, snap.Check("/", &snapshot.CheckOptions{}))
	require.NoError(t, snap.Check("/", &snapshot.CheckOptions{}))

	// Fast check exercises the BlobExists branch.
	require.NoError(t, snap.Check("/", &snapshot.CheckOptions{FastCheck: true}))
}
