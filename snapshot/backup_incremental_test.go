package snapshot_test

import (
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/PlakarKorp/integration-sdk/location"
	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/snapshot"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// backupOnce runs a single backup of the records emitted by gen into repo,
// optionally seeding the builder's VFS cache from a previous snapshot's
// filesystem (to drive the incremental checkVFSCache path).
func backupOnce(t *testing.T, repo *repository.Repository, name string, vfsCache *vfs.Filesystem, flags location.Flags, gen func(chan<- *connectors.Record)) *snapshot.Snapshot {
	t.Helper()

	builder, err := snapshot.Create(repo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{Name: name})
	require.NoError(t, err)
	if vfsCache != nil {
		builder.WithVFSCache(vfsCache)
	}

	imp, err := ptesting.NewMockImporter(repo.AppContext(), &connectors.Options{}, "mock", map[string]string{"location": "mock://place"})
	require.NoError(t, err)
	imp.(*ptesting.MockImporter).SetGenerator(gen)
	imp.(*ptesting.MockImporter).SetFlags(flags)

	src, err := snapshot.NewSource(repo.AppContext(), imp)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- builder.Backup(src) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("backup hung")
	}
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

	first := backupOnce(t, repo, "inc-1", nil, 0, incrementalTree)
	defer first.Close()

	firstFS, err := first.FilesystemWithCache()
	require.NoError(t, err)

	second := backupOnce(t, repo, "inc-2", firstFS, 0, incrementalTree)
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

// closeNotifier closes done the first time the record is closed.
type closeNotifier struct {
	io.ReadCloser
	once sync.Once
	done chan struct{}
}

func (c *closeNotifier) Close() error {
	c.once.Do(func() { close(c.done) })
	return c.ReadCloser.Close()
}

// sequentialTree emits incrementalTree one record at a time, waiting for each
// to be closed before sending the next, like an importer streaming content.
func sequentialTree(ch chan<- *connectors.Record) {
	recs := make(chan *connectors.Record, 16)
	incrementalTree(recs)
	close(recs)

	for rec := range recs {
		done := make(chan struct{})
		rec.Reader = &closeNotifier{ReadCloser: rec.Reader, done: done}
		ch <- rec
		<-done
	}
}

// TestIncrementalBackupNoCache backs up a sequential importer twice with a VFS
// cache: FLAG_NOCACHE must keep the second backup from hanging in the warm
// stage, and since nothing was looked up, from storing entries or objects twice.
func TestIncrementalBackupNoCache(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	first := backupOnce(t, repo, "nocache-1", nil, location.FLAG_NOCACHE, sequentialTree)
	defer first.Close()

	firstFS, err := first.FilesystemWithCache()
	require.NoError(t, err)

	second := backupOnce(t, repo, "nocache-2", firstFS, location.FLAG_NOCACHE, sequentialTree)
	defer second.Close()

	for _, typ := range []resources.Type{resources.RT_VFS_ENTRY, resources.RT_OBJECT} {
		seen := make(map[objects.MAC]objects.MAC)
		for de, err := range repo.ListBlobs(typ) {
			require.NoError(t, err)
			if packfile, ok := seen[de.Blob]; ok {
				require.Equal(t, packfile, de.Location.Packfile, "%s %x stored twice", typ, de.Blob)
			}
			seen[de.Blob] = de.Location.Packfile
		}
	}

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

	snap := backupOnce(t, repo, "big-1", nil, 0, gen)
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
