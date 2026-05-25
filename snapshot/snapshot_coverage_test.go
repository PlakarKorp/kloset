package snapshot_test

import (
	"context"
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// TestVerifyUnsigned covers the early-return branch of Snapshot.Verify when
// the snapshot has no identity attached.
func TestVerifyUnsigned(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	ok, err := snap.Verify()
	require.NoError(t, err)
	require.False(t, ok)
}

// TestListPackfiles walks the entire snapshot via Snapshot.ListPackfiles,
// driving the iterator over RT_SNAPSHOT, RT_VFS_BTREE, RT_VFS_NODE,
// RT_VFS_ENTRY, RT_OBJECT, and RT_CHUNK branches.
func TestListPackfiles(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	it, err := snap.ListPackfiles()
	require.NoError(t, err)
	require.NotNil(t, it)

	count := 0
	for _, err := range it {
		require.NoError(t, err)
		count++
	}
	require.Greater(t, count, 0)
}

// TestSearchRecursiveAll exercises Search with no filters — every file in the
// tree is yielded.
func TestSearchRecursiveAll(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	opts := &snapshot.SearchOpts{Recursive: true}
	it, err := snap.Search(context.Background(), opts)
	require.NoError(t, err)
	require.NotNil(t, it)

	count := 0
	for _, err := range it {
		require.NoError(t, err)
		count++
	}
	require.Greater(t, count, 0)
}

// TestSearchWithPrefix exercises Search with a Prefix filter — uses both the
// prefix-cleaning path and the prefix-matching loop.
func TestSearchWithPrefix(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	opts := &snapshot.SearchOpts{Recursive: true, Prefix: "/docs"}
	it, err := snap.Search(context.Background(), opts)
	require.NoError(t, err)
	require.NotNil(t, it)

	for range it {
	}
}

// TestSearchNonRecursive exercises the !Recursive branch in visitfiles which
// uses Children instead of Files.
func TestSearchNonRecursive(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	opts := &snapshot.SearchOpts{Recursive: false, Prefix: "/docs"}
	it, err := snap.Search(context.Background(), opts)
	require.NoError(t, err)
	require.NotNil(t, it)

	for range it {
	}
}

// TestSearchWithNameFilterGlob exercises the path.Match name-filter branch.
func TestSearchWithNameFilterGlob(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	opts := &snapshot.SearchOpts{Recursive: true, NameFilter: "*.txt"}
	it, err := snap.Search(context.Background(), opts)
	require.NoError(t, err)
	require.NotNil(t, it)

	for range it {
	}
}

// TestSearchWithNameFilterRegex exercises the regexp.Match fallback when
// path.Match doesn't match.
func TestSearchWithNameFilterRegex(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	opts := &snapshot.SearchOpts{Recursive: true, NameFilter: ".*\\.txt$"}
	it, err := snap.Search(context.Background(), opts)
	require.NoError(t, err)

	for range it {
	}
}

// TestSearchWithExactNameFilter exercises the "exact match" branch of the
// name filter.
func TestSearchWithExactNameFilter(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	opts := &snapshot.SearchOpts{Recursive: true, NameFilter: "readme.txt"}
	it, err := snap.Search(context.Background(), opts)
	require.NoError(t, err)

	for range it {
	}
}

// TestSearchPagination covers the Offset/Limit branches of Search.
func TestSearchPagination(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	opts := &snapshot.SearchOpts{Recursive: true, Offset: 1, Limit: 1}
	it, err := snap.Search(context.Background(), opts)
	require.NoError(t, err)

	for range it {
	}
}

// TestSearchWithMimes exercises the visitmimes path when Recursive is true
// and a Mimes filter is set.
func TestSearchWithMimes(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	opts := &snapshot.SearchOpts{Recursive: true, Mimes: []string{"text/plain"}}
	it, err := snap.Search(context.Background(), opts)
	require.NoError(t, err)
	require.NotNil(t, it)

	for range it {
	}
}

// TestSearchCancelled exercises the ctx.Err() branch in visitfiles by
// cancelling the context before the iterator runs.
func TestSearchCancelled(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	opts := &snapshot.SearchOpts{Recursive: true}
	it, err := snap.Search(ctx, opts)
	require.NoError(t, err)

	for range it {
	}
}

// TestCreateWithRepositoryWriter exercises the CreateWithRepositoryWriter
// constructor. We need a RepositoryWriter whose scan cache is on a *different*
// snapshot ID than the one we hand to CreateWithRepositoryWriter, because
// CreateWithRepositoryWriter internally opens its own scan cache for the
// snapshot ID and a single pebble DB cannot be opened twice.
func TestCreateWithRepositoryWriter(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	// Reserve one MAC for the writer's scan cache, a different one for the
	// new snapshot.
	writerSnapID := objects.RandomMAC()
	newSnapID := objects.RandomMAC()

	writerScan, err := repo.AppContext().GetCache().Scan(writerSnapID)
	require.NoError(t, err)
	defer writerScan.Close()

	writer := repo.NewRepositoryWriter(writerScan, writerSnapID, repository.DefaultType, "")
	require.NotNil(t, writer)

	builder, err := snapshot.CreateWithRepositoryWriter(writer, &snapshot.BuilderOptions{
		Name:         "with-writer",
		NoCheckpoint: true,
	}, newSnapID)
	require.NoError(t, err)
	require.NotNil(t, builder)
	defer builder.Close()
}

// TestCreateWithRepositoryWriterNilMac exercises the snapId == NilMac
// branch, where CreateWithRepositoryWriter generates a random MAC.
func TestCreateWithRepositoryWriterNilMac(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	writerSnapID := objects.RandomMAC()
	writerScan, err := repo.AppContext().GetCache().Scan(writerSnapID)
	require.NoError(t, err)
	defer writerScan.Close()

	writer := repo.NewRepositoryWriter(writerScan, writerSnapID, repository.DefaultType, "")
	require.NotNil(t, writer)

	builder, err := snapshot.CreateWithRepositoryWriter(writer, &snapshot.BuilderOptions{
		Name:         "with-writer-nil",
		NoCheckpoint: true,
	}, objects.NilMac)
	require.NoError(t, err)
	require.NotNil(t, builder)
	defer builder.Close()
}

// TestWithVFSCache exercises the trivial WithVFSCache setter — it does not
// validate behavior, just that the method does not panic.
func TestWithVFSCache(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	builder, err := snapshot.Create(repo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{
		Name:         "vfs-cache-test",
		NoCheckpoint: true,
	})
	require.NoError(t, err)
	require.NotNil(t, builder)
	defer builder.Close()

	// Passing a nil cache is enough to exercise the assignment path.
	builder.WithVFSCache(nil)
}

// TestGetSnapshotMissing exercises the not-found error path of GetSnapshot.
func TestGetSnapshotMissing(t *testing.T) {
	repo, _ := generateSnapshot(t)

	_, found, err := snapshot.GetSnapshot(repo, objects.MAC{0xDE, 0xAD, 0xBE, 0xEF})
	require.Error(t, err)
	require.False(t, found)
}

// TestGetSnapshotPresent exercises the happy path (and the cache-population
// branch) of GetSnapshot using an existing snapshot identifier.
func TestGetSnapshotPresent(t *testing.T) {
	repo, snap := generateSnapshot(t)
	defer snap.Close()

	hdr, _, err := snapshot.GetSnapshot(repo, snap.Header.Identifier)
	require.NoError(t, err)
	require.NotNil(t, hdr)
	require.Equal(t, snap.Header.Identifier, hdr.Identifier)

	// Calling it a second time should hit the cache branch.
	hdr2, fromCache, err := snapshot.GetSnapshot(repo, snap.Header.Identifier)
	require.NoError(t, err)
	require.NotNil(t, hdr2)
	require.True(t, fromCache)
}

// TestSnapshotStoreUpdateAndClose exercises the read-only Update path and the
// trivial Close on a SnapshotStore. Since SnapshotStore is unexported we go
// through Persist indirectly — but the simpler approach is via the
// public-but-trivial Close() method on any tree we can fetch. Instead, exercise
// SnapshotStore by re-using the existing snapshot's filesystem tree which goes
// through Get; this naturally drives the SnapshotStore.Get path. The other
// methods are reached when the snapshot is built.
//
// We focus on the test we *can* write: verify that Snapshot loading and using
// the vfs/contenttype/dirpack indices end-to-end touches SnapshotStore.Get
// and the public side of the API.
func TestSnapshotStoreIndirect(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	// Loading the filesystem tree drives SnapshotStore.Get through the btree.
	fs, err := snap.Filesystem()
	require.NoError(t, err)
	require.NotNil(t, fs)

	// Loading the ContentTypeIdx tree drives another SnapshotStore.Get.
	idx, err := snap.ContentTypeIdx()
	require.NoError(t, err)
	_ = idx
}
