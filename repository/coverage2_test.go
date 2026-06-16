package repository_test

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/hashing"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

// newStateCache returns a fresh caching.StateCache (an SQLState, the same
// backend repository.New uses for its own state) in a temporary directory,
// cleaned up at the end of the test.
func newStateCache(t *testing.T) caching.StateCache {
	t.Helper()
	tmp, err := os.MkdirTemp("", "kloset-statecache-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmp) })

	sc, err := caching.NewSQLState(tmp, false)
	require.NoError(t, err)
	return sc
}

// TestRebuildStateWithCacheEmpty drives RebuildStateWithCache against a fresh
// repository with an empty external cache. There are no remote states yet, so
// the function should walk every branch (no missing, no outdated) and succeed.
func TestRebuildStateWithCacheEmpty(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	require.NoError(t, repo.RebuildStateWithCache(newStateCache(t)))
}

// TestRebuildStateWithCacheAfterBackup exercises the "missing states" branch of
// RebuildStateWithCache: a backup writes a remote state that the fresh external
// cache does not know about, so the function must fetch, topo-sort and merge it.
func TestRebuildStateWithCacheAfterBackup(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	files := []ptesting.MockFile{
		ptesting.NewMockDir("/"),
		ptesting.NewMockFile("/rebuild.txt", 0644, strings.Repeat("rebuild ", 64)),
	}
	_ = ptesting.GenerateSnapshot(t, repo, files)

	// A brand-new cache has none of the remote states -> exercises the
	// missingStates / TopoSort / MergeState loop.
	require.NoError(t, repo.RebuildStateWithCache(newStateCache(t)))

	// At least one snapshot must be visible after the rebuild.
	var count int
	for _, err := range repo.ListSnapshots() {
		require.NoError(t, err)
		count++
	}
	require.Greater(t, count, 0)
}

// TestRebuildStateWithCacheOutdated exercises the "outdated states" deletion
// branch: the external cache contains a state MAC that is absent from the
// remote store, so RebuildStateWithCache must delete it locally.
func TestRebuildStateWithCacheOutdated(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	sc := newStateCache(t)
	// Seed the cache with a bogus state that does not exist remotely.
	require.NoError(t, sc.PutState(objects.MAC{0xDE, 0xAD, 0xBE, 0xEF}, []byte("stale")))

	require.NoError(t, repo.RebuildStateWithCache(sc))
}

// TestGetStateNotFound confirms GetState surfaces an error for a MAC that was
// never written.
func TestGetStateNotFound(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	_, _, err := repo.GetState(objects.MAC{0x01, 0x02, 0x03, 0x04})
	require.Error(t, err)
}

// TestGetStateCorrupt writes raw garbage to the state slot via the underlying
// store and confirms GetState fails to deserialize it.
func TestGetStateCorrupt(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	// Write raw, non-deserializable bytes straight into the state slot via the
	// store handle so GetState's read path encounters invalid content.
	badMac := objects.MAC{0x99}
	store := repo.Store()
	_, err := store.Put(repo.AppContext(), storage.StorageResourceState, badMac, bytes.NewReader([]byte("not a valid serialized state")))
	require.NoError(t, err)

	_, _, err = repo.GetState(badMac)
	require.Error(t, err)
}

// TestGetPackfileCorrupt writes invalid bytes into a packfile slot and confirms
// GetPackfile returns an error rather than panicking.
func TestGetPackfileCorrupt(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	mac := objects.MAC{0xAB, 0xCD}
	store := repo.Store()
	_, err := store.Put(repo.AppContext(), storage.StorageResourcePackfile, mac, bytes.NewReader([]byte("not a packfile")))
	require.NoError(t, err)

	_, err = repo.GetPackfile(mac)
	require.Error(t, err)
}

// TestDeleteSnapshotAfterBackup runs the full DeleteSnapshot path against a
// real snapshot: it derives a delta state, colours the snapshot resource,
// serializes it and writes a new state.
func TestDeleteSnapshotAfterBackup(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	files := []ptesting.MockFile{
		ptesting.NewMockDir("/"),
		ptesting.NewMockFile("/gone.txt", 0644, "delete this snapshot"),
	}
	snap := ptesting.GenerateSnapshot(t, repo, files)
	require.NotNil(t, snap)

	require.NoError(t, repo.DeleteSnapshot(snap.Header.Identifier))
	require.NoError(t, repo.RebuildState())

	var deleted int
	for range repo.ListDeletedSnapShots() {
		deleted++
	}
	require.GreaterOrEqual(t, deleted, 1)
}

// TestNewWithCorruptConfig exercises the storage.Deserialize error branch of
// repository.New by handing it bytes that are not a valid wrapped config.
func TestNewWithCorruptConfig(t *testing.T) {
	ctx := ptesting.GenerateContext(t, nil, nil)

	tmpCacheDir, err := os.MkdirTemp("", "tmp_cache_badcfg")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpCacheDir) })
	ctx.CacheDir = tmpCacheDir

	st, err := storage.New(ctx, map[string]string{"location": "mock:///badcfg"})
	require.NoError(t, err)
	require.NotNil(t, st)

	_, err = repository.New(ctx, nil, st, []byte("totally not a config"))
	require.Error(t, err)
}

// TestNewNoRebuildWithCorruptConfig is the NewNoRebuild analogue of the above.
func TestNewNoRebuildWithCorruptConfig(t *testing.T) {
	ctx := ptesting.GenerateContext(t, nil, nil)

	tmpCacheDir, err := os.MkdirTemp("", "tmp_cache_badcfg2")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpCacheDir) })
	ctx.CacheDir = tmpCacheDir

	st, err := storage.New(ctx, map[string]string{"location": "mock:///badcfg2"})
	require.NoError(t, err)

	_, err = repository.NewNoRebuild(ctx, nil, st, []byte("garbage config bytes"), false)
	require.Error(t, err)
}

// TestNewNoRebuildReadonlyCache walks the readonlyCache=true branch of
// NewNoRebuild with a valid config.
func TestNewNoRebuildReadonlyCache(t *testing.T) {
	ctx := ptesting.GenerateContext(t, nil, nil)

	tmpCacheDir, err := os.MkdirTemp("", "tmp_cache_ro")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpCacheDir) })
	ctx.CacheDir = tmpCacheDir

	st, err := storage.New(ctx, map[string]string{"location": "mock:///ro"})
	require.NoError(t, err)

	config := storage.NewConfiguration()
	config.Compression = nil
	config.Encryption = nil
	hasher := hashing.GetHasher(hashing.DEFAULT_HASHING_ALGORITHM)

	serialized, err := config.ToBytes()
	require.NoError(t, err)

	wrappedRd, err := storage.Serialize(hasher, resources.RT_CONFIG, versioning.GetCurrentVersion(resources.RT_CONFIG), bytes.NewReader(serialized))
	require.NoError(t, err)
	wrapped, err := io.ReadAll(wrappedRd)
	require.NoError(t, err)

	require.NoError(t, st.Create(ctx, wrapped))

	repo, err := repository.NewNoRebuild(ctx, nil, st, wrapped, true)
	require.NoError(t, err)
	require.NotNil(t, repo)
}

// TestGetObjectContentMultiChunk backs up a large file so its content spans
// many chunks, then reads the whole object back and verifies the bytes match
// the original payload. This drives the multi-range accumulation logic in
// GetObjectContent.
func TestGetObjectContentMultiChunk(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	// Large, varied payload to force the chunker to emit several chunks.
	var sb strings.Builder
	for i := 0; i < 20000; i++ {
		sb.WriteString("the quick brown fox jumps over the lazy dog ")
	}
	payload := sb.String()

	files := []ptesting.MockFile{
		ptesting.NewMockDir("/"),
		ptesting.NewMockFile("/big.txt", 0644, payload),
	}
	snap := ptesting.GenerateSnapshot(t, repo, files)
	require.NotNil(t, snap)

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	entry, err := fs.GetEntry("/big.txt")
	require.NoError(t, err)
	require.NotNil(t, entry.ResolvedObject)

	obj := entry.ResolvedObject
	var totalLen uint32
	for _, c := range obj.Chunks {
		totalLen += c.Length
	}

	collected := make([]byte, 0, totalLen)
	for data, gErr := range repo.GetObjectContent(obj, 0, totalLen+1) {
		require.NoError(t, gErr)
		collected = append(collected, data...)
	}
	require.Equal(t, payload, string(collected))
}

// TestGetObjectContentStartOffset reads an object starting from a non-zero
// chunk index, exercising the `start` parameter handling.
func TestGetObjectContentStartOffset(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	var sb strings.Builder
	for i := 0; i < 20000; i++ {
		sb.WriteString("offset content payload data block ")
	}

	files := []ptesting.MockFile{
		ptesting.NewMockDir("/"),
		ptesting.NewMockFile("/offset.txt", 0644, sb.String()),
	}
	snap := ptesting.GenerateSnapshot(t, repo, files)

	fs, err := snap.Filesystem()
	require.NoError(t, err)
	entry, err := fs.GetEntry("/offset.txt")
	require.NoError(t, err)
	obj := entry.ResolvedObject
	require.NotNil(t, obj)

	if len(obj.Chunks) < 2 {
		t.Skip("payload did not produce multiple chunks; chunker tuning changed")
	}

	var totalLen uint32
	for _, c := range obj.Chunks {
		totalLen += c.Length
	}

	// Reading from chunk index 1 must yield fewer bytes than the whole object.
	collected := make([]byte, 0)
	for data, gErr := range repo.GetObjectContent(obj, 1, totalLen+1) {
		require.NoError(t, gErr)
		collected = append(collected, data...)
	}
	require.Less(t, len(collected), int(totalLen))
}
