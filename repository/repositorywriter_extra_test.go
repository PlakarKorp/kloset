package repository_test

import (
	"os"
	"testing"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/caching/pebble"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// makeWriter is a small helper used by the extra-coverage tests.
func makeWriter(t *testing.T) (*repository.Repository, *repository.RepositoryWriter, *caching.ScanCache, objects.MAC) {
	t.Helper()
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	tmpCacheDir, err := os.MkdirTemp("", "kloset-extra-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpCacheDir) })

	cacheManager := caching.NewManager(pebble.Constructor(tmpCacheDir))

	snapshotID := objects.MAC{0x42}
	scanCache, err := cacheManager.Scan(snapshotID)
	require.NoError(t, err)

	writer := repo.NewRepositoryWriter(scanCache, snapshotID, repository.DefaultType, "")
	require.NotNil(t, writer)

	return repo, writer, scanCache, snapshotID
}

// TestRemoveTransaction exercises RemoveTransaction (a pass-through that
// removes a per-state delta entry under the transaction mutex).
func TestRemoveTransaction(t *testing.T) {
	_, writer, _, snapshotID := makeWriter(t)
	writer.RemoveTransaction(snapshotID)
	// Calling it again on an already-removed transaction is a no-op.
	writer.RemoveTransaction(snapshotID)
}

// TestMergeLocalStateWith calls MergeLocalStateWith with a freshly-created
// (empty) cache. The underlying state.MergeStateFromCache is a no-op on an
// empty cache, so we just assert no error is returned.
func TestMergeLocalStateWith(t *testing.T) {
	_, writer, scanCache, snapshotID := makeWriter(t)
	require.NoError(t, writer.MergeLocalStateWith(snapshotID, scanCache))
}

// TestRemovePackfile exercises the RemovePackfile pass-through.
func TestRemovePackfile(t *testing.T) {
	_, writer, _, _ := makeWriter(t)
	require.NoError(t, writer.RemovePackfile(objects.MAC{0x01, 0x02, 0x03}))
}

// TestUncolourPackfile exercises the UncolourPackfile pass-through.
func TestUncolourPackfile(t *testing.T) {
	_, writer, _, _ := makeWriter(t)
	require.NoError(t, writer.UncolourPackfile(objects.MAC{0xAA, 0xBB}))
}

// TestRemoveBlob exercises the RemoveBlob pass-through.
func TestRemoveBlob(t *testing.T) {
	_, writer, _, _ := makeWriter(t)
	require.NoError(t, writer.RemoveBlob(resources.RT_CHUNK, objects.MAC{0x10}, objects.MAC{0x20}))
}

// TestStoreClose exercises the trivial RepositoryStore.Close that returns nil.
func TestStoreClose(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	store := repository.NewRepositoryStore[string, int](repo, resources.RT_CHUNK)
	require.NoError(t, store.Close())
}
