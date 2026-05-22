package caching_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/caching/pebble"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/stretchr/testify/require"
)

func newScanCache(t *testing.T) *caching.ScanCache {
	t.Helper()
	tmpDir := t.TempDir()
	manager := caching.NewManager(pebble.Constructor(tmpDir))
	t.Cleanup(func() { manager.Close() })

	snapshotID := objects.MAC{0xde, 0xad}
	cache, err := manager.Scan(snapshotID)
	require.NoError(t, err)
	t.Cleanup(func() { cache.Close() })
	return cache
}

// TestScanBatchDirectoryAndFile tests the ScanBatch methods accessed via NewScanBatch.
func TestScanBatchDirectoryAndFile(t *testing.T) {
	cache := newScanCache(t)

	batch := cache.NewScanBatch()
	require.NoError(t, batch.PutDirectory(0, "/home", []byte("dir data")))
	require.NoError(t, batch.PutFile(0, "/home/readme.txt", []byte("file data")))
	require.NoError(t, batch.Commit())

	// Verify via direct cache reads.
	got, err := cache.GetDirectory(0, "/home")
	require.NoError(t, err)
	require.Equal(t, []byte("dir data"), got)

	got, err = cache.GetFile(0, "/home/readme.txt")
	require.NoError(t, err)
	require.Equal(t, []byte("file data"), got)
}

// TestScanBatchDelta tests that PutDelta via batch can be read back.
func TestScanBatchDelta(t *testing.T) {
	cache := newScanCache(t)

	blobMAC := objects.MAC{0x01}
	packMAC := objects.MAC{0x02}
	data := []byte("batch delta")

	batch := cache.NewScanBatch()
	require.NoError(t, batch.PutDelta(resources.RT_CHUNK, blobMAC, packMAC, data))
	require.NoError(t, batch.Commit())

	found := false
	for _, got := range cache.GetDelta(resources.RT_CHUNK, blobMAC) {
		require.Equal(t, data, got)
		found = true
	}
	require.True(t, found)
}

// TestScanCacheNewBatch verifies that NewBatch is the same as NewScanBatch.
func TestScanCacheNewBatch(t *testing.T) {
	cache := newScanCache(t)

	batch := cache.NewBatch()
	require.NotNil(t, batch)
	require.NoError(t, batch.Commit())
}

// TestScanCacheGetLatestState verifies it returns NilMac (not an error).
func TestScanCacheGetLatestState(t *testing.T) {
	cache := newScanCache(t)

	mac, err := cache.GetLatestState()
	require.NoError(t, err)
	require.Equal(t, objects.NilMac, mac)
}

// TestScanCachePutDeletedRoundTrip tests PutDeleted / GetDeletedEntries.
func TestScanCachePutDeletedRoundTrip(t *testing.T) {
	cache := newScanCache(t)

	blobMAC := objects.MAC{0xab, 0xcd}
	require.NoError(t, cache.PutDeleted(0, blobMAC, []byte("deleted data")))

	count := 0
	for range cache.GetDeletedEntries() {
		count++
	}
	require.Equal(t, 1, count)
}
