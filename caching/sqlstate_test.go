package caching_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/stretchr/testify/require"
)

func newSQLState(t *testing.T) *caching.SQLState {
	t.Helper()
	dir := t.TempDir()
	st, err := caching.NewSQLState(dir, false)
	require.NoError(t, err)
	return st
}

func TestSQLStateStateRoundTrip(t *testing.T) {
	st := newSQLState(t)

	mac := objects.MAC{1, 2, 3}
	data := []byte("state payload")

	// Put
	require.NoError(t, st.PutState(mac, data))

	// Has
	ok, err := st.HasState(mac)
	require.NoError(t, err)
	require.True(t, ok)

	// Get
	got, err := st.GetState(mac)
	require.NoError(t, err)
	require.Equal(t, data, got)

	// GetLatestState
	latest, err := st.GetLatestState()
	require.NoError(t, err)
	require.Equal(t, mac, latest)

	// GetStates
	all, err := st.GetStates()
	require.NoError(t, err)
	require.Len(t, all, 1)
	require.Equal(t, data, all[mac])

	// Del
	require.NoError(t, st.DelState(mac))

	ok, err = st.HasState(mac)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestSQLStateGetLatestStateEmpty(t *testing.T) {
	st := newSQLState(t)

	latest, err := st.GetLatestState()
	require.NoError(t, err)
	require.Equal(t, objects.NilMac, latest)
}

func TestSQLStateBatchPutDeltaCommit(t *testing.T) {
	st := newSQLState(t)

	blobMAC := objects.MAC{0xaa}
	packMAC := objects.MAC{0xbb}
	payload := []byte("delta data")

	batch := st.NewBatch()
	require.NoError(t, batch.PutDelta(resources.RT_CHUNK, blobMAC, packMAC, payload))
	require.Equal(t, uint32(1), batch.Count())
	require.NoError(t, batch.Commit())

	// Verify via GetDelta
	found := false
	for _, got := range st.GetDelta(resources.RT_CHUNK, blobMAC) {
		require.Equal(t, payload, got)
		found = true
	}
	require.True(t, found)
}

func TestSQLStateBatchEmptyCommit(t *testing.T) {
	st := newSQLState(t)
	batch := st.NewBatch()
	require.Equal(t, uint32(0), batch.Count())
	require.NoError(t, batch.Commit())
}

func TestSQLStateDeltaOperations(t *testing.T) {
	st := newSQLState(t)

	blobMAC := objects.MAC{0x01}
	packMAC := objects.MAC{0x02}
	data := []byte("delta payload")

	// Direct PutDelta
	require.NoError(t, st.PutDelta(resources.RT_CHUNK, blobMAC, packMAC, data))

	// GetDeltasByType
	count := 0
	for _, _ = range st.GetDeltasByType(resources.RT_CHUNK) {
		count++
	}
	require.Equal(t, 1, count)

	// GetDeltas
	count = 0
	for _, _ = range st.GetDeltas() {
		count++
	}
	require.Equal(t, 1, count)

	// DelDelta
	require.NoError(t, st.DelDelta(resources.RT_CHUNK, blobMAC, packMAC))

	count = 0
	for _, _ = range st.GetDeltas() {
		count++
	}
	require.Equal(t, 0, count)
}

func TestSQLStateColouredOperations(t *testing.T) {
	st := newSQLState(t)

	blobMAC := objects.MAC{0xcc}
	data := []byte("coloured data")

	// Not present yet
	ok, err := st.HasColoured(resources.RT_CHUNK, blobMAC)
	require.NoError(t, err)
	require.False(t, ok)

	// Put
	require.NoError(t, st.PutColoured(resources.RT_CHUNK, blobMAC, data))

	ok, err = st.HasColoured(resources.RT_CHUNK, blobMAC)
	require.NoError(t, err)
	require.True(t, ok)

	// GetColouredEntries
	count := 0
	for _, _ = range st.GetColouredEntries() {
		count++
	}
	require.Equal(t, 1, count)

	// GetColouredEntriesByType
	count = 0
	for _, _ = range st.GetColouredEntriesByType(resources.RT_CHUNK) {
		count++
	}
	require.Equal(t, 1, count)

	// Wrong type returns nothing
	count = 0
	for _, _ = range st.GetColouredEntriesByType(resources.RT_PACKFILE) {
		count++
	}
	require.Equal(t, 0, count)

	// Del
	require.NoError(t, st.DelColoured(resources.RT_CHUNK, blobMAC))

	ok, err = st.HasColoured(resources.RT_CHUNK, blobMAC)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestSQLStatePackfileOperations(t *testing.T) {
	st := newSQLState(t)

	packMAC := objects.MAC{0xde}
	data := []byte("packfile data")

	// Not present
	ok, err := st.HasPackfile(packMAC)
	require.NoError(t, err)
	require.False(t, ok)

	// Put
	require.NoError(t, st.PutPackfile(packMAC, data))

	ok, err = st.HasPackfile(packMAC)
	require.NoError(t, err)
	require.True(t, ok)

	// GetPackfiles
	count := 0
	for _, _ = range st.GetPackfiles() {
		count++
	}
	require.Equal(t, 1, count)

	// DelPackfile (also cascades deltas for that packfile)
	require.NoError(t, st.DelPackfile(packMAC))

	ok, err = st.HasPackfile(packMAC)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestSQLStateConfigurationOperations(t *testing.T) {
	st := newSQLState(t)

	// Put and Get
	require.NoError(t, st.PutConfiguration("key1", []byte("value1")))
	require.NoError(t, st.PutConfiguration("key2", []byte("value2")))

	got, err := st.GetConfiguration("key1")
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), got)

	// GetConfigurations iterates all
	count := 0
	for range st.GetConfigurations() {
		count++
	}
	require.Equal(t, 2, count)
}

func TestSQLStatePanics(t *testing.T) {
	st := newSQLState(t)

	require.Panics(t, func() {
		_ = st.PutDeleted(0, objects.MAC{}, nil)
	})
	require.Panics(t, func() {
		for range st.GetDeletedEntries() {
		}
	})
}

func TestSQLStateReadOnly(t *testing.T) {
	dir := t.TempDir()

	// First create the database in write mode.
	st, err := caching.NewSQLState(dir, false)
	require.NoError(t, err)
	require.NoError(t, st.PutPackfile(objects.MAC{0x01}, []byte("pack")))

	// Open read-only.
	ro, err := caching.NewSQLState(dir, true)
	require.NoError(t, err)

	ok, err := ro.HasPackfile(objects.MAC{0x01})
	require.NoError(t, err)
	require.True(t, ok)
}
