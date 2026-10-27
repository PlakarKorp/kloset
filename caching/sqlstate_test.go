package caching_test

import (
	"testing"
	"time"

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

// inTx runs fn in its own transaction and commits it: writes to the state
// cache only go through transactions.
func inTx(t *testing.T, st *caching.SQLState, fn func(tx *caching.SQLStateTx) error) {
	t.Helper()

	tx, err := st.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, fn(tx))
	require.NoError(t, tx.Commit())
}

func hasState(st *caching.SQLState, mac objects.MAC) (bool, error) {
	tx, err := st.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	return tx.HasState(mac)
}

func TestSQLStateRollbackDiscardsWrites(t *testing.T) {
	st := newSQLState(t)

	stateMAC := objects.MAC{0x01}
	blobMAC := objects.MAC{0x02}
	packMAC := objects.MAC{0x03}

	tx, err := st.Begin()
	require.NoError(t, err)
	require.NoError(t, tx.PutDelta(resources.RT_CHUNK, blobMAC, packMAC, 0, 10))
	require.NoError(t, tx.PutPackfile(packMAC, stateMAC, time.Now()))
	require.NoError(t, tx.PutColoured(resources.RT_SNAPSHOT, blobMAC, time.Now()))
	require.NoError(t, tx.PutState(stateMAC, []byte("metadata")))
	require.NoError(t, tx.Rollback())

	ok, err := hasState(st, stateMAC)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = st.HasPackfile(packMAC)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = st.HasColoured(resources.RT_SNAPSHOT, blobMAC)
	require.NoError(t, err)
	require.False(t, ok)

	for range st.GetDeltas() {
		t.Fatal("rolled back delta is visible")
	}
}

func TestSQLStateStateRoundTrip(t *testing.T) {
	st := newSQLState(t)

	mac := objects.MAC{1, 2, 3}
	data := []byte("state payload")

	// Put
	inTx(t, st, func(tx *caching.SQLStateTx) error { return tx.PutState(mac, data) })

	// Has
	ok, err := hasState(st, mac)
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

	ok, err = hasState(st, mac)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestSQLStateGetLatestStateEmpty(t *testing.T) {
	st := newSQLState(t)

	latest, err := st.GetLatestState()
	require.NoError(t, err)
	require.Equal(t, objects.NilMac, latest)
}

func TestSQLStateDeltaOperations(t *testing.T) {
	st := newSQLState(t)

	blobMAC := objects.MAC{0x01}
	packMAC := objects.MAC{0x02}

	// Direct PutDelta
	inTx(t, st, func(tx *caching.SQLStateTx) error { return tx.PutDelta(resources.RT_CHUNK, blobMAC, packMAC, 42, 1337) })

	// GetDeltasByType round-trips the typed columns
	count := 0
	for b, err := range st.GetDeltasByType(resources.RT_CHUNK) {
		require.NoError(t, err)
		require.Equal(t, blobMAC, b.MAC)
		require.Equal(t, resources.RT_CHUNK, b.Type)
		require.Equal(t, packMAC, b.Packfile)
		require.Equal(t, uint64(42), b.Offset)
		require.Equal(t, uint32(1337), b.Length)
		count++
	}
	require.Equal(t, 1, count)

	// GetDeltas
	count = 0
	for _, err := range st.GetDeltas() {
		require.NoError(t, err)
		count++
	}
	require.Equal(t, 1, count)

	// DelDelta
	inTx(t, st, func(tx *caching.SQLStateTx) error { return tx.DelDelta(resources.RT_CHUNK, blobMAC, packMAC) })

	count = 0
	for _, err := range st.GetDeltas() {
		require.NoError(t, err)
		count++
	}
	require.Equal(t, 0, count)
}

func TestSQLStateColouredOperations(t *testing.T) {
	st := newSQLState(t)

	blobMAC := objects.MAC{0xcc}
	when := time.Unix(0, 1700000000000000000).UTC()

	// Not present yet
	ok, err := st.HasColoured(resources.RT_CHUNK, blobMAC)
	require.NoError(t, err)
	require.False(t, ok)

	// Put
	inTx(t, st, func(tx *caching.SQLStateTx) error { return tx.PutColoured(resources.RT_CHUNK, blobMAC, when) })

	ok, err = st.HasColoured(resources.RT_CHUNK, blobMAC)
	require.NoError(t, err)
	require.True(t, ok)

	// GetColouredEntries round-trips the typed columns
	count := 0
	for cb, err := range st.GetColouredEntries() {
		require.NoError(t, err)
		require.Equal(t, blobMAC, cb.MAC)
		require.Equal(t, resources.RT_CHUNK, cb.Type)
		require.Equal(t, when.UnixNano(), cb.When.UnixNano())
		count++
	}
	require.Equal(t, 1, count)

	// GetColouredEntriesByType
	count = 0
	for _, err := range st.GetColouredEntriesByType(resources.RT_CHUNK) {
		require.NoError(t, err)
		count++
	}
	require.Equal(t, 1, count)

	// Wrong type returns nothing
	count = 0
	for _, err := range st.GetColouredEntriesByType(resources.RT_PACKFILE) {
		require.NoError(t, err)
		count++
	}
	require.Equal(t, 0, count)

	// Del
	inTx(t, st, func(tx *caching.SQLStateTx) error { return tx.DelColoured(resources.RT_CHUNK, blobMAC) })

	ok, err = st.HasColoured(resources.RT_CHUNK, blobMAC)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestSQLStatePackfileOperations(t *testing.T) {
	st := newSQLState(t)

	packMAC := objects.MAC{0xde}
	stateMAC := objects.MAC{0xad}
	when := time.Unix(0, 1700000000000000000).UTC()

	// Not present
	ok, err := st.HasPackfile(packMAC)
	require.NoError(t, err)
	require.False(t, ok)

	// Put
	inTx(t, st, func(tx *caching.SQLStateTx) error { return tx.PutPackfile(packMAC, stateMAC, when) })

	ok, err = st.HasPackfile(packMAC)
	require.NoError(t, err)
	require.True(t, ok)

	// GetPackfiles round-trips the typed columns
	count := 0
	for p, err := range st.GetPackfiles() {
		require.NoError(t, err)
		require.Equal(t, packMAC, p.MAC)
		require.Equal(t, stateMAC, p.State)
		require.Equal(t, when.UnixNano(), p.Timestamp.UnixNano())
		count++
	}
	require.Equal(t, 1, count)

	// DelPackfile (also cascades deltas for that packfile)
	inTx(t, st, func(tx *caching.SQLStateTx) error { return tx.DelPackfile(packMAC) })

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

func TestSQLStateReadOnly(t *testing.T) {
	dir := t.TempDir()

	// First create the database in write mode.
	st, err := caching.NewSQLState(dir, false)
	require.NoError(t, err)
	inTx(t, st, func(tx *caching.SQLStateTx) error {
		return tx.PutPackfile(objects.MAC{0x01}, objects.MAC{0x02}, time.Now())
	})

	// Open read-only.
	ro, err := caching.NewSQLState(dir, true)
	require.NoError(t, err)

	ok, err := ro.HasPackfile(objects.MAC{0x01})
	require.NoError(t, err)
	require.True(t, ok)
}
