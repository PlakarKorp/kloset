package caching

import (
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/stretchr/testify/require"
)

// TestDelPackfileCascadesAndPurges checks the raw tables after deleting a
// packfile: every read joins packfiles, so leftover deltas of a purged packfile
// would be invisible through the API and only show up here.
func TestDelPackfileCascadesAndPurges(t *testing.T) {
	st, err := NewSQLState(t.TempDir(), false)
	require.NoError(t, err)
	defer st.Close()

	count := func(query string) int {
		t.Helper()
		var n int
		require.NoError(t, st.db.QueryRow(query).Scan(&n))
		return n
	}

	inTx := func(fn func(tx *SQLStateTx) error) {
		t.Helper()
		tx, err := st.Begin()
		require.NoError(t, err)
		defer tx.Rollback()
		require.NoError(t, fn(tx))
		require.NoError(t, tx.Commit())
	}

	packA, packB := objects.MAC{0xaa}, objects.MAC{0xbb}
	blobB := objects.MAC{0x03}

	inTx(func(tx *SQLStateTx) error {
		for _, d := range []struct{ blob, pack objects.MAC }{
			{objects.MAC{0x01}, packA},
			{objects.MAC{0x02}, packA},
			{blobB, packB},
		} {
			if err := tx.PutDelta(resources.RT_CHUNK, d.blob, d.pack, 0, 1); err != nil {
				return err
			}
		}
		if err := tx.PutPackfile(packA, objects.MAC{0xff}, time.Now()); err != nil {
			return err
		}
		return tx.PutPackfile(packB, objects.MAC{0xff}, time.Now())
	})
	require.Equal(t, 3, count("SELECT count(*) FROM deltas"))
	require.Equal(t, 2, count("SELECT count(*) FROM packfiles"))

	inTx(func(tx *SQLStateTx) error { return tx.DelPackfile(packA) })

	require.Equal(t, 1, count("SELECT count(*) FROM deltas"), "packA's deltas must be gone")
	require.Equal(t, 1, count("SELECT count(*) FROM packfiles"), "packA must be purged")
	require.Equal(t, 0, count("SELECT count(*) FROM packfiles WHERE deleted = 1"),
		"no soft-deleted packfile may survive a commit")

	var blobs []objects.MAC
	for b, err := range st.GetDeltas() {
		require.NoError(t, err)
		blobs = append(blobs, b.MAC)
	}
	require.Equal(t, []objects.MAC{blobB}, blobs)
}

func TestPackfileIDCacheClearsWhenFull(t *testing.T) {
	c := newPackfileIDCache()

	mac := func(i int) objects.MAC {
		var m objects.MAC
		m[0], m[1], m[2] = byte(i), byte(i>>8), byte(i>>16)
		return m
	}

	_, ok := c.Get(mac(0))
	require.False(t, ok)

	for i := 0; i < cacheSz; i++ {
		c.Put(mac(i), int64(i))
	}
	v, ok := c.Get(mac(42))
	require.True(t, ok)
	require.Equal(t, int64(42), v)

	// One past the threshold: the map is cleared and only the new entry is
	// left.
	c.Put(mac(cacheSz), int64(cacheSz))
	require.Len(t, c.m, 1)

	v, ok = c.Get(mac(cacheSz))
	require.True(t, ok)
	require.Equal(t, int64(cacheSz), v)

	_, ok = c.Get(mac(42))
	require.False(t, ok)
}
