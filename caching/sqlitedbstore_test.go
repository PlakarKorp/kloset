package caching_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/PlakarKorp/kloset/caching"
	"github.com/stretchr/testify/require"
)

func TestSQLiteDBStorePutGetUpdate(t *testing.T) {
	dir := t.TempDir()
	store, err := caching.NewSQLiteDBStore[string, int](dir, "test.db")
	require.NoError(t, err)
	defer store.Close()

	node := &btree.Node[string, int, int]{
		Keys:   []string{"alpha", "beta"},
		Values: []int{1, 2},
	}

	idx, err := store.Put(node)
	require.NoError(t, err)
	require.Greater(t, idx, 0)

	retrieved, err := store.Get(idx)
	require.NoError(t, err)
	require.Equal(t, node.Keys, retrieved.Keys)
	require.Equal(t, node.Values, retrieved.Values)

	// Update the node
	retrieved.Values = []int{10, 20}
	require.NoError(t, store.Update(idx, retrieved))

	updated, err := store.Get(idx)
	require.NoError(t, err)
	require.Equal(t, []int{10, 20}, updated.Values)
}

func TestSQLiteDBStoreMultiplePuts(t *testing.T) {
	dir := t.TempDir()
	store, err := caching.NewSQLiteDBStore[int, string](dir, "multi.db")
	require.NoError(t, err)
	defer store.Close()

	var indices []int
	for i := 0; i < 5; i++ {
		node := &btree.Node[int, int, string]{
			Keys:   []int{i},
			Values: []string{"value"},
		}
		idx, err := store.Put(node)
		require.NoError(t, err)
		indices = append(indices, idx)
	}

	// All indices should be distinct
	seen := make(map[int]bool)
	for _, idx := range indices {
		require.False(t, seen[idx], "duplicate index %d", idx)
		seen[idx] = true
	}
}

func TestSQLiteDBStoreClose(t *testing.T) {
	dir := t.TempDir()
	store, err := caching.NewSQLiteDBStore[string, int](dir, "close.db")
	require.NoError(t, err)
	require.NoError(t, store.Close())
}

