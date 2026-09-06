package storage

import (
	"os"
	"sync"
	"testing"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/stretchr/testify/require"
)

func mkstore[k, v any](t *testing.T, name string) *FileStore[k, v] {
	store, err := New[k, v](t.TempDir(), name)
	require.NoError(t, err)
	return store
}

func TestFileStorePutGetUpdate(t *testing.T) {
	store := mkstore[string, int](t, "test.db")
	defer store.Close()

	node := &btree.Node[string, int, int]{
		Keys:   []string{"alpha", "beta"},
		Values: []int{1, 2},
	}

	idx, err := store.Put(node)
	require.NoError(t, err)

	retrieved, err := store.Get(idx)
	require.NoError(t, err)
	require.Equal(t, node.Keys, retrieved.Keys)
	require.Equal(t, node.Values, retrieved.Values)

	// Update the node with a larger payload than the original, to
	// exercise the "rewritten at a new offset" path.
	retrieved.Keys = []string{"alpha", "beta", "gamma"}
	retrieved.Values = []int{10, 20, 30}
	require.NoError(t, store.Update(idx, retrieved))

	updated, err := store.Get(idx)
	require.NoError(t, err)
	require.Equal(t, []string{"alpha", "beta", "gamma"}, updated.Keys)
	require.Equal(t, []int{10, 20, 30}, updated.Values)
}

func TestFileStoreMultiplePuts(t *testing.T) {
	store := mkstore[int, string](t, "multi.db")
	defer store.Close()

	var indices []int
	for i := range 5 {
		node := &btree.Node[int, int, string]{
			Keys:   []int{i},
			Values: []string{"value"},
		}
		idx, err := store.Put(node)
		require.NoError(t, err)
		indices = append(indices, idx)
	}

	seen := make(map[int]bool)
	for _, idx := range indices {
		require.False(t, seen[idx], "duplicate index %d", idx)
		seen[idx] = true
	}
}

func TestFileStoreGetUnknown(t *testing.T) {
	store := mkstore[string, int](t, "unknown.db")
	defer store.Close()

	_, err := store.Get(0)
	require.Error(t, err)
}

func TestFileStoreCloseRemovesFile(t *testing.T) {
	store := mkstore[string, int](t, "close.db")

	_, err := os.Stat(store.path)
	require.NoError(t, err)

	require.NoError(t, store.Close())

	_, err = os.Stat(store.path)
	require.True(t, os.IsNotExist(err))
}

// TestFileStoreConcurrentPut exercises the bump-allocator path: many
// goroutines Put into the same store concurrently.
func TestFileStoreConcurrentPut(t *testing.T) {
	store := mkstore[int, int](t, "concurrent.db")
	defer store.Close()

	const n = 500
	ids := make([]int, n)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			node := &btree.Node[int, int, int]{
				Keys:   []int{i},
				Values: []int{i * i},
			}
			idx, err := store.Put(node)
			require.NoError(t, err)
			ids[i] = idx
		}(i)
	}
	wg.Wait()

	seen := make(map[int]bool)
	for i, idx := range ids {
		require.False(t, seen[idx], "duplicate index %d", idx)
		seen[idx] = true

		node, err := store.Get(idx)
		require.NoError(t, err)
		require.Equal(t, []int{i}, node.Keys)
		require.Equal(t, []int{i * i}, node.Values)
	}
}
