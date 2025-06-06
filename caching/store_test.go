package caching

import (
	"testing"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/stretchr/testify/require"
)

func TestDBStore(t *testing.T) {
	// Create a temporary cache manager for testing
	tmpDir := t.TempDir()
	manager := NewManager(tmpDir)
	defer manager.Close()

	// Create a new scan cache
	snapshotID := objects.MAC{1, 2, 3}
	cache, err := newScanCache(manager, snapshotID)
	require.NoError(t, err)
	defer cache.Close()

	// Create a new DBStore
	store := &DBStore[string, int]{
		Prefix: "test_prefix",
		Cache:  cache,
	}

	// Test Get operation
	t.Run("Get Operation", func(t *testing.T) {
		// Create a test node
		node := &btree.Node[string, int, int]{
			Keys:   []string{"test_key"},
			Values: []int{42},
		}

		// Put the node into the store
		idx, err := store.Put(node)
		require.NoError(t, err)

		// Get the node from the store
		retrievedNode, err := store.Get(idx)
		require.NoError(t, err)
		require.Equal(t, node.Keys[0], retrievedNode.Keys[0])
		require.Equal(t, node.Values[0], retrievedNode.Values[0])
	})

	// Test Update operation
	t.Run("Update Operation", func(t *testing.T) {
		// Create a test node
		node := &btree.Node[string, int, int]{
			Keys:   []string{"test_key"},
			Values: []int{42},
		}

		// Put the node into the store
		idx, err := store.Put(node)
		require.NoError(t, err)

		// Update the node
		node.Values[0] = 100
		err = store.Update(idx, node)
		require.NoError(t, err)

		// Get the updated node from the store
		retrievedNode, err := store.Get(idx)
		require.NoError(t, err)
		require.Equal(t, node.Keys[0], retrievedNode.Keys[0])
		require.Equal(t, node.Values[0], retrievedNode.Values[0])
	})

	// Test Put operation
	t.Run("Put Operation", func(t *testing.T) {
		// Create a test node
		node := &btree.Node[string, int, int]{
			Keys:   []string{"test_key"},
			Values: []int{42},
		}

		// Put the node into the store
		idx, err := store.Put(node)
		require.NoError(t, err)
		require.NotZero(t, idx)
	})
}
