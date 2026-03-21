package btree_test

import (
	"cmp"
	"slices"
	"testing"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/stretchr/testify/require"
)

func TestScanAll(t *testing.T) {
	store := btree.InMemoryStore_t[rune, int]{}
	tree, err := btree.New(&store, cmp.Compare, 3)
	require.NoError(t, err)

	alphabet := []rune("abcdefghijklmnopqrstuvwxyz")
	for i, r := range alphabet {
		err := tree.Insert(r, i)
		require.NoError(t, err)
	}

	iter, err := tree.ScanAll()
	require.NoError(t, err)

	for i, r := range alphabet {
		require.True(t, iter.Next())
		k, v := iter.Current()
		require.Equal(t, k, r)
		require.Equal(t, v, i)
	}

	require.False(t, iter.Next())
}

func TestScanFrom(t *testing.T) {
	store := btree.InMemoryStore_t[rune, int]{}
	tree, err := btree.New(&store, cmp.Compare, 8)
	require.NoError(t, err)

	alphabet := []rune("abcdefghijklmnopqrstuvwxyz")
	for i, r := range alphabet {
		err := tree.Insert(r, i)
		require.NoError(t, err)
	}

	iter, err := tree.ScanFrom(rune('e'))
	require.NoError(t, err)

	for i := 4; i < len(alphabet); i++ {
		r := alphabet[i]
		require.True(t, iter.Next())
		k, v := iter.Current()
		require.Equal(t, k, r)
		require.Equal(t, v, i)
	}

	require.False(t, iter.Next())
}

func TestScanAllReverse(t *testing.T) {
	store := btree.InMemoryStore_t[rune, int]{}
	tree, err := btree.New(&store, cmp.Compare, 3)
	require.NoError(t, err)

	alphabet := []rune("abcdefghijklmnopqrstuvwxyz")
	for i, r := range alphabet {
		err := tree.Insert(r, i)
		require.NoError(t, err)
	}

	iter, err := tree.ScanAllReverse()
	require.NoError(t, err)

	for i := len(alphabet) - 1; i >= 0; i-- {
		r := alphabet[i]
		require.True(t, iter.Next())
		k, v := iter.Current()
		require.Equal(t, k, r)
		require.Equal(t, v, i)
	}

	require.False(t, iter.Next())
}

func TestVisitDFS(t *testing.T) {
	store := btree.InMemoryStore_t[rune, int]{}
	tree, err := btree.New(&store, cmp.Compare, 3)
	require.NoError(t, err)

	alphabet := []rune("abcdefghijklmnopqrstuvwxyz")
	for i, r := range alphabet {
		err := tree.Insert(r, i)
		require.NoError(t, err)
	}

	keySaw := []rune{}
	it := tree.IterDFS()
	for it.Next() {
		_, node := it.Current()
		if len(node.Pointers) == 0 {
			for i := range node.Keys {
				keySaw = append(keySaw, node.Keys[i])
			}
		}
	}
	require.NoError(t, it.Err())
	require.Zero(t, slices.Compare(alphabet, keySaw))
}
