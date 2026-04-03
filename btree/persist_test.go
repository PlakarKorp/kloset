package btree_test

import (
	"cmp"
	"testing"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/stretchr/testify/require"
)

func TestPersist(t *testing.T) {
	order := 3
	store := btree.InMemoryStore_t[rune, int]{}
	tree1, err := btree.New(&store, cmp.Compare, order)
	require.NoError(t, err)

	alphabet := []rune("abcdefghijklmnopqrstuvwxyz")
	for i, r := range alphabet {
		err := tree1.Insert(r, i)
		require.NoError(t, err)
	}

	store2 := btree.InMemoryStore_t[rune, int]{}
	root, err := btree.Persist(tree1, &store2, func(e int) (int, error) { return e, nil })
	require.NoError(t, err)

	tree2, err := btree.FromStorage(root, &store2, cmp.Compare, order)
	require.NoError(t, err)
	require.NotNil(t, tree2)
	for i, r := range alphabet {
		v, found, err := tree2.Find(r)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, v, i)
	}

	nonexist := 'A'
	_, found, err := tree2.Find(nonexist)
	require.NoError(t, err)
	require.False(t, found)

	iter, err := tree2.ScanAll()
	require.NoError(t, err)

	for i, r := range alphabet {
		require.True(t, iter.Next())
		k, v := iter.Current()
		require.Equal(t, k, r)
		require.Equal(t, v, i)
	}

	require.False(t, iter.Next())
}
