package btree_test

import (
	"cmp"
	"errors"
	"slices"
	"testing"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/stretchr/testify/require"
)

func TestScanAll(t *testing.T) {
	buildTreeRunes := func(
		t *testing.T,
		st *btree.InMemoryStore_t[rune, int],
		order int,
		keys []rune,
	) *btree.BTree[rune, int, int] {
		t.Helper()

		tree, err := btree.New[rune, int, int](st, cmp.Compare, order)
		require.NoError(t, err)

		for i, k := range keys {
			require.NoError(t, tree.Insert(k, i))
		}
		return tree
	}

	t.Run("IteratesInOrder", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}
		keys := []rune("abcdefghijklmnopqrstuvwxyz")
		tree := buildTreeRunes(t, &st, 3, keys)

		it, err := tree.ScanAll()
		require.NoError(t, err)
		require.NoError(t, it.Err())

		for i, k := range keys {
			require.True(t, it.Next())
			gotK, gotV := it.Current()
			require.Equal(t, k, gotK)
			require.Equal(t, i, gotV)
		}

		require.False(t, it.Next())
		require.NoError(t, it.Err())
	})

	t.Run("Fails_BecauseStoreGetFails", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}
		keys := []rune("abcdefghijklmnopqrstuvwxyz")
		tree := buildTreeRunes(t, &st, 3, keys)

		fresh, err := btree.FromStorage[rune, int, int](tree.Root, &st, cmp.Compare, 3)
		require.NoError(t, err)
		require.NotNil(t, fresh)

		getErr := errors.New("Store.Get() failed")
		st.GetFn = func(ptr int) (*btree.Node[rune, int, int], error) { return nil, getErr }

		_, err = fresh.ScanAll()
		require.ErrorIs(t, err, getErr)
	})
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
