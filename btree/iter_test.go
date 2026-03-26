package btree_test

import (
	"cmp"
	"errors"
	"slices"
	"testing"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/stretchr/testify/require"
)

func BuildTreeRunes(
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

func TestScanAll(t *testing.T) {
	t.Run("IteratesInOrder", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}
		keys := []rune("abcdefghijklmnopqrstuvwxyz")
		tree := BuildTreeRunes(t, &st, 3, keys)

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
		tree := BuildTreeRunes(t, &st, 3, keys)

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
	t.Run("StartsExactlyAtKeyIfKeyExists", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}
		keys := []rune("abcdefghijklmnopqrstuvwxyz")
		tree := BuildTreeRunes(t, &st, 8, keys)

		it, err := tree.ScanFrom('e')
		require.NoError(t, err)
		require.NoError(t, it.Err())

		for i := 4; i < len(keys); i++ {
			require.True(t, it.Next())
			gotK, gotV := it.Current()
			require.Equal(t, keys[i], gotK)
			require.Equal(t, i, gotV)
		}

		require.False(t, it.Next())
		require.NoError(t, it.Err())
	})

	t.Run("StartsAtFirstGreaterIfKeyMissing_inSameNode", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}
		keys := []rune{'a', 'c', 'e', 'g', 'i'}
		tree := BuildTreeRunes(t, &st, 8, keys)

		it, err := tree.ScanFrom('d')
		require.NoError(t, err)
		require.NoError(t, it.Err())

		require.True(t, it.Next())
		gotK, gotV := it.Current()
		require.Equal(t, rune('e'), gotK)
		require.Equal(t, 2, gotV)

		require.True(t, it.Next())
		gotK, gotV = it.Current()
		require.Equal(t, rune('g'), gotK)
		require.Equal(t, 3, gotV)
	})

	t.Run("StartsAtFirstGreaterIfKeyMissing_inAnotherNode", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}
		keys := []rune("abdefghijklmnopqrstuvwxyz")
		tree := BuildTreeRunes(t, &st, 3, keys)

		it, err := tree.ScanFrom('c')
		require.NoError(t, err)
		require.NoError(t, it.Err())

		require.True(t, it.Next())
		gotK, _ := it.Current()
		require.Equal(t, rune('d'), gotK)
	})

	t.Run("EmptyIteratorIfKeyGreaterThanMax", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}
		tree := BuildTreeRunes(t, &st, 8, []rune("abc"))

		it, err := tree.ScanFrom('z')
		require.NoError(t, err)
		require.NoError(t, it.Err())

		require.False(t, it.Next())
		require.NoError(t, it.Err())
	})

	t.Run("ScanFrom_ReturnsError_WhenFindLeafFails", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}
		keys := []rune("abcdefghijklmnopqrstuvwxyz")
		tree := BuildTreeRunes(t, &st, 8, keys)

		fresh, err := btree.FromStorage[rune, int, int](tree.Root, &st, cmp.Compare, 3)
		require.NoError(t, err)
		require.NotNil(t, fresh)

		getErr := errors.New("Store.Get() failed")
		st.GetFn = func(ptr int) (*btree.Node[rune, int, int], error) {
			return nil, getErr
		}

		_, err = fresh.ScanFrom('m')
		require.ErrorIs(t, err, getErr)
	})

	t.Run("Fails_BecauseKeyMissing_AndCacheGetOnNextLeafFails", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}

		leaf1 := btree.Node[rune, int, int]{
			Keys:   []rune{'a', 'b'},
			Values: []int{1, 2},
		}
		ptr1, err := st.Put(&leaf1)
		require.NoError(t, err)

		leaf2 := btree.Node[rune, int, int]{
			Keys:   []rune{'d', 'e'},
			Values: []int{4, 5},
		}
		ptr2, err := st.Put(&leaf2)
		require.NoError(t, err)

		n1, err := st.Get(ptr1)
		require.NoError(t, err)
		n1.Next = &ptr2
		require.NoError(t, st.Update(ptr1, n1))

		root := btree.Node[rune, int, int]{
			Keys:     []rune{'d'},
			Pointers: []int{ptr1, ptr2},
		}
		rootPtr, err := st.Put(&root)
		require.NoError(t, err)

		b, err := btree.FromStorage[rune, int, int](rootPtr, &st, cmp.Compare, 3)
		require.NoError(t, err)
		require.NotNil(t, b)

		getErr := errors.New("Store.Get() failed")
		st.GetFn = func(ptr int) (*btree.Node[rune, int, int], error) {
			if ptr == ptr2 {
				return nil, getErr
			}
			prev := st.GetFn
			st.GetFn = nil
			n, err := st.Get(ptr)
			st.GetFn = prev
			return n, err
		}

		_, err = b.ScanFrom('c')
		require.ErrorIs(t, err, getErr)
	})
}

func TestScanAllReverse(t *testing.T) {
	t.Run("IteratesInReverseOrder", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}
		keys := []rune("abcdefghijklmnopqrstuvwxyz")
		tree := BuildTreeRunes(t, &st, 3, keys)

		it, err := tree.ScanAllReverse()
		require.NoError(t, err)
		require.NoError(t, it.Err())

		// Expect reverse order
		for i := len(keys) - 1; i >= 0; i-- {
			require.True(t, it.Next())
			gotK, gotV := it.Current()
			require.Equal(t, keys[i], gotK)
			require.Equal(t, i, gotV)
		}

		require.False(t, it.Next())
		require.NoError(t, it.Err())
	})

	t.Run("Fails_BecauseStoreGetFails", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}
		keys := []rune("abcdefghijklmnopqrstuvwxyz")
		tree := BuildTreeRunes(t, &st, 3, keys)

		fresh, err := btree.FromStorage[rune, int, int](tree.Root, &st, cmp.Compare, 3)
		require.NoError(t, err)

		getErr := errors.New("Store.Get() failed")
		st.GetFn = func(_ int) (*btree.Node[rune, int, int], error) { return nil, getErr }

		_, err = fresh.ScanAllReverse()
		require.ErrorIs(t, err, getErr)
	})
}

func TestIterDFS(t *testing.T) {
	t.Run("VisitsAllLeafKeysInOrder", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}
		keys := []rune("abcdefghijklmnopqrstuvwxyz")
		tree := BuildTreeRunes(t, &st, 3, keys)

		it := tree.IterDFS()

		var keysSaw []rune
		for it.Next() {
			_, node := it.Current()
			if len(node.Pointers) == 0 {
				keysSaw = append(keysSaw, node.Keys...)
			}
		}
		require.NoError(t, it.Err())
		require.Zero(t, slices.Compare(keys, keysSaw))
	})

	t.Run("Fails_BecauseStoreGetFails", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}
		keys := []rune("abcdefghijklmnopqrstuvwxyz")
		tree := BuildTreeRunes(t, &st, 3, keys)

		fresh, err := btree.FromStorage[rune, int, int](tree.Root, &st, cmp.Compare, 3)
		require.NoError(t, err)

		getErr := errors.New("Store.Get() failed")
		st.GetFn = func(_ int) (*btree.Node[rune, int, int], error) { return nil, getErr }

		it := fresh.IterDFS()
		require.False(t, it.Next())
		require.ErrorIs(t, it.Err(), getErr)
	})
}
