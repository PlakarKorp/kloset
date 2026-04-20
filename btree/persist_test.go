package btree_test

import (
	"cmp"
	"errors"
	"fmt"
	"testing"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/stretchr/testify/require"
)

type countingStore[K comparable, V any] struct {
	inner *btree.InMemoryStore_t[K, V]

	putCalls    int
	getCalls    int
	updateCalls int
	closeCalls  int
}

func (s *countingStore[K, V]) Get(ptr int) (*btree.Node[K, int, V], error) {
	s.getCalls++
	return s.inner.Get(ptr)
}

func (s *countingStore[K, V]) Put(n *btree.Node[K, int, V]) (int, error) {
	s.putCalls++
	return s.inner.Put(n)
}

func (s *countingStore[K, V]) Update(ptr int, n *btree.Node[K, int, V]) error {
	s.updateCalls++
	return s.inner.Update(ptr, n)
}

func (s *countingStore[K, V]) Close() error {
	s.closeCalls++
	return s.inner.Close()
}

func TestPersist(t *testing.T) {
	t.Run("CopiesAllKeysValues_AndDoesNotReadDestination", func(t *testing.T) {
		src := btree.InMemoryStore_t[rune, int]{}
		dstInner := btree.InMemoryStore_t[rune, string]{}
		dst := &countingStore[rune, string]{inner: &dstInner}

		keys := []rune("abcdefghijklmnopqrstuvwxyz")
		tree := buildTreeRunes(t, &src, 3, keys)

		conv := func(v int) (string, error) { return fmt.Sprintf("v=%d", v), nil }

		rootPtr, err := btree.Persist[rune, int, int, int, string](tree, dst, conv)
		require.NoError(t, err)

		require.Greater(t, dst.putCalls, 0)
		require.Equal(t, 0, dst.getCalls)
		require.Equal(t, 0, dst.updateCalls)
		require.Equal(t, 0, dst.closeCalls)

		out, err := btree.FromStorage[rune, int, string](rootPtr, &dstInner, cmp.Compare, 3)
		require.NoError(t, err)
		it, err := out.ScanAll()
		require.NoError(t, err)

		for i, k := range keys {
			require.True(t, it.Next())
			gotK, gotV := it.Current()
			require.Equal(t, k, gotK)
			require.Equal(t, fmt.Sprintf("v=%d", i), gotV)
		}
		require.False(t, it.Next())
		require.NoError(t, it.Err())
	})

	t.Run("Fails_BecauseConversionFails", func(t *testing.T) {
		src := btree.InMemoryStore_t[rune, int]{}
		dst := btree.InMemoryStore_t[rune, string]{}

		tree := buildTreeRunes(t, &src, 3, []rune{'a', 'b', 'c'})

		boom := errors.New("conversion failed")
		conv := func(v int) (string, error) {
			if v == 1 {
				return "", boom
			}
			return fmt.Sprintf("%d", v), nil
		}

		_, err := btree.Persist[rune, int, int, int, string](tree, &dst, conv)
		require.ErrorIs(t, err, boom)
	})
}
