package btree_test

import (
	"cmp"
	"runtime"
	"testing"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/stretchr/testify/require"
)

func TestNew_NilStore(t *testing.T) {
	empty, err := btree.New[rune, int, string](nil, cmp.Compare, 2)
	require.Error(t, err)
	require.Nil(t, empty)
}

func TestFromStorage(t *testing.T) {
	t.Run("Storage Nil", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				e, _ := r.(runtime.Error)
				require.Errorf(t, e, "Expected panic: %v", e)
			}
		}()
		btree.FromStorage[rune, int, string](42, nil, cmp.Compare, 3)
	})

	t.Run("Empty Storage", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, string]{}
		tree := btree.FromStorage[rune, int, string](42, &storage, cmp.Compare, 3)
		require.NotNil(t, tree)
		require.Equal(t, tree.Root, 42)
		require.Equal(t, tree.Order, 3)
	})
}
