package btree

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Get(t *testing.T) {
	t.Run("Fails_BecauseLRUFlushFails", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}
		n1 := Node[rune, int, int]{
			Keys:     []rune{'a'},
			Pointers: []int{1},
		}
		n2 := Node[rune, int, int]{
			Keys:     []rune{'b'},
			Pointers: []int{2},
		}
		ptr1, err := st.Put(&n1)
		require.NoError(t, err)
		ptr2, err := st.Put(&n2)
		require.NoError(t, err)

		c := cachefor[rune, int, int](&st, 1)
		n, err := st.Get(ptr1)
		require.NoError(t, err)
		require.NoError(t, c.Update(ptr1, n))

		flushErr := errors.New("Store.Update() failed")
		st.UpdateFn = func(ptr int, n *Node[rune, int, int]) error { return flushErr }

		got, err := c.Get(ptr2)
		require.ErrorIs(t, err, flushErr)
		require.Nil(t, got)
	})
}
