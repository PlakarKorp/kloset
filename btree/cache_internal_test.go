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
		n3 := Node[rune, int, int]{
			Keys:     []rune{'c'},
			Pointers: []int{3},
		}
		n4 := Node[rune, int, int]{
			Keys:     []rune{'d'},
			Pointers: []int{4},
		}
		ptr1, err := st.Put(&n1)
		require.NoError(t, err)
		ptr2, err := st.Put(&n2)
		require.NoError(t, err)
		ptr3, err := st.Put(&n3)
		require.NoError(t, err)
		ptr4, err := st.Put(&n4)
		require.NoError(t, err)

		c := cachefor[rune, int, int](&st, 2)

		// warm up the cache
		for _, ptrn := range []int{ptr1, ptr2, ptr3, ptr4} {
			n, err := st.Get(ptrn)
			require.NoError(t, err)
			require.NoError(t, c.Update(ptrn, n))
		}

		flushErr := errors.New("Store.Update() failed")
		st.UpdateFn = func(ptr int, n *Node[rune, int, int]) error { return flushErr }

		got, err := c.Get(ptr2)
		require.ErrorIs(t, err, flushErr)
		require.Nil(t, got)
	})
}
