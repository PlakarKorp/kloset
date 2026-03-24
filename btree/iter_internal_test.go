package btree

import (
	"cmp"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func newIteratorFWD(
	t *testing.T,
	st *InMemoryStore_t[rune, int],
	cacheSize int,
	ptr int,
	e error,
) (*BTree[rune, int, int], *forwardIter[rune, int, int], *Node[rune, int, int]) {
	t.Helper()

	n, err := st.Get(ptr)
	require.NoError(t, err)

	b := &BTree[rune, int, int]{
		Order:   3,
		Root:    ptr,
		cache:   cachefor[rune, int, int](st, cacheSize),
		compare: cmp.Compare[rune],
	}

	it := &forwardIter[rune, int, int]{
		b:       b,
		ptr:     ptr,
		current: n,
		idx:     -1,
		err:     e,
	}
	return b, it, n
}

func TestForwardIter(t *testing.T) {
	/* -------------------------
	   Err
	-------------------------- */
	t.Run("Err_InitiallyToAnError", func(t *testing.T) {
		oops := errors.New("oops")
		st := InMemoryStore_t[rune, int]{}
		n := Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		ptr, err := st.Put(&n)
		require.NoError(t, err)

		_, it, _ := newIteratorFWD(t, &st, 2, ptr, oops)
		require.ErrorIs(t, it.Err(), oops)
	})

	t.Run("Err_InitiallyNil", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}
		n := Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		ptr, err := st.Put(&n)
		require.NoError(t, err)

		_, it, _ := newIteratorFWD(t, &st, 2, ptr, nil)
		require.NoError(t, it.Err())
	})

	/* -------------------------
	   Next
	-------------------------- */
	t.Run("Next_ValidAccess", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}
		n := Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		ptr, err := st.Put(&n)
		require.NoError(t, err)

		_, it, _ := newIteratorFWD(t, &st, 2, ptr, nil)
		require.True(t, it.Next())
	})

	t.Run("NextFails_BecauseOfPreviousError", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}
		n := Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		ptr, err := st.Put(&n)
		require.NoError(t, err)

		oops := errors.New("oops")
		_, it, _ := newIteratorFWD(t, &st, 2, ptr, oops)

		require.False(t, it.Next())
		require.ErrorIs(t, it.Err(), oops)
	})

	t.Run("Next_AtEnd", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}
		n := Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		ptr, err := st.Put(&n)
		require.NoError(t, err)

		_, it, _ := newIteratorFWD(t, &st, 2, ptr, nil)

		require.True(t, it.Next())
		require.False(t, it.Next())
		require.NoError(t, it.Err())
	})

	t.Run("NextInvalid_WhenCurrentInvalid", func(t *testing.T) {
		it := forwardIter[rune, int, int]{current: nil}
		require.Panics(t, func() { it.Next() })
	})

	t.Run("Next_SetsErrOnCacheMissError", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}
		n2 := Node[rune, int, int]{
			Keys:   []rune{'c'},
			Values: []int{3},
		}
		ptr2, err := st.Put(&n2)
		require.NoError(t, err)

		n1 := Node[rune, int, int]{
			Keys:   []rune{'a', 'b'},
			Values: []int{1, 2},
			Next:   &ptr2,
		}
		ptr1, err := st.Put(&n1)
		require.NoError(t, err)

		getErr := errors.New("Store.Get() failed")
		st.GetFn = func(ptr int) (*Node[rune, int, int], error) {
			if ptr == ptr2 {
				return nil, getErr
			}
			if ptr >= len(st.store) {
				return nil, notfound
			}
			return &st.store[ptr], nil
		}

		// enforce a cache miss on ptr2
		_, it, _ := newIteratorFWD(t, &st, 1, ptr1, nil)

		n, err := st.Get(ptr1)
		require.NoError(t, err)
		require.NoError(t, it.b.cache.lru.Put(ptr1, &cacheitem[rune, int, int]{node: n}))

		require.True(t, it.Next())
		require.True(t, it.Next())
		require.False(t, it.Next())
		require.ErrorIs(t, it.Err(), getErr)
	})

	t.Run("Next_ValidCacheMiss", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}
		n2 := Node[rune, int, int]{
			Keys:   []rune{'c'},
			Values: []int{3},
		}
		ptr2, err := st.Put(&n2)
		require.NoError(t, err)

		n1 := Node[rune, int, int]{
			Keys:   []rune{'a', 'b'},
			Values: []int{1, 2},
			Next:   &ptr2,
		}
		ptr1, err := st.Put(&n1)
		require.NoError(t, err)

		// enforce a cache miss on ptr2
		_, it, n := newIteratorFWD(t, &st, 1, ptr1, nil)
		require.NoError(t, it.b.cache.lru.Put(ptr1, &cacheitem[rune, int, int]{node: n}))

		require.True(t, it.Next())
		require.True(t, it.Next())
		// cache miss valid
		require.True(t, it.Next())
		require.Equal(t, ptr2, it.ptr)
		require.NotNil(t, it.current)
		require.Equal(t, 0, it.idx)
		require.Equal(t, rune('c'), it.current.Keys[it.idx])
		require.Equal(t, 3, it.current.Values[it.idx])
		require.NoError(t, it.Err())
	})

	/* -------------------------
	   Current
	-------------------------- */
	t.Run("CurrentInvalid", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}
		n := Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		ptr, err := st.Put(&n)
		require.NoError(t, err)

		_, it, _ := newIteratorFWD(t, &st, 2, ptr, nil)
		require.Panics(t, func() { _, _ = it.Current() })
	})

	t.Run("CurrentValid", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}
		n := Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		ptr, err := st.Put(&n)
		require.NoError(t, err)

		_, it, _ := newIteratorFWD(t, &st, 2, ptr, nil)

		require.True(t, it.Next())
		k, v := it.Current()
		require.Equal(t, rune('a'), k)
		require.Equal(t, 1, v)
	})

	t.Run("CurrentInvalid_BecauseAtEnd", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}
		n := Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		ptr, err := st.Put(&n)
		require.NoError(t, err)

		_, it, _ := newIteratorFWD(t, &st, 2, ptr, nil)

		require.True(t, it.Next())
		require.False(t, it.Next())
		require.Panics(t, func() { _, _ = it.Current() })
	})
}
