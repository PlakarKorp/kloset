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

func newIteratorBWD(
	t *testing.T,
	st *InMemoryStore_t[rune, int],
	cacheSize int,
	ptr int,
	e error,
) (*BTree[rune, int, int], *backwardIter[rune, int, int]) {
	t.Helper()

	b := &BTree[rune, int, int]{
		Order:   3,
		Root:    ptr,
		cache:   cachefor[rune, int, int](st, cacheSize),
		compare: cmp.Compare[rune],
	}

	it := &backwardIter[rune, int, int]{
		b:   b,
		err: e,
	}
	return b, it
}

func TestBackwardIter(t *testing.T) {
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

		_, it := newIteratorBWD(t, &st, 2, ptr, oops)
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

		_, it := newIteratorBWD(t, &st, 2, ptr, nil)
		require.NoError(t, it.Err())
	})

	/* -------------------------
	   dive
	-------------------------- */

	t.Run("Dive_Initialization", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}

		leafL := Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		ptrL, err := st.Put(&leafL)
		require.NoError(t, err)

		leafR := Node[rune, int, int]{
			Keys:   []rune{'b', 'c'},
			Values: []int{2, 3},
		}
		ptrR, err := st.Put(&leafR)
		require.NoError(t, err)

		root := Node[rune, int, int]{
			Keys:     []rune{'b'},
			Pointers: []int{ptrL, ptrR},
		}
		rootPtr, err := st.Put(&root)
		require.NoError(t, err)

		_, it := newIteratorBWD(t, &st, 4, rootPtr, nil)

		require.NoError(t, it.dive(rootPtr))
		require.NotEmpty(t, it.steps)
		require.NotNil(t, it.cur)
		require.Equal(t, leafR.Keys, it.cur.Keys)
		require.Equal(t, len(leafR.Keys), it.steps[len(it.steps)-1].idx)
	})

	t.Run("DiveFails_BecauseStoreGetFails", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}
		n := Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		ptr, err := st.Put(&n)
		require.NoError(t, err)

		_, it := newIteratorBWD(t, &st, 2, ptr, nil)

		getErr := errors.New("Store.Get() failed")
		st.GetFn = func(_ int) (*Node[rune, int, int], error) { return nil, getErr }

		require.ErrorIs(t, it.dive(ptr), getErr)
	})

	/* -------------------------
	   Next
	-------------------------- */
	t.Run("NextFails_BecauseOfPreviousError", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}
		n := Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		ptr, err := st.Put(&n)
		require.NoError(t, err)

		oops := errors.New("oops")
		_, it := newIteratorBWD(t, &st, 2, ptr, oops)

		require.False(t, it.Next())
		require.ErrorIs(t, it.Err(), oops)
	})

	t.Run("NoNext_WhenNoSteps", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}
		n := Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		ptr, err := st.Put(&n)
		require.NoError(t, err)

		_, it := newIteratorBWD(t, &st, 2, ptr, nil)

		require.False(t, it.Next())
		require.NoError(t, it.Err())
	})

	t.Run("Next_CheckLeafIdx_WhenIdxGreaterThanZero", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}
		n := Node[rune, int, int]{
			Keys:   []rune{'a', 'b'},
			Values: []int{1, 2},
		}
		ptr, err := st.Put(&n)
		require.NoError(t, err)

		_, it := newIteratorBWD(t, &st, 2, ptr, nil)
		require.NoError(t, it.dive(ptr))

		before := it.steps[len(it.steps)-1].idx
		require.True(t, it.Next())
		after := it.steps[len(it.steps)-1].idx

		require.Equal(t, before-1, after)
		require.NoError(t, it.Err())
	})

	t.Run("Next_AtEnd", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}
		n := Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		ptr, err := st.Put(&n)
		require.NoError(t, err)

		_, it := newIteratorBWD(t, &st, 2, ptr, nil)
		require.NoError(t, it.dive(ptr))

		require.True(t, it.Next())
		require.False(t, it.Next())
		require.NoError(t, it.Err())
	})

	t.Run("Next_RewindsAndDivesLef_tWhenRighDone", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}

		leafL := Node[rune, int, int]{
			Keys:   []rune{'a', 'b'},
			Values: []int{1, 2},
		}
		ptrL, err := st.Put(&leafL)
		require.NoError(t, err)

		leafR := Node[rune, int, int]{
			Keys:   []rune{'c', 'd'},
			Values: []int{3, 4},
		}
		ptrR, err := st.Put(&leafR)
		require.NoError(t, err)

		root := Node[rune, int, int]{
			Keys:     []rune{'c'},
			Pointers: []int{ptrL, ptrR},
		}
		rootPtr, err := st.Put(&root)
		require.NoError(t, err)

		_, it := newIteratorBWD(t, &st, 4, rootPtr, nil)
		require.NoError(t, it.dive(rootPtr))

		// Dive right to the end
		require.True(t, it.Next())
		require.True(t, it.Next())

		// Now Next rewinds
		require.True(t, it.Next())
		require.NoError(t, it.Err())
		require.Equal(t, leafL.Keys, it.cur.Keys)
		require.Equal(t, leafL.Values, it.cur.Values)
		require.Equal(t, len(leafL.Keys)-1, it.steps[len(it.steps)-1].idx)
	})

	t.Run("NextFails_BecauseStoreGetFailsDuringRewind", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}

		leafL := Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		ptrL, err := st.Put(&leafL)
		require.NoError(t, err)

		leafR := Node[rune, int, int]{
			Keys:   []rune{'b'},
			Values: []int{2},
		}
		ptrR, err := st.Put(&leafR)
		require.NoError(t, err)

		root := Node[rune, int, int]{
			Keys:     []rune{'b'},
			Pointers: []int{ptrL, ptrR},
		}
		rootPtr, err := st.Put(&root)
		require.NoError(t, err)

		_, it := newIteratorBWD(t, &st, 1, rootPtr, nil)
		require.NoError(t, it.dive(rootPtr))

		// Go right once
		require.True(t, it.Next())

		getErr := errors.New("Store.Get() failed")
		st.GetFn = func(ptr int) (*Node[rune, int, int], error) {
			if ptr == rootPtr {
				return nil, getErr
			}
			if ptr >= len(st.store) {
				return nil, notfound
			}
			return &st.store[ptr], nil
		}

		require.False(t, it.Next())
		require.ErrorIs(t, it.Err(), getErr)
	})

	t.Run("Next_RewindLoopBubblesUp", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}

		leafA := Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		ptrA, err := st.Put(&leafA)
		require.NoError(t, err)

		leafB := Node[rune, int, int]{
			Keys:   []rune{'b'},
			Values: []int{2},
		}
		ptrB, err := st.Put(&leafB)
		require.NoError(t, err)

		leafC := Node[rune, int, int]{
			Keys:   []rune{'c'},
			Values: []int{3},
		}
		ptrC, err := st.Put(&leafC)
		require.NoError(t, err)

		internalR := Node[rune, int, int]{
			Keys:     []rune{'c'},
			Pointers: []int{ptrB, ptrC},
		}
		internalPtr, err := st.Put(&internalR)
		require.NoError(t, err)

		root := Node[rune, int, int]{
			Keys:     []rune{'b'},
			Pointers: []int{ptrA, internalPtr},
		}
		rootPtr, err := st.Put(&root)
		require.NoError(t, err)

		_, it := newIteratorBWD(t, &st, 8, rootPtr, nil)
		require.NoError(t, it.dive(rootPtr))

		// Sanity check
		require.NotNil(t, it.cur)
		require.Equal(t, leafC.Keys, it.cur.Keys)
		require.Equal(t, 1, it.steps[len(it.steps)-1].idx)
		require.NoError(t, it.Err())

		// Consume C
		require.True(t, it.Next())
		require.NoError(t, it.Err())
		require.Equal(t, 0, it.steps[len(it.steps)-1].idx, "precondition: last.idx must be 0 before triggering rewind")

		// Rewind to B
		require.True(t, it.Next())
		require.NoError(t, it.Err())
		require.Equal(t, leafB.Keys, it.cur.Keys)
		require.Equal(t, 0, it.steps[len(it.steps)-1].idx)

		// Rewind to A
		require.True(t, it.Next())
		require.NoError(t, it.Err())

		// Check position on A
		require.Equal(t, leafA.Keys, it.cur.Keys)
		require.Equal(t, 0, it.steps[len(it.steps)-1].idx)

		// Check if Internal node popped out of the stack
		require.Equal(t, 2, len(it.steps))
		require.Equal(t, rootPtr, it.steps[0].ptr)
		require.Equal(t, ptrA, it.steps[1].ptr)

		// End of tree
		require.False(t, it.Next())
		require.NoError(t, it.Err())
	})

	/* -------------------------
	   Current
	-------------------------- */
	t.Run("Current_InvalidAccess", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}
		n := Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		ptr, err := st.Put(&n)
		require.NoError(t, err)

		_, it := newIteratorBWD(t, &st, 4, ptr, nil)
		require.NoError(t, it.dive(ptr))

		require.Panics(t, func() { _, _ = it.Current() })
	})

	t.Run("Current_ValidAccess", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}
		n := Node[rune, int, int]{
			Keys:   []rune{'a', 'b'},
			Values: []int{1, 2},
		}
		ptr, err := st.Put(&n)
		require.NoError(t, err)

		_, it := newIteratorBWD(t, &st, 4, ptr, nil)
		require.NoError(t, it.dive(ptr))

		require.True(t, it.Next())
		require.NoError(t, it.Err())

		k, v := it.Current()
		require.Equal(t, rune('b'), k)
		require.Equal(t, 2, v)
	})

	t.Run("Current_ValidAccess_GoUpOnParent", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}

		n := Node[rune, int, int]{
			Keys:   []rune{'a', 'b'},
			Values: []int{1, 2},
		}
		ptr, err := st.Put(&n)
		require.NoError(t, err)

		_, it := newIteratorBWD(t, &st, 4, ptr, nil)
		require.NoError(t, it.dive(ptr))

		require.True(t, it.Next()) // now at 'b'
		require.True(t, it.Next()) // now at 'a'
		require.NoError(t, it.Err())

		k, v := it.Current()
		require.Equal(t, rune('a'), k)
		require.Equal(t, 1, v)
	})

	t.Run("Current_ValidAccess_AfterRewind", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}

		leafL := Node[rune, int, int]{
			Keys:   []rune{'a', 'b'},
			Values: []int{1, 2},
		}
		ptrL, err := st.Put(&leafL)
		require.NoError(t, err)

		leafR := Node[rune, int, int]{
			Keys:   []rune{'c'},
			Values: []int{3},
		}
		ptrR, err := st.Put(&leafR)
		require.NoError(t, err)

		root := Node[rune, int, int]{
			Keys:     []rune{'c'},
			Pointers: []int{ptrL, ptrR},
		}
		rootPtr, err := st.Put(&root)
		require.NoError(t, err)

		_, it := newIteratorBWD(t, &st, 8, rootPtr, nil)
		require.NoError(t, it.dive(rootPtr))

		// Consume leaf right
		require.True(t, it.Next())

		// Rewind on B
		require.True(t, it.Next())
		require.NoError(t, it.Err())

		k, v := it.Current()
		require.Equal(t, rune('b'), k)
		require.Equal(t, 2, v)
	})

	t.Run("Current_ValidAccessAtEnd", func(t *testing.T) {
		st := InMemoryStore_t[rune, int]{}

		n := Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		ptr, err := st.Put(&n)
		require.NoError(t, err)

		_, it := newIteratorBWD(t, &st, 4, ptr, nil)
		require.NoError(t, it.dive(ptr))

		// on A
		require.True(t, it.Next())
		// End of tree
		require.False(t, it.Next())
		require.NoError(t, it.Err())

		k, v := it.Current()
		require.Equal(t, rune('a'), k)
		require.Equal(t, 1, v)
	})
}
