package btree

import (
	"cmp"
	"testing"

	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

func TestFromStorage_RootMustExists(t *testing.T) {
	root := 999

	calls := 0
	var firstCall int
	storage := InMemoryStore_t[rune, string]{}
	storage.GetFn = func(ptr int) (*Node[rune, int, string], error) {
		if calls == 0 {
			firstCall = ptr
		}
		calls++
		if ptr >= len(storage.store) {
			return nil, notfound
		}
		return &storage.store[ptr], nil
	}

	tree, err := FromStorage(root, &storage, cmp.Compare, 3)
	require.NoError(t, err)
	require.NotNil(t, tree)
	_, _, err = tree.findleaf(12)

	require.NotZerof(t, calls, "expected Get to be called at least once")
	require.Equalf(t, firstCall, root, "expected first Get(%d), got Get(%d)", root, firstCall)
	require.ErrorIs(t, err, notfound)
}

func TestNewNodeFrom(t *testing.T) {
	t.Run("EmptyNode_FromNothing", func(t *testing.T) {
		n := newNodeFrom[rune, int, int](nil, nil, nil)

		require.Equal(t, versioning.FromString(NODE_VERSION), n.Version)
		require.Len(t, n.Keys, 0)
		require.Len(t, n.Pointers, 0)
		require.Len(t, n.Values, 0)
	})

	t.Run("CheckDeepCopy", func(t *testing.T) {
		keys := []rune{'a', 'b'}
		ptrs := []int{10, 11, 12}
		vals := []int{1, 2}

		n := newNodeFrom[rune, int, int](keys, ptrs, vals)

		require.Equal(t, versioning.FromString(NODE_VERSION), n.Version)
		require.Equal(t, keys, n.Keys)
		require.Equal(t, ptrs, n.Pointers)
		require.Equal(t, vals, n.Values)

		require.NotSame(t, &keys[0], &n.Keys[0])
		require.NotSame(t, &ptrs[0], &n.Pointers[0])
		require.NotSame(t, &vals[0], &n.Values[0])
		keys[0] = 'z'
		ptrs[0] = 99
		vals[0] = 42
		require.Equal(t, []rune{'a', 'b'}, n.Keys)
		require.Equal(t, []int{10, 11, 12}, n.Pointers)
		require.Equal(t, []int{1, 2}, n.Values)
		n.Keys[1] = 'y'
		n.Pointers[1] = 88
		n.Values[1] = 77
		require.Equal(t, []rune{'z', 'b'}, keys)
		require.Equal(t, []int{99, 11, 12}, ptrs)
		require.Equal(t, []int{42, 2}, vals)
	})

	t.Run("CompareTo_cloneNode", func(t *testing.T) {
		prev := 42
		next := 34
		orig := Node[rune, int, int]{
			Version:  versioning.FromString(NODE_VERSION),
			Keys:     []rune{'a', 'b'},
			Pointers: []int{10, 11, 12},
			Values:   []int{1, 2},
			Prev:     &prev,
			Next:     &next,
		}

		from := newNodeFrom(orig.Keys, orig.Pointers, orig.Values)
		cl := cloneNode(&orig)

		require.Equal(t, cl.Version, from.Version)
		require.Equal(t, cl.Keys, from.Keys)
		require.Equal(t, cl.Pointers, from.Pointers)
		require.Equal(t, cl.Values, from.Values)

		require.Nil(t, from.Next)
		require.Equal(t, next, *cl.Next)
		require.Nil(t, from.Prev)
		require.Equal(t, prev, *cl.Prev)

		// Deep copy (backing arrays differ from the original)
		require.NotSame(t, &orig.Keys[0], &from.Keys[0])
		require.NotSame(t, &orig.Keys[0], &cl.Keys[0])

		require.NotSame(t, &orig.Pointers[0], &from.Pointers[0])
		require.NotSame(t, &orig.Pointers[0], &cl.Pointers[0])

		require.NotSame(t, &orig.Values[0], &from.Values[0])
		require.NotSame(t, &orig.Values[0], &cl.Values[0])

		require.NotSame(t, &from.Keys[0], &cl.Keys[0])
		require.NotSame(t, &from.Pointers[0], &cl.Pointers[0])
		require.NotSame(t, &from.Values[0], &cl.Values[0])
	})
}

func Test_split(t *testing.T) {
	// cutoff==0 branch + leaf
	t.Run("CutoffZero_Leaf", func(t *testing.T) {
		n := Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		_ = n.split()
	})

	t.Run("InternalBranch", func(t *testing.T) {
		n := Node[rune, int, int]{
			Keys:     []rune{'b', 'c', 'd'},
			Pointers: []int{10, 11, 12, 13},
			Values:   nil,
		}
		_ = n.split()
	})
}
