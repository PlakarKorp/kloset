package btree_test

import (
	"bytes"
	"cmp"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/stretchr/testify/require"
)

func TestVerify(t *testing.T) {
	var stdOutMutex sync.Mutex

	stdOutCapture := func(t *testing.T, fn func()) string {
		t.Helper()

		stdOutMutex.Lock()
		defer stdOutMutex.Unlock()

		old := os.Stdout
		r, w, err := os.Pipe()
		require.NoError(t, err)
		defer r.Close()

		os.Stdout = w

		done := make(chan string, 1)
		go func() {
			var buf bytes.Buffer
			_, _ = io.Copy(&buf, r)
			done <- buf.String()
		}()

		fn()

		_ = w.Close()
		os.Stdout = old

		return <-done
	}

	t.Run("Root is a leaf", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New[rune, int, int](&st, cmp.Compare, 3)
		require.NoError(t, err)

		out := stdOutCapture(t, func() { require.NoError(t, tree.Verify()) })
		require.NotContains(t, out, "Verify ended")
	})

	t.Run("Filled tree", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}
		tree := buildTreeRunes(t, &st, 3, []rune("abcdefghijklmnopqrstuvwxyz"))

		out := stdOutCapture(t, func() { require.NoError(t, tree.Verify()) })
		require.Contains(t, out, "Verify ended, visited")
	})

	t.Run("Fails_WhenRootCannotBeLoaded", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}
		tree := buildTreeRunes(t, &st, 3, []rune("abcdefghijklmnopqrstuvwxyz"))

		// empty cache => Verify must call store.Get(root)
		fresh, err := btree.FromStorage[rune, int, int](tree.Root, &st, cmp.Compare, 3)
		require.NoError(t, err)

		getErr := errors.New("Store.Get() failed")
		rootPtr := tree.Root

		st.GetFn = func(ptr int) (*btree.Node[rune, int, int], error) {
			if ptr == rootPtr {
				return nil, getErr
			}
			prev := st.GetFn
			st.GetFn = nil
			defer func() { st.GetFn = prev }()
			return st.Get(ptr)
		}

		out := stdOutCapture(t, func() {
			err := fresh.Verify()
			require.Error(t, err)
			require.Contains(t, err.Error(), "failed to get root node")
			require.ErrorIs(t, err, getErr)
		})
		require.NotContains(t, out, "Verify ended, visited")
	})

	t.Run("Fails_WhenChildCannotBeLoaded_DuringTraversal", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}
		tree := buildTreeRunes(t, &st, 3, []rune("abcdefghijklmnopqrstuvwxyz"))

		// empty cache => Verify must call store.Get(internal node)
		fresh, err := btree.FromStorage[rune, int, int](tree.Root, &st, cmp.Compare, 3)
		require.NoError(t, err)

		boom := errors.New("boom: child get failed")
		rootPtr := tree.Root
		getCalls := 0

		st.GetFn = func(ptr int) (*btree.Node[rune, int, int], error) {
			getCalls++
			if getCalls >= 2 {
				return nil, boom
			}

			prev := st.GetFn
			st.GetFn = nil
			defer func() { st.GetFn = prev }()

			if ptr != rootPtr {
				return nil, errors.New("unexpected first Get() pointer (expected root)")
			}
			return st.Get(ptr)
		}

		out := stdOutCapture(t, func() {
			err := fresh.Verify()
			require.Error(t, err)
			require.Contains(t, err.Error(), "Failed to fetch node")
		})
		require.Contains(t, out, "Verify ended, visited")
	})

	t.Run("LeafDepthMismatch", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}

		leaf := btree.Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		leafL := btree.Node[rune, int, int]{
			Keys:   []rune{'n'},
			Values: []int{2},
		}
		leafR := btree.Node[rune, int, int]{
			Keys:   []rune{'z'},
			Values: []int{3},
		}
		ptr, err := st.Put(&leaf)
		require.NoError(t, err)
		ptrL, err := st.Put(&leafL)
		require.NoError(t, err)
		ptrR, err := st.Put(&leafR)
		require.NoError(t, err)

		internalNode := btree.Node[rune, int, int]{
			Keys:     []rune{'z'},
			Pointers: []int{ptrL, ptrR},
		}
		internalPtr, err := st.Put(&internalNode)
		require.NoError(t, err)

		root := btree.Node[rune, int, int]{
			Keys:     []rune{'m'},
			Pointers: []int{ptr, internalPtr},
		}
		rootPtr, err := st.Put(&root)
		require.NoError(t, err)

		tree, err := btree.FromStorage[rune, int, int](rootPtr, &st, cmp.Compare, 3)
		require.NoError(t, err)

		stdOutCapture(t, func() { err = tree.Verify() })
		require.Error(t, err)
		require.Contains(t, err.Error(), "Leaf: broken invariant: Left-most leaf depth")
	})

	t.Run("LeafKeysOccupancyTooSmall", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}

		badLeaf := btree.Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		leafM := btree.Node[rune, int, int]{
			Keys:   []rune{'n', 'o'},
			Values: []int{2, 3},
		}
		leafR := btree.Node[rune, int, int]{
			Keys:   []rune{'u', 'v'},
			Values: []int{4, 5},
		}
		ptrBad, err := st.Put(&badLeaf)
		require.NoError(t, err)
		ptrM, err := st.Put(&leafM)
		require.NoError(t, err)
		ptrR, err := st.Put(&leafR)
		require.NoError(t, err)

		root := btree.Node[rune, int, int]{
			Keys:     []rune{'m', 't'},
			Pointers: []int{ptrBad, ptrM, ptrR},
		}
		rootPtr, err := st.Put(&root)
		require.NoError(t, err)

		tree, err := btree.FromStorage[rune, int, int](rootPtr, &st, cmp.Compare, 4)
		require.NoError(t, err)

		stdOutCapture(t, func() { err = tree.Verify() })
		require.Error(t, err)
		require.Contains(t, err.Error(), "Leaf: broken invariant: Keys occupancy")
	})

	t.Run("LeafValuesOccupancyTooSmall", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}

		badLeaf := btree.Node[rune, int, int]{
			Keys:   []rune{'a', 'b'},
			Values: []int{1},
		}
		leafM := btree.Node[rune, int, int]{
			Keys:   []rune{'n', 'o'},
			Values: []int{2, 3},
		}
		leafR := btree.Node[rune, int, int]{
			Keys:   []rune{'u', 'v'},
			Values: []int{4, 5},
		}
		ptrBad, err := st.Put(&badLeaf)
		require.NoError(t, err)
		ptrM, err := st.Put(&leafM)
		require.NoError(t, err)
		ptrR, err := st.Put(&leafR)
		require.NoError(t, err)

		root := btree.Node[rune, int, int]{
			Keys:     []rune{'m', 't'},
			Pointers: []int{ptrBad, ptrM, ptrR},
		}
		rootPtr, err := st.Put(&root)
		require.NoError(t, err)

		tree, err := btree.FromStorage[rune, int, int](rootPtr, &st, cmp.Compare, 4)
		require.NoError(t, err)

		stdOutCapture(t, func() { err = tree.Verify() })
		require.Error(t, err)
		require.Contains(t, err.Error(), "Leaf: broken invariant: Values occupancy")
	})

	t.Run("InternalKeysOccupancyTooSmall", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}

		leafL := btree.Node[rune, int, int]{
			Keys:   []rune{'a', 'b'},
			Values: []int{1},
		}
		leafR := btree.Node[rune, int, int]{
			Keys:   []rune{'y', 'z'},
			Values: []int{3, 4},
		}
		ptrL, err := st.Put(&leafL)
		require.NoError(t, err)
		ptrR, err := st.Put(&leafR)
		require.NoError(t, err)

		root := btree.Node[rune, int, int]{
			Keys:     []rune{'m'},
			Pointers: []int{ptrL, ptrR},
		}
		rootPtr, err := st.Put(&root)
		require.NoError(t, err)

		tree, err := btree.FromStorage[rune, int, int](rootPtr, &st, cmp.Compare, 4)
		require.NoError(t, err)

		stdOutCapture(t, func() { err = tree.Verify() })
		require.Error(t, err)
		require.Contains(t, err.Error(), "InternalNode: broken invariant: Keys occupancy")
	})

	t.Run("InternalValuesNotEmpty", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}

		leafL := btree.Node[rune, int, int]{
			Keys:   []rune{'a', 'b'},
			Values: []int{1, 2},
		}
		leafR := btree.Node[rune, int, int]{
			Keys:   []rune{'y', 'z'},
			Values: []int{3, 4},
		}
		ptrL, err := st.Put(&leafL)
		require.NoError(t, err)
		ptrR, err := st.Put(&leafR)
		require.NoError(t, err)

		root := btree.Node[rune, int, int]{
			Keys:     []rune{'m'},
			Pointers: []int{ptrL, ptrR},
			Values:   []int{999},
		}
		rootPtr, err := st.Put(&root)
		require.NoError(t, err)

		tree, err := btree.FromStorage[rune, int, int](rootPtr, &st, cmp.Compare, 3)
		require.NoError(t, err)

		stdOutCapture(t, func() { err = tree.Verify() })
		require.Error(t, err)
		require.Contains(t, err.Error(), "InteralNode: broken invariant: Values is not empty")
	})

	t.Run("InternalPointersOccupancyTooSmall", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}

		leaf := btree.Node[rune, int, int]{
			Keys:   []rune{'a', 'b'},
			Values: []int{1, 2},
		}
		ptr, err := st.Put(&leaf)
		require.NoError(t, err)

		root := btree.Node[rune, int, int]{
			Keys:     []rune{'m', 't'},
			Pointers: []int{ptr},
		}
		rootPtr, err := st.Put(&root)
		require.NoError(t, err)

		tree, err := btree.FromStorage[rune, int, int](rootPtr, &st, cmp.Compare, 4)
		require.NoError(t, err)

		stdOutCapture(t, func() { err = tree.Verify() })
		require.Error(t, err)
		require.Contains(t, err.Error(), "InternalNode: broken invariant: Pointers occupancy")
	})

	t.Run("NodeKeysOrderingBroken_InternalNode", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}

		leafL := btree.Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		leafR := btree.Node[rune, int, int]{
			Keys:   []rune{'b'},
			Values: []int{2},
		}
		leafM := btree.Node[rune, int, int]{
			Keys:   []rune{'c'},
			Values: []int{3},
		}
		ptrL, err := st.Put(&leafL)
		require.NoError(t, err)
		ptrR, err := st.Put(&leafR)
		require.NoError(t, err)
		ptrM, err := st.Put(&leafM)
		require.NoError(t, err)

		root := btree.Node[rune, int, int]{
			Keys:     []rune{'t', 'n'},
			Pointers: []int{ptrL, ptrM, ptrR},
		}
		rootPtr, err := st.Put(&root)
		require.NoError(t, err)

		tree, err := btree.FromStorage[rune, int, int](rootPtr, &st, cmp.Compare, 3)
		require.NoError(t, err)

		stdOutCapture(t, func() { err = tree.Verify() })
		require.Error(t, err)
		require.Contains(t, err.Error(), "Node: broken ordering of keys")
	})

	t.Run("NodeKeysOrderingBroken_Leaf", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}

		badLeaf := btree.Node[rune, int, int]{
			Keys:   []rune{'a', 'b', 'b'},
			Values: []int{1, 2, 3},
		}
		leafM := btree.Node[rune, int, int]{
			Keys:   []rune{'n', 'o'},
			Values: []int{4, 5},
		}
		leafR := btree.Node[rune, int, int]{
			Keys:   []rune{'u', 'v'},
			Values: []int{6, 7},
		}
		ptrBad, err := st.Put(&badLeaf)
		require.NoError(t, err)
		ptrM, err := st.Put(&leafM)
		require.NoError(t, err)
		ptrR, err := st.Put(&leafR)
		require.NoError(t, err)

		rootPtr, err := st.Put(&btree.Node[rune, int, int]{
			Keys:     []rune{'m', 't'},
			Pointers: []int{ptrBad, ptrM, ptrR},
		})
		require.NoError(t, err)

		tree, err := btree.FromStorage[rune, int, int](rootPtr, &st, cmp.Compare, 4)
		require.NoError(t, err)

		stdOutCapture(t, func() { err = tree.Verify() })

		require.Error(t, err)
		require.Contains(t, err.Error(), "Node: broken ordering of keys")
	})

	t.Run("ParentChildOrdering_LeftMostChild", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}

		badLeaf := btree.Node[rune, int, int]{
			Keys:   []rune{'x', 'z'},
			Values: []int{1, 2},
		}
		leafM := btree.Node[rune, int, int]{
			Keys:   []rune{'n', 'o'},
			Values: []int{3, 4},
		}
		leafR := btree.Node[rune, int, int]{
			Keys:   []rune{'u', 'v'},
			Values: []int{5, 6},
		}
		ptrBad, err := st.Put(&badLeaf)
		require.NoError(t, err)
		ptrM, err := st.Put(&leafM)
		require.NoError(t, err)
		ptrR, err := st.Put(&leafR)
		require.NoError(t, err)

		root := btree.Node[rune, int, int]{
			Keys:     []rune{'m', 't'},
			Pointers: []int{ptrBad, ptrM, ptrR},
		}
		rootPtr, err := st.Put(&root)
		require.NoError(t, err)

		tree, err := btree.FromStorage[rune, int, int](rootPtr, &st, cmp.Compare, 4)
		require.NoError(t, err)

		stdOutCapture(t, func() { err = tree.Verify() })
		require.Error(t, err)
		require.Contains(t, err.Error(), "Parent/Child ordering is wrong Parent (")
		require.Contains(t, err.Error(), "('-inf' /")
		require.Contains(t, err.Error(), " / '109')")
	})

	t.Run("ParentChildOrdering_RightMostChild", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}

		leaf := btree.Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		badLeaf := btree.Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{2},
		}
		ptrLeaf, err := st.Put(&leaf)
		require.NoError(t, err)
		ptrBad, err := st.Put(&badLeaf)
		require.NoError(t, err)

		root := btree.Node[rune, int, int]{
			Keys:     []rune{'m'},
			Pointers: []int{ptrLeaf, ptrBad},
		}
		rootPtr, err := st.Put(&root)
		require.NoError(t, err)

		tree, err := btree.FromStorage[rune, int, int](rootPtr, &st, cmp.Compare, 3)
		require.NoError(t, err)

		stdOutCapture(t, func() { err = tree.Verify() })
		require.Error(t, err)
		require.Contains(t, err.Error(), "Parent/Child ordering is wrong Parent (")
		require.Contains(t, err.Error(), "/ '+inf')")
	})

	t.Run("ParentChildOrdering_MiddleChild", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}

		leafL := btree.Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{1},
		}
		leafMidBad := btree.Node[rune, int, int]{
			Keys:   []rune{'a'},
			Values: []int{2},
		}
		leafR := btree.Node[rune, int, int]{
			Keys:   []rune{'z'},
			Values: []int{3},
		}
		ptrL, err := st.Put(&leafL)
		require.NoError(t, err)
		ptrMidBad, err := st.Put(&leafMidBad)
		require.NoError(t, err)
		ptrR, err := st.Put(&leafR)
		require.NoError(t, err)

		root := btree.Node[rune, int, int]{
			Keys:     []rune{'m', 't'},
			Pointers: []int{ptrL, ptrMidBad, ptrR},
		}
		rootPtr, err := st.Put(&root)
		require.NoError(t, err)

		tree, err := btree.FromStorage[rune, int, int](rootPtr, &st, cmp.Compare, 3)
		require.NoError(t, err)

		stdOutCapture(t, func() { err = tree.Verify() })
		require.Error(t, err)
		require.Contains(t, err.Error(), "Parent/Child ordering is wrong Parent (")
		require.Contains(t, err.Error(), "('109' / '116')")
	})
}

func TestDot(t *testing.T) {
	t.Run("ValidGraph", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}

		tree := buildTreeRunes(t, &st, 3, []rune("abcdefghijklmnopqrstuvwxyz"))

		var buf bytes.Buffer
		err := tree.Dot(&buf, false)
		require.NoError(t, err)

		out := buf.String()
		require.True(t, strings.HasPrefix(out, "digraph G {"))
		require.True(t, strings.HasSuffix(strings.TrimSpace(out), "}"))
		require.Contains(t, out, "->")
	})

	t.Run("ShowNextPtr", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}

		tree := buildTreeRunes(t, &st, 3, []rune("abcdefghijklmnopqrstuvwxyz"))

		var bufNo, bufYes bytes.Buffer
		err := tree.Dot(&bufNo, false)
		require.NoError(t, err)

		err = tree.Dot(&bufYes, true)
		require.NoError(t, err)

		outNo := bufNo.String()
		outYes := bufYes.String()

		require.True(t, strings.HasPrefix(outNo, "digraph G {"))
		require.True(t, strings.HasSuffix(strings.TrimSpace(outNo), "}"))
		require.True(t, strings.HasPrefix(outYes, "digraph G {"))
		require.True(t, strings.HasSuffix(strings.TrimSpace(outYes), "}"))

		require.GreaterOrEqual(t, len(outYes), len(outNo))
	})

	t.Run("Fails_BecauseStoreGetFails", func(t *testing.T) {
		st := btree.InMemoryStore_t[rune, int]{}

		tree := buildTreeRunes(t, &st, 3, []rune("abcdefghijklmnopqrstuvwxyz"))

		fresh, err := btree.FromStorage[rune, int, int](tree.Root, &st, cmp.Compare, 3)
		require.NoError(t, err)

		getErr := errors.New("Store.Get() failed")
		st.GetFn = func(_ int) (*btree.Node[rune, int, int], error) { return nil, getErr }

		var buf bytes.Buffer
		err = fresh.Dot(&buf, false)
		require.Error(t, err)
	})
}
