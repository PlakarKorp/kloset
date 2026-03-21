package btree

import (
	"cmp"
	"testing"

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
