package btree

import (
	"cmp"
	"errors"
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
	node, path, err := tree.findleaf(12)

	if calls == 0 {
		t.Fatal("expected Get to be called at least once")
	}
	if firstCall != root {
		t.Fatalf("expected first Get(%d), got Get(%d)", root, firstCall)
	}

	if err == nil {
		t.Fatalf("expected error, got nil (node=%v path=%v)", node, path)
	}
	if !errors.Is(err, notfound) {
		t.Fatalf("expected error notfound, got %v", err)
	}
}
