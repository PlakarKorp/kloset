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

func TestPersist(t *testing.T) {
	order := 3
	store := InMemoryStore_t[rune, int]{}
	tree1, err := New(&store, cmp.Compare, order)
	require.NoError(t, err)

	alphabet := []rune("abcdefghijklmnopqrstuvwxyz")
	for i, r := range alphabet {
		err := tree1.Insert(r, i)
		require.NoError(t, err)
	}

	store2 := InMemoryStore_t[rune, int]{}
	root, err := Persist(tree1, &store2, func(e int) (int, error) { return e, nil })
	require.NoError(t, err)

	tree2, err := FromStorage(root, &store2, cmp.Compare, order)
	require.NoError(t, err)
	require.NotNil(t, tree2)
	for i, r := range alphabet {
		v, found, err := tree2.Find(r)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, v, i)
	}

	nonexist := 'A'
	_, found, err := tree2.Find(nonexist)
	require.NoError(t, err)
	require.False(t, found)

	iter, err := tree2.ScanAll()
	require.NoError(t, err)

	for i, r := range alphabet {
		require.True(t, iter.Next())
		k, v := iter.Current()
		require.Equal(t, k, r)
		require.Equal(t, v, i)
	}

	require.False(t, iter.Next())
}
