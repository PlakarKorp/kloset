package btree

import (
	"cmp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBTree(t *testing.T) {
	store := InMemoryStore_t[rune, int]{}
	tree, err := New(&store, cmp.Compare, 3)
	require.NoError(t, err)

	alphabet := []rune("abcdefghijklmnopqrstuvwxyz")
	for i, r := range alphabet {
		err := tree.Insert(r, i)
		require.NoError(t, err)
	}

	for i, r := range alphabet {
		err := tree.Insert(r, i)
		require.ErrorIs(t, err, ErrExists)
	}

	for i, r := range alphabet {
		v, found, err := tree.Find(r)
		if err != nil {
			t.Fatalf("Find(%v) unexpectedly failed", r)
		}
		if !found {
			t.Fatalf("Find(%v) unexpectedly not found", r)
		}
		if v != i {
			t.Fatalf("Find(%v) yielded %v, want %v", r, v, i)
		}
	}

	for i := len(alphabet) - 1; i >= 0; i-- {
		r := alphabet[i]
		v, found, err := tree.Find(r)
		if err != nil {
			t.Fatalf("Find(%v) unexpectedly failed", r)
		}
		if !found {
			t.Fatalf("Find(%v) unexpectedly not found", r)
		}
		if v != i {
			t.Fatalf("Find(%v) yielded %v, want %v", r, v, i)
		}
	}

	nonexist := 'A'
	v, found, err := tree.Find(nonexist)
	if err != nil {
		t.Fatalf("Find(%v) unexpectedly failed", nonexist)
	}
	if found {
		t.Fatalf("Find(%v) unexpectedly found %v", nonexist, v)
	}
}

func TestInsert(t *testing.T) {
	store := InMemoryStore_t[string, int]{}
	tree, err := New(&store, strings.Compare, 30)
	require.NoError(t, err)

	items := []string{"e", "z", "a", "b", "a", "a", "b", "b", "a", "c", "d"}
	for i, r := range items {
		err := tree.Insert(r, i)
		require.True(t, err == nil || err == ErrExists)
	}

	unique := []struct {
		key string
		val int
	}{
		{"a", 2},
		{"b", 3},
		{"c", 9},
		{"d", 10},
		{"e", 0},
	}

	for _, u := range unique {
		v, found, err := tree.Find(u.key)
		if err != nil {
			t.Fatalf("Find(%v) unexpectedly failed", u.key)
		}
		if !found {
			t.Errorf("Find(%v) unexpectedly not found", u.key)
		}
		if v != u.val {
			t.Errorf("Find(%v) yielded %v, want %v", u.key, v, u.val)
		}
	}
}

func TestScanAll(t *testing.T) {
	store := InMemoryStore_t[rune, int]{}
	tree, err := New(&store, cmp.Compare, 3)
	require.NoError(t, err)

	alphabet := []rune("abcdefghijklmnopqrstuvwxyz")
	for i, r := range alphabet {
		err := tree.Insert(r, i)
		require.NoError(t, err)
	}

	iter, err := tree.ScanAll()
	require.NoError(t, err)

	for i, r := range alphabet {
		if !iter.Next() {
			t.Fatalf("iterator stopped too early!")
		}
		k, v := iter.Current()
		if k != r {
			t.Errorf("Got key %v; want %v", k, r)
		}
		if v != i {
			t.Errorf("Got value %v; want %v", v, i)
		}
	}

	if iter.Next() {
		t.Fatalf("iterator could unexpectedly continue")
	}
}

func TestScanFrom(t *testing.T) {
	store := InMemoryStore_t[rune, int]{}
	tree, err := New(&store, cmp.Compare, 8)
	require.NoError(t, err)

	alphabet := []rune("abcdefghijklmnopqrstuvwxyz")
	for i, r := range alphabet {
		err := tree.Insert(r, i)
		require.NoError(t, err)
	}

	iter, err := tree.ScanFrom(rune('e'))
	require.NoError(t, err)

	for i := 4; i < len(alphabet); i++ {
		r := alphabet[i]
		if !iter.Next() {
			t.Fatalf("iterator stopped too early!")
		}
		k, v := iter.Current()
		if k != r {
			t.Errorf("Got key %c; want %c", k, r)
		}
		if v != i {
			t.Errorf("Got value %v; want %v", v, i)
		}
	}

	if iter.Next() {
		t.Fatalf("iterator could unexpectedly continue")
	}
}

func TestScanAllReverse(t *testing.T) {
	store := InMemoryStore_t[rune, int]{}
	tree, err := New(&store, cmp.Compare, 3)
	require.NoError(t, err)

	alphabet := []rune("abcdefghijklmnopqrstuvwxyz")
	for i, r := range alphabet {
		err := tree.Insert(r, i)
		require.NoError(t, err)
	}

	iter, err := tree.ScanAllReverse()
	require.NoError(t, err)

	for i := len(alphabet) - 1; i >= 0; i-- {
		r := alphabet[i]
		if !iter.Next() {
			t.Fatalf("iterator stopped too early at %v (%c)", i, r)
		}
		k, v := iter.Current()
		if k != r {
			t.Errorf("Got key %c; want %c", k, r)
		}
		if v != i {
			t.Errorf("Got value %v; want %v", v, i)
		}
	}

	if iter.Next() {
		t.Fatalf("iterator could unexpectedly continue")
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

	tree2 := FromStorage(root, &store2, cmp.Compare, order)
	for i, r := range alphabet {
		v, found, err := tree2.Find(r)
		if err != nil {
			t.Fatalf("Find(%v) unexpectedly failed", r)
		}
		if !found {
			t.Fatalf("Find(%v) unexpectedly not found", r)
		}
		if v != i {
			t.Fatalf("Find(%v) yielded %v, want %v", r, v, i)
		}
	}

	nonexist := 'A'
	v, found, err := tree2.Find(nonexist)
	if err != nil {
		t.Fatalf("Find(%v) unexpectedly failed", nonexist)
	}
	if found {
		t.Fatalf("Find(%v) unexpectedly found %v", nonexist, v)
	}

	iter, err := tree2.ScanAll()
	require.NoError(t, err)

	for i, r := range alphabet {
		if !iter.Next() {
			t.Fatalf("iterator stopped too early!")
		}
		k, v := iter.Current()
		if k != r {
			t.Errorf("Got key %v; want %v", k, r)
		}
		if v != i {
			t.Errorf("Got value %v; want %v", v, i)
		}
	}

	if iter.Next() {
		t.Fatalf("iterator could unexpectedly continue")
	}
}

func TestVisitDFS(t *testing.T) {
	store := InMemoryStore_t[rune, int]{}
	tree, err := New(&store, cmp.Compare, 3)
	require.NoError(t, err)

	alphabet := []rune("abcdefghijklmnopqrstuvwxyz")
	for i, r := range alphabet {
		err := tree.Insert(r, i)
		require.NoError(t, err)
	}

	keySaw := []rune{}
	it := tree.IterDFS()
	for it.Next() {
		_, node := it.Current()
		if node.isleaf() {
			for i := range node.Keys {
				keySaw = append(keySaw, node.Keys[i])
			}
		}
	}
	if err := it.Err(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if slices.Compare(alphabet, keySaw) != 0 {
		t.Errorf("some keys weren't seen; got %v but want %v",
			keySaw, alphabet)
	}
}
