package btree_test

import (
	"bytes"
	"cmp"
	"errors"
	"io"
	"slices"
	"testing"
	"testing/iotest"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestNew(t *testing.T) {
	t.Run("Storage Nil", func(t *testing.T) {
		empty, err := btree.New[rune, int, string](nil, cmp.Compare, 3)
		require.ErrorIs(t, err, btree.ErrStore)
		require.Nil(t, empty)
	})

	t.Run("Invalid Order", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, string]{}
		empty, err := btree.New(&storage, cmp.Compare, 1)
		require.ErrorIs(t, err, btree.ErrOrder)
		require.Nil(t, empty)
	})

	t.Run("NewFails_BecauseStoreFails", func(t *testing.T) {
		putErr := errors.New("Store.Put() failed")
		storage := btree.InMemoryStore_t[rune, string]{}
		storage.PutFn = func(*btree.Node[rune, int, string]) (int, error) {
			return 0, putErr
		}
		empty, err := btree.New(&storage, cmp.Compare, 5)
		require.ErrorIs(t, err, putErr)
		require.Nil(t, empty)
	})
}

func TestFromStorage(t *testing.T) {
	t.Run("Storage Nil", func(t *testing.T) {
		tree, err := btree.FromStorage[rune, int, string](42, nil, cmp.Compare, 3)
		require.ErrorIs(t, err, btree.ErrStore)
		require.Nil(t, tree)
	})

	t.Run("Invalid Order", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, string]{}
		tree, err := btree.FromStorage[rune, int, string](42, &storage, cmp.Compare, 2)
		require.ErrorIs(t, err, btree.ErrOrder)
		require.Nil(t, tree)
	})

	t.Run("Empty Storage", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, string]{}
		tree, err := btree.FromStorage[rune, int, string](42, &storage, cmp.Compare, 3)
		require.NoError(t, err)
		require.NotNil(t, tree)
		require.Equal(t, 42, tree.Root)
		require.Equal(t, 3, tree.Order)
	})
}

func TestDeserialize(t *testing.T) {
	t.Run("Reader Nil", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, string]{}
		require.Panics(t, func() {
			btree.Deserialize(nil, &storage, cmp.Compare)
		})
	})

	t.Run("Invalid Reader", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, string]{}
		rd := iotest.ErrReader(io.ErrUnexpectedEOF)

		tree, err := btree.Deserialize(rd, &storage, cmp.Compare)

		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.Nil(t, tree)
	})

	t.Run("Invalid Byte Encoding", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, string]{}

		rd := bytes.NewReader([]byte{0x00, 0x01, 0x02})
		tree, err := btree.Deserialize(rd, &storage, cmp.Compare)

		require.Error(t, err)
		require.Nil(t, tree)
	})

	t.Run("Invalid Order", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		payload := struct {
			Root  int
			Order int
		}{
			Root:  123,
			Order: 2,
		}

		var buf bytes.Buffer
		enc := msgpack.NewEncoder(&buf)
		err := enc.Encode(&payload)
		require.NoError(t, err)

		_, err = btree.Deserialize(&buf, &storage, cmp.Compare)
		require.ErrorIs(t, err, btree.ErrOrder)
	})

	t.Run("Valid Deserialize", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		payload := struct {
			Root  int
			Order int
		}{
			Root:  123,
			Order: 3,
		}

		var buf bytes.Buffer
		enc := msgpack.NewEncoder(&buf)
		err := enc.Encode(&payload)
		require.NoError(t, err)

		tree, err := btree.Deserialize(&buf, &storage, cmp.Compare)
		require.NoError(t, err)
		require.NotNil(t, tree)
		require.Equal(t, payload.Root, tree.Root)
		require.Equal(t, payload.Order, tree.Order)
	})
}

func TestInsert(t *testing.T) {
	// lookupInStore does a minimal Btree lookup by walking through nodes in
	// the cache using store.Get().
	// Tests intentionally do not use tree.Find(): not tested at this point
	lookupInStore := func(
		t *testing.T,
		tree *btree.BTree[rune, int, int],
		store interface {
			Get(int) (*btree.Node[rune, int, int], error)
		},
		compare func(rune, rune) int,
		key rune,
	) (val int, ok bool) {
		t.Helper()

		ptr := tree.Root
		for {
			node, err := store.Get(ptr)
			if err != nil {
				t.Logf("store.Get(%d) failed: %v", ptr, err)
				return -1, false
			}

			// Leaf: search key in Keys, return corresponding value.
			if len(node.Values) == len(node.Keys) {
				idx, found := slices.BinarySearchFunc(node.Keys, key, compare)
				if !found {
					return -1, false
				}
				return node.Values[idx], true
			}

			if len(node.Pointers) != len(node.Keys)+1 {
				t.Logf("invalid internal node: keys=%d pointers=%d", len(node.Keys), len(node.Pointers))
				return -1, false
			}

			// Internal node
			idx, found := slices.BinarySearchFunc(node.Keys, key, compare)
			if found {
				idx++
			}
			ptr = node.Pointers[idx]
		}
	}

	t.Run("InsertNewKey_persistsInLeaf", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, err)

		err = tree.Insert('k', 11)
		require.NoError(t, err)

		v, ok := lookupInStore(t, tree, &storage, cmp.Compare, 'k')
		require.True(t, ok)
		require.Equal(t, 11, v)
	})

	t.Run("InsertDuplicate_andDoesNotOverwrite", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, err)

		err = tree.Insert('k', 1)
		require.NoError(t, err)

		err = tree.Insert('k', 2)
		require.ErrorIs(t, err, btree.ErrExists)

		v, ok := lookupInStore(t, tree, &storage, cmp.Compare, 'k')
		require.True(t, ok)
		require.Equal(t, 1, v)
	})

	t.Run("InsertManyKeys_triggersSplits_butAllKeysPresent", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 4)
		require.NoError(t, err)

		const n = 1550
		base := rune(0)
		for i := range n {
			k := base + rune(i)
			err = tree.Insert(k, i)
			require.NoError(t, err)
		}

		for i := range n {
			k := base + rune(i)
			v, ok := lookupInStore(t, tree, &storage, cmp.Compare, k)
			require.True(t, ok)
			require.Equal(t, i, v)
		}
	})

	t.Run("InsertFails_BecauseStoreGetFails", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, err)

		getErr := errors.New("Store.Get() failed")
		storage.GetFn = func(ptr int) (*btree.Node[rune, int, int], error) {
			return nil, getErr
		}

		err = tree.Insert('x', 99)
		require.ErrorIs(t, err, getErr)
	})

	t.Run("InsertFails_BecauseStorePutFails", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, err)

		for _, k := range []rune{'a', 'b', 'c', 'd', 'e', 'f'} {
			require.NoError(t, tree.Insert(k, int(k)))
		}

		putErr := errors.New("Store.Put() failed")
		storage.PutFn = func(*btree.Node[rune, int, int]) (int, error) {
			return 0, putErr
		}
		err = tree.Insert('x', 99)
		require.ErrorIs(t, err, putErr)
	})
}

func TestClose(t *testing.T) {
	t.Run("Close_Twice", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, _ := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, tree.Close())
		require.NoError(t, tree.Close())
	})

	t.Run("EmptyTree", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, err)
		require.NoError(t, tree.Close())
	})

	t.Run("Close_AfterInsert", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, err)

		require.NoError(t, tree.Insert('a', 1))
		require.NoError(t, tree.Close())
	})

	t.Run("Close_Fails_WhenStoreCloseFails", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, err)

		require.NoError(t, tree.Insert('a', 1))

		closeErr := errors.New("Store.Close() failed")
		storage.CloseFn = func() error { return closeErr }

		err = tree.Close()
		require.ErrorIs(t, err, closeErr)
	})

	t.Run("Close_Fails_BecauseStoreUpdateFailsOnFlush", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, err)

		require.NoError(t, tree.Insert('a', 1))

		flushErr := errors.New("Store.Update() failed")
		storage.UpdateFn = func(ptr int, n *btree.Node[rune, int, int]) error {
			return flushErr
		}
		require.ErrorIs(t, tree.Close(), flushErr)
	})
}

func TestFind(t *testing.T) {
	t.Run("EmptyTree", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, err)

		_, found, err := tree.Find('a')
		require.NoError(t, err)
		require.False(t, found)
	})

	t.Run("Find_MatchValues_AfterInsert", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, err)

		require.NoError(t, tree.Insert('k', 11))

		v, found, err := tree.Find('k')
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, 11, v)
	})

	t.Run("MissingKeyNotFound", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, err)

		require.NoError(t, tree.Insert('a', 1))
		require.NoError(t, tree.Insert('z', 26))

		_, found, err := tree.Find('A')
		require.NoError(t, err)
		require.False(t, found)
	})

	t.Run("FindAllInsertedKeys", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, err)

		alphabet := []rune("abcdefghijklmnopqrstuvwxyz")
		for i, r := range alphabet {
			require.NoError(t, tree.Insert(r, i))
		}

		for i, r := range alphabet {
			v, found, err := tree.Find(r)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, i, v)
		}

		slices.Reverse(alphabet)
		lastIdx := len(alphabet) - 1
		for i, r := range alphabet {
			v, found, err := tree.Find(r)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, lastIdx-i, v)
		}
	})

	t.Run("FindFails_BecauseStoreFails", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, err)
		require.NoError(t, tree.Insert('a', 1))

		// New Tree with an empty cache in order to get store.Get() called
		fresh, err := btree.FromStorage[rune, int, int](tree.Root, &storage, cmp.Compare, 3)
		require.NoError(t, err)

		getErr := errors.New("Store.Get() failed")
		call := 0
		storage.GetFn = func(int) (*btree.Node[rune, int, int], error) {
			call++
			return nil, getErr
		}

		_, _, err = fresh.Find('a')
		require.Equal(t, call, 1)
		require.ErrorIs(t, err, getErr)
	})
}

func TestUpdate(t *testing.T) {
	t.Run("UpdateNewKey_InsertInLeaf", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, err)

		err = tree.Update('k', 11)
		require.NoError(t, err)

		v, found, err := tree.Find('k')
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, 11, v)
	})

	t.Run("UpdateSameKey_Overwrite", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, err)

		err = tree.Update('k', 1)
		require.NoError(t, err)
		err = tree.Update('k', 2)
		require.NoError(t, err)

		v, found, err := tree.Find('k')
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, 2, v)
	})

	t.Run("UpdateManyKeys_triggersSplits_butAllKeysPresent", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 4)
		require.NoError(t, err)

		const n = 1550
		const upN = 450
		base := rune(0)
		for i := range n {
			k := base + rune(i)
			err = tree.Insert(k, i)
			require.NoError(t, err)
		}

		for i := range n {
			k := base + rune(i) + upN
			err = tree.Update(k, i)
			require.NoError(t, err)
		}

		for i := range n {
			k := base + rune(i) + upN
			v, found, err := tree.Find(k)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, i, v)
		}
	})

	t.Run("UpdateFails_BecauseStoreGetFails", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, err)

		getErr := errors.New("Store.Get() failed")
		storage.GetFn = func(ptr int) (*btree.Node[rune, int, int], error) {
			return nil, getErr
		}

		err = tree.Update('x', 99)
		require.ErrorIs(t, err, getErr)
	})

	t.Run("UpdateFails_BecauseStorePutFails", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, err)

		for _, k := range []rune{'a', 'b', 'c', 'd', 'e', 'f'} {
			require.NoError(t, tree.Insert(k, int(k)))
		}

		putErr := errors.New("Store.Put() failed")
		storage.PutFn = func(*btree.Node[rune, int, int]) (int, error) {
			return 0, putErr
		}
		err = tree.Update('x', 99)
		require.ErrorIs(t, err, putErr)
	})

	t.Run("UpdateThenClose", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, err)

		require.NoError(t, tree.Insert('a', 1))
		require.NoError(t, tree.Update('a', 2))
		require.NoError(t, tree.Close())
	})

	t.Run("UpdateFails_BecauseStoreUpdateFails", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, int]{}
		tree, err := btree.New(&storage, cmp.Compare, 3)
		require.NoError(t, err)

		require.NoError(t, tree.Insert('a', 1))
		require.NoError(t, tree.Update('a', 2))

		updateErr := errors.New("Store.Update() failed")
		storage.UpdateFn = func(ptr int, n *btree.Node[rune, int, int]) error {
			return updateErr
		}

		err = tree.Close()
		require.ErrorIs(t, err, updateErr)
	})
}

func TestCount(t *testing.T) {
	t.Run("EmptyTree", func(t *testing.T) {
		storage := &btree.InMemoryStore[rune, int]{}
		tree, err := btree.New(storage, cmp.Compare, 3)
		require.NoError(t, err)
		require.Equal(t, 0, tree.Count)
	})

	t.Run("Insert_onlyNewKeys", func(t *testing.T) {
		storage := &btree.InMemoryStore[rune, int]{}
		tree, err := btree.New[rune, int, int](storage, cmp.Compare, 3)
		require.NoError(t, err)

		alphabet := []rune("abcdefghijklmnopqrstuvwxyz")
		for i, r := range alphabet {
			require.NoError(t, tree.Insert(r, i))
			require.Equal(t, i+1, tree.Count)
		}
	})

	t.Run("InsertDuplicates", func(t *testing.T) {
		storage := &btree.InMemoryStore[rune, int]{}
		tree, err := btree.New[rune, int, int](storage, cmp.Compare, 3)
		require.NoError(t, err)

		require.NoError(t, tree.Insert('a', 1))
		require.Equal(t, 1, tree.Count)

		require.ErrorIs(t, tree.Insert('a', 2), btree.ErrExists)
		require.Equal(t, 1, tree.Count)
	})

	t.Run("UpdateExistingKey", func(t *testing.T) {
		storage := &btree.InMemoryStore[rune, int]{}
		tree, err := btree.New[rune, int, int](storage, cmp.Compare, 3)
		require.NoError(t, err)

		require.NoError(t, tree.Insert('a', 1))
		require.Equal(t, 1, tree.Count)

		require.NoError(t, tree.Update('a', 99))
		require.Equal(t, 1, tree.Count)
	})

	t.Run("UpdateOnMissingKey", func(t *testing.T) {
		storage := &btree.InMemoryStore[rune, int]{}
		tree, err := btree.New[rune, int, int](storage, cmp.Compare, 3)
		require.NoError(t, err)

		require.NoError(t, tree.Update('x', 7))
		require.Equal(t, 1, tree.Count)
	})

	t.Run("ManySameKeyInserts", func(t *testing.T) {
		storage := &btree.InMemoryStore[rune, int]{}
		tree, err := btree.New[rune, int, int](storage, cmp.Compare, 4)
		require.NoError(t, err)

		const n = 1000
		for i := range 1000 {
			require.NoError(t, tree.Insert(rune(i), i))
		}
		require.Equal(t, n, tree.Count)

		for i := range n / 2 {
			require.ErrorIs(t, tree.Insert(rune(i), i), btree.ErrExists)
		}
		require.Equal(t, n, tree.Count)
	})
}
