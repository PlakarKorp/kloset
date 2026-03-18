package btree_test

import (
	"bytes"
	"cmp"
	"errors"
	"io"
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
