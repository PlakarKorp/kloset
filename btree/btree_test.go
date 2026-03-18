package btree_test

import (
	"bytes"
	"cmp"
	"io"
	"runtime"
	"testing"
	"testing/iotest"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestNew_NilStore(t *testing.T) {
	empty, err := btree.New[rune, int, string](nil, cmp.Compare, 2)
	require.Error(t, err)
	require.Nil(t, empty)
}

func TestFromStorage(t *testing.T) {
	t.Run("Storage Nil", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				e, _ := r.(runtime.Error)
				require.Errorf(t, e, "Expected panic: %v", e)
			}
		}()
		btree.FromStorage[rune, int, string](42, nil, cmp.Compare, 3)
	})

	t.Run("Empty Storage", func(t *testing.T) {
		storage := btree.InMemoryStore_t[rune, string]{}
		tree := btree.FromStorage[rune, int, string](42, &storage, cmp.Compare, 3)
		require.NotNil(t, tree)
		require.Equal(t, 42, tree.Root)
		require.Equal(t, 3, tree.Order)
	})
}

func TestDeserialize(t *testing.T) {
	t.Run("Reader Nil", func(t *testing.T) {
		defer func() {
			r := recover()
			if r != nil {
				e, _ := r.(runtime.Error)
				require.Errorf(t, e, "Expected panic: %v", e)
			}
		}()
		storage := btree.InMemoryStore_t[rune, string]{}
		btree.Deserialize(nil, &storage, cmp.Compare)
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
