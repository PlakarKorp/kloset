package btree_test

import (
	"bytes"
	"io"
	"testing"
	"testing/iotest"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/vmihailenco/msgpack/v5"
)

func cmp(a, b rune) int {
	if a < b {
		return -1
	}
	if a == b {
		return 0
	}
	return +1
}

func TestNew_NilStore(t *testing.T) {
	empty, err := btree.New[rune, int, string](nil, cmp, 2)

	if err == nil || empty != nil {
		t.Fatalf("A node should not be instantiated: %v", err)
	}
}

func TestFromStorage_NilStore(t *testing.T) {
	root := 42
	order := 3
	tree := btree.FromStorage[rune, int, string](root, nil, cmp, order)

	if tree == nil {
		t.Fatal("expected non-nil tree")
	}
	if tree.Root != root {
		t.Fatalf("expected root=%q, got %q", root, tree.Root)
	}
	if tree.Order != order {
		t.Fatalf("expected order=%d, got %d", order, tree.Order)
	}
}

func TestDeserialize(t *testing.T) {
	t.Run("Reader Nil", func(t *testing.T) {
		storage := btree.InMemoryStore[rune, string]{}

		tree, err := btree.Deserialize(nil, &storage, cmp)

		if err == nil || tree != nil {
			t.Fatalf("expected error; tree=%v err=%v", tree, err)
		}
	})

	t.Run("Invalid Reader", func(t *testing.T) {
		storage := btree.InMemoryStore[rune, string]{}
		rd := iotest.ErrReader(io.ErrUnexpectedEOF)

		tree, err := btree.Deserialize[rune, int, string](rd, &storage, cmp)

		if err == nil || tree != nil {
			t.Fatalf("expected error; tree=%v err=%v", tree, err)
		}
	})

	t.Run("Invalid Byte Encoding", func(t *testing.T) {
		storage := btree.InMemoryStore[rune, string]{}

		rd := bytes.NewReader([]byte{0x00, 0x01, 0x02})
		tree, err := btree.Deserialize(rd, &storage, cmp)

		if err == nil || tree != nil {
			t.Fatalf("expected error; tree=%v err=%v", tree, err)
		}
	})

	t.Run("Valid Deserialize", func(t *testing.T) {
		storage := btree.InMemoryStore[rune, int]{}
		payload := struct {
			Root  int
			Order int
		}{
			Root:  123,
			Order: 3,
		}

		var buf bytes.Buffer
		enc := msgpack.NewEncoder(&buf)
		if err := enc.Encode(&payload); err != nil {
			t.Fatalf("encode msgpack: %v", err)
		}

		tree, err := btree.Deserialize(&buf, &storage, cmp)
		if err != nil {
			t.Fatalf("expected nil err, got %v", err)
		}
		if tree == nil {
			t.Fatal("expected non-nil tree")
		}
		if tree.Root != payload.Root {
			t.Fatalf("expected Root=%d, got %d", payload.Root, tree.Root)
		}
		if tree.Order != payload.Order {
			t.Fatalf("expected Order=%d, got %d", payload.Order, tree.Order)
		}
	})
}
