package btree_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/btree"
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
