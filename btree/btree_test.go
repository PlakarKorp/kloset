package btree_test

import (
	"cmp"
	"testing"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/stretchr/testify/require"
)

func TestNew_NilStore(t *testing.T) {
	empty, err := btree.New[rune, int, string](nil, cmp.Compare, 2)
	require.Error(t, err)
	require.Nil(t, empty)
}
