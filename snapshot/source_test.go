package snapshot_test

import (
	"context"
	"testing"

	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

func TestSourceDedupEqual(t *testing.T) {
	imp1 := &ptesting.MockImporter{
		FakeRoot: "/home/bar",
	}
	imp2 := &ptesting.MockImporter{
		FakeRoot: "/home/bar/",
	}
	imp3 := &ptesting.MockImporter{
		FakeRoot: "/home/bar",
	}

	s, err := snapshot.NewSource(context.Background(), 0, imp1, imp2, imp3)
	require.NoError(t, err)

	require.Len(t, s.Importers(), 1)
}

func TestSourceDedupCommonPrefix(t *testing.T) {
	imp1 := &ptesting.MockImporter{
		FakeRoot: "/home/bar/baz",
	}
	imp2 := &ptesting.MockImporter{
		FakeRoot: "/home/bar/baz/foo",
	}
	imp3 := &ptesting.MockImporter{
		FakeRoot: "/home/bar/quux",
	}
	imp4 := &ptesting.MockImporter{
		FakeRoot: "/home/foo",
	}
	imp5 := &ptesting.MockImporter{
		FakeRoot: "/home/bar",
	}

	s, err := snapshot.NewSource(t.Context(), 0, imp1, imp2, imp3, imp4, imp5)
	require.NoError(t, err)

	roots := []string{}
	for _, i := range s.Importers() {
		roots = append(roots, i.Root())
	}
	require.Len(t, s.Importers(), 2)
	require.ElementsMatch(t, roots, []string{"/home/bar", "/home/foo"}) // It should be sorted
}
