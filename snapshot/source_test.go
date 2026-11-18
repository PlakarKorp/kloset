package snapshot_test

import (
	"context"
	"path"
	"testing"

	"github.com/PlakarKorp/integration-sdk"
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

	s, err := snapshot.NewSource(context.Background(), imp1, imp2, imp3)
	require.NoError(t, err)

	require.Len(t, s.Importers(), 1)
}

func TestSourceGetters(t *testing.T) {
	imp := &ptesting.MockImporter{
		FakeRoot: "/home/foo",
	}

	s, err := snapshot.NewSource(context.Background(), imp)
	require.NoError(t, err)

	require.Equal(t, "mock", s.Origin())
	require.Equal(t, "mock", s.Type())
	require.Equal(t, "/home/foo", s.Root())
	require.Equal(t, imp.Flags(), s.Flags())
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

	s, err := snapshot.NewSource(t.Context(), imp1, imp2, imp3, imp4, imp5)
	require.NoError(t, err)

	roots := []string{}
	for _, i := range s.Importers() {
		roots = append(roots, i.Root())
	}
	require.Len(t, s.Importers(), 2)
	require.ElementsMatch(t, roots, []string{"/home/bar", "/home/foo"}) // It should be sorted
}

func TestSourceSanitizeRoot(t *testing.T) {
	for _, tc := range []struct {
		roots []string
		want  string
	}{
		{[]string{"home/foo"}, "/home/foo"},
		{[]string{"home/foo/"}, "/home/foo"},
		{[]string{""}, "/"},
		{[]string{"."}, "/"},
		{[]string{"./home/foo/../bar"}, "/home/bar"},
		{[]string{"home/bar", "/home/bar/baz"}, "/home/bar"},
		{[]string{"home/bar/baz", "/home/bar/quux"}, "/home/bar"},
	} {
		imps := []integration.Importer{}
		for _, r := range tc.roots {
			imps = append(imps, &ptesting.MockImporter{FakeRoot: r})
		}
		s, err := snapshot.NewSource(t.Context(), imps...)
		require.NoError(t, err)
		require.Equal(t, tc.want, s.Root(), "roots %q", tc.roots)
		require.True(t, path.IsAbs(s.Root()))
	}
}
