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

func TestSourceRejectsRelativeRoot(t *testing.T) {
	for _, root := range []string{
		"",
		".",
		"..",
		"home/foo",
		"home/foo/",
		"./home/foo",
		"../home/foo",
		"C:/Users/foo", // a Windows path the importer forgot to convert to /C:/...
	} {
		t.Run(root, func(t *testing.T) {
			bad := &ptesting.MockImporter{FakeRoot: root}

			s, err := snapshot.NewSource(t.Context(), bad)
			require.Error(t, err)
			require.Nil(t, s)

			// One bad importer must fail the whole source, wherever it is.
			s, err = snapshot.NewSource(t.Context(), &ptesting.MockImporter{FakeRoot: "/home/bar"}, bad)
			require.Error(t, err)
			require.Nil(t, s)

			s, err = snapshot.NewSource(t.Context(), bad, &ptesting.MockImporter{FakeRoot: "/home/bar"})
			require.Error(t, err)
			require.Nil(t, s)
		})
	}
}

func TestSourceAcceptsAbsoluteRoot(t *testing.T) {
	for _, tc := range []struct {
		root string
		want string
	}{
		{"/", "/"},
		{"/home/foo", "/home/foo"},
		{"/home/foo/", "/home/foo"},
		{"/home/foo/../bar", "/home/bar"},
		{"/C:/Users/foo", "/C:/Users/foo"}, // Windows roots as the fs importer emits them
	} {
		t.Run(tc.root, func(t *testing.T) {
			s, err := snapshot.NewSource(t.Context(), &ptesting.MockImporter{FakeRoot: tc.root})
			require.NoError(t, err)
			require.Equal(t, tc.want, s.Root())
		})
	}
}
