package snapshot_test

import (
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// TestBuilderImport exercises Builder.Import (the public alias for Backup)
// against a small mock source.
func TestBuilderImport(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	builder, err := snapshot.Create(repo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{
		Name: "import-test",
	})
	require.NoError(t, err)
	require.NotNil(t, builder)

	imp, err := ptesting.NewMockImporter(repo.AppContext(), &connectors.Options{},
		"mock", map[string]string{"location": "mock://place"})
	require.NoError(t, err)
	imp.(*ptesting.MockImporter).SetFiles([]ptesting.MockFile{
		ptesting.NewMockDir("subdir"),
		ptesting.NewMockFile("subdir/file.txt", 0644, "import payload"),
	})

	src, err := snapshot.NewSource(repo.AppContext(), imp)
	require.NoError(t, err)
	require.NoError(t, src.SetExcludes(nil))

	require.NoError(t, builder.Import(src))
	require.NoError(t, builder.Commit())
	require.NoError(t, builder.Close())
	require.NoError(t, builder.Repository().RebuildState())

	loaded, err := snapshot.Load(repo, builder.Header.Identifier)
	require.NoError(t, err)
	defer loaded.Close()

	fs, err := loaded.Filesystem()
	require.NoError(t, err)

	var found bool
	for p, err := range fs.Pathnames() {
		require.NoError(t, err)
		if strings.HasSuffix(p, "file.txt") {
			found = true
		}
	}
	require.True(t, found, "imported file should be visible in the resulting snapshot")
}
