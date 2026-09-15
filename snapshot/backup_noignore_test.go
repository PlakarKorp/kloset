package snapshot_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

type noIgnoreImporter struct {
	*ptesting.MockImporter
}

func (imp *noIgnoreImporter) Flags() location.Flags {
	return location.FLAG_NOIGNORE
}

func TestBackupNoIgnoreImporterKeepsExcludedEntries(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	var logs bytes.Buffer
	repo.Logger().SetOutput(&logs)

	builder, err := snapshot.Create(repo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{
		Name: "noignore-test",
	})
	require.NoError(t, err)

	baseImporter, err := ptesting.NewMockImporter(repo.AppContext(), &connectors.Options{},
		"mock", map[string]string{"location": "mock://place"})
	require.NoError(t, err)
	baseImporter.(*ptesting.MockImporter).SetFiles([]ptesting.MockFile{
		ptesting.NewMockFile("keep.tmp", 0644, "payload"),
	})

	source, err := snapshot.NewSource(repo.AppContext(), &noIgnoreImporter{
		MockImporter: baseImporter.(*ptesting.MockImporter),
	})
	require.NoError(t, err)
	require.NoError(t, source.SetExcludes([]string{"*.tmp"}))

	require.NoError(t, builder.Backup(source))
	require.NoError(t, builder.Commit())
	require.NoError(t, builder.Close())
	require.NoError(t, builder.Repository().RebuildState())

	loaded, err := snapshot.Load(repo, builder.Header.Identifier)
	require.NoError(t, err)
	defer loaded.Close()

	filesystem, err := loaded.Filesystem()
	require.NoError(t, err)

	found := false
	for pathname, err := range filesystem.Pathnames() {
		require.NoError(t, err)
		found = found || strings.HasSuffix(pathname, "keep.tmp")
	}
	require.True(t, found, "FLAG_NOIGNORE importer should keep entries matching exclude rules")
	require.Contains(t, logs.String(), "ignoring exclude rules")
}
