package snapshot_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/snapshot"
	"github.com/PlakarKorp/kloset/snapshot/exporter"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

func TestRestore(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	tmpRestoreDir, err := os.MkdirTemp("", "tmp_to_restore")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(tmpRestoreDir)
	})
	var exporterInstance exporter.Exporter

	ctx := context.Background()
	exporterInstance, err = ptesting.NewMockExporter(ctx, nil, "mock", map[string]string{"location": "mock://" + tmpRestoreDir})
	require.NoError(t, err)
	defer exporterInstance.Close(ctx)

	opts := &snapshot.RestoreOptions{
		MaxConcurrency: 1,
		Strip:          snap.Header.GetSource(0).Importer.Directory,
	}

	// search for the correct filepath as the path was mkdir temp we cannot hardcode it
	var filepath string
	fs, err := snap.Filesystem()
	require.NoError(t, err)
	for pathname, err := range fs.Pathnames() {
		require.NoError(t, err)
		if strings.Contains(pathname, "dummy.txt") {
			filepath = pathname
		}
	}
	require.NotEmpty(t, filepath)

	root, err := exporterInstance.Root(ctx)
	require.NoError(t, err)
	err = snap.Restore(exporterInstance, root, filepath, opts)
	require.NoError(t, err)

	mockExporter, ok := exporterInstance.(*ptesting.MockExporter)
	require.True(t, ok)

	files := mockExporter.Files()
	require.Equal(t, 1, len(files))

	contents, ok := files[fmt.Sprintf("%s/dummy.txt", root)]
	require.True(t, ok)
	require.Equal(t, "hello", string(contents))
}
