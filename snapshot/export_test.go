package snapshot_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/connectors/exporter"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

func TestExport(t *testing.T) {
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

	opts := &snapshot.ExportOptions{
		Strip: snap.Header.GetSource(0).Importer.Directory,
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

	err = snap.Export(exporterInstance, filepath, opts)
	require.NoError(t, err)

	mockExporter, ok := exporterInstance.(*ptesting.MockExporter)
	require.True(t, ok)

	files := mockExporter.Files()
	require.Equal(t, 1, len(files))

	contents, ok := files["/dummy.txt"]
	require.True(t, ok)
	require.Equal(t, "hello", string(contents))
}

// TestExportDirectory exercises Export on a directory entry (rather than a
// single file), which takes a different code path inside Export: the
// `entry.IsDir()` branch where `tostrip` stays as-is and no synthetic root
// record is injected.
func TestExportDirectory(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	tmp := t.TempDir()
	exp, err := ptesting.NewMockExporter(context.Background(), nil, "mock", map[string]string{"location": "mock://" + tmp})
	require.NoError(t, err)
	defer exp.Close(context.Background())

	fs, err := snap.Filesystem()
	require.NoError(t, err)
	var dirPath string
	for p, err := range fs.Pathnames() {
		require.NoError(t, err)
		if strings.HasSuffix(p, "/docs") {
			dirPath = p
			break
		}
	}
	require.NotEmpty(t, dirPath, "expected /docs in snapshot pathnames")

	require.NoError(t, snap.Export(exp, dirPath, &snapshot.ExportOptions{
		Strip: snap.Header.GetSource(0).Importer.Directory,
	}))

	mockExp, ok := exp.(*ptesting.MockExporter)
	require.True(t, ok)
	files := mockExp.Files()
	require.GreaterOrEqual(t, len(files), 2)
}

// TestExportFromRoot exercises Export starting at the snapshot root with no
// Strip option, forcing the path-rewrite branch.
func TestExportFromRoot(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	tmp := t.TempDir()
	exp, err := ptesting.NewMockExporter(context.Background(), nil, "mock", map[string]string{"location": "mock://" + tmp})
	require.NoError(t, err)
	defer exp.Close(context.Background())

	require.NoError(t, snap.Export(exp, "/", &snapshot.ExportOptions{}))

	mockExp, ok := exp.(*ptesting.MockExporter)
	require.True(t, ok)
	require.NotEmpty(t, mockExp.Files())
}

// TestExportMissingPath verifies Export returns an error when the pathname
// does not exist in the snapshot.
func TestExportMissingPath(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	tmp := t.TempDir()
	exp, err := ptesting.NewMockExporter(context.Background(), nil, "mock", map[string]string{"location": "mock://" + tmp})
	require.NoError(t, err)
	defer exp.Close(context.Background())

	err = snap.Export(exp, "/no/such/path", &snapshot.ExportOptions{})
	require.Error(t, err)
}
