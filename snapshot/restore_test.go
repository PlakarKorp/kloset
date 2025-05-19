package snapshot_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/appcontext"
	"github.com/PlakarKorp/kloset/snapshot"
	"github.com/PlakarKorp/kloset/snapshot/exporter"
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
	appCtx := appcontext.NewAppContext()
	exporterInstance, err = exporter.NewExporter(appCtx, map[string]string{"location": tmpRestoreDir})
	require.NoError(t, err)
	defer exporterInstance.Close()

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

	err = snap.Restore(exporterInstance, exporterInstance.Root(), filepath, opts)
	require.NoError(t, err)

	files, err := os.ReadDir(exporterInstance.Root())
	fmt.Println(files)
	require.NoError(t, err)
	require.Equal(t, 1, len(files))

	contents, err := os.ReadFile(fmt.Sprintf("%s/dummy.txt", exporterInstance.Root()))
	require.NoError(t, err)
	require.Equal(t, "hello", string(contents))
}
