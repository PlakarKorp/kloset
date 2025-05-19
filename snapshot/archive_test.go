package snapshot_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/snapshot"
	"github.com/stretchr/testify/require"
)

func TestArchive(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

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

	fmts := []snapshot.ArchiveFormat{snapshot.ArchiveTar, snapshot.ArchiveTar,
		snapshot.ArchiveZip}

	for _, format := range fmts {
		bufOut := bytes.NewBuffer(nil)
		err = snap.Archive(bufOut, format, []string{filepath}, true)
		require.NoError(t, err)
	}
}
