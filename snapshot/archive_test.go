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

// TestArchiveTarballAndDir covers the tarball (gzip) format and archiving a
// directory path (the "." / rebase branches).
func TestArchiveTarballAndDir(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	var buf bytes.Buffer
	// Tarball over the whole tree, with rebase off, exercises the gzip
	// fallthrough and the non-rebased output-path branch.
	require.NoError(t, snap.Archive(&buf, snapshot.ArchiveTarball, []string{"/"}, false))
	require.NotZero(t, buf.Len())

	// Directory path with rebase on hits the "outpath == ''" => "." branch.
	var buf2 bytes.Buffer
	require.NoError(t, snap.Archive(&buf2, snapshot.ArchiveTar, []string{"/"}, true))
}

// TestArchiveInvalidFormat covers the ErrInvalidArchiveFormat branch.
func TestArchiveInvalidFormat(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	err := snap.Archive(&bytes.Buffer{}, "bogus", []string{"/"}, true)
	require.ErrorIs(t, err, snapshot.ErrInvalidArchiveFormat)
}

// TestArchiveMissingPath covers the WalkDir-error branch in Archive.
func TestArchiveMissingPath(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	err := snap.Archive(&bytes.Buffer{}, snapshot.ArchiveTar, []string{"/no/such/path"}, true)
	require.Error(t, err)
}
