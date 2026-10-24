package snapshot_test

import (
	"archive/tar"
	"bytes"
	"io"
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

// TestArchiveTarXattr checks that a tar archive carries each file's extended
// attributes as SCHILY.xattr PAX records, the convention GNU tar and bsdtar
// read back, and that zip archives still build without them.
func TestArchiveTarXattr(t *testing.T) {
	snap := generateXattrBackup(t)
	defer snap.Close()

	var buf bytes.Buffer
	require.NoError(t, snap.Archive(&buf, snapshot.ArchiveTar, []string{"/tmp/src"}, true))

	want := map[string]map[string]string{
		"plain.txt": {"SCHILY.xattr.com.apple.provenance": "p1"},
		"sub/file.txt": {
			"SCHILY.xattr.com.apple.provenance": "p2",
			"SCHILY.xattr.user.bin":             "a\tb",
			"SCHILY.xattr.user.note":            "note value",
		},
	}

	tr := tar.NewReader(&buf)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, want[hdr.Name], hdr.PAXRecords, hdr.Name)
		delete(want, hdr.Name)
	}
	require.Empty(t, want)

	buf.Reset()
	require.NoError(t, snap.Archive(&buf, snapshot.ArchiveZip, []string{"/tmp/src"}, true))
	require.NotZero(t, buf.Len())
}
