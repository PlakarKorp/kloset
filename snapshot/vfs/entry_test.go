package vfs_test

import (
	"encoding/json"
	"io"
	iofs "io/fs"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVfile(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	// search for the correct filepath as the path was mkdir temp we cannot hardcode it
	var filepath string
	for pathname, err := range fs.Pathnames() {
		require.NoError(t, err)
		if strings.Contains(pathname, "dummy.txt") {
			filepath = pathname
		}
	}
	require.NotEmpty(t, filepath)

	entry, err := fs.GetEntry(filepath)
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, "dummy.txt", entry.Name())

	require.Equal(t, "text/plain; charset=utf-8", entry.ContentType())
	require.Equal(t, float64(1.9219280948873625), entry.Entropy())

	entry.AddClassification("foo", []string{"bar"})

	require.NotNil(t, entry.Stat())
	require.Equal(t, "dummy.txt", entry.Name())
	require.Equal(t, int64(5), entry.Size())
	require.Equal(t, iofs.FileMode(0x1a4), entry.Type())
	require.Equal(t, false, entry.IsDir())
	fileinfo, err := entry.Info()
	require.NoError(t, err)
	require.Implements(t, (*iofs.FileInfo)(nil), fileinfo)

	entryJson, err := json.Marshal(entry)
	require.NoError(t, err)
	// can't test the whole json content as there are some random part included
	require.Contains(t, string(entryJson), `"file_info":{"name":"dummy.txt","size":5,"mode":420`)

	vFile := entry.Open(fs)

	seeked, err := vFile.(io.ReadSeeker).Seek(2, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(2), seeked)

	dst := make([]byte, 10)
	require.Implements(t, (*io.ReadSeeker)(nil), vFile)
	seeked, err = vFile.(io.ReadSeeker).Seek(0, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(5), seeked)

	seeked, err = vFile.(io.ReadSeeker).Seek(2, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(2), seeked)

	seeked, err = vFile.(io.ReadSeeker).Seek(1, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(3), seeked)

	n, err := vFile.Read(dst)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, "lo", strings.Trim(string(dst), "\x00"))

	statinfo, err := vFile.Stat()
	require.NoError(t, nil, err)
	require.Implements(t, (*iofs.FileInfo)(nil), statinfo)
}

func TestVdir(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	// search for the correct filepath as the path was mkdir temp we cannot hardcode it
	var filepath string
	for pathname, err := range fs.Pathnames() {
		require.NoError(t, err)
		if strings.Contains(pathname, "subdir") {
			filepath = pathname
			break
		}
	}
	require.NotEmpty(t, filepath)

	entry, err := fs.GetEntry(filepath)
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.True(t, entry.IsDir())

	dents, err := entry.Getdents(fs)
	require.NoError(t, err)
	for d, err := range dents {
		require.NoError(t, err)
		require.Equal(t, "dummy.txt", d.Name())
	}

	dst := make([]byte, 100)
	dirFile := entry.Open(fs)
	_, err = dirFile.Read(dst)
	require.Error(t, iofs.ErrInvalid, err)

	require.Implements(t, (*io.ReadSeeker)(nil), dirFile)
	_, err = dirFile.(io.ReadSeeker).Seek(1, 1)
	require.Error(t, iofs.ErrInvalid, err)

	fileinfo, err := dirFile.Stat()
	require.NoError(t, nil, err)
	require.Implements(t, (*iofs.FileInfo)(nil), fileinfo)
}
