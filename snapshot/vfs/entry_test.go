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
		if n := d.Name(); n != "dummy.txt" && n != "big" {
			t.Error("unexpected file", n)
		}
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

func TestBigFile(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	entry, err := fs.GetEntry("/subdir/big")
	require.NoError(t, err)
	require.NotNil(t, entry)

	size := int64(6 * 10 * 1024 * 1024)

	require.Equal(t, size, entry.Size())
	require.NotNil(t, entry.ResolvedObject)

	if n := len(entry.ResolvedObject.Chunks); n <= 2 {
		t.Error("not enough chunks; would have expected at least two, got", n)
	}

	fp := entry.Open(fs).(io.ReadSeeker)

	buf := make([]byte, 256)
	var seeked int64

	n, err := fp.Read(buf[:12])
	require.NoError(t, err)
	require.Equal(t, 12, n)
	require.Equal(t, "hello\nhello\n", string(buf[:n]))

	seeked, err = fp.Seek(0, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(0), seeked)

	for i := int64(0); i < size/6; i++ {
		n, err := fp.Read(buf[:6])
		require.NoError(t, err, "at iteration %v", i)
		require.Equal(t, 6, n, "at iteration %v", i)
		require.Equal(t, "hello\n", string(buf[:n]), "at iteration %v", i)
	}

	// must get an EOF now
	n, err = fp.Read(buf[:])
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 0, n)

	// rewind and read because then i can...
	seeked, err = fp.Seek(0, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(0), seeked)
	n, err = fp.Read(buf[:12])
	require.NoError(t, err)
	require.Equal(t, 12, n)
	require.Equal(t, "hello\nhello\n", string(buf[:n]))

	// ...attempt to seek to the end of the file and...
	seeked, err = fp.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, size, seeked)

	// ...make sure we can't read anymore
	n, err = fp.Read(buf[:])
	require.NotNil(t, err)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 0, n)

	// ensure we don't seek past the end of the file
	seeked, err = fp.Seek(5, io.SeekEnd)
	require.Error(t, err)

	// attempt to read the last message
	seeked, err = fp.Seek(-6, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, size-6, seeked)

	n, err = fp.Read(buf[:])
	require.NoError(t, err)
	require.Equal(t, 6, n)
	require.Equal(t, "hello\n", string(buf[:n]))

	// make sure we're at the end now
	seeked, err = fp.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, size, seeked)

	// we're at the end, attempt to seek backward but from current
	// position...
	seeked, err = fp.Seek(-12, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, size-12, seeked)

	// ...and read
	n, err = fp.Read(buf[:])
	require.NoError(t, err)
	require.Equal(t, 12, n)
	require.Equal(t, "hello\nhello\n", string(buf[:n]))

	// attempt to seek after the first chunk0
	chunk0 := entry.ResolvedObject.Chunks[0]
	seeked, err = fp.Seek(int64(chunk0.Length)+6, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(chunk0.Length)+6, seeked)

	// so we want to read 6 bytes ("hello\n") but the chunks might
	// not end up exactly where a new "hello\n" starts.  So, we
	// need to account for the leftover.
	l := 6 + 6 - (chunk0.Length % 6)
	n, err = fp.Read(buf[:l])
	require.NoError(t, err)
	require.Equal(t, int(l), n)
	start := 6 - (chunk0.Length % 6)
	require.Equal(t, "hello\n", string(buf[start:start+6]))

	// retry again but with a bigger seek (also, relative)
	seeked, err = fp.Seek(0, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(0), seeked)

	chunk1 := entry.ResolvedObject.Chunks[1]
	third := chunk0.Length + chunk1.Length
	seeked, err = fp.Seek(int64(third)+6, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(third)+6, seeked, "out of a total of %v", size)

	// see above
	l = 6 + 6 - (third % 6)
	n, err = fp.Read(buf[:l])
	require.NoError(t, err)
	require.Equal(t, int(l), n)
	start = 6 - (third % 6)
	require.Equal(t, "hello\n", string(buf[start:start+6]))

	// just for the lulz^W coverage
	seeked, err = fp.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, size, seeked)

	seeked, err = fp.Seek(-6, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, size-6, seeked)

	// no, really!  because otherwise we're at the end and trigger
	// a different code-path.
	seeked, err = fp.Seek(-6, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, size-12, seeked)

	n, err = fp.Read(buf[:])
	require.NoError(t, err)
	require.Equal(t, 12, n)
	require.Equal(t, "hello\nhello\n", string(buf[:n]))

}
