package vfs_test

import (
	"bytes"
	"io"
	"math/rand/v2"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// largeFile spans more read-ahead windows than a sequential reader keeps in
// flight, and repeats its first third so some chunks occur twice.
func largeFile(t *testing.T) (*repository.Repository, *vfs.Filesystem, []byte) {
	t.Helper()

	rng := rand.New(rand.NewChaCha8([32]byte{}))
	a := make([]byte, 16<<20)
	b := make([]byte, 16<<20)
	for i := range a {
		a[i] = byte(rng.Uint32())
		b[i] = byte(rng.Uint32())
	}
	content := slices3(a, b, a)

	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	snap := ptesting.GenerateSnapshot(t, repo, nil, ptesting.WithGenerator(func(ch chan<- *connectors.Record) {
		ch <- connectors.NewRecord("/", "", objects.FileInfo{
			Lname: "/",
			Lmode: os.ModeDir | 0755,
		}, nil, nil)
		ch <- connectors.NewRecord("/large", "", objects.FileInfo{
			Lname: "large",
			Lmode: 0644,
			Lsize: int64(len(content)),
		}, nil, func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(content)), nil
		})
	}))
	t.Cleanup(func() { snap.Close() })

	fs, err := snap.Filesystem()
	require.NoError(t, err)
	return repo, fs, content
}

func slices3(a, b, c []byte) []byte {
	out := make([]byte, 0, len(a)+len(b)+len(c))
	return append(append(append(out, a...), b...), c...)
}

func TestOpenSequentialReadsWholeFile(t *testing.T) {
	_, fs, content := largeFile(t)

	entry, err := fs.GetEntry("/large")
	require.NoError(t, err)

	f, err := entry.OpenSequential(fs)
	require.NoError(t, err)
	defer f.Close()

	got, err := io.ReadAll(f)
	require.NoError(t, err)
	require.True(t, bytes.Equal(content, got), "sequential read differs from the backed up content")
}

func TestOpenSequentialSeekAfterRead(t *testing.T) {
	_, fs, content := largeFile(t)

	entry, err := fs.GetEntry("/large")
	require.NoError(t, err)

	f, err := entry.OpenSequential(fs)
	require.NoError(t, err)
	defer f.Close()
	rs := f.(io.ReadSeeker)

	// read past a few windows, then jump both ways.
	head := make([]byte, 13<<20)
	_, err = io.ReadFull(rs, head)
	require.NoError(t, err)
	require.True(t, bytes.Equal(content[:len(head)], head))

	for _, off := range []int64{5<<20 + 17, 40<<20 + 3, 0} {
		pos, err := rs.Seek(off, io.SeekStart)
		require.NoError(t, err)
		require.Equal(t, off, pos)

		buf := make([]byte, 1<<20)
		_, err = io.ReadFull(rs, buf)
		require.NoError(t, err)
		require.True(t, bytes.Equal(content[off:off+int64(len(buf))], buf), "wrong bytes after seek to %d", off)
	}
}

func TestOpenSequentialCloseStopsReadahead(t *testing.T) {
	_, fs, _ := largeFile(t)

	entry, err := fs.GetEntry("/large")
	require.NoError(t, err)

	before := runtime.NumGoroutine()

	f, err := entry.OpenSequential(fs)
	require.NoError(t, err)

	buf := make([]byte, 1<<20)
	_, err = io.ReadFull(f, buf)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		b := make([]byte, 1<<20)
		t.Fatalf("%d goroutines after Close, %d before\n%s", n, before, b[:runtime.Stack(b, true)])
	}
}

func TestGetObjectChunksRange(t *testing.T) {
	repo, fs, content := largeFile(t)

	entry, err := fs.GetEntry("/large")
	require.NoError(t, err)
	f, err := entry.Open(fs)
	require.NoError(t, err)
	f.Close() // Open resolved the object.

	chunks := entry.ResolvedObject.Chunks
	require.Greater(t, len(chunks), 4)

	start, end := 2, len(chunks)-1
	var off int64
	for _, c := range chunks[:start] {
		off += int64(c.Length)
	}
	var want []byte
	want = content[off:]
	want = want[:len(want)-int(chunks[len(chunks)-1].Length)]

	var got []byte
	n := 0
	for data, err := range repo.GetObjectChunks(entry.ResolvedObject, start, end) {
		require.NoError(t, err)
		got = append(got, data...)
		n++
	}
	require.Equal(t, end-start, n)
	require.True(t, bytes.Equal(want, got), "chunks [%d, %d) differ from the content", start, end)
}
