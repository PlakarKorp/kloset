package repository_test

import (
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// TestGetObjectContent drives GetObjectContent over a real object retrieved
// from a snapshot's vfs after a backup.
func TestGetObjectContent(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	// Use enough data to produce at least one chunk; the chunker's minimum
	// chunk size is small enough that 1KB is sufficient.
	payload := strings.Repeat("get-object-content payload ", 64)

	files := []ptesting.MockFile{
		ptesting.NewMockDir("/"),
		ptesting.NewMockFile("/content.txt", 0644, payload),
	}
	snap := ptesting.GenerateSnapshot(t, repo, files)
	require.NotNil(t, snap)

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	entry, err := fs.GetEntry("/content.txt")
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.NotNil(t, entry.ResolvedObject)
	require.NotEmpty(t, entry.ResolvedObject.Chunks)

	obj := entry.ResolvedObject
	var totalLen uint32
	for _, c := range obj.Chunks {
		totalLen += c.Length
	}

	collected := make([]byte, 0, totalLen)
	for data, gErr := range repo.GetObjectContent(obj, 0, totalLen+1) {
		require.NoError(t, gErr)
		collected = append(collected, data...)
	}
	require.NotEmpty(t, collected)
}

// TestGetObjectContentLimited exercises the maxSize cutoff path of
// GetObjectContent — pass a very small maxSize so the iterator breaks out of
// the loop early.
func TestGetObjectContentLimited(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	payload := strings.Repeat("limited content payload ", 256)

	files := []ptesting.MockFile{
		ptesting.NewMockDir("/"),
		ptesting.NewMockFile("/limited.txt", 0644, payload),
	}
	snap := ptesting.GenerateSnapshot(t, repo, files)
	require.NotNil(t, snap)

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	entry, err := fs.GetEntry("/limited.txt")
	require.NoError(t, err)
	require.NotNil(t, entry)

	obj := entry.ResolvedObject
	require.NotNil(t, obj)

	for _, gErr := range repo.GetObjectContent(obj, 0, 1) {
		require.NoError(t, gErr)
	}
}

// TestGetObjectContentEmpty hits the zero-chunk path: the iterator should
// yield nothing and not error.
func TestGetObjectContentEmpty(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	obj := &objects.Object{}

	var seen int
	for _, err := range repo.GetObjectContent(obj, 0, 1024) {
		require.NoError(t, err)
		seen++
	}
	require.Equal(t, 0, seen)
}

// TestGetObjectContentMissingBlob exercises the !exists path: the iterator
// should yield ErrPackfileNotFound when an object references an unknown chunk.
func TestGetObjectContentMissingBlob(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	obj := &objects.Object{
		Chunks: []objects.Chunk{
			{ContentMAC: objects.MAC{0xDE, 0xAD}, Length: 16},
		},
	}

	for _, err := range repo.GetObjectContent(obj, 0, 1024) {
		require.Error(t, err)
		return
	}
	t.Fatalf("expected at least one error from GetObjectContent")
}
