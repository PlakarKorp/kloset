package repository_test

import (
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// TestGetObjectContentAcrossPackfiles drives the packfile-boundary-crossing
// path of GetObjectContent. Two separate backups each produce their own
// packfile; we then assemble a synthetic object whose chunks reference real
// chunk blobs from both files. Because the chunks live in different packfiles
// (and at non-contiguous offsets), the iterator must flush one accumulated
// range and start a new one — the branch that single-backup tests never hit.
func TestGetObjectContentAcrossPackfiles(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	// First backup -> packfile A.
	snapA := ptesting.GenerateSnapshot(t, repo, []ptesting.MockFile{
		ptesting.NewMockDir("/"),
		ptesting.NewMockFile("/a.txt", 0644, strings.Repeat("alpha-block ", 128)),
	})
	// Second backup -> packfile B.
	snapB := ptesting.GenerateSnapshot(t, repo, []ptesting.MockFile{
		ptesting.NewMockDir("/"),
		ptesting.NewMockFile("/b.txt", 0644, strings.Repeat("bravo-block ", 128)),
	})

	require.NoError(t, repo.RebuildState())

	chunkA := firstChunk(t, snapA, "/a.txt")
	chunkB := firstChunk(t, snapB, "/b.txt")

	// Build an object interleaving chunks from the two packfiles so the
	// iterator crosses a packfile boundary mid-stream.
	obj := &objects.Object{
		Chunks: []objects.Chunk{chunkA, chunkB, chunkA},
	}

	var totalLen uint32
	for _, c := range obj.Chunks {
		totalLen += c.Length
	}

	var collected int
	for data, err := range repo.GetObjectContent(obj, 0, totalLen+1) {
		require.NoError(t, err)
		collected += len(data)
	}
	require.Greater(t, collected, 0)
}

// firstChunk resolves a file in a snapshot and returns its first chunk.
func firstChunk(t *testing.T, snap *snapshot.Snapshot, path string) objects.Chunk {
	t.Helper()
	fs, err := snap.Filesystem()
	require.NoError(t, err)

	entry, err := fs.GetEntry(path)
	require.NoError(t, err)
	require.NotNil(t, entry.ResolvedObject)
	require.NotEmpty(t, entry.ResolvedObject.Chunks)

	return entry.ResolvedObject.Chunks[0]
}
