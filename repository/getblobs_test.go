package repository_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// chunkRequests backs up a few files and returns a BlobReq per chunk
// of every resolved object, so GetBlobs can be exercised against blobs
// that really live in packfiles.
func chunkRequests(t *testing.T, repo *repository.Repository, paths ...string) []repository.BlobReq {
	files := []ptesting.MockFile{ptesting.NewMockDir("/")}
	for i, p := range paths {
		files = append(files, ptesting.NewMockFile(p, 0644,
			strings.Repeat(fmt.Sprintf("get-blobs payload %d ", i), 64)))
	}
	snap := ptesting.GenerateSnapshot(t, repo, files)
	require.NotNil(t, snap)

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	var reqs []repository.BlobReq
	for _, p := range paths {
		entry, err := fs.GetEntry(p)
		require.NoError(t, err)
		require.NotNil(t, entry.ResolvedObject)
		for _, c := range entry.ResolvedObject.Chunks {
			reqs = append(reqs, repository.BlobReq{Type: resources.RT_CHUNK, MAC: c.ContentMAC})
		}
	}
	require.NotEmpty(t, reqs)
	return reqs
}

func TestGetBlobsRoundtrip(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	reqs := chunkRequests(t, repo, "/a.txt", "/b.txt", "/c.txt")

	// Duplicate every request: set semantics must collapse them.
	got := make(map[repository.BlobReq][]byte)
	for res, err := range repo.GetBlobs(context.Background(), append(reqs, reqs...), nil) {
		require.NoError(t, err)
		req := repository.BlobReq{Type: res.Type, MAC: res.MAC}
		_, dup := got[req]
		require.False(t, dup, "blob %x delivered twice", res.MAC)
		got[req] = res.Data
	}

	distinct := make(map[repository.BlobReq]struct{})
	for _, req := range reqs {
		distinct[req] = struct{}{}
	}
	require.Len(t, got, len(distinct))

	// Every blob must byte-match the single-blob read path.
	for req, data := range got {
		expected, err := repo.GetBlobBytes(req.Type, req.MAC)
		require.NoError(t, err)
		require.Equal(t, expected, data)
	}
}

func TestGetBlobsMissingBlob(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	reqs := chunkRequests(t, repo, "/a.txt")

	bogus := objects.MAC{0xDE, 0xAD, 0xBE, 0xEF}
	reqs = append(reqs, repository.BlobReq{Type: resources.RT_CHUNK, MAC: bogus})

	var missing, found int
	for res, err := range repo.GetBlobs(context.Background(), reqs, nil) {
		if res.MAC == bogus {
			require.ErrorIs(t, err, repository.ErrBlobNotFound)
			require.Nil(t, res.Data)
			missing++
		} else {
			require.NoError(t, err)
			found++
		}
	}
	require.Equal(t, 1, missing)
	require.Equal(t, len(reqs)-1, found)
}

func TestGetBlobsAllMissing(t *testing.T) {
	// A batch where NO request resolves: every result carries
	// ErrBlobNotFound and the iterator terminates cleanly.
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	_ = chunkRequests(t, repo, "/a.txt") // populate the repo, request none of it

	reqs := []repository.BlobReq{
		{Type: resources.RT_CHUNK, MAC: objects.MAC{0xDE, 0xAD}},
		{Type: resources.RT_CHUNK, MAC: objects.MAC{0xBE, 0xEF}},
	}

	count := 0
	for res, err := range repo.GetBlobs(context.Background(), reqs, nil) {
		require.ErrorIs(t, err, repository.ErrBlobNotFound)
		require.Nil(t, res.Data)
		count++
	}
	require.Equal(t, len(reqs), count)
}

func TestGetBlobsRangeReadError(t *testing.T) {
	// Blobs resolve in the state but the packfile is gone from the
	// store: every blob of the failed range yields its identity plus
	// the read error — no panic, no phantom success.
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	reqs := chunkRequests(t, repo, "/a.txt", "/b.txt")

	for mac := range repo.ListPackfiles() {
		require.NoError(t, repo.DeletePackfile(mac))
	}

	count := 0
	for res, err := range repo.GetBlobs(context.Background(), reqs, nil) {
		require.Error(t, err)
		require.Nil(t, res.Data)
		count++
	}
	require.Equal(t, len(reqs), count)
}

func TestGetBlobsEmpty(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	for res, err := range repo.GetBlobs(context.Background(), nil, nil) {
		t.Fatalf("unexpected result %x (err %v)", res.MAC, err)
	}
}
