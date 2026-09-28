package repository_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

func TestListBlobs(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	reqs := chunkRequests(t, repo, "/a.txt", "/b.txt")

	packfiles := make(map[objects.MAC]struct{})
	for mac := range repo.ListPackfiles() {
		packfiles[mac] = struct{}{}
	}
	require.NotEmpty(t, packfiles)

	listed := make(map[objects.MAC]struct{})
	for de, err := range repo.ListBlobs(resources.RT_CHUNK) {
		require.NoError(t, err)
		require.Equal(t, resources.RT_CHUNK, de.Type)
		require.Contains(t, packfiles, de.Location.Packfile)
		require.NotZero(t, de.Location.Length)
		listed[de.Blob] = struct{}{}
	}

	for _, req := range reqs {
		require.Contains(t, listed, req.MAC)
	}
}

func TestListBlobsReadable(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	_ = chunkRequests(t, repo, "/a.txt")

	count := 0
	for _, typ := range resources.Types() {
		for de, err := range repo.ListBlobs(typ) {
			require.NoError(t, err)
			require.True(t, repo.BlobExists(typ, de.Blob))
			count++
		}
	}
	require.NotZero(t, count)
}

func TestListBlobsEmptyRepository(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	for _, typ := range resources.Types() {
		for de, err := range repo.ListBlobs(typ) {
			t.Fatalf("unexpected blob %x of type %s (err %v)", de.Blob, typ, err)
		}
	}
}
