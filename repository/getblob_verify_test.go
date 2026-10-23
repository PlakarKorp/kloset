package repository_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/repository/state"
	"github.com/PlakarKorp/kloset/resources"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// swapChunks backs up two same-sized files and exchanges their chunks
// inside the packfile, the way a storage backend with write access could.
// Each blob is encoded on its own, so the swapped bytes still decode:
// only the content MAC tells them apart. It returns the two objects.
func swapChunks(t *testing.T, repo *repository.Repository) []*objects.Object {
	files := []ptesting.MockFile{
		ptesting.NewMockDir("/"),
		ptesting.NewMockFile("/a.txt", 0644, strings.Repeat("A", 4096)),
		ptesting.NewMockFile("/b.txt", 0644, strings.Repeat("B", 4096)),
	}
	snap := ptesting.GenerateSnapshot(t, repo, files)
	require.NotNil(t, snap)

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	var objs []*objects.Object
	var locs []state.Location
	for _, p := range []string{"/a.txt", "/b.txt"} {
		entry, err := fs.GetEntry(p)
		require.NoError(t, err)
		require.NotNil(t, entry.ResolvedObject)
		require.Len(t, entry.ResolvedObject.Chunks, 1)
		objs = append(objs, entry.ResolvedObject)

		for de, err := range repo.ListBlobs(resources.RT_CHUNK) {
			require.NoError(t, err)
			if de.Blob == entry.ResolvedObject.Chunks[0].ContentMAC {
				locs = append(locs, de.Location)
			}
		}
	}
	require.Len(t, locs, 2)
	require.Equal(t, locs[0].Packfile, locs[1].Packfile)
	require.Equal(t, locs[0].Length, locs[1].Length)

	ctx := context.Background()
	store := repo.Store()
	rd, err := store.Get(ctx, storage.StorageResourcePackfile, locs[0].Packfile, nil)
	require.NoError(t, err)
	raw, err := io.ReadAll(rd)
	require.NoError(t, err)
	require.NoError(t, rd.Close())

	a := raw[uint64(storage.STORAGE_HEADER_SIZE)+locs[0].Offset:][:locs[0].Length]
	b := raw[uint64(storage.STORAGE_HEADER_SIZE)+locs[1].Offset:][:locs[1].Length]
	tmp := make([]byte, len(a))
	copy(tmp, a)
	copy(a, b)
	copy(b, tmp)

	require.NoError(t, store.Delete(ctx, storage.StorageResourcePackfile, locs[0].Packfile))
	_, err = store.Put(ctx, storage.StorageResourcePackfile, locs[0].Packfile, bytes.NewReader(raw))
	require.NoError(t, err)

	return objs
}

func TestGetBlobRejectsSwappedChunks(t *testing.T) {
	passphrase := []byte("secret")
	for name, pass := range map[string]*[]byte{"plaintext": nil, "encrypted": &passphrase} {
		t.Run(name, func(t *testing.T) {
			repo := ptesting.GenerateRepository(t, nil, nil, pass)
			objs := swapChunks(t, repo)

			var reqs []repository.BlobReq
			for _, obj := range objs {
				mac := obj.Chunks[0].ContentMAC
				reqs = append(reqs, repository.BlobReq{Type: resources.RT_CHUNK, MAC: mac})

				_, err := repo.GetBlob(resources.RT_CHUNK, mac)
				require.ErrorIs(t, err, repository.ErrBlobCorrupted)

				_, err = repo.GetBlobBytes(resources.RT_CHUNK, mac)
				require.ErrorIs(t, err, repository.ErrBlobCorrupted)

				for data, err := range repo.GetObjectContent(obj, 0, 1<<20) {
					require.ErrorIs(t, err, repository.ErrBlobCorrupted)
					require.Nil(t, data)
				}
			}

			count := 0
			for res, err := range repo.GetBlobs(context.Background(), reqs, nil) {
				require.ErrorIs(t, err, repository.ErrBlobCorrupted)
				require.Nil(t, res.Data)
				count++
			}
			require.Equal(t, len(reqs), count)
		})
	}
}

// Headers are stored under the snapshot identifier, not under their own
// MAC: they must still load.
func TestGetBlobSnapshotHeaderNotVerified(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	snap := ptesting.GenerateSnapshot(t, repo, []ptesting.MockFile{ptesting.NewMockDir("/")})
	require.NotNil(t, snap)

	_, err := repo.GetBlobBytes(resources.RT_SNAPSHOT, snap.Header.Identifier)
	require.NoError(t, err)
}
