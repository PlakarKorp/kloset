package repository_test

import (
	"bytes"
	"testing"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/caching/pebble"
	"github.com/PlakarKorp/kloset/compression"
	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// compressedRepository returns a repository that compresses its blobs, so a
// run of zeros is stored small and expands on read.
func compressedRepository(t *testing.T) *repository.Repository {
	return ptesting.GenerateRepositoryWithConfig(t, nil, nil, nil, func(c *storage.Configuration) {
		c.Compression = compression.NewDefaultConfiguration()
	})
}

// putBlob writes data as a blob of Type through a writer and commits it, so
// it lives in a packfile and resolves through the repository state. It
// bypasses the chunker and so can store a chunk of any size.
func putBlob(t *testing.T, repo *repository.Repository, Type resources.Type, mac objects.MAC, data []byte) {
	snapshotID := mac
	scanCache, err := caching.NewManager(pebble.Constructor(t.TempDir())).Scan(snapshotID)
	require.NoError(t, err)

	writer := repo.NewRepositoryWriter(scanCache, snapshotID, repository.DefaultType, "")
	require.NoError(t, writer.PutBlob(Type, mac, data, true))
	writer.PackerManager.Wait()
	require.NoError(t, writer.CommitTransaction(snapshotID))
	require.NoError(t, repo.RebuildState())
}

// TestGetBlobCeiling stores runs of zeros, which compress to almost nothing,
// and checks that a read is refused once the decoded size passes the ceiling
// for the blob's type, and only then.
func TestGetBlobCeiling(t *testing.T) {
	repo := compressedRepository(t)
	maxChunk := int(repo.Configuration().Chunking.MaxSize)

	tests := []struct {
		name     string
		typ      resources.Type
		size     int
		tooLarge bool
	}{
		{"largest chunk the chunker produces", resources.RT_CHUNK, maxChunk, false},
		{"chunk past the chunker maximum", resources.RT_CHUNK, maxChunk + 1, true},
		{"metadata past 64 MiB", resources.RT_VFS_ENTRY, 64<<20 + 1, true},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mac := objects.MAC{byte(i + 1)}
			stored := bytes.Repeat([]byte{0}, tt.size)
			putBlob(t, repo, tt.typ, mac, stored)

			data, err := repo.GetBlobBytes(tt.typ, mac)
			if tt.tooLarge {
				require.ErrorIs(t, err, repository.ErrBlobTooLarge)
				return
			}
			require.NoError(t, err)
			require.Equal(t, stored, data)
		})
	}
}
