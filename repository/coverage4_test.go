package repository_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/encryption"
	"github.com/PlakarKorp/kloset/hashing"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/repository/state"
	"github.com/PlakarKorp/kloset/resources"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

// TestGetPackfileRangeMissing exercises the store.Get error branch of
// GetPackfileRange: requesting a range from a packfile MAC that was never
// written must surface an error rather than return data.
func TestGetPackfileRangeMissing(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	loc := state.Location{
		Packfile: objects.MAC{0xDE, 0xAD, 0xBE, 0xEF},
		Offset:   0,
		Length:   32,
	}
	_, err := repo.GetPackfileRange(loc)
	require.Error(t, err)
}

// TestGetPackfileBlobMissing exercises the GetPackfileRange error propagation
// inside GetPackfileBlob for an unknown packfile.
func TestGetPackfileBlobMissing(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	loc := state.Location{
		Packfile: objects.MAC{0xBA, 0xDB, 0xAD},
		Offset:   0,
		Length:   16,
	}
	_, err := repo.GetPackfileBlob(loc)
	require.Error(t, err)
}

// TestInexistentBadLocation drives the storage.New error branch of Inexistent
// by passing a store configuration with an unknown backend scheme.
func TestInexistentBadLocation(t *testing.T) {
	ctx := ptesting.GenerateRepository(t, nil, nil, nil).AppContext()

	_, err := repository.Inexistent(ctx, map[string]string{
		"location": "this-scheme-does-not-exist://nowhere",
	})
	require.Error(t, err)
}

// TestGetPackfileForBlobAfterBackup resolves a real blob location after a
// backup, exercising the found branch of GetPackfileForBlob (the existing
// tests only cover the not-found case).
func TestGetPackfileForBlobAfterBackup(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	files := []ptesting.MockFile{
		ptesting.NewMockDir("/"),
		ptesting.NewMockFile("/lookup.txt", 0644, "find this blob's packfile"),
	}
	snap := ptesting.GenerateSnapshot(t, repo, files)
	require.NotNil(t, repo)
	require.NoError(t, repo.RebuildState())

	fs, err := snap.Filesystem()
	require.NoError(t, err)
	entry, err := fs.GetEntry("/lookup.txt")
	require.NoError(t, err)
	require.NotNil(t, entry.ResolvedObject)
	require.NotEmpty(t, entry.ResolvedObject.Chunks)

	chunkMAC := entry.ResolvedObject.Chunks[0].ContentMAC
	_, exists, err := repo.GetPackfileForBlob(resources.RT_CHUNK, chunkMAC)
	require.NoError(t, err)
	require.True(t, exists, "a chunk written during backup must resolve to a packfile")
}

// TestNewNoRebuildEncrypted exercises the secret != nil (MAC-hasher) branch of
// NewNoRebuild by building an encrypted configuration and opening it with the
// derived key.
func TestNewNoRebuildEncrypted(t *testing.T) {
	ctx := ptesting.GenerateContext(t, nil, nil)

	tmpCacheDir, err := os.MkdirTemp("", "tmp_cache_enc")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpCacheDir) })
	ctx.CacheDir = tmpCacheDir

	st, err := storage.New(ctx, map[string]string{"location": "mock:///enc"})
	require.NoError(t, err)

	config := storage.NewConfiguration()
	config.Compression = nil

	passphrase := []byte("new-no-rebuild-encrypted-pass")
	key, err := encryption.DeriveKey(config.Encryption.KDFParams, passphrase)
	require.NoError(t, err)

	canary, err := encryption.DeriveCanary(config.Encryption, key)
	require.NoError(t, err)
	config.Encryption.Canary = canary

	hasher := hashing.GetMACHasher(storage.DEFAULT_HASHING_ALGORITHM, key)

	serialized, err := config.ToBytes()
	require.NoError(t, err)

	wrappedRd, err := storage.Serialize(hasher, resources.RT_CONFIG, versioning.GetCurrentVersion(resources.RT_CONFIG), bytes.NewReader(serialized))
	require.NoError(t, err)
	wrapped, err := io.ReadAll(wrappedRd)
	require.NoError(t, err)

	require.NoError(t, st.Create(ctx, wrapped))

	repo, err := repository.NewNoRebuild(ctx, key, st, wrapped, false)
	require.NoError(t, err)
	require.NotNil(t, repo)
}
