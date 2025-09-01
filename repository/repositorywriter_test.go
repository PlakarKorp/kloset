package repository_test

import (
	"io"
	"os"
	"testing"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/packfile"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/repository/packer"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/storage"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

func TestNewRepositoryWriter(t *testing.T) {
	// Create a repository
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	require.NotNil(t, repo)

	// Create a temporary directory for the cache
	tmpCacheDir, err := os.MkdirTemp("", "kloset-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpCacheDir)

	// Create a cache manager
	cacheManager := caching.NewManager(tmpCacheDir)
	require.NotNil(t, cacheManager)

	// Create a scan cache
	snapshotID := objects.MAC{}
	scanCache, err := cacheManager.Scan(snapshotID)
	require.NoError(t, err)
	require.NotNil(t, scanCache)

	// Create a repository writer
	writer := repo.NewRepositoryWriter(scanCache, snapshotID, repository.DefaultType)
	require.NotNil(t, writer)
}

func TestBlobExists(t *testing.T) {
	// Create a repository
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	require.NotNil(t, repo)

	// Create a temporary directory for the cache
	tmpCacheDir, err := os.MkdirTemp("", "kloset-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpCacheDir)

	// Create a cache manager
	cacheManager := caching.NewManager(tmpCacheDir)
	require.NotNil(t, cacheManager)

	// Create a scan cache
	snapshotID := objects.MAC{}
	scanCache, err := cacheManager.Scan(snapshotID)
	require.NoError(t, err)
	require.NotNil(t, scanCache)

	// Create a repository writer
	writer := repo.NewRepositoryWriter(scanCache, snapshotID, repository.DefaultType)
	require.NotNil(t, writer)

	// Test blob existence
	exists := writer.BlobExists(resources.RT_CONFIG, objects.MAC{})
	require.False(t, exists)
}

func TestPutBlobIfNotExists(t *testing.T) {
	// Create a repository
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	require.NotNil(t, repo)

	// Create a temporary directory for the cache
	tmpCacheDir, err := os.MkdirTemp("", "kloset-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpCacheDir)

	// Create a cache manager
	cacheManager := caching.NewManager(tmpCacheDir)
	require.NotNil(t, cacheManager)

	// Create a scan cache
	snapshotID := objects.MAC{}
	scanCache, err := cacheManager.Scan(snapshotID)
	require.NoError(t, err)
	require.NotNil(t, scanCache)

	// Create a repository writer
	writer := repo.NewRepositoryWriter(scanCache, snapshotID, repository.DefaultType)
	require.NotNil(t, writer)

	// Test putting a blob
	err = writer.PutBlobIfNotExists(resources.RT_CONFIG, objects.MAC{}, []byte("test"))
	require.NoError(t, err)

	// Test putting the same blob again
	err = writer.PutBlobIfNotExists(resources.RT_CONFIG, objects.MAC{}, []byte("test"))
	require.NoError(t, err)
}

func TestPutBlob(t *testing.T) {
	// Create a repository
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	require.NotNil(t, repo)

	// Create a temporary directory for the cache
	tmpCacheDir, err := os.MkdirTemp("", "kloset-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpCacheDir)

	// Create a cache manager
	cacheManager := caching.NewManager(tmpCacheDir)
	require.NotNil(t, cacheManager)

	// Create a scan cache
	snapshotID := objects.MAC{}
	scanCache, err := cacheManager.Scan(snapshotID)
	require.NoError(t, err)
	require.NotNil(t, scanCache)

	// Create a repository writer
	writer := repo.NewRepositoryWriter(scanCache, snapshotID, repository.DefaultType)
	require.NotNil(t, writer)

	// Test putting a blob
	err = writer.PutBlob(resources.RT_CONFIG, objects.MAC{}, []byte("test"))
	require.NoError(t, err)
}

func TestDeleteStateResource(t *testing.T) {
	// Create a repository
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	require.NotNil(t, repo)

	// Create a temporary directory for the cache
	tmpCacheDir, err := os.MkdirTemp("", "kloset-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpCacheDir)

	// Create a cache manager
	cacheManager := caching.NewManager(tmpCacheDir)
	require.NotNil(t, cacheManager)

	// Create a scan cache
	snapshotID := objects.MAC{}
	scanCache, err := cacheManager.Scan(snapshotID)
	require.NoError(t, err)
	require.NotNil(t, scanCache)

	// Create a repository writer
	writer := repo.NewRepositoryWriter(scanCache, snapshotID, repository.DefaultType)
	require.NotNil(t, writer)

	// Test deleting a state resource
	err = writer.DeleteStateResource(resources.RT_CONFIG, objects.MAC{})
	require.NoError(t, err)
}

func TestFlushTransaction(t *testing.T) {
	// Create a repository
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	require.NotNil(t, repo)

	// Create a temporary directory for the cache
	tmpCacheDir, err := os.MkdirTemp("", "kloset-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpCacheDir)

	// Create a cache manager
	cacheManager := caching.NewManager(tmpCacheDir)
	require.NotNil(t, cacheManager)

	// Create a scan cache
	snapshotID := objects.MAC{}
	scanCache, err := cacheManager.Scan(snapshotID)
	require.NoError(t, err)
	require.NotNil(t, scanCache)

	// Create a repository writer
	writer := repo.NewRepositoryWriter(scanCache, snapshotID, repository.DefaultType)
	require.NotNil(t, writer)

	// Test flushing a transaction
	err = writer.FlushTransaction(scanCache, snapshotID)
	require.NoError(t, err)
}

func TestCommitTransaction(t *testing.T) {
	// Create a repository
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	require.NotNil(t, repo)

	// Create a temporary directory for the cache
	tmpCacheDir, err := os.MkdirTemp("", "kloset-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpCacheDir)

	// Create a cache manager
	cacheManager := caching.NewManager(tmpCacheDir)
	require.NotNil(t, cacheManager)

	// Create a scan cache
	snapshotID := objects.MAC{}
	scanCache, err := cacheManager.Scan(snapshotID)
	require.NoError(t, err)
	require.NotNil(t, scanCache)

	// Create a repository writer
	writer := repo.NewRepositoryWriter(scanCache, snapshotID, repository.DefaultType)
	require.NotNil(t, writer)

	// Test committing a transaction
	err = writer.CommitTransaction(snapshotID)
	require.NoError(t, err)
}

func TestPutPackfile(t *testing.T) {
	// Create a repository
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	require.NotNil(t, repo)

	// Create a temporary directory for the cache
	tmpCacheDir, err := os.MkdirTemp("", "kloset-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpCacheDir)

	// Create a cache manager
	cacheManager := caching.NewManager(tmpCacheDir)
	require.NotNil(t, cacheManager)

	// Create a scan cache
	snapshotID := objects.MAC{}
	scanCache, err := cacheManager.Scan(snapshotID)
	require.NoError(t, err)
	require.NotNil(t, scanCache)

	// Create a repository writer
	writer := repo.NewRepositoryWriter(scanCache, snapshotID, repository.DefaultType)
	require.NotNil(t, writer)

	// Create a packfile
	pfile, err := packfile.NewPackfileInMemory(writer.GetMACHasher)
	require.NoError(t, err)
	require.NotNil(t, pfile)

	// Test putting a packfile
	err = writer.PutPackfile(pfile)
	require.NoError(t, err)
}

func TestPutPtarPackfile(t *testing.T) {
	// Create a repository
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	require.NotNil(t, repo)

	// Create a temporary directory for the cache
	tmpCacheDir, err := os.MkdirTemp("", "kloset-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpCacheDir)

	// Create a cache manager
	cacheManager := caching.NewManager(tmpCacheDir)
	require.NotNil(t, cacheManager)

	// Create a scan cache
	snapshotID := objects.MAC{}
	scanCache, err := cacheManager.Scan(snapshotID)
	require.NoError(t, err)
	require.NotNil(t, scanCache)

	// Create a repository writer
	writer := repo.NewRepositoryWriter(scanCache, snapshotID, repository.DefaultType)
	require.NotNil(t, writer)

	// Create a packing cache
	packingCache, err := cacheManager.Packing()
	require.NoError(t, err)
	require.NotNil(t, packingCache)

	// Create a pack writer
	pwriter := packer.NewPackWriter(
		func(pw *packer.PackWriter) error {
			return nil
		},
		func(r io.Reader) (io.Reader, error) {
			return storage.Serialize(writer.GetMACHasher(), resources.RT_PACKFILE, versioning.GetCurrentVersion(resources.RT_PACKFILE), r)
		},
		writer.GetMACHasher,
		packingCache,
	)
	require.NotNil(t, pwriter)

	// Test putting a PTAR packfile
	err = writer.PutPtarPackfile(pwriter)
	require.NoError(t, err)
}
