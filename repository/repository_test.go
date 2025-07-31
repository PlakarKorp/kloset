package repository_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/PlakarKorp/kloset/hashing"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/storage"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

func TestRepository(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	t.Run("NewRepository", func(t *testing.T) {
		require.NotNil(t, repo)
		require.NotNil(t, repo.Store())
		require.NotNil(t, repo.AppContext())
	})

	t.Run("GetMACHasher", func(t *testing.T) {
		hasher := repo.GetMACHasher()
		require.NotNil(t, hasher)
	})

	t.Run("GetPooledMACHasher", func(t *testing.T) {
		hasher, release := repo.GetPooledMACHasher()
		require.NotNil(t, hasher)
		require.NotNil(t, release)
		release()
	})

	t.Run("ComputeMAC", func(t *testing.T) {
		data := []byte("test data")
		mac := repo.ComputeMAC(data)
		require.NotEqual(t, objects.MAC{}, mac)
	})

	t.Run("GetSnapshots", func(t *testing.T) {
		snapshots, err := repo.GetSnapshots()
		require.NoError(t, err)
		require.NotNil(t, snapshots)
	})

	t.Run("GetStates", func(t *testing.T) {
		states, err := repo.GetStates()
		require.NoError(t, err)
		require.NotNil(t, states)
	})

	t.Run("GetPackfiles", func(t *testing.T) {
		packfiles, err := repo.GetPackfiles()
		require.NoError(t, err)
		require.NotNil(t, packfiles)
	})

	t.Run("BlobExists", func(t *testing.T) {
		exists := repo.BlobExists(resources.RT_CONFIG, objects.MAC{})
		require.False(t, exists)
	})

	t.Run("GetBlob", func(t *testing.T) {
		_, err := repo.GetBlob(resources.RT_CONFIG, objects.MAC{})
		require.Error(t, err)
		require.Equal(t, repository.ErrPackfileNotFound, err)
	})

	t.Run("GetBlobBytes", func(t *testing.T) {
		_, err := repo.GetBlobBytes(resources.RT_CONFIG, objects.MAC{})
		require.Error(t, err)
		require.Equal(t, repository.ErrPackfileNotFound, err)
	})

	t.Run("GetLocks", func(t *testing.T) {
		locks, err := repo.GetLocks()
		require.NoError(t, err)
		require.NotNil(t, locks)
	})

	t.Run("GetLock", func(t *testing.T) {
		_, _, err := repo.GetLock(objects.MAC{})
		require.Error(t, err)
	})

	t.Run("DeleteLock", func(t *testing.T) {
		err := repo.DeleteLock(objects.MAC{})
		require.NoError(t, err)
	})

	t.Run("StorageSize", func(t *testing.T) {
		size, err := repo.StorageSize()
		require.NoError(t, err)
		require.GreaterOrEqual(t, size, int64(0))
	})

	t.Run("RBytes", func(t *testing.T) {
		bytes := repo.RBytes()
		require.GreaterOrEqual(t, bytes, int64(0))
	})

	t.Run("WBytes", func(t *testing.T) {
		bytes := repo.WBytes()
		require.GreaterOrEqual(t, bytes, int64(0))
	})

	t.Run("Close", func(t *testing.T) {
		err := repo.Close()
		require.NoError(t, err)
	})
}

func TestRepositoryState(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	t.Run("RebuildState", func(t *testing.T) {
		err := repo.RebuildState()
		require.NoError(t, err)
	})

	t.Run("PutCurrentState", func(t *testing.T) {
		err := repo.PutCurrentState()
		require.NoError(t, err)
	})

	t.Run("ListSnapshots", func(t *testing.T) {
		snapshots := repo.ListSnapshots()
		require.NotNil(t, snapshots)
	})

	t.Run("ListPackfiles", func(t *testing.T) {
		packfiles := repo.ListPackfiles()
		require.NotNil(t, packfiles)
	})

	t.Run("ListOrphanBlobs", func(t *testing.T) {
		orphans := repo.ListOrphanBlobs()
		require.NotNil(t, orphans)
	})

	t.Run("ListDeletedPackfiles", func(t *testing.T) {
		deleted := repo.ListDeletedPackfiles()
		require.NotNil(t, deleted)
	})

	t.Run("ListDeletedSnapShots", func(t *testing.T) {
		deleted := repo.ListDeletedSnapShots()
		require.NotNil(t, deleted)
	})
}

func TestRepositoryPackfileOperations(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	t.Run("GetPackfile", func(t *testing.T) {
		_, err := repo.GetPackfile(objects.MAC{})
		require.Error(t, err)
	})

	t.Run("DeletePackfile", func(t *testing.T) {
		err := repo.DeletePackfile(objects.MAC{})
		require.NoError(t, err)
	})

	t.Run("RemovePackfile", func(t *testing.T) {
		err := repo.RemovePackfile(objects.MAC{})
		require.NoError(t, err)
	})

	t.Run("HasDeletedPackfile", func(t *testing.T) {
		has, err := repo.HasDeletedPackfile(objects.MAC{})
		require.NoError(t, err)
		require.False(t, has)
	})

	t.Run("RemoveDeletedPackfile", func(t *testing.T) {
		err := repo.RemoveDeletedPackfile(objects.MAC{})
		require.NoError(t, err)
	})

	t.Run("GetPackfileForBlob", func(t *testing.T) {
		_, exists, err := repo.GetPackfileForBlob(resources.RT_CONFIG, objects.MAC{})
		require.NoError(t, err)
		require.False(t, exists)
	})
}

func TestRepositorySnapshotOperations(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	t.Run("DeleteSnapshot", func(t *testing.T) {
		err := repo.DeleteSnapshot(objects.MAC{})
		require.NoError(t, err)
	})
}

func TestRepositoryStateOperations(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	t.Run("GetState", func(t *testing.T) {
		_, _, err := repo.GetState(objects.MAC{})
		require.Error(t, err)
	})

	t.Run("PutState", func(t *testing.T) {
		data := bytes.NewReader([]byte("test state data"))
		err := repo.PutState(objects.MAC{}, data)
		require.NoError(t, err)
	})

	t.Run("DeleteState", func(t *testing.T) {
		err := repo.DeleteState(objects.MAC{})
		require.NoError(t, err)
	})
}

func TestRepositoryBlobOperations(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	t.Run("RemoveBlob", func(t *testing.T) {
		err := repo.RemoveBlob(resources.RT_CONFIG, objects.MAC{}, objects.MAC{})
		require.NoError(t, err)
	})
}

func TestRepositoryCreation(t *testing.T) {
	t.Run("Inexistent", func(t *testing.T) {
		ctx := ptesting.GenerateRepository(t, nil, nil, nil).AppContext()
		storeConfig := map[string]string{
			"type":     "mock",
			"location": "mock://test",
		}
		repo, err := repository.Inexistent(ctx, storeConfig)
		require.NoError(t, err)
		require.NotNil(t, repo)
		size, err := repo.StorageSize()
		require.NoError(t, err)
		require.Equal(t, int64(0), size)
	})

	t.Run("NewNoRebuild", func(t *testing.T) {
		ctx := ptesting.GenerateContext(t, nil, nil)

		// create a storage
		r, err := storage.New(ctx, map[string]string{"location": "mock:///nonexistent"})
		require.NotNil(t, r)
		require.NoError(t, err)

		config := storage.NewConfiguration()
		config.Compression = nil
		hasher := hashing.GetHasher(hashing.DEFAULT_HASHING_ALGORITHM)

		var key []byte
		config.Encryption = nil
		serialized, err := config.ToBytes()
		require.NoError(t, err)

		wrappedConfigRd, err := storage.Serialize(hasher, resources.RT_CONFIG, versioning.GetCurrentVersion(resources.RT_CONFIG), bytes.NewReader(serialized))
		require.NoError(t, err)

		wrappedConfig, err := io.ReadAll(wrappedConfigRd)
		require.NoError(t, err)

		err = r.Create(ctx, wrappedConfig)
		require.NoError(t, err)

		repo, err := repository.NewNoRebuild(ctx, key, r, wrappedConfig)
		require.NoError(t, err)
		require.NotNil(t, repo)
		size, err := repo.StorageSize()
		require.NoError(t, err)
		require.Equal(t, int64(0), size)
	})
}
