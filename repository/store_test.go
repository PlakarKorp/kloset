package repository_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

func TestRepositoryStore(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	blobtype := resources.RT_CONFIG

	t.Run("NewRepositoryStore", func(t *testing.T) {
		store := repository.NewRepositoryStore[string, int](repo, blobtype)
		require.NotNil(t, store)

		store2 := repository.NewRepositoryStore[int, string](repo, blobtype)
		require.NotNil(t, store2)
	})

	t.Run("Get returns error for non-existent node", func(t *testing.T) {
		store := repository.NewRepositoryStore[string, int](repo, blobtype)
		mac := objects.MAC{}
		node, err := store.Get(mac)
		require.Error(t, err)
		require.Nil(t, node)
	})

	t.Run("Update returns ErrStoreReadOnly", func(t *testing.T) {
		store := repository.NewRepositoryStore[string, int](repo, blobtype)
		mac := objects.MAC{}
		node := &btree.Node[string, objects.MAC, int]{}
		err := store.Update(mac, node)
		require.Error(t, err)
		require.Equal(t, repository.ErrStoreReadOnly, err)
	})

	t.Run("Put returns ErrStoreReadOnly", func(t *testing.T) {
		store := repository.NewRepositoryStore[string, int](repo, blobtype)
		node := &btree.Node[string, objects.MAC, int]{}
		mac, err := store.Put(node)
		require.Error(t, err)
		require.Equal(t, repository.ErrStoreReadOnly, err)
		require.Equal(t, objects.MAC{}, mac)
	})
}
