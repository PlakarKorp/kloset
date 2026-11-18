package snapshot_test

import (
	"context"
	"testing"

	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

func countStates(t *testing.T, store storage.Store) int {
	n := 0
	for _, err := range store.List(context.Background(), storage.StorageResourceState, 0) {
		require.NoError(t, err)
		n++
	}
	return n
}

func TestCommitChecksLock(t *testing.T) {
	for _, tc := range []struct {
		name       string
		checkpoint bool
		lockLost   bool
	}{
		{"held", false, false},
		{"lost", false, true},
		{"held with checkpoints", true, false},
		{"lost with checkpoints", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, store := ptesting.GenerateLockStoreRepository(t)
			builder, err := snapshot.Create(repo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{
				NoCheckpoint: !tc.checkpoint,
			})
			require.NoError(t, err)

			states := countStates(t, store)

			if tc.lockLost {
				require.NoError(t, store.RemoveLock(builder.Header.Identifier))
			}

			err = builder.Commit()
			if tc.lockLost {
				require.ErrorContains(t, err, "repository lock lost")
				require.Equal(t, states, countStates(t, store))
			} else {
				require.NoError(t, err)
				require.Equal(t, states+1, countStates(t, store))
			}

			require.NoError(t, builder.Close())
		})
	}
}

func TestCommitWithoutLock(t *testing.T) {
	repo, store := ptesting.GenerateLockStoreRepository(t)
	builder, err := snapshot.Create(repo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{
		NoCheckpoint: true,
		NoLock:       true,
	})
	require.NoError(t, err)
	store.RequireNoLocks(t)

	states := countStates(t, store)
	require.NoError(t, builder.Commit())
	require.Equal(t, states+1, countStates(t, store))

	require.NoError(t, builder.Close())
	require.Empty(t, store.LandedLocks())
}
