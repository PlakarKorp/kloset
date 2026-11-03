package snapshot_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

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

			ctx := context.Background()
			states, err := store.List(ctx, storage.StorageResourceState)
			require.NoError(t, err)

			if tc.lockLost {
				require.NoError(t, store.RemoveLock(builder.Header.Identifier))
			}

			err = builder.Commit()
			after, listErr := store.List(ctx, storage.StorageResourceState)
			require.NoError(t, listErr)

			if tc.lockLost {
				require.ErrorContains(t, err, "repository lock lost")
				require.Len(t, after, len(states))
			} else {
				require.NoError(t, err)
				require.Len(t, after, len(states)+1)
			}

			require.NoError(t, builder.Close())
		})
	}
}

func TestCommitRefusesStaleLock(t *testing.T) {
	t.Setenv("PLAKAR_LOCKLESS", "false")
	repo, store := ptesting.GenerateLockStoreRepository(t)

	synctest.Test(t, func(t *testing.T) {
		// Create takes the lock, so it must run in the bubble for the
		// refresher to run on the fake clock.
		builder, err := snapshot.Create(repo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{
			NoCheckpoint: true,
		})
		require.NoError(t, err)

		// Refreshes fail fast, then the retry issued at 9m hangs until its
		// deadline at 10m: the refresher only notices at its next tick,
		// 10m30s, leaving the lock stale but the backup not yet cancelled.
		store.Set(true, 0)
		time.Sleep(repository.LOCK_TTL - repository.LOCK_REFRESH_RATE/4)
		store.Set(false, repository.LOCK_REFRESH_RATE)

		time.Sleep(repository.LOCK_REFRESH_RATE/4 + repository.LOCK_REFRESH_RATE/30)
		synctest.Wait()
		require.NoError(t, builder.AppContext().Err())

		require.ErrorContains(t, builder.Commit(), "repository lock went stale")

		require.NoError(t, builder.Close())
		store.RequireNoLocks(t)
	})
}
