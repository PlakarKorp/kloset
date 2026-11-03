package repository_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// newLockRepository returns a repository over a LockStore with locking
// enabled. The lock itself must be taken inside the test's synctest bubble so
// that the refresher runs on the fake clock.
func newLockRepository(t *testing.T) (*repository.Repository, *ptesting.LockStore) {
	t.Setenv("PLAKAR_LOCKLESS", "false")
	return ptesting.GenerateLockStoreRepository(t)
}

// requireReleased releases the lock and checks it was removed. It must run
// inside the bubble that took the lock.
func requireReleased(t *testing.T, lock *repository.LockedRepository, store *ptesting.LockStore) {
	require.NoError(t, lock.Unlock())
	store.RequireNoLocks(t)
}

func TestLockRefresherKeepsLockFresh(t *testing.T) {
	repo, store := newLockRepository(t)

	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		lock, err := repo.Lock(objects.RandomMAC())
		require.NoError(t, err)

		time.Sleep(3*repository.LOCK_TTL + time.Minute)
		synctest.Wait()

		require.NoError(t, repo.AppContext().Err())

		landed := store.LandedLocks()
		require.Len(t, landed, 7)
		for i, at := range landed {
			require.Equal(t, start.Add(time.Duration(i)*repository.LOCK_REFRESH_RATE), at)
		}

		requireReleased(t, lock, store)
	})
}

func TestLockRefresherAbortsWhenRefreshFails(t *testing.T) {
	repo, store := newLockRepository(t)

	synctest.Test(t, func(t *testing.T) {
		lock, err := repo.Lock(objects.RandomMAC())
		require.NoError(t, err)
		store.Set(true, 0)

		time.Sleep(repository.LOCK_TTL - time.Second)
		synctest.Wait()
		require.NoError(t, repo.AppContext().Err(), "aborted before the lock went stale")

		time.Sleep(time.Second + repository.LOCK_REFRESH_RATE/10)
		synctest.Wait()
		require.ErrorContains(t, context.Cause(repo.AppContext()), "lock refresher")
		require.Len(t, store.LandedLocks(), 1)

		requireReleased(t, lock, store)
	})
}

func TestLockRefresherRejectsLateRefresh(t *testing.T) {
	repo, store := newLockRepository(t)

	synctest.Test(t, func(t *testing.T) {
		lock, err := repo.Lock(objects.RandomMAC())
		require.NoError(t, err)

		// Refreshes fail fast until 8m45s, then the retry issued at 9m is
		// slow: left alone it would land at 10m15s, after the lock went
		// stale and maintenance could have removed it.
		store.Set(true, 0)
		time.Sleep(repository.LOCK_TTL - repository.LOCK_REFRESH_RATE/4)
		store.Set(false, repository.LOCK_REFRESH_RATE/4)

		time.Sleep(repository.LOCK_REFRESH_RATE / 2)
		synctest.Wait()

		require.Len(t, store.LandedLocks(), 1, "a refresh landed after the lock went stale")
		require.ErrorContains(t, context.Cause(repo.AppContext()), "lock refresher")

		requireReleased(t, lock, store)
	})
}

func TestLockRefresherDoesNotRecreateRemovedLock(t *testing.T) {
	repo, store := newLockRepository(t)

	synctest.Test(t, func(t *testing.T) {
		lockID := objects.RandomMAC()
		lock, err := repo.Lock(lockID)
		require.NoError(t, err)

		// Someone removes our lock between two refreshes, e.g. a host whose
		// clock runs ahead and sees it stale.
		time.Sleep(repository.LOCK_REFRESH_RATE / 2)
		require.NoError(t, store.RemoveLock(lockID))

		time.Sleep(repository.LOCK_TTL)
		synctest.Wait()

		require.Len(t, store.LandedLocks(), 1, "the refresher recreated a removed lock")
		require.ErrorContains(t, context.Cause(repo.AppContext()), "lock refresher")

		requireReleased(t, lock, store)
	})
}
