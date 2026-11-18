package repository_test

import (
	"bytes"
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/PlakarKorp/kloset/connectors/storage"
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
		lock, err := repo.LockShared(objects.RandomMAC())
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
		lock, err := repo.LockShared(objects.RandomMAC())
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
		lock, err := repo.LockShared(objects.RandomMAC())
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
		lock, err := repo.LockShared(lockID)
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

func TestUnlockTwice(t *testing.T) {
	repo, store := newLockRepository(t)

	synctest.Test(t, func(t *testing.T) {
		lock, err := repo.LockShared(objects.RandomMAC())
		require.NoError(t, err)

		requireReleased(t, lock, store)
		requireReleased(t, lock, store)
	})
}

func TestUnlockInterruptsRefresh(t *testing.T) {
	repo, store := newLockRepository(t)

	synctest.Test(t, func(t *testing.T) {
		lock, err := repo.LockShared(objects.RandomMAC())
		require.NoError(t, err)

		// The refresh at 5m hangs: left alone it would only give up at its
		// 10m deadline.
		store.Set(false, repository.LOCK_REFRESH_RATE)
		time.Sleep(repository.LOCK_REFRESH_RATE + time.Second)
		synctest.Wait()

		start := time.Now()
		requireReleased(t, lock, store)
		require.Equal(t, start, time.Now(), "Unlock waited for the refresh in flight")

		require.NoError(t, repo.AppContext().Err(), "Unlock cancelled the application")
		require.Len(t, store.LandedLocks(), 1)
	})
}

func TestLockCheck(t *testing.T) {
	t.Run("held", func(t *testing.T) {
		repo, store := newLockRepository(t)

		synctest.Test(t, func(t *testing.T) {
			lock, err := repo.LockShared(objects.RandomMAC())
			require.NoError(t, err)

			require.NoError(t, lock.Check(repo.AppContext()))

			requireReleased(t, lock, store)
		})
	})

	t.Run("removed", func(t *testing.T) {
		repo, store := newLockRepository(t)

		synctest.Test(t, func(t *testing.T) {
			lockID := objects.RandomMAC()
			lock, err := repo.LockShared(lockID)
			require.NoError(t, err)

			require.NoError(t, store.RemoveLock(lockID))
			require.ErrorContains(t, lock.Check(repo.AppContext()), "repository lock lost")

			requireReleased(t, lock, store)
		})
	})

	t.Run("stale", func(t *testing.T) {
		repo, store := newLockRepository(t)

		synctest.Test(t, func(t *testing.T) {
			lock, err := repo.LockShared(objects.RandomMAC())
			require.NoError(t, err)

			// Refreshes fail fast, then the retry issued at 9m hangs until
			// its deadline at 10m: the refresher only notices at its next
			// tick, 10m30s, leaving the lock stale but the application not
			// yet cancelled.
			store.Set(true, 0)
			time.Sleep(repository.LOCK_TTL - repository.LOCK_REFRESH_RATE/4)
			store.Set(false, repository.LOCK_REFRESH_RATE)

			time.Sleep(repository.LOCK_REFRESH_RATE/4 + repository.LOCK_REFRESH_RATE/30)
			synctest.Wait()
			require.NoError(t, repo.AppContext().Err())

			require.ErrorContains(t, lock.Check(repo.AppContext()), "repository lock stale")

			requireReleased(t, lock, store)
		})
	})
}

// plantLock writes another process's lock into the repository.
func plantLock(t *testing.T, repo *repository.Repository, exclusive, stale bool) objects.MAC {
	lock := repository.NewSharedLock("other-host")
	if exclusive {
		lock = repository.NewExclusiveLock("other-host")
	}
	if stale {
		lock.Timestamp = time.Now().Add(-repository.LOCK_TTL)
	}

	buffer := &bytes.Buffer{}
	require.NoError(t, lock.SerializeToStream(buffer))

	lockID := objects.RandomMAC()
	_, err := repo.PutLock(context.Background(), lockID, buffer)
	require.NoError(t, err)

	return lockID
}

func lockExists(t *testing.T, store *ptesting.LockStore, lockID objects.MAC) bool {
	for entry, err := range store.List(context.Background(), storage.StorageResourceLock, 0) {
		require.NoError(t, err)
		if entry.MAC == lockID {
			return true
		}
	}
	return false
}

func TestLockConflicts(t *testing.T) {
	for _, tc := range []struct {
		name           string
		exclusive      bool
		otherExclusive bool
		otherStale     bool
		refused        bool
	}{
		{"shared vs shared", false, false, false, false},
		{"shared vs exclusive", false, true, false, true},
		{"exclusive vs shared", true, false, false, true},
		{"exclusive vs exclusive", true, true, false, true},
		{"shared vs stale shared", false, false, true, false},
		{"shared vs stale exclusive", false, true, true, false},
		{"exclusive vs stale shared", true, false, true, false},
		{"exclusive vs stale exclusive", true, true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, store := newLockRepository(t)

			synctest.Test(t, func(t *testing.T) {
				otherID := plantLock(t, repo, tc.otherExclusive, tc.otherStale)

				lockID := objects.RandomMAC()
				var lock *repository.LockedRepository
				var err error
				if tc.exclusive {
					lock, err = repo.LockExclusive(lockID)
				} else {
					lock, err = repo.LockShared(lockID)
				}

				if tc.refused {
					require.ErrorContains(t, err, "already locked by other-host")
					require.False(t, lockExists(t, store, lockID), "refused lock left behind")
					require.True(t, lockExists(t, store, otherID), "live lock removed")
					return
				}

				require.NoError(t, err)
				require.True(t, lockExists(t, store, lockID))
				require.Equal(t, !tc.otherStale, lockExists(t, store, otherID), "stale lock kept or live lock removed")

				require.NoError(t, lock.Unlock())
				require.False(t, lockExists(t, store, lockID))
			})
		})
	}
}

func TestLockExclusiveRefreshesExclusive(t *testing.T) {
	repo, store := newLockRepository(t)

	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		lockID := objects.RandomMAC()
		lock, err := repo.LockExclusive(lockID)
		require.NoError(t, err)

		time.Sleep(repository.LOCK_REFRESH_RATE + time.Second)
		synctest.Wait()
		require.Len(t, store.LandedLocks(), 2)

		rd, err := repo.GetLock(context.Background(), lockID)
		require.NoError(t, err)
		refreshed, err := repository.NewLockFromStream(rd)
		rd.Close()
		require.NoError(t, err)

		require.Equal(t, start.Add(repository.LOCK_REFRESH_RATE), refreshed.Timestamp)
		require.True(t, refreshed.Exclusive, "the refresher downgraded the exclusive lock")

		requireReleased(t, lock, store)
	})
}
