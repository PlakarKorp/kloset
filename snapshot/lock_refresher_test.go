package snapshot_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
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

// lockStore fails or delays lock writes on demand and records when each
// successful one landed.
type lockStore struct {
	storage.Store

	mu     sync.Mutex
	fail   bool
	delay  time.Duration
	landed []time.Time
}

func (s *lockStore) set(fail bool, delay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail, s.delay = fail, delay
}

func (s *lockStore) landedLocks() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.landed...)
}

func (s *lockStore) Put(ctx context.Context, res storage.StorageResource, mac objects.MAC, rd io.Reader) (int64, error) {
	if res != storage.StorageResourceLock {
		return s.Store.Put(ctx, res, mac, rd)
	}

	s.mu.Lock()
	fail, delay := s.fail, s.delay
	s.mu.Unlock()

	if fail {
		return 0, errors.New("injected lock write failure")
	}

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	n, err := s.Store.Put(ctx, res, mac, rd)
	if err == nil {
		s.landed = append(s.landed, time.Now())
	}
	return n, err
}

func (s *lockStore) Get(ctx context.Context, res storage.StorageResource, mac objects.MAC, rg *storage.Range) (io.ReadCloser, error) {
	if res == storage.StorageResourceLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	return s.Store.Get(ctx, res, mac, rg)
}

// removeLock deletes a lock the way maintenance removes one it thinks stale.
func (s *lockStore) removeLock(mac objects.MAC) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.Delete(context.Background(), storage.StorageResourceLock, mac)
}

func newLockStoreRepository(t *testing.T) (*repository.Repository, *lockStore) {
	base := ptesting.GenerateRepository(t, &bytes.Buffer{}, &bytes.Buffer{}, nil)
	store := &lockStore{Store: base.Store()}

	config, err := store.Open(base.AppContext())
	require.NoError(t, err)

	repo, err := repository.New(base.AppContext(), nil, store, config)
	require.NoError(t, err)

	return repo, store
}

// newRefresherBuilder returns a builder over a lockStore that holds no lock
// yet: Create would start the refresher on the real clock, so each test calls
// Lock() itself inside its synctest bubble.
func newRefresherBuilder(t *testing.T) (*snapshot.Builder, *lockStore) {
	repo, store := newLockStoreRepository(t)

	t.Setenv("PLAKAR_LOCKLESS", "true")
	builder, err := snapshot.Create(repo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{
		NoCheckpoint: true,
	})
	require.NoError(t, err)
	t.Setenv("PLAKAR_LOCKLESS", "false")

	return builder, store
}

// requireReleased closes the builder and checks the refresher removed our
// lock. It must run inside the bubble that called Lock().
func requireReleased(t *testing.T, builder *snapshot.Builder, store *lockStore) {
	require.NoError(t, builder.Close())

	locks, err := store.List(context.Background(), storage.StorageResourceLock)
	require.NoError(t, err)
	require.Empty(t, locks)
}

func TestLockRefresherKeepsLockFresh(t *testing.T) {
	builder, store := newRefresherBuilder(t)

	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		done, err := builder.Lock()
		require.NoError(t, err)

		time.Sleep(3*repository.LOCK_TTL + time.Minute)
		synctest.Wait()

		require.NoError(t, builder.AppContext().Err())

		landed := store.landedLocks()
		require.Len(t, landed, 7)
		for i, at := range landed {
			require.Equal(t, start.Add(time.Duration(i)*repository.LOCK_REFRESH_RATE), at)
		}

		close(done)
		requireReleased(t, builder, store)
	})
}

func TestLockRefresherAbortsWhenRefreshFails(t *testing.T) {
	builder, store := newRefresherBuilder(t)

	synctest.Test(t, func(t *testing.T) {
		done, err := builder.Lock()
		require.NoError(t, err)
		store.set(true, 0)

		time.Sleep(repository.LOCK_TTL - time.Second)
		synctest.Wait()
		require.NoError(t, builder.AppContext().Err(), "aborted before the lock went stale")

		time.Sleep(time.Second + repository.LOCK_REFRESH_RATE/10)
		synctest.Wait()
		require.ErrorContains(t, context.Cause(builder.AppContext()), "lock refresher")
		require.Len(t, store.landedLocks(), 1)

		close(done)
		requireReleased(t, builder, store)
	})
}

func TestLockRefresherRejectsLateRefresh(t *testing.T) {
	builder, store := newRefresherBuilder(t)

	synctest.Test(t, func(t *testing.T) {
		done, err := builder.Lock()
		require.NoError(t, err)

		// Refreshes fail fast until 8m45s, then the retry issued at 9m is
		// slow: left alone it would land at 10m15s, after the lock went
		// stale and maintenance could have removed it.
		store.set(true, 0)
		time.Sleep(repository.LOCK_TTL - repository.LOCK_REFRESH_RATE/4)
		store.set(false, repository.LOCK_REFRESH_RATE/4)

		time.Sleep(repository.LOCK_REFRESH_RATE / 2)
		synctest.Wait()

		require.Len(t, store.landedLocks(), 1, "a refresh landed after the lock went stale")
		require.ErrorContains(t, context.Cause(builder.AppContext()), "lock refresher")

		close(done)
		requireReleased(t, builder, store)
	})
}

func TestLockRefresherDoesNotRecreateRemovedLock(t *testing.T) {
	builder, store := newRefresherBuilder(t)

	synctest.Test(t, func(t *testing.T) {
		done, err := builder.Lock()
		require.NoError(t, err)

		// Someone removes our lock between two refreshes, e.g. a host whose
		// clock runs ahead and sees it stale.
		time.Sleep(repository.LOCK_REFRESH_RATE / 2)
		require.NoError(t, store.removeLock(builder.Header.Identifier))

		time.Sleep(repository.LOCK_TTL)
		synctest.Wait()

		require.Len(t, store.landedLocks(), 1, "the refresher recreated a removed lock")
		require.ErrorContains(t, context.Cause(builder.AppContext()), "lock refresher")

		close(done)
		requireReleased(t, builder, store)
	})
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
			repo, store := newLockStoreRepository(t)
			builder, err := snapshot.Create(repo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{
				NoCheckpoint: !tc.checkpoint,
			})
			require.NoError(t, err)

			ctx := context.Background()
			states, err := store.List(ctx, storage.StorageResourceState)
			require.NoError(t, err)

			if tc.lockLost {
				require.NoError(t, store.removeLock(builder.Header.Identifier))
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
	builder, store := newRefresherBuilder(t)

	synctest.Test(t, func(t *testing.T) {
		done, err := builder.Lock()
		require.NoError(t, err)

		// Refreshes fail fast, then the retry issued at 9m hangs until its
		// deadline at 10m: the refresher only notices at its next tick,
		// 10m30s, leaving the lock stale but the backup not yet cancelled.
		store.set(true, 0)
		time.Sleep(repository.LOCK_TTL - repository.LOCK_REFRESH_RATE/4)
		store.set(false, repository.LOCK_REFRESH_RATE)

		time.Sleep(repository.LOCK_REFRESH_RATE/4 + repository.LOCK_REFRESH_RATE/30)
		synctest.Wait()
		require.NoError(t, builder.AppContext().Err())

		require.ErrorContains(t, builder.Commit(), "repository lock went stale")

		close(done)
		requireReleased(t, builder, store)
	})
}
