package testing

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/stretchr/testify/require"
)

// LockStore fails or delays lock writes on demand and records when each
// successful one landed.
type LockStore struct {
	storage.Store

	mu     sync.Mutex
	fail   bool
	delay  time.Duration
	landed []time.Time
}

func (s *LockStore) Set(fail bool, delay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail, s.delay = fail, delay
}

func (s *LockStore) LandedLocks() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.landed...)
}

func (s *LockStore) Put(ctx context.Context, res storage.StorageResource, mac objects.MAC, rd io.Reader) (int64, error) {
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

func (s *LockStore) Get(ctx context.Context, res storage.StorageResource, mac objects.MAC, rg *storage.Range) (io.ReadCloser, error) {
	if res == storage.StorageResourceLock {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	return s.Store.Get(ctx, res, mac, rg)
}

// RemoveLock deletes a lock the way maintenance removes one it thinks stale.
func (s *LockStore) RemoveLock(mac objects.MAC) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Store.Delete(context.Background(), storage.StorageResourceLock, mac)
}

// RequireNoLocks fails the test if any lock is left in the store.
func (s *LockStore) RequireNoLocks(t *testing.T) {
	locks, err := s.List(context.Background(), storage.StorageResourceLock)
	require.NoError(t, err)
	require.Empty(t, locks)
}

// GenerateLockStoreRepository is GenerateRepository with its store wrapped in
// a LockStore.
func GenerateLockStoreRepository(t *testing.T) (*repository.Repository, *LockStore) {
	base := GenerateRepository(t, &bytes.Buffer{}, &bytes.Buffer{}, nil)
	store := &LockStore{Store: base.Store()}

	config, err := store.Open(base.AppContext())
	require.NoError(t, err)

	repo, err := repository.New(base.AppContext(), nil, store, config)
	require.NoError(t, err)

	return repo, store
}
