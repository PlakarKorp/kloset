package repository

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/vmihailenco/msgpack/v5"
)

const LOCK_REFRESH_RATE = 5 * time.Minute
const LOCK_TTL = 2 * LOCK_REFRESH_RATE
const LOCK_VERSION = "1.0.0"

func init() {
	versioning.Register(resources.RT_LOCK, versioning.FromString(LOCK_VERSION))
}

type Lock struct {
	Version   versioning.Version `msgpack:"version"`
	Timestamp time.Time          `msgpack:"timestamp"`
	Hostname  string             `msgpack:"hostname"`
	Exclusive bool               `msgpack:"exclusive"`
}

func newLock(hostname string, exclusive bool) *Lock {
	return &Lock{
		Version:   versioning.FromString(LOCK_VERSION),
		Timestamp: time.Now(),
		Hostname:  hostname,
		Exclusive: exclusive,
	}
}

func NewExclusiveLock(hostname string) *Lock {
	return newLock(hostname, true)
}

func NewSharedLock(hostname string) *Lock {
	return newLock(hostname, false)
}

func NewLockFromStream(rd io.Reader) (*Lock, error) {
	var lock Lock
	if err := msgpack.NewDecoder(rd).Decode(&lock); err != nil {
		return nil, err
	}

	return &lock, nil
}

func (lock *Lock) SerializeToStream(w io.Writer) error {
	return msgpack.NewEncoder(w).Encode(lock)
}

func (lock *Lock) IsStale() bool {
	return time.Since(lock.Timestamp) >= LOCK_TTL
}

type LockedRepository struct {
	exclusive bool
	repo      *Repository
	lockid    objects.MAC
	cancel    context.CancelFunc
	stopped   chan struct{}
}

func (repo *Repository) LockShared(lockid objects.MAC) (*LockedRepository, error) {
	return repo.lock(lockid, false)
}

func (repo *Repository) LockExclusive(lockid objects.MAC) (*LockedRepository, error) {
	return repo.lock(lockid, true)
}

func (repo *Repository) lock(lockid objects.MAC, exclusive bool) (*LockedRepository, error) {
	l := &LockedRepository{
		exclusive: exclusive,
		repo:      repo,
		lockid:    lockid,
		stopped:   make(chan struct{}),
	}

	lock := l.newLock()

	buffer := &bytes.Buffer{}
	err := lock.SerializeToStream(buffer)
	if err != nil {
		return nil, err
	}

	_, err = repo.PutLock(repo.AppContext(), lockid, buffer)
	if err != nil {
		return nil, err
	}

	// We installed the lock, now let's see if there is a conflicting exclusive lock or not.
	locksID, err := repo.GetLocks(repo.AppContext())
	if err != nil {
		// We still need to delete it, and we need to do so manually.
		repo.DeleteLock(repo.AppContext(), lockid)
		return nil, err
	}

	for _, lockID := range locksID {
		if lockID == l.lockid {
			continue
		}

		rd, err := repo.GetLock(repo.AppContext(), lockID)
		if err != nil {
			// Err can't tell us if it's not found or a transport error, so
			// relist because it might have been Deleted by another process in
			// which case it's fine to continue
			if lockIDs, err := repo.GetLocks(repo.AppContext()); err == nil {
				if !slices.Contains(lockIDs, lockID) {
					continue
				}
			}

			// Okay it was an actual error we can't proceed further
			repo.DeleteLock(repo.AppContext(), l.lockid)
			return nil, err
		}

		other, err := NewLockFromStream(rd)
		rd.Close()
		if err != nil {
			repo.DeleteLock(repo.AppContext(), l.lockid)
			return nil, err
		}

		/* Kick out stale locks */
		if other.IsStale() {
			if err := repo.DeleteLock(repo.AppContext(), lockID); err != nil {
				// Failure to delete is not an hardstop, except for exclusive
				// locks, either it was already deleted, or there is a real
				// failure to do it, but this lock is Shared so we can keep
				// going on.
				if exclusive {
					repo.DeleteLock(repo.AppContext(), l.lockid)
					return nil, fmt.Errorf("failed to remove stale lock %x: %w", lockID, err)
				}

				repo.Logger().Warn("failed to remove stale lock %x: %s", lockID, err)
			}

			continue
		}

		// There is a lock in place, it's a conflict if either we are trying to
		// lock excludively, or the lock in place is an exclusive one.
		if exclusive || other.Exclusive {
			err := repo.DeleteLock(repo.AppContext(), l.lockid)
			if err != nil {
				return nil, err
			}

			return nil, fmt.Errorf("can't take repository lock, it's already locked by %s", other.Hostname)
		}
	}

	ctx, cancel := context.WithCancel(repo.AppContext())
	l.cancel = cancel
	go l.ping(ctx, lock.Timestamp)

	return l, nil
}

func (l *LockedRepository) Unlock() error {
	// Cancel the ping goroutine.
	l.cancel()
	<-l.stopped

	return l.repo.DeleteLock(l.repo.AppContext(), l.lockid)
}

// The following bit is a "ping" mechanism, Lock() is a bit badly named at this point,
// we are just refreshing the existing lock so that the others don't consider
// us stale and kick us out.
func (l *LockedRepository) ping(ctx context.Context, lastRefresh time.Time) {
	refresh := time.After(time.Until(lastRefresh.Add(LOCK_REFRESH_RATE)))
	defer close(l.stopped)

	for {
		select {
		case <-ctx.Done():
			return
		case <-refresh:
			// We retried hard to update the lock but failed and it's now
			// stale, so abort everything before a concurrent operation
			// thinks it's okay to start.
			if time.Since(lastRefresh) >= LOCK_TTL {
				l.repo.AppContext().Cancel(fmt.Errorf("lock refresher: failed to refresh the repository lock for %s", time.Since(lastRefresh)))
				refresh = nil
				continue
			}

			lock := l.newLock()
			buffer := &bytes.Buffer{}
			lock.SerializeToStream(buffer)
			tctx, cancel := context.WithDeadline(ctx, lastRefresh.Add(LOCK_TTL))

			// Before updating our lock, check if it's still present. If it
			// was removed, either someone is doing something fishy, or
			// another process thought we were stale. In any case this will
			// lead to an abort (after a retry period).
			_, err := l.getLock(tctx)
			if err == nil {
				_, err = l.repo.PutLock(tctx, l.lockid, buffer)
			}
			cancel()

			if err != nil {
				// For some storage reason the refresh failed, warn the user
				// and retry sooner than the normal rate so that we don't
				// have to abort the backup.
				l.repo.Logger().Warn("failed to refresh the repository lock: %s", err)
				refresh = time.After(LOCK_REFRESH_RATE / 10)
				continue
			}

			lastRefresh = lock.Timestamp
			refresh = time.After(time.Until(lastRefresh.Add(LOCK_REFRESH_RATE)))
		}
	}
}

func (l *LockedRepository) Check(ctx context.Context) error {
	lck, err := l.getLock(ctx)
	if err != nil {
		return fmt.Errorf("repository lock lost %x: %w", l.lockid, err)
	}

	if lck.IsStale() {
		return fmt.Errorf("repository lock stale %x", l.lockid)
	}

	return nil
}

func (l *LockedRepository) newLock() *Lock {
	if l.exclusive {
		return NewExclusiveLock(l.repo.AppContext().Hostname)
	}
	return NewSharedLock(l.repo.AppContext().Hostname)
}

func (l *LockedRepository) getLock(ctx context.Context) (*Lock, error) {
	rd, err := l.repo.GetLock(ctx, l.lockid)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch the repository lock: %w", err)
	}
	defer rd.Close()

	lock, err := NewLockFromStream(rd)
	if err != nil {
		return nil, fmt.Errorf("failed to read the repository lock: %w", err)
	}

	return lock, nil
}
