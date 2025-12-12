package caching

import (
	"fmt"
	"iter"
	"sync"
	"sync/atomic"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/google/uuid"
)

const CACHE_VERSION = "2.0.0"

var (
	ErrClosed = fmt.Errorf("cache closed")
)

type Option int

const (
	None Option = iota

	DeleteOnClose
)

// This is not goroutine safe, external synchronization must be provided.
type Batch interface {
	Put([]byte, []byte) error
	Count() uint32
	Commit() error
}

type Cache interface {
	Put([]byte, []byte) error
	Has([]byte) (bool, error)
	Get([]byte) ([]byte, error)

	// Must support Delete during Scan
	Delete([]byte) error

	// reverse is only to accomodate EnumerateKeysWithPrefix, once
	// that doesn't need it anymore this can become simpler again.
	// key returned by an iteration might get invalidated on
	// subsequent iterations.
	Scan([]byte, bool) iter.Seq2[[]byte, []byte]

	NewBatch() Batch

	Close() error
}

type Constructor func(version, name, repoid string, opts Option) (Cache, error)

type Manager struct {
	closed atomic.Bool

	cons Constructor

	repositoryCache      map[uuid.UUID]*_RepositoryCache
	repositoryCacheMutex sync.Mutex

	vfsCacheMutex sync.Mutex

	maintenanceCache      map[uuid.UUID]*MaintenanceCache
	maintenanceCacheMutex sync.Mutex
}

func NewManager(cons Constructor) *Manager {
	return &Manager{
		cons: cons,

		repositoryCache:  make(map[uuid.UUID]*_RepositoryCache),
		maintenanceCache: make(map[uuid.UUID]*MaintenanceCache),
	}
}

func (m *Manager) Close() error {
	if !m.closed.CompareAndSwap(false, true) {
		// the cache was already closed
		return nil
	}

	m.vfsCacheMutex.Lock()
	defer m.vfsCacheMutex.Unlock()

	for _, cache := range m.repositoryCache {
		cache.Close()
	}

	for _, cache := range m.maintenanceCache {
		cache.Close()
	}

	// we may rework the interface later to allow for error handling
	// at this point closing is best effort
	return nil
}

func (m *Manager) Repository(repositoryID uuid.UUID) (*_RepositoryCache, error) {
	if m.closed.Load() {
		return nil, ErrClosed
	}

	m.repositoryCacheMutex.Lock()
	defer m.repositoryCacheMutex.Unlock()

	if cache, ok := m.repositoryCache[repositoryID]; ok {
		return cache, nil
	}

	if cache, err := newRepositoryCache(m.cons, repositoryID); err != nil {
		return nil, err
	} else {
		m.repositoryCache[repositoryID] = cache
		return cache, nil
	}
}

func (m *Manager) Maintenance(repositoryID uuid.UUID) (*MaintenanceCache, error) {
	if m.closed.Load() {
		return nil, ErrClosed
	}

	m.maintenanceCacheMutex.Lock()
	defer m.maintenanceCacheMutex.Unlock()

	if cache, ok := m.maintenanceCache[repositoryID]; ok {
		return cache, nil
	}

	if cache, err := newMaintenanceCache(m.cons, repositoryID); err != nil {
		return nil, err
	} else {
		m.maintenanceCache[repositoryID] = cache
		return cache, nil
	}
}

// XXX - beware that caller has responsibility to call Close() on the returned cache
func (m *Manager) Scan(snapshotID objects.MAC) (*ScanCache, error) {
	return newScanCache(m.cons, snapshotID)
}

// XXX - beware that caller has responsibility to call Close() on the returned cache
func (m *Manager) Check() (*CheckCache, error) {
	return newCheckCache(m.cons)
}

// XXX - beware that caller has responsibility to call Close() on the returned cache
func (m *Manager) Packing() (*PackingCache, error) {
	return newPackingCache(m.cons)
}
