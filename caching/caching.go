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

type Cache interface {
	Put([]byte, []byte) error
	Has([]byte) (bool, error)
	Get([]byte) ([]byte, error)

	// Must support Delete during Scan
	Delete([]byte) error

	Close() error

	// reverse is only to accomodate EnumerateKeysWithPrefix, once
	// that doesn't need it anymore this can become simpler again.
	Scan([]byte, bool) iter.Seq2[[]byte, []byte]
}

type Constructor func(version, name, repoid string, opts Option) (Cache, error)

type Manager struct {
	closed atomic.Bool

	// cacheDir string
	cons Constructor

	repositoryCache      map[uuid.UUID]*_RepositoryCache
	repositoryCacheMutex sync.Mutex

	vfsCache      map[string]*VFSCache
	vfsCacheMutex sync.Mutex

	maintenanceCache      map[uuid.UUID]*MaintenanceCache
	maintenanceCacheMutex sync.Mutex
}

func NewManager(cons Constructor) *Manager {
	return &Manager{
		//cacheDir: filepath.Join(cacheDir, CACHE_VERSION),
		cons: cons,

		repositoryCache:  make(map[uuid.UUID]*_RepositoryCache),
		vfsCache:         make(map[string]*VFSCache),
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

	for _, cache := range m.vfsCache {
		cache.Close()
	}

	for _, cache := range m.maintenanceCache {
		cache.Close()
	}

	// we may rework the interface later to allow for error handling
	// at this point closing is best effort
	return nil
}

func (m *Manager) VFS(repositoryID uuid.UUID, scheme string, origin string, deleteOnClose bool) (*VFSCache, error) {
	if m.closed.Load() {
		return nil, ErrClosed
	}

	m.vfsCacheMutex.Lock()
	defer m.vfsCacheMutex.Unlock()

	key := fmt.Sprintf("%s://%s", scheme, origin)

	if cache, ok := m.vfsCache[key]; ok {
		return cache, nil
	}

	var opt Option
	if deleteOnClose {
		opt = DeleteOnClose
	}

	if cache, err := newVFSCache(m.cons, repositoryID, scheme, origin, opt); err != nil {
		return nil, err
	} else {
		m.vfsCache[key] = cache
		return cache, nil
	}
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
