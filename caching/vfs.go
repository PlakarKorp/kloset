package caching

import (
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

type VFSCache struct {
	*PebbleCache
	manager       *Manager
	repoId        uuid.UUID
	deleteOnClose bool
}

func newVFSCache(cacheManager *Manager, repositoryID uuid.UUID, scheme string, origin string, deleteOnClose bool) (*VFSCache, error) {
	cacheDir := filepath.Join(cacheManager.cacheDir, "vfs", repositoryID.String(), scheme, origin)

	db, err := New(cacheDir)
	if err != nil {
		return nil, err
	}

	return &VFSCache{
		PebbleCache:   db,
		manager:       cacheManager,
		repoId:        repositoryID,
		deleteOnClose: deleteOnClose,
	}, nil
}

func (c *VFSCache) Close() error {
	if err := c.PebbleCache.Close(); err != nil {
		return err
	}

	if c.deleteOnClose {
		// Note this is two level above the cache dir, because we don't want to
		// leave behind empty directories.
		return os.RemoveAll(filepath.Join(c.manager.cacheDir, "vfs", c.repoId.String()))
	} else {
		return nil
	}
}

func (c *VFSCache) PutCachedPath(pathname string, data []byte) error {
	return c.put("__path__", pathname, data)
}

func (c *VFSCache) GetCachedPath(pathname string) ([]byte, error) {
	return c.get("__path__", pathname)
}
