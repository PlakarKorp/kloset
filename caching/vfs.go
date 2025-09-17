package caching

import (
	"fmt"
	"path"

	"github.com/google/uuid"
)

type VFSCache struct {
	cache Cache
}

func newVFSCache(cons Constructor, repositoryID uuid.UUID, scheme string, origin string, opt Option) (*VFSCache, error) {
	cache, err := cons(CACHE_VERSION, "vfs", path.Join(repositoryID.String(), scheme, origin), opt)
	if err != nil {
		return nil, err
	}

	return &VFSCache{
		cache: cache,
	}, nil
}

func (c *VFSCache) Close() error {
	return c.cache.Close()
}

func (c *VFSCache) PutCachedPath(pathname string, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprint("__path__:", pathname)), data)
}

func (c *VFSCache) GetCachedPath(pathname string) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprint("__path__:", pathname)))
}
