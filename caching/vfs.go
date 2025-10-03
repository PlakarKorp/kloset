package caching

import (
	"path"

	"github.com/google/uuid"
)

type VFSCache struct {
	kvcache
}

func newVFSCache(cons Constructor, repositoryID uuid.UUID, scheme string, origin string, opt Option) (*VFSCache, error) {
	cache, err := cons(CACHE_VERSION, "vfs", path.Join(repositoryID.String(), scheme, origin), opt)
	if err != nil {
		return nil, err
	}

	return &VFSCache{kvcache{cache}}, nil
}

func (c *VFSCache) PutCachedPath(pathname string, data []byte) error {
	return c.put("__path__", pathname, data)
}

func (c *VFSCache) GetCachedPath(pathname string) ([]byte, error) {
	return c.get("__path__", pathname)
}
