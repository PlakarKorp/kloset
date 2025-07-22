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

func (c *VFSCache) PutDirectory(pathname string, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprint("__directory__:", pathname)), data)
}

func (c *VFSCache) GetDirectory(pathname string) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprint("__directory__:", pathname)))
}

func (c *VFSCache) PutFilename(pathname string, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprint("__filename__:", pathname)), data)
}

func (c *VFSCache) GetFilename(pathname string) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprint("__filename__:", pathname)))
}

func (c *VFSCache) PutFileSummary(pathname string, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprint("__file_summary__:", pathname)), data)
}

func (c *VFSCache) GetFileSummary(pathname string) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprint("__file_summary__:", pathname)))
}

func (c *VFSCache) PutObject(mac [32]byte, data []byte) error {
	return c.cache.Put(key("__object__", mac), data)
}

func (c *VFSCache) GetObject(mac [32]byte) ([]byte, error) {
	return c.cache.Get(key("__object__", mac))
}
