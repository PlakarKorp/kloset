package caching

import (
	"fmt"
	"path/filepath"

	"github.com/google/uuid"
)

type VFSCache struct {
	cache Cache
}

func newVFSCache(cons CacheConstructor, repositoryID uuid.UUID, scheme string, origin string, opt Option) (*VFSCache, error) {
	cache, err := cons(CACHE_VERSION, "vfs", filepath.Join(repositoryID.String(), scheme, origin), opt)
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
	return c.cache.Put([]byte(fmt.Sprintf("__directory__:%s", pathname)), data)
}

func (c *VFSCache) GetDirectory(pathname string) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprintf("__directory__:%s", pathname)))
}

func (c *VFSCache) PutFilename(pathname string, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__filename__:%s", pathname)), data)
}

func (c *VFSCache) GetFilename(pathname string) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprintf("__filename__:%s", pathname)))
}

func (c *VFSCache) PutFileSummary(pathname string, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__file_summary__:%s", pathname)), data)
}

func (c *VFSCache) GetFileSummary(pathname string) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprintf("__file_summary__:%s", pathname)))
}

func (c *VFSCache) PutObject(mac [32]byte, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__object__:%x", mac)), data)
}

func (c *VFSCache) GetObject(mac [32]byte) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprintf("__object__:%x", mac)))
}
