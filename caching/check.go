package caching

import (
	"fmt"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/google/uuid"
)

type CheckCache struct {
	cache Cache
}

func newCheckCache(cons CacheConstructor) (*CheckCache, error) {
	cache, err := cons(CACHE_VERSION, "check", uuid.NewString(), DeleteOnClose)
	if err != nil {
		return nil, err
	}

	return &CheckCache{
		cache: cache,
	}, nil
}

func (c *CheckCache) Close() error {
	return c.cache.Close()
}

func (c *CheckCache) PutPackfileStatus(mac objects.MAC, err []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__packfile__:%x", mac)), err)
}

func (c *CheckCache) GetPackfileStatus(mac objects.MAC) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprintf("__packfile__:%x", mac)))
}

func (c *CheckCache) PutVFSStatus(mac objects.MAC, err []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__vfs__:%x", mac)), err)
}

func (c *CheckCache) GetVFSStatus(mac objects.MAC) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprintf("__vfs__:%x", mac)))
}

func (c *CheckCache) PutVFSEntryStatus(mac objects.MAC, err []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__vfs_entry__:%x", mac)), err)
}

func (c *CheckCache) GetVFSEntryStatus(mac objects.MAC) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprintf("__vfs_entry__:%x", mac)))
}

func (c *CheckCache) PutObjectStatus(mac objects.MAC, err []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__object__:%x", mac)), err)
}

func (c *CheckCache) GetObjectStatus(mac objects.MAC) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprintf("__object__:%x", mac)))
}

func (c *CheckCache) PutChunkStatus(mac objects.MAC, err []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__chunk__:%x", mac)), err)
}

func (c *CheckCache) GetChunkStatus(mac objects.MAC) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprintf("__chunk__:%x", mac)))
}
