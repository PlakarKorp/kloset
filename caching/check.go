package caching

import (
	"github.com/PlakarKorp/kloset/objects"
	"github.com/google/uuid"
)

type CheckCache struct {
	cache Cache
}

func newCheckCache(cons Constructor) (*CheckCache, error) {
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
	return c.cache.Put(key("__packfile__", mac), err)
}

func (c *CheckCache) GetPackfileStatus(mac objects.MAC) ([]byte, error) {
	return c.cache.Get(key("__packfile__", mac))
}

func (c *CheckCache) PutVFSStatus(mac objects.MAC, err []byte) error {
	return c.cache.Put(key("__vfs__", mac), err)
}

func (c *CheckCache) GetVFSStatus(mac objects.MAC) ([]byte, error) {
	return c.cache.Get(key("__vfs__", mac))
}

func (c *CheckCache) PutVFSEntryStatus(mac objects.MAC, err []byte) error {
	return c.cache.Put(key("__vfs_entry__", mac), err)
}

func (c *CheckCache) GetVFSEntryStatus(mac objects.MAC) ([]byte, error) {
	return c.cache.Get(key("__vfs_entry__", mac))
}

func (c *CheckCache) PutObjectStatus(mac objects.MAC, err []byte) error {
	return c.cache.Put(key("__object__", mac), err)
}

func (c *CheckCache) GetObjectStatus(mac objects.MAC) ([]byte, error) {
	return c.cache.Get(key("__object__", mac))
}

func (c *CheckCache) PutChunkStatus(mac objects.MAC, err []byte) error {
	return c.cache.Put(key("__chunk__", mac), err)
}

func (c *CheckCache) GetChunkStatus(mac objects.MAC) ([]byte, error) {
	return c.cache.Get(key("__chunk__", mac))
}
