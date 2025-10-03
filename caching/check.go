package caching

import (
	"fmt"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/google/uuid"
)

type CheckCache struct {
	kvcache
}

func newCheckCache(cons Constructor) (*CheckCache, error) {
	cache, err := cons(CACHE_VERSION, "check", uuid.NewString(), DeleteOnClose)
	if err != nil {
		return nil, err
	}

	return &CheckCache{kvcache{cache}}, nil
}

func (c *CheckCache) PutPackfileStatus(mac objects.MAC, err []byte) error {
	return c.put("__packfile__", fmt.Sprintf("%x", mac), err)
}

func (c *CheckCache) GetPackfileStatus(mac objects.MAC) ([]byte, error) {
	return c.get("__packfile__", fmt.Sprintf("%x", mac))
}

func (c *CheckCache) PutVFSStatus(mac objects.MAC, err []byte) error {
	return c.put("__vfs__", fmt.Sprintf("%x", mac), err)
}

func (c *CheckCache) GetVFSStatus(mac objects.MAC) ([]byte, error) {
	return c.get("__vfs__", fmt.Sprintf("%x", mac))
}

func (c *CheckCache) PutVFSEntryStatus(mac objects.MAC, err []byte) error {
	return c.put("__vfs_entry__", fmt.Sprintf("%x", mac), err)
}

func (c *CheckCache) GetVFSEntryStatus(mac objects.MAC) ([]byte, error) {
	return c.get("__vfs_entry__", fmt.Sprintf("%x", mac))
}

func (c *CheckCache) PutObjectStatus(mac objects.MAC, err []byte) error {
	return c.put("__object__", fmt.Sprintf("%x", mac), err)
}

func (c *CheckCache) GetObjectStatus(mac objects.MAC) ([]byte, error) {
	return c.get("__object__", fmt.Sprintf("%x", mac))
}

func (c *CheckCache) PutChunkStatus(mac objects.MAC, err []byte) error {
	return c.put("__chunk__", fmt.Sprintf("%x", mac), err)
}

func (c *CheckCache) GetChunkStatus(mac objects.MAC) ([]byte, error) {
	return c.get("__chunk__", fmt.Sprintf("%x", mac))
}
