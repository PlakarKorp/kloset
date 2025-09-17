package caching

import (
	"fmt"
	"iter"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/google/uuid"
)

type _RepositoryCache struct {
	cache Cache
}

func newRepositoryCache(cons Constructor, repositoryID uuid.UUID) (*_RepositoryCache, error) {
	cache, err := cons(CACHE_VERSION, "repository", repositoryID.String(), None)
	if err != nil {
		return nil, err
	}

	return &_RepositoryCache{cache}, nil
}

func (c *_RepositoryCache) Close() error { return c.cache.Close() }

func (c *_RepositoryCache) PutState(stateID objects.MAC, data []byte) error {
	return c.cache.Put(key("__state__", stateID), data)
}

func (c *_RepositoryCache) HasState(stateID objects.MAC) (bool, error) {
	return c.cache.Has(key("__state__", stateID))
}

func (c *_RepositoryCache) GetState(stateID objects.MAC) ([]byte, error) {
	return c.cache.Get(key("__state__", stateID))
}

func (c *_RepositoryCache) DelState(stateID objects.MAC) error {
	return c.cache.Delete(key("__state__", stateID))
}

func (c *_RepositoryCache) GetStates() (map[objects.MAC][]byte, error) {
	ret := make(map[objects.MAC][]byte, 0)

	for key, val := range scanMAC2(c.cache, []byte("__state__:")) {
		value := make([]byte, len(val))
		copy(value, val)

		ret[key] = value
	}

	return ret, nil
}

func (c *_RepositoryCache) GetDelta(blobType resources.Type, blobCsum objects.MAC) iter.Seq2[objects.MAC, []byte] {
	return scanMAC2(c.cache, keyT("__delta__", blobType, blobCsum))
}

func (c *_RepositoryCache) PutDelta(blobType resources.Type, blobCsum, packfile objects.MAC, data []byte) error {
	return c.cache.Put(keyT2("__delta__", blobType, blobCsum, packfile), data)
}

func (c *_RepositoryCache) GetDeltasByType(blobType resources.Type) iter.Seq2[objects.MAC, []byte] {
	return scanMAC2(c.cache, []byte(fmt.Sprintf("__delta__:%d:", blobType)))
}

func (c *_RepositoryCache) GetDeltas() iter.Seq2[objects.MAC, []byte] {
	return scanMAC2(c.cache, []byte("__delta__:"))
}

func (c *_RepositoryCache) DelDelta(blobType resources.Type, blobCsum, packfileMAC objects.MAC) error {
	return c.cache.Delete(keyT2("__delta__", blobType, blobCsum, packfileMAC))
}

func (c *_RepositoryCache) PutDeleted(blobType resources.Type, blobCsum objects.MAC, data []byte) error {
	return c.cache.Put(keyT("__deleted__", blobType, blobCsum), data)
}

func (c *_RepositoryCache) HasDeleted(blobType resources.Type, blobCsum objects.MAC) (bool, error) {
	return c.cache.Has(keyT("__deleted__", blobType, blobCsum))
}

func (c *_RepositoryCache) GetDeleteds() iter.Seq2[objects.MAC, []byte] {
	return scanMAC2(c.cache, []byte("__deleted__:"))
}

func (c *_RepositoryCache) GetDeletedsByType(blobType resources.Type) iter.Seq2[objects.MAC, []byte] {
	return scanMAC2(c.cache, []byte(fmt.Sprintf("__deleted__:%d:", blobType)))
}

func (c *_RepositoryCache) DelDeleted(blobType resources.Type, blobCsum objects.MAC) error {
	return c.cache.Delete(keyT("__deleted__", blobType, blobCsum))
}

func (c *_RepositoryCache) PutPackfile(packfile objects.MAC, data []byte) error {
	return c.cache.Put(key("__packfile__", packfile), data)
}

func (c *_RepositoryCache) HasPackfile(packfile objects.MAC) (bool, error) {
	return c.cache.Has(key("__packfile__", packfile))
}

func (c *_RepositoryCache) DelPackfile(packfile objects.MAC) error {
	return c.cache.Delete(key("__packfile__", packfile))
}

func (c *_RepositoryCache) GetPackfiles() iter.Seq2[objects.MAC, []byte] {
	return scanMAC2(c.cache, []byte("__packfile__:"))
}

func (c *_RepositoryCache) PutConfiguration(key string, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprint("__configuration__:", key)), data)
}

func (c *_RepositoryCache) GetConfiguration(key string) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprint("__configuration__:", key)))
}

func (c *_RepositoryCache) GetConfigurations() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for _, v := range c.cache.Scan([]byte("__configuration__:"), false) {
			if !yield(v) {
				return
			}
		}
	}
}

func (c *_RepositoryCache) PutSnapshot(stateID objects.MAC, data []byte) error {
	return c.cache.Put(key("__snapshot__", stateID), data)
}

func (c *_RepositoryCache) HasSnapshot(stateID objects.MAC) (bool, error) {
	return c.cache.Has(key("__snapshot__", stateID))
}

func (c *_RepositoryCache) GetSnapshot(stateID objects.MAC) ([]byte, error) {
	return c.cache.Get(key("__snapshot__", stateID))
}

func (c *_RepositoryCache) DelSnapshot(stateID objects.MAC) error {
	return c.cache.Delete(key("__snapshot__", stateID))
}
