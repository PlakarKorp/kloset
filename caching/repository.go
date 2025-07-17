package caching

import (
	"fmt"
	"iter"
	"log"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/google/uuid"
)

type _RepositoryCache struct {
	cache Cache
}

func newRepositoryCache(cons CacheConstructor, repositoryID uuid.UUID) (*_RepositoryCache, error) {
	cache, err := cons(CACHE_VERSION, "repository", repositoryID.String(), None)
	if err != nil {
		return nil, err
	}

	return &_RepositoryCache{
		cache: cache,
	}, nil
}

func (c *_RepositoryCache) PutState(stateID objects.MAC, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__state__:%x", stateID)), data)
}

func (c *_RepositoryCache) HasState(stateID objects.MAC) (bool, error) {
	return c.cache.Has([]byte(fmt.Sprintf("__state__:%x", stateID)))
}

func (c *_RepositoryCache) GetState(stateID objects.MAC) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprintf("__state__:%x", stateID)))
}

func (c *_RepositoryCache) DelState(stateID objects.MAC) error {
	return c.cache.Delete([]byte(fmt.Sprintf("__state__:%x", stateID)))
}

func (c *_RepositoryCache) GetStates() (map[objects.MAC][]byte, error) {
	ret := make(map[objects.MAC][]byte, 0)

	for key, val := range c.cache.Scan([]byte("__state__:"), false) {
		value := make([]byte, len(val))
		copy(value, val)

		ret[objects.MAC(key)] = value
	}

	return ret, nil
}

func (c *_RepositoryCache) GetDelta(blobType resources.Type, blobCsum objects.MAC) iter.Seq2[[]byte, []byte] {
	return c.cache.Scan([]byte(fmt.Sprintf("__delta__:%d:%x:", blobType, blobCsum)), false)
}

func (c *_RepositoryCache) PutDelta(blobType resources.Type, blobCsum, packfile objects.MAC, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__delta__:%d:%x:%x", blobType, blobCsum, packfile)), data)
}

func (c *_RepositoryCache) GetDeltasByType(blobType resources.Type) iter.Seq2[[]byte, []byte] {
	return c.cache.Scan([]byte(fmt.Sprintf("__delta__:%d:", blobType)), false)
}

func (c *_RepositoryCache) GetDeltas() iter.Seq2[[]byte, []byte] {
	return c.cache.Scan([]byte("__delta__:"), false)
}

func (c *_RepositoryCache) DelDelta(blobType resources.Type, blobCsum, packfileMAC objects.MAC) error {
	return c.cache.Delete([]byte(fmt.Sprintf("__delta__:%d:%x:%x", blobType, blobCsum, packfileMAC)))
}

func (c *_RepositoryCache) PutDeleted(blobType resources.Type, blobCsum objects.MAC, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__deleted__:%d:%x", blobType, blobCsum)), data)
}

func (c *_RepositoryCache) HasDeleted(blobType resources.Type, blobCsum objects.MAC) (bool, error) {
	return c.cache.Has([]byte(fmt.Sprintf("__deleted__:%d:%x", blobType, blobCsum)))
}

func (c *_RepositoryCache) GetDeleteds() iter.Seq2[[]byte, []byte] {
	return c.cache.Scan([]byte("__deleted__:"), false)
}

func (c *_RepositoryCache) GetDeletedsByType(blobType resources.Type) iter.Seq2[[]byte, []byte] {
	return c.cache.Scan([]byte(fmt.Sprintf("__deleted__:%d:", blobType)), false)
}

func (c *_RepositoryCache) DelDeleted(blobType resources.Type, blobCsum objects.MAC) error {
	return c.cache.Delete([]byte(fmt.Sprintf("__deleted__:%d:%x", blobType, blobCsum)))
}

func (c *_RepositoryCache) PutPackfile(packfile objects.MAC, data []byte) error {
	log.Printf("x putting packfile %x", packfile)
	return c.cache.Put([]byte(fmt.Sprintf("__packfile__:%x", packfile)), data)
}

func (c *_RepositoryCache) HasPackfile(packfile objects.MAC) (bool, error) {
	return c.cache.Has([]byte(fmt.Sprintf("__packfile__:%x", packfile)))
}

func (c *_RepositoryCache) DelPackfile(packfile objects.MAC) error {
	return c.cache.Delete([]byte(fmt.Sprintf("__packfile__:%x", packfile)))
}

func (c *_RepositoryCache) GetPackfiles() iter.Seq2[[]byte, []byte] {
	return c.cache.Scan([]byte("__packfile__:"), false)
}

func (c *_RepositoryCache) PutConfiguration(key string, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__configuration__:%s", key)), data)
}

func (c *_RepositoryCache) GetConfiguration(key string) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprintf("__configuration__:%s", key)))
}

func (c *_RepositoryCache) GetConfigurations() iter.Seq2[[]byte, []byte] {
	return c.cache.Scan([]byte("__configuration__:"), false)
}

func (c *_RepositoryCache) PutSnapshot(stateID objects.MAC, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__snapshot__:%x", stateID)), data)
}

func (c *_RepositoryCache) HasSnapshot(stateID objects.MAC) (bool, error) {
	return c.cache.Has([]byte(fmt.Sprintf("__snapshot__:%x", stateID)))
}

func (c *_RepositoryCache) GetSnapshot(stateID objects.MAC) ([]byte, error) {
	return c.cache.Get([]byte(fmt.Sprintf("__snapshot__:%x", stateID)))
}

func (c *_RepositoryCache) DelSnapshot(stateID objects.MAC) error {
	return c.cache.Delete([]byte(fmt.Sprintf("__snapshot__:%x", stateID)))
}

func (c *_RepositoryCache) Close() error {
	return c.cache.Close()
}
