package caching

import (
	"fmt"
	"iter"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/google/uuid"
)

type PackingCache struct {
	cache Cache
}

func newPackingCache(cons CacheConstructor) (*PackingCache, error) {
	cache, err := cons(CACHE_VERSION, "packing", uuid.NewString(), DeleteOnClose)
	if err != nil {
		return nil, err
	}

	return &PackingCache{
		cache: cache,
	}, nil
}

func (c *PackingCache) Close() error {
	return c.cache.Close()
}

func (c *PackingCache) PutBlob(Type resources.Type, mac objects.MAC) error {
	return c.cache.Put([]byte(fmt.Sprintf("__blob__:%d:%x", Type, mac)), nil)
}

func (c *PackingCache) HasBlob(Type resources.Type, mac objects.MAC) (bool, error) {
	return c.cache.Has([]byte(fmt.Sprintf("__blob__:%d:%x", Type, mac)))
}

func (c *PackingCache) PutIndexBlob(Type resources.Type, mac objects.MAC, data []byte) error {
	return c.cache.Put([]byte(fmt.Sprintf("__index__:%d:%x", Type, mac)), data)
}

func (c *PackingCache) GetIndexesBlob() iter.Seq2[[]byte, []byte] {
	return c.cache.Scan([]byte("__index__:"), false)
}
