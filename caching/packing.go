package caching

import (
	"iter"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/google/uuid"
)

type PackingCache struct {
	cache Cache
}

func newPackingCache(cons Constructor) (*PackingCache, error) {
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
	return c.cache.Put(keyT("__blob__", Type, mac), nil)
}

func (c *PackingCache) HasBlob(Type resources.Type, mac objects.MAC) (bool, error) {
	return c.cache.Has(keyT("__blob__", Type, mac))
}

func (c *PackingCache) PutIndexBlob(Type resources.Type, mac objects.MAC, data []byte) error {
	return c.cache.Put(keyT("__index__", Type, mac), data)
}

func (c *PackingCache) GetIndexesBlob() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for _, v := range c.cache.Scan([]byte("__index__"), false) {
			if !yield(v) {
				return
			}
		}
	}
}
