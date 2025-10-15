package caching

import (
	"fmt"
	"iter"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/google/uuid"
)

type PackingCache struct {
	kvcache
}

func newPackingCache(cons Constructor) (*PackingCache, error) {
	cache, err := cons(CACHE_VERSION, "packing", uuid.NewString(), DeleteOnClose)
	if err != nil {
		return nil, err
	}

	return &PackingCache{kvcache{cache}}, nil
}

func (c *PackingCache) PutBlob(Type resources.Type, mac objects.MAC) error {
	return c.put("__blob__", fmt.Sprintf("%d:%x", Type, mac), nil)
}

func (c *PackingCache) HasBlob(Type resources.Type, mac objects.MAC) (bool, error) {
	return c.has("__blob__", fmt.Sprintf("%d:%x", Type, mac))
}

func (c *PackingCache) PutIndexBlob(Type resources.Type, mac objects.MAC, data []byte) error {
	return c.put("__index__", fmt.Sprintf("%d:%x", Type, mac), data)
}

func (c *PackingCache) GetIndexesBlob() iter.Seq[[]byte] {
	return c.getObjects("__index__")
}
