package btree

import (
	"github.com/PlakarKorp/kloset/caching/lru"
)

type cacheitem[K any, P comparable, V any] struct {
	dirty bool
	node  *Node[K, P, V]
}

type cache[K any, P comparable, V any] struct {
	store Storer[K, P, V]
	lru   *lru.Cache[P, *cacheitem[K, P, V]]
}

func cachefor[K any, P comparable, V any](store Storer[K, P, V], order int) *cache[K, P, V] {
	return &cache[K, P, V]{
		store: store,
		lru: lru.New(order, func(ptr P, n *cacheitem[K, P, V]) error {
			if !n.dirty {
				return nil
			}
			return store.Update(ptr, n.node)
		}),
	}
}

func (c *cache[K, P, V]) Get(ptr P) (*Node[K, P, V], error) {
	it, ok := c.lru.Get(ptr)
	if ok {
		return it.node, nil
	}

	node, err := c.store.Get(ptr)
	if err != nil {
		return nil, err
	}

	if err := c.lru.Put(ptr, &cacheitem[K, P, V]{node: node}); err != nil {
		return nil, err
	}
	return node, nil
}

func (c *cache[K, P, V]) Update(ptr P, node *Node[K, P, V]) error {
	return c.lru.Put(ptr, &cacheitem[K, P, V]{node: node, dirty: true})
}

func (c *cache[K, P, V]) Put(node *Node[K, P, V]) (P, error) {
	return c.store.Put(node)
}

func (c *cache[K, P, V]) Close() error {
	return c.lru.Close()
}
