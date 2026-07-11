package lru

import (
	"sync"
	"sync/atomic"
)

type node[K comparable, V any] struct {
	key  K
	val  V
	next *node[K, V]
}

type Cache[K comparable, V any] struct {
	mtx sync.RWMutex

	size   int
	target int

	onevict func(K, V) error

	items map[K]*node[K, V]
	head  *node[K, V]
	tail  *node[K, V]

	hits   atomic.Uint64
	misses atomic.Uint64
}

func New[K comparable, V any](target int, onevict func(K, V) error) *Cache[K, V] {
	return &Cache[K, V]{
		target:  target,
		onevict: onevict,
		items:   make(map[K]*node[K, V], target),
	}
}

func (c *Cache[K, V]) put(key K, val V) {
	c.size++
	n := &node[K, V]{key: key, val: val}
	if c.head == nil {
		c.head = n
		c.tail = n
	} else {
		c.tail.next = n
		c.tail = n
	}

	c.items[key] = n
}

// assume that the item was just removed from the linked list
func (c *Cache[K, V]) flush(key K) error {
	n := c.items[key]
	if c.onevict != nil {
		if err := c.onevict(key, n.val); err != nil {
			return err
		}
	}

	delete(c.items, key)
	c.size--
	return nil
}

func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mtx.RLock()
	n, ok := c.items[key]
	c.mtx.RUnlock()

	if ok {
		c.hits.Add(1)
		return n.val, true
	} else {
		c.misses.Add(1)
		var zero V
		return zero, false
	}
}

// Put adds or overrides an element in the cache.
func (c *Cache[K, V]) Put(key K, val V) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if _, ok := c.items[key]; ok {
		c.items[key].val = val
		return nil
	}

	if c.size == c.target {
		if err := c.flush(c.head.key); err != nil {
			return err
		}
		c.head = c.head.next
	}

	c.put(key, val)
	return nil
}

func (c *Cache[K, V]) Close() error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	var err error
	for n := c.head; n != nil; n = n.next {
		if e := c.flush(n.key); e != nil {
			err = e
		}
	}
	c.head = nil
	c.tail = nil
	return err
}

func (c *Cache[K, V]) Stats() (hit, miss, size uint64) {
	return c.hits.Load(), c.misses.Load(), uint64(c.size)
}
