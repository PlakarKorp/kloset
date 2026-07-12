package lru

import (
	"sync"
	"sync/atomic"
)

type node[K comparable, V any] struct {
	key  K
	val  V
	next *node[K, V]
	prev *node[K, V]
}

// Cache implements a LRU caching strategy.  All methods beside Close
// are safe for concurrent use.
type Cache[K comparable, V any] struct {
	mtx sync.Mutex

	size   int
	target int

	onevict func(K, V) error

	items map[K]*node[K, V]
	head  *node[K, V]
	tail  *node[K, V]

	hits   atomic.Uint64
	misses atomic.Uint64
}

// New constructs a cache of maximum size target.  The minimum size is
// two, and any value minor than that will silently be bumped to at
// least 2.  onevict is an optional callback that is called when
// evicting an item from the cache.
func New[K comparable, V any](target int, onevict func(K, V) error) *Cache[K, V] {
	// it's simpler to assume that we'll always have something in
	// the linked list, rather than trying to cope with
	// patological cases like zero or one.
	target = max(target, 2)
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
		n.next = c.head
		c.head.prev = n
		c.head = n
	}

	c.items[key] = n
}

// evictItem removes an item from the `items' map, without touching
// the list.
func (c *Cache[K, V]) evictItem(key K) error {
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

// Get retrieves a key from the cache, returning the value and true,
// or the zero value and false if not found.  It also bumps the
// "recent-ness" of the key.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mtx.Lock()
	n, ok := c.items[key]
	if ok && n != c.head {
		// move it as the first item in the list
		prev := n.prev
		next := n.next
		prev.next = next
		if next != nil {
			next.prev = prev
		}

		if c.tail == n {
			c.tail = n.prev
			c.tail.next = nil
		}

		n.next = c.head
		c.head.prev = n
		c.head = n
		n.prev = nil
	}
	c.mtx.Unlock()

	if ok {
		c.hits.Add(1)
		return n.val, true
	} else {
		c.misses.Add(1)
		var zero V
		return zero, false
	}
}

// Put adds or overrides an element in the cache, eventually evicting
// the less recent item in the cache.  It can only fail if the
// `onevict` callback fails, in which case it aborts the operation.
func (c *Cache[K, V]) Put(key K, val V) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if _, ok := c.items[key]; ok {
		c.items[key].val = val
		return nil
	}

	if c.size == c.target {
		if err := c.evictItem(c.tail.key); err != nil {
			return err
		}
		c.tail = c.tail.prev
		if c.tail != nil {
			c.tail.next.prev = nil
			c.tail.next = nil
		}
	}

	c.put(key, val)
	return nil
}

// Close flushes all the items in the cache using the `onevict`
// callback, if provided.  It is safe to reuse a cache after it has
// been closed, provided that the size and `onevict` callback are
// still fine: this method behaves like a reset.  It can only fail if
// `onevict` fails, in which case it returns the last error.
func (c *Cache[K, V]) Close() error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	var err error
	if c.onevict != nil {
		for key, n := range c.items {
			if e := c.onevict(key, n.val); e != nil {
				err = e
			}
		}
	}
	clear(c.items)
	c.size = 0
	c.head = nil
	c.tail = nil
	return err
}

// Stats return the number of cache hit, misses and the size of the
// cache.  It does *not* reset them.
func (c *Cache[K, V]) Stats() (hit, miss, size uint64) {
	return c.hits.Load(), c.misses.Load(), uint64(c.size)
}
