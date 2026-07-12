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
		n.next = c.head
		c.head.prev = n
		c.head = n
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

// Put adds or overrides an element in the cache.
func (c *Cache[K, V]) Put(key K, val V) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if _, ok := c.items[key]; ok {
		c.items[key].val = val
		return nil
	}

	if c.size == c.target {
		if err := c.flush(c.tail.key); err != nil {
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
