package fifo

import (
	"sync"
	"sync/atomic"
)

// Cache implements a FIFO caching strategy.
type Cache[K comparable, V any] struct {
	mtx sync.RWMutex

	size int

	items map[K]V

	buffer []K
	head   int
	tail   int

	onevict func(K, V) error

	hits   atomic.Uint64
	misses atomic.Uint64
}

// New constructs a new FIFO cache of the given size.  The optional
// callback onevict is called when an item is evicted: if it retuns an
// error, the operation is aborted.
func New[K comparable, V any](size int, onevict func(K, V) error) *Cache[K, V] {
	// since we're working in modulo fashion, bump the size by
	// one, so that we actually can hold `size' items.  Also,
	// silently bump the size if it's non-sensical.
	size = max(size, 1) + 1

	return &Cache[K, V]{
		size:    size,
		items:   make(map[K]V, size),
		buffer:  make([]K, size),
		tail:    -1,
		onevict: onevict,
	}
}

func (c *Cache[K, V]) put(key K, val V) error {
	// special case: on duplicate, we override the value and leave
	// it in its place.
	if _, ok := c.items[key]; ok {
		c.items[key] = val
		return nil
	}

	nexthead := (c.head + 1) % c.size

	if nexthead == c.tail {
		oldk := c.buffer[c.tail]
		if c.onevict != nil {
			if err := c.onevict(oldk, c.items[oldk]); err != nil {
				return err
			}
		}
		delete(c.items, oldk)
	}
	c.buffer[c.head] = key
	c.items[key] = val

	c.head = nexthead
	if c.tail == -1 || c.tail == c.head {
		c.tail = (c.tail + 1) % c.size
	}

	return nil
}

// Put inserts an item in the cache.  If the cache is full, the oldest
// element is evicted.  Put can fail only if the onevict callback
// returns an error.
func (c *Cache[K, V]) Put(key K, val V) error {
	c.mtx.Lock()
	err := c.put(key, val)
	c.mtx.Unlock()
	return err
}

// Get retrieves the given key from the cache and returns its value
// and a boolean indicating whether it was found.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mtx.RLock()
	val, ok := c.items[key]
	c.mtx.RUnlock()

	if ok {
		c.hits.Add(1)
	} else {
		c.misses.Add(1)
	}

	return val, ok
}

// Close empties the cache, and calls onevict on all pending items.
// It is safe to re-use a cache after Close was called assuming the
// size and onevict are okay, as it really is a reset operation.  If
// onevict fails, Close keeps evicting all the other items, and
// returns the last error occurred.
func (c *Cache[K, V]) Close() error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	var err error
	if c.onevict != nil {
		for key, val := range c.items {
			if e := c.onevict(key, val); e != nil {
				err = e
			}
		}
	}

	// reset
	c.tail = -1
	c.head = 0
	clear(c.items)
	clear(c.buffer)

	return err
}

// Stats return the number of cache hit, misses and the size of the
// cache.  It does *not* reset them.
func (c *Cache[K, V]) Stats() (hit, miss, size uint64) {
	c.mtx.RLock()
	head := uint64(c.head)
	tail := c.tail
	c.mtx.RUnlock()

	if tail != -1 {
		size = (head - uint64(tail) + uint64(c.size)) % uint64(c.size)
	}

	return c.hits.Load(), c.misses.Load(), size
}
