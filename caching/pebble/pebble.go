package pebble

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"syscall"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/cockroachdb/pebble/v2"
)

var ErrInUse = fmt.Errorf("cache in use")

// cache implements caching.cache, it's a caching backend on pebble
type cache struct {
	db *pebble.DB

	dir           string
	deleteOnClose bool
}

// Lifter from internal/logger pebble.
type noopLoggerAndTracer struct{}

func (l noopLoggerAndTracer) Infof(format string, args ...any)                       {}
func (l noopLoggerAndTracer) Errorf(format string, args ...any)                      {}
func (l noopLoggerAndTracer) Fatalf(format string, args ...any)                      {}
func (l noopLoggerAndTracer) Eventf(ctx context.Context, format string, args ...any) {}
func (l noopLoggerAndTracer) IsTracingEnabled(ctx context.Context) bool              { return false }

func Constructor(dir string) caching.Constructor {
	return func(version, name, repoid string, opt caching.Option) (caching.Cache, error) {
		dest := filepath.Join(dir, version, name, filepath.FromSlash(repoid))
		return New(dest, opt == caching.DeleteOnClose)
	}
}

func New(dir string, deletedOnClose bool) (caching.Cache, error) {
	opts := pebble.Options{
		MemTableSize: 256 << 20,
		Logger:       noopLoggerAndTracer{},
	}
	db, err := pebble.Open(dir, &opts)
	if err != nil {
		if errors.Is(err, syscall.EAGAIN) {
			return nil, ErrInUse
		}
		return nil, err
	}

	return &cache{db, dir, deletedOnClose}, nil
}

func (c *cache) Put(key, data []byte) error {
	return c.db.Set(key, data, pebble.NoSync)
}

func (c *cache) Has(key []byte) (bool, error) {
	_, del, err := c.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	del.Close()

	return true, nil
}

func (c *cache) Get(key []byte) ([]byte, error) {
	data, del, err := c.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}

	ret := make([]byte, len(data))
	copy(ret, data)
	del.Close()

	return ret, nil
}

func makeKeyUpperBound(key []byte) []byte {
	end := make([]byte, len(key))
	copy(end, key)

	for i := len(end) - 1; i >= 0; i-- {
		end[i] = end[i] + 1
		if end[i] != 0 {
			return end[:i+1]
		}
	}

	return nil // no upper-bound
}

func (c *cache) Scan(prefix []byte, reverse bool) iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		opts := pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: makeKeyUpperBound(prefix),
		}

		// It's safe to ignore the error here, the implementation always return nil
		iter, _ := c.db.NewIter(&opts)
		defer iter.Close()

		if reverse {
			iter.Last()
		} else {
			iter.First()
		}

		for iter.Valid() {
			key := iter.Key()
			key = key[len(prefix):]

			if !yield(key, iter.Value()) {
				return
			}

			if reverse {
				iter.Prev()
			} else {
				iter.Next()
			}
		}
	}
}

func (c *cache) Delete(key []byte) error {
	return c.db.Delete(key, pebble.NoSync)
}

func (c *cache) Close() error {
	ret := c.db.Close()
	if c.deleteOnClose {
		if err := os.RemoveAll(c.dir); err != nil {
			ret = err
		}
	}
	return ret
}
