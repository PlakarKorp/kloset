package pebble

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/cockroachdb/pebble/v2"
	"github.com/dustin/go-humanize"
)

var ErrInUse = fmt.Errorf("cache in use")

// cache implements caching.cache, it's a caching backend on pebble
type cache struct {
	db *pebble.DB

	dir           string
	deleteOnClose bool

	putCount int64
	putBytes int64

	hasCount  int64
	hasMisses int64
	hasErrors int64

	getCount  int64
	getBytes  int64
	getMisses int64
	getErrors int64

	deleteCount int64

	scanCount int64
	scanItems int64
	scanBytes int64
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

	return &cache{db: db, dir: dir, deleteOnClose: deletedOnClose}, nil
}

func (c *cache) Put(key, data []byte) error {
	atomic.AddInt64(&c.putCount, 1)
	atomic.AddInt64(&c.putBytes, int64(len(data)))
	return c.db.Set(key, data, pebble.NoSync)
}

func (c *cache) Has(key []byte) (bool, error) {
	atomic.AddInt64(&c.hasCount, 1)
	_, del, err := c.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			atomic.AddInt64(&c.hasMisses, 1)
			return false, nil
		}
		atomic.AddInt64(&c.hasErrors, 1)
		return false, err
	}
	del.Close()

	return true, nil
}

func (c *cache) Get(key []byte) ([]byte, error) {
	atomic.AddInt64(&c.getCount, 1)
	data, del, err := c.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			atomic.AddInt64(&c.getMisses, 1)
			return nil, nil
		}
		atomic.AddInt64(&c.getErrors, 1)
		return nil, err
	}

	ret := make([]byte, len(data))
	copy(ret, data)
	del.Close()
	atomic.AddInt64(&c.getBytes, int64(len(ret)))
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
	atomic.AddInt64(&c.scanCount, 1)
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
			atomic.AddInt64(&c.scanItems, 1)
			if !yield(iter.Key(), iter.Value()) {
				atomic.AddInt64(&c.scanBytes, int64(len(iter.Value())))
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
	atomic.AddInt64(&c.deleteCount, 1)
	return c.db.Delete(key, pebble.NoSync)
}

type cacheStats struct {
	Key   string
	Value int64
}

func (c *cache) stats() []cacheStats {
	return []cacheStats{
		{Key: "put_count", Value: atomic.LoadInt64(&c.putCount)},
		{Key: "put_bytes", Value: atomic.LoadInt64(&c.putBytes)},
		{Key: "has_count", Value: atomic.LoadInt64(&c.hasCount)},
		{Key: "has_misses", Value: atomic.LoadInt64(&c.hasMisses)},
		{Key: "has_errors", Value: atomic.LoadInt64(&c.hasErrors)},
		{Key: "get_count", Value: atomic.LoadInt64(&c.getCount)},
		{Key: "get_bytes", Value: atomic.LoadInt64(&c.getBytes)},
		{Key: "get_misses", Value: atomic.LoadInt64(&c.getMisses)},
		{Key: "get_errors", Value: atomic.LoadInt64(&c.getErrors)},
		{Key: "delete_count", Value: atomic.LoadInt64(&c.deleteCount)},
		{Key: "scan_count", Value: atomic.LoadInt64(&c.scanCount)},
		{Key: "scan_items", Value: atomic.LoadInt64(&c.scanItems)},
		{Key: "scan_bytes", Value: atomic.LoadInt64(&c.scanBytes)},
	}
}

func (c *cache) Close() error {
	if os.Getenv("DEBUG_CACHE_STATS") != "" {
		// /Users/gilles/.cache/plakar-agentless/2.0.0/vfs/dd58be03-9269-43b0-9b4c-b4db18e6dfde/fs/Gilless-Mac-mini.local:
		// match the first atom before the UUID
		re := regexp.MustCompile(`.*/([a-zA-Z0-9_-]+)/[0-9a-fA-F-]{36}.*`)
		matches := re.FindStringSubmatch(c.dir)
		cacheType := c.dir
		if len(matches) == 2 {
			cacheType = matches[1]
		}
		fmt.Printf("Cache stats for %s:\n", cacheType)

		// all keys are constructed with <op>_<counter>,
		// display the op, then all counters for that op on same line:
		for _, stat := range c.stats() {
			if !strings.HasSuffix(stat.Key, "_bytes") {
				fmt.Printf("  %s: %d\n", stat.Key, stat.Value)
			} else {
				fmt.Printf("  %s: %s\n", stat.Key, humanize.IBytes(uint64(stat.Value)))
			}
		}
	}

	ret := c.db.Close()
	if c.deleteOnClose {
		if err := os.RemoveAll(c.dir); err != nil {
			ret = err
		}
	}
	return ret
}
