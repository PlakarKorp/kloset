package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
	"golang.org/x/sync/singleflight"
)

// PackfileCachePolicy decides whether a given packfile should be
// fully downloaded and cached locally when first accessed.
type PackfileCachePolicy interface {
	ShouldCache(ctx context.Context, mac objects.MAC) bool
}

// AlwaysCachePolicy caches every packfile unconditionally.
type AlwaysCachePolicy struct{}

func (AlwaysCachePolicy) ShouldCache(_ context.Context, _ objects.MAC) bool { return true }

// FuncCachePolicy delegates the decision to a user-supplied callback.
type FuncCachePolicy struct {
	Fn func(ctx context.Context, mac objects.MAC) bool
}

func (f FuncCachePolicy) ShouldCache(ctx context.Context, mac objects.MAC) bool {
	return f.Fn(ctx, mac)
}

// CachingStore wraps any Store and caches full packfiles on local disk.
// On the first range request for a packfile whose policy returns true,
// the entire packfile is downloaded to cacheDir and all subsequent
// requests are served from the local file.
//
// Non-packfile resources and packfiles whose policy returns false are
// passed through to the inner store unchanged.
type CachingStore struct {
	inner    Store
	cacheDir string
	policy   PackfileCachePolicy
	sf       singleflight.Group
}

// NewCachingStore creates a CachingStore that wraps inner, stores cached
// packfiles under cacheDir, and consults policy for each packfile MAC.
func NewCachingStore(inner Store, cacheDir string, policy PackfileCachePolicy) (*CachingStore, error) {
	if err := os.MkdirAll(cacheDir, 0700); err != nil {
		return nil, err
	}
	return &CachingStore{
		inner:    inner,
		cacheDir: cacheDir,
		policy:   policy,
	}, nil
}

func (c *CachingStore) cachedPath(mac objects.MAC) string {
	return filepath.Join(c.cacheDir, mac.FormatHex())
}

// download fetches the full packfile from the inner store and writes it
// atomically to dst. It is intended to be called inside singleflight.Do.
func (c *CachingStore) download(ctx context.Context, mac objects.MAC, dst string) error {
	rc, err := c.inner.Get(ctx, StorageResourcePackfile, mac, nil)
	if err != nil {
		return err
	}
	defer rc.Close()

	tmp := dst + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	if _, err := io.Copy(f, rc); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}

	return os.Rename(tmp, dst)
}

// openRange opens the cached file and returns an io.ReadCloser that
// reads exactly the bytes described by r. If r is nil the full file is
// returned.
func openRange(path string, r *Range) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	if r == nil {
		return f, nil
	}

	if _, err := f.Seek(int64(r.Offset), io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}

	fmt.Fprintf(os.Stderr, "cache: opened cached packfile %s (offset=%d length=%d)\n", filepath.Base(path), r.Offset, r.Length)

	return struct {
		io.Reader
		io.Closer
	}{
		Reader: io.LimitReader(f, int64(r.Length)),
		Closer: f,
	}, nil
}

// Get implements Store. For packfiles whose policy returns true it
// ensures the full packfile is cached locally before serving the
// requested range. All other resources pass through to the inner store.
func (c *CachingStore) Get(ctx context.Context, resource StorageResource, mac objects.MAC, r *Range) (io.ReadCloser, error) {
	if resource != StorageResourcePackfile || !c.policy.ShouldCache(ctx, mac) {
		fmt.Fprintf(os.Stderr, "cache: not caching packfile %s\n", mac.FormatHex())
		return c.inner.Get(ctx, resource, mac, r)
	}

	cached := c.cachedPath(mac)

	if _, err := os.Stat(cached); err != nil {
		// Cache miss: download the full packfile exactly once even if
		// multiple goroutines request blobs from the same packfile
		// concurrently.
		_, dlErr, _ := c.sf.Do(mac.FormatHex(), func() (any, error) {
			return nil, c.download(ctx, mac, cached)
		})
		if dlErr != nil {
			// Download failed: fall back to a direct range request so
			// the caller is not left empty-handed.
			return c.inner.Get(ctx, resource, mac, r)
		}
	}

	fmt.Fprintf(os.Stderr, "cache: serving packfile %s from cache\n", mac.FormatHex())
	return openRange(cached, r)
}

// Delete passes the delete through to the inner store and also removes
// the locally cached file if present.
func (c *CachingStore) Delete(ctx context.Context, resource StorageResource, mac objects.MAC) error {
	if resource == StorageResourcePackfile {
		os.Remove(c.cachedPath(mac))
	}
	return c.inner.Delete(ctx, resource, mac)
}

// All remaining Store methods delegate to the inner store unchanged.

func (c *CachingStore) Create(ctx context.Context, config []byte) error {
	return c.inner.Create(ctx, config)
}

func (c *CachingStore) Open(ctx context.Context) ([]byte, error) {
	return c.inner.Open(ctx)
}

func (c *CachingStore) Ping(ctx context.Context) error {
	return c.inner.Ping(ctx)
}

func (c *CachingStore) Origin() string        { return c.inner.Origin() }
func (c *CachingStore) Type() string          { return c.inner.Type() }
func (c *CachingStore) Root() string          { return c.inner.Root() }
func (c *CachingStore) Flags() location.Flags { return c.inner.Flags() }

func (c *CachingStore) Mode(ctx context.Context) (Mode, error) {
	return c.inner.Mode(ctx)
}

func (c *CachingStore) Size(ctx context.Context) (int64, error) {
	return c.inner.Size(ctx)
}

func (c *CachingStore) List(ctx context.Context, resource StorageResource) ([]objects.MAC, error) {
	return c.inner.List(ctx, resource)
}

func (c *CachingStore) Put(ctx context.Context, resource StorageResource, mac objects.MAC, rd io.Reader) (int64, error) {
	return c.inner.Put(ctx, resource, mac, rd)
}

func (c *CachingStore) Close(ctx context.Context) error {
	return c.inner.Close(ctx)
}
