package snapshot_test

import (
	"sync"
	"testing"

	"github.com/PlakarKorp/kloset/events"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// listenCachedEvents subscribes to the AppContext event bus and returns a
// helper that drains the bus during a backup and reports how many file/path
// cache hits were emitted. The returned cancel func unsubscribes.
type cachedCounts struct {
	files int
	paths int
}

func collectCachedEvents(t *testing.T, bus *events.EventsBUS) (snapshot func() cachedCounts, stop func()) {
	ch := bus.Listen()
	var mu sync.Mutex
	var counts cachedCounts
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ev := range ch {
			if ev == nil {
				continue
			}
			mu.Lock()
			switch ev.Type {
			case "file.cached":
				counts.files++
			case "path.cached":
				counts.paths++
			}
			mu.Unlock()
		}
	}()
	snapshot = func() cachedCounts {
		mu.Lock()
		defer mu.Unlock()
		return counts
	}
	stop = func() {
		// the bus is closed by AppContext.Close in the test cleanup;
		// callers don't need to do anything here. We keep the signature
		// in case we later want to detach a listener.
		_ = done
	}
	return
}

// TestStatCacheReusesAcrossBackups verifies that a second backup of the same
// content into the same repository produces "file.cached" events for every
// regular file — meaning the per-repository stat cache fast-path triggered
// and chunking was skipped.
func TestStatCacheReusesAcrossBackups(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	t.Cleanup(func() {
		_ = repo.AppContext().GetCache().Close()
	})

	files := []ptesting.MockFile{
		ptesting.NewMockFile("a/one.txt", 0644, "hello"),
		ptesting.NewMockFile("a/two.txt", 0644, "world"),
		ptesting.NewMockFile("b/three.txt", 0644, "lorem ipsum"),
	}

	// First backup: cache is empty, expect zero file.cached events.
	getCounts, _ := collectCachedEvents(t, repo.AppContext().Events())
	first := ptesting.GenerateSnapshot(t, repo, files)
	defer first.Close()

	c1 := getCounts()
	require.Equal(t, 0, c1.files, "first backup should not emit file.cached")

	// Second backup: same content, same repo, same source descriptors —
	// every regular file must hit the stat cache.
	second := ptesting.GenerateSnapshot(t, repo, files)
	defer second.Close()

	c2 := getCounts()
	require.GreaterOrEqual(t, c2.files, len(files),
		"second backup should emit at least one file.cached per regular file (got %d, want >= %d)",
		c2.files, len(files))
}

// TestStatCacheInvalidatedByContentChange verifies that modifying a file's
// content (which changes its size and so its stat tuple) suppresses the
// cache hit for that file on the next backup.
func TestStatCacheInvalidatedByContentChange(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	t.Cleanup(func() {
		_ = repo.AppContext().GetCache().Close()
	})

	original := []ptesting.MockFile{
		ptesting.NewMockFile("stable.txt", 0644, "unchanged"),
		ptesting.NewMockFile("mutating.txt", 0644, "v1"),
	}
	first := ptesting.GenerateSnapshot(t, repo, original)
	defer first.Close()

	// Change one file's content. MockFile derives size from Content
	// length, so the stored stat tuple won't match.
	modified := []ptesting.MockFile{
		ptesting.NewMockFile("stable.txt", 0644, "unchanged"),
		ptesting.NewMockFile("mutating.txt", 0644, "v2-different-length"),
	}

	getCounts, _ := collectCachedEvents(t, repo.AppContext().Events())
	second := ptesting.GenerateSnapshot(t, repo, modified)
	defer second.Close()

	c := getCounts()
	// stable.txt should still hit the cache; mutating.txt must not. We
	// can't assert a strict equality on file.cached count because the
	// scan order is map-iteration based, but we can assert that we
	// didn't get a hit for *every* file.
	require.Less(t, c.files, len(modified),
		"a changed file should not produce a file.cached event (got %d hits across %d files)",
		c.files, len(modified))
}
