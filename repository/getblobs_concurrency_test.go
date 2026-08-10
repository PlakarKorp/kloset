package repository_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/repository"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// leakCheck captures the goroutine count and returns an assertion that
// the count settled back, catching iterator goroutines (feeder, workers,
// closer) left parked by an early break or a dropped iterator.
func leakCheck(t *testing.T) func() {
	t.Helper()
	before := runtime.NumGoroutine()
	return func() {
		t.Helper()
		// Polled inline rather than via require.Eventually: Eventually
		// evaluates its condition in a goroutine of its own, which
		// permanently inflates the count it is trying to settle.
		deadline := time.Now().Add(2 * time.Second)
		for {
			now := runtime.NumGoroutine()
			if now <= before {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("goroutines leaked: %d before, %d after", before, now)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestGetBlobsConcurrentRoundtrip(t *testing.T) {
	// The concurrent executor must preserve the sequential contract:
	// every distinct blob delivered exactly once, byte-identical to the
	// single-blob read path, duplicates collapsed. Run under -race this
	// also exercises decodeBuffer in parallel workers.
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	reqs := chunkRequests(t, repo, "/a.txt", "/b.txt", "/c.txt", "/d.txt", "/e.txt", "/f.txt")

	opts := &repository.GetBlobsOpts{Concurrency: 8}
	got := make(map[repository.BlobReq][]byte)
	for resp, err := range repo.GetBlobs(context.Background(), append(reqs, reqs...), opts) {
		require.NoError(t, err)
		req := repository.BlobReq{Type: resp.Type, MAC: resp.MAC}
		_, dup := got[req]
		require.False(t, dup, "blob %x delivered twice", resp.MAC)
		got[req] = resp.Data
	}

	distinct := make(map[repository.BlobReq]struct{})
	for _, req := range reqs {
		distinct[req] = struct{}{}
	}
	require.Len(t, got, len(distinct))

	for req, data := range got {
		expected, err := repo.GetBlobBytes(req.Type, req.MAC)
		require.NoError(t, err)
		require.Equal(t, expected, data)
	}
}

func TestGetBlobsConcurrentRangeReadError(t *testing.T) {
	// Failed range reads under concurrency: every blob of every failed
	// plan still yields identity+error, successes and errors interleave
	// freely, nothing panics, nothing is lost.
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	reqs := chunkRequests(t, repo, "/a.txt", "/b.txt", "/c.txt")

	for mac := range repo.ListPackfiles() {
		require.NoError(t, repo.DeletePackfile(mac))
	}

	count := 0
	for resp, err := range repo.GetBlobs(context.Background(), reqs, &repository.GetBlobsOpts{Concurrency: 8}) {
		require.Error(t, err)
		require.Nil(t, resp.Data)
		count++
	}
	require.Equal(t, len(reqs), count)
}

func TestGetBlobsZeroValueOpts(t *testing.T) {
	// A non-nil opts with the Concurrency zero value must behave like
	// the default (1), not spawn zero workers: all blobs delivered, no
	// feeder goroutine left blocked on an undrained plan channel.
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	reqs := chunkRequests(t, repo, "/a.txt", "/b.txt")

	check := leakCheck(t)

	count := 0
	for _, err := range repo.GetBlobs(context.Background(), reqs, &repository.GetBlobsOpts{}) {
		require.NoError(t, err)
		count++
	}
	require.Equal(t, len(reqs), count,
		"zero-value opts must deliver every blob, not silently none")
	check()
}

func TestGetBlobsEarlyBreakNoLeak(t *testing.T) {
	// Breaking out of the iteration must tear the pipeline down: the
	// feeder, the workers, and the closer all exit without waiting on
	// the caller's context.
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	reqs := chunkRequests(t, repo, "/a.txt", "/b.txt", "/c.txt", "/d.txt", "/e.txt", "/f.txt")

	check := leakCheck(t)

	for resp, err := range repo.GetBlobs(context.Background(), reqs, &repository.GetBlobsOpts{Concurrency: 2}) {
		require.NoError(t, err)
		require.NotEmpty(t, resp.Data)
		break
	}
	check()
}

func TestGetBlobsCancelledContext(t *testing.T) {
	// A pre-cancelled context: the iteration terminates (possibly after
	// delivering already-buffered results) and every goroutine exits.
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	reqs := chunkRequests(t, repo, "/a.txt", "/b.txt")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	check := leakCheck(t)
	for range repo.GetBlobs(ctx, reqs, &repository.GetBlobsOpts{Concurrency: 4}) {
	}
	check()
}

func TestGetBlobsMoreWorkersThanReads(t *testing.T) {
	// Concurrency far above the plan count: idle workers must not
	// deadlock shutdown or duplicate deliveries.
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	reqs := chunkRequests(t, repo, "/a.txt")

	check := leakCheck(t)

	count := 0
	for _, err := range repo.GetBlobs(context.Background(), reqs, &repository.GetBlobsOpts{Concurrency: 64}) {
		require.NoError(t, err)
		count++
	}
	require.Equal(t, len(reqs), count)
	check()
}
