package repository

import (
	"sort"
	"sync"

	"github.com/PlakarKorp/kloset/objects"
	"golang.org/x/sync/errgroup"
)

type PackfileFetchRange struct {
	Offset uint64
	Length uint32
}

type FetchPlan map[objects.MAC][]PackfileFetchRange

const defaultPrefetchPlanMaxGap = 512 * 1024

const defaultPrefetchPlanConcurrency = 16

type PrefetchPlanOptions struct {
	MaxGap      uint64
	Concurrency int
}

type cachedSpan struct {
	offset uint64
	data   []byte
}

type packfileSpanCache struct {
	mu    sync.RWMutex
	spans map[objects.MAC][]cachedSpan
}

func newPackfileSpanCache() *packfileSpanCache {
	return &packfileSpanCache{spans: make(map[objects.MAC][]cachedSpan)}
}

func (c *packfileSpanCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.spans = make(map[objects.MAC][]cachedSpan)
}

func (c *packfileSpanCache) lookup(packfile objects.MAC, offset uint64, length uint32) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	end := offset + uint64(length)
	for _, span := range c.spans[packfile] {
		spanEnd := span.offset + uint64(len(span.data))
		if span.offset <= offset && end <= spanEnd {
			start := offset - span.offset
			return span.data[start : start+uint64(length)], true
		}
	}
	return nil, false
}

func (c *packfileSpanCache) store(packfile objects.MAC, offset uint64, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.spans[packfile] = append(c.spans[packfile], cachedSpan{offset: offset, data: data})
}

func mergeFetchRanges(ranges []PackfileFetchRange, maxGap uint64) []PackfileFetchRange {
	if len(ranges) == 0 {
		return nil
	}

	sorted := make([]PackfileFetchRange, len(ranges))
	copy(sorted, ranges)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Offset < sorted[j].Offset })

	merged := []PackfileFetchRange{sorted[0]}
	for _, r := range sorted[1:] {
		last := &merged[len(merged)-1]
		lastEnd := last.Offset + uint64(last.Length)
		rEnd := r.Offset + uint64(r.Length)

		if r.Offset <= lastEnd+maxGap {
			if rEnd > lastEnd {
				last.Length = uint32(rEnd - last.Offset)
			}
			continue
		}

		merged = append(merged, r)
	}

	return merged
}

func (r *Repository) PrefetchPlan(plan FetchPlan, opts PrefetchPlanOptions) error {
	maxGap := opts.MaxGap
	if maxGap == 0 {
		maxGap = defaultPrefetchPlanMaxGap
	}
	concurrency := opts.Concurrency
	if concurrency <= 0 {
		concurrency = defaultPrefetchPlanConcurrency
	}

	type job struct {
		packfile objects.MAC
		span     PackfileFetchRange
	}

	var jobs []job
	for packfile, ranges := range plan {
		for _, span := range mergeFetchRanges(ranges, maxGap) {
			jobs = append(jobs, job{packfile: packfile, span: span})
		}
	}

	cache := r.getSpanCache()

	wg := new(errgroup.Group)
	wg.SetLimit(concurrency)
	for _, j := range jobs {
		wg.Go(func() error {
			data, err := r.fetchPaddedRange(j.packfile, j.span.Offset, j.span.Length)
			if err != nil {
				return err
			}
			cache.store(j.packfile, j.span.Offset, data)
			return nil
		})
	}
	return wg.Wait()
}
