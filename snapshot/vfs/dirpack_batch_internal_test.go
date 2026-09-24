package vfs

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func jobsOf(n int) []prefetchJob {
	jobs := make([]prefetchJob, n)
	for i := range jobs {
		jobs[i] = prefetchJob{path: fmt.Sprintf("/dir%03d", i)}
	}
	return jobs
}

func TestCollectGroupsWaitingJobs(t *testing.T) {
	want := jobsOf(2*dirpackBatchSize + 5)

	p := &dirpackPrefetcher{
		jobs:    make(chan prefetchJob, len(want)),
		batches: make(chan []prefetchJob),
	}
	for _, j := range want {
		p.jobs <- j
	}
	close(p.jobs)

	go p.collect()

	var got []prefetchJob
	var sizes []int
	for batch := range p.batches {
		require.LessOrEqual(t, len(batch), dirpackBatchSize)
		sizes = append(sizes, len(batch))
		got = append(got, batch...)
	}

	require.Equal(t, want, got)
	require.Equal(t, []int{dirpackBatchSize, dirpackBatchSize, 5}, sizes)
}

func TestCollectDoesNotWaitForAFullBatch(t *testing.T) {
	p := &dirpackPrefetcher{
		jobs:    make(chan prefetchJob, 1),
		batches: make(chan []prefetchJob),
	}
	go p.collect()
	defer func() {
		close(p.jobs)
		for range p.batches {
		}
	}()

	p.jobs <- prefetchJob{path: "/only"}
	select {
	case batch := <-p.batches:
		require.Equal(t, []prefetchJob{{path: "/only"}}, batch)
	case <-time.After(5 * time.Second):
		t.Fatal("a lone job was held back")
	}
}
