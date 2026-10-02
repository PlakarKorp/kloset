package vfs

import "testing"

// SetDirpackBatchSize overrides the payload budget of WalkDirpack batches for
// the duration of t.
func SetDirpackBatchSize(t *testing.T, size int64) {
	old := dirpackBatchSize
	dirpackBatchSize = size
	t.Cleanup(func() { dirpackBatchSize = old })
}
