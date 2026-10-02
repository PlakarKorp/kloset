package vfs

import "golang.org/x/sync/semaphore"

// SetReadaheadBudget replaces the read-ahead budget for readers started after
// it, and returns a function that restores the previous one.
func SetReadaheadBudget(windows int64) func() {
	prev := readaheadBudget
	readaheadBudget = semaphore.NewWeighted(windows)
	return func() { readaheadBudget = prev }
}
