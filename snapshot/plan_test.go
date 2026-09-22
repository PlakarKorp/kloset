package snapshot_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlanPackfileFetches(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	plan, err := snap.PlanPackfileFetches("/", 1)
	require.NoError(t, err)
	require.NotEmpty(t, plan)

	var total int
	for _, ranges := range plan {
		total += len(ranges)
	}
	require.Greater(t, total, 0)
}

func TestPlanPackfileFetchesMissingPath(t *testing.T) {
	_, snap := generateSnapshotWithFiles(t)
	defer snap.Close()

	_, err := snap.PlanPackfileFetches("/no/such/path", 1)
	require.Error(t, err)
}
