package repository_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// TestDeleteSnapshotsWritesASingleState is the reason DeleteSnapshots exists:
// deleting N snapshots one by one derives and writes N delta states, while the
// batch colours every snapshot into one delta state and writes it once.
func TestDeleteSnapshotsWritesASingleState(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	ids := make([]objects.MAC, 0, 3)
	for _, name := range []string{"/one.txt", "/two.txt", "/three.txt"} {
		snap := ptesting.GenerateSnapshot(t, repo, []ptesting.MockFile{
			ptesting.NewMockDir("/"),
			ptesting.NewMockFile(name, 0644, "content of "+name),
		})
		require.NotNil(t, snap)
		ids = append(ids, snap.Header.Identifier)
	}

	before, err := repo.GetStates()
	require.NoError(t, err)

	require.NoError(t, repo.DeleteSnapshots(ids))

	after, err := repo.GetStates()
	require.NoError(t, err)
	require.Len(t, after, len(before)+1, "the batch must write exactly one state")

	require.NoError(t, repo.RebuildState())

	deleted := make(map[objects.MAC]bool)
	for id := range repo.ListDeletedSnapShots() {
		deleted[id] = true
	}
	for _, id := range ids {
		require.True(t, deleted[id], "snapshot %x must be deleted", id)
	}
}

// TestDeleteSnapshotsEmptyWritesNothing keeps an empty list from writing a
// state that colours nothing. The control plane builds the list from its own
// records, so an empty list is a normal outcome, not an error.
func TestDeleteSnapshotsEmptyWritesNothing(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	before, err := repo.GetStates()
	require.NoError(t, err)

	require.NoError(t, repo.DeleteSnapshots(nil))

	after, err := repo.GetStates()
	require.NoError(t, err)
	require.Len(t, after, len(before))
}

// TestDeleteSnapshotsUnknownIDIsAccepted pins the behaviour the control plane
// relies on: colouring a resource is an unconditional tombstone write, so an
// identifier the repository never held is accepted and deletes nothing. A
// stale list therefore cannot fail a batch.
func TestDeleteSnapshotsUnknownIDIsAccepted(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	snap := ptesting.GenerateSnapshot(t, repo, []ptesting.MockFile{
		ptesting.NewMockDir("/"),
		ptesting.NewMockFile("/kept.txt", 0644, "kept"),
	})
	require.NotNil(t, snap)

	unknown := objects.MAC{0xDE, 0xAD, 0xBE, 0xEF}
	require.NoError(t, repo.DeleteSnapshots([]objects.MAC{unknown}))
	require.NoError(t, repo.RebuildState())

	live := 0
	for range repo.ListSnapshots() {
		live++
	}
	require.Equal(t, 1, live, "the real snapshot must survive")
}
