package snapshot_test

import (
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

func generateSnapshot(t *testing.T) (*repository.Repository, *snapshot.Snapshot) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	snap := ptesting.GenerateSnapshot(t, repo, []ptesting.MockFile{
		ptesting.NewMockFile("dummy.txt", 0644, "hello"),
	})
	return repo, snap
}

func TestSnapshot(t *testing.T) {
	repo, snap := generateSnapshot(t)
	defer snap.Close()

	appCtx := snap.AppContext()
	require.NotNil(t, appCtx)
	require.NotNil(t, appCtx.GetCache())
	defer appCtx.GetCache().Close()

	events := appCtx.Events()
	require.NotNil(t, events)

	snapFs, err := snap.Filesystem()
	require.NoError(t, err)
	require.NotNil(t, snapFs)

	snap2, err := snapshot.Load(repo, snap.Header.Identifier)
	require.NoError(t, err)
	require.NotNil(t, snap2)

	require.Equal(t, snap.Header.Identifier, snap2.Header.Identifier)
	require.Equal(t, snap.Header.Timestamp.Truncate(time.Nanosecond), snap2.Header.Timestamp.Truncate(time.Nanosecond))
}
