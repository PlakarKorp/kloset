package snapshot_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

func TestBuilderCommitReturnsFailedCheckpoint(t *testing.T) {
	t.Setenv("PLAKAR_LOCKLESS", "true")
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	checkpointErr := errors.New("checkpoint refresh failed")
	tick := make(chan time.Time)
	builder, err := snapshot.Create(repo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{
		Name:         "failed-checkpoint",
		NoCheckpoint: true,
		StateRefresher: func(_ objects.MAC, final bool) error {
			if !final {
				return checkpointErr
			}
			return nil
		},
	})
	require.NoError(t, err)
	snapshot.StartCheckpointFlusherForTest(builder, tick)

	select {
	case tick <- time.Time{}:
	case <-time.After(time.Second):
		t.Fatal("checkpoint flusher did not start")
	}
	select {
	case <-repo.AppContext().Done():
		require.ErrorIs(t, context.Cause(repo.AppContext()), checkpointErr)
	case <-time.After(time.Second):
		t.Fatal("failed checkpoint did not cancel the backup")
	}

	committed := make(chan error, 1)
	go func() { committed <- builder.Commit() }()
	select {
	case err := <-committed:
		require.ErrorIs(t, err, checkpointErr)
	case <-time.After(time.Second):
		t.Fatal("Commit blocked after the checkpoint failed")
	}
	require.NoError(t, builder.Close())
}
