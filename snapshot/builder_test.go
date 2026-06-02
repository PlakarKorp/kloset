package snapshot_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

func TestBuilderOptionsDatasetAndDataClasses(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	builder, err := snapshot.Create(repo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{
		Name:         "dataset-test",
		Dataset:      "a1b2c3d4-0000-0000-0000-000000000001",
		DataClasses:  []string{"pii", "phi"},
		NoCheckpoint: true,
	})
	require.NoError(t, err)
	require.NotNil(t, builder)
	err = builder.Close()
	require.NoError(t, err)

	require.Equal(t, "a1b2c3d4-0000-0000-0000-000000000001", builder.Header.Dataset)
	require.Equal(t, []string{"pii", "phi"}, builder.Header.DataClasses)
}

func TestBuilderOptionsDataClassesDefault(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	builder, err := snapshot.Create(repo, repository.DefaultType, "", objects.NilMac, &snapshot.BuilderOptions{
		Name:         "dataset-default-test",
		NoCheckpoint: true,
	})
	require.NoError(t, err)
	require.NotNil(t, builder)
	err = builder.Close()
	require.NoError(t, err)

	require.Equal(t, "", builder.Header.Dataset)
	require.NotNil(t, builder.Header.DataClasses)
	require.Empty(t, builder.Header.DataClasses)
}
