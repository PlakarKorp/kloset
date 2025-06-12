package classifier_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/classifier"
	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
	_ "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

func TestNewClassifier(t *testing.T) {
	// Create a new classifier
	ctx := kcontext.NewKContext()
	cf, err := classifier.NewClassifier(ctx)
	require.NoError(t, err)
	require.NotNil(t, cf)

	// Test Close
	err = cf.Close()
	require.NoError(t, err)
}

func TestProcessor(t *testing.T) {
	// Create a new classifier
	ctx := kcontext.NewKContext()
	cf, err := classifier.NewClassifier(ctx)
	require.NoError(t, err)
	require.NotNil(t, cf)
	defer cf.Close()

	// Get a processor
	pathname := "test/path"
	processor := cf.Processor(pathname)
	require.NotNil(t, processor)

	// Test File classification
	fileEntry := &vfs.Entry{}
	classifications := processor.File(fileEntry)
	require.Len(t, classifications, 1)
	require.Equal(t, "mock", classifications[0].Analyzer)
	require.Equal(t, []string{"test-file-class"}, classifications[0].Classes)

	// Test Directory classification
	dirEntry := &vfs.Entry{}
	classifications = processor.Directory(dirEntry)
	require.Len(t, classifications, 1)
	require.Equal(t, "mock", classifications[0].Analyzer)
	require.Equal(t, []string{"test-dir-class"}, classifications[0].Classes)

	// Test Write
	processor.Write([]byte("test data"))

	// Test Finalize
	classifications = processor.Finalize()
	require.Len(t, classifications, 1)
	require.Equal(t, "mock", classifications[0].Analyzer)
	require.Equal(t, []string{"test-final-class"}, classifications[0].Classes)
}
