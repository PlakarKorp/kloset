package exporter

import (
	"context"
	"io"
	"testing"

	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/stretchr/testify/require"
)

type MockedExporter struct{}

func (m MockedExporter) Root(ctx context.Context) (string, error) {
	return "", nil
}

func (m MockedExporter) CreateDirectory(ctx context.Context, pathname string) error {
	return nil
}

func (m MockedExporter) StoreFile(ctx context.Context, pathname string, fp io.Reader, size int64) error {
	return nil
}

func (m MockedExporter) SetPermissions(ctx context.Context, pathname string, fileinfo *objects.FileInfo) error {
	return nil
}

func (m MockedExporter) CreateLink(ctx context.Context, oldname string, newname string, ltype LinkType) error {
	return nil
}

func (m MockedExporter) Close(ctx context.Context) error {
	return nil
}

func TestBackends(t *testing.T) {
	// Setup: Register some backends
	Register("fs1", 0, func(appCtx context.Context, opts *Options, name string, config map[string]string) (Exporter, error) {
		return nil, nil
	})
	Register("s33", 0, func(appCtx context.Context, opts *Options, name string, config map[string]string) (Exporter, error) {
		return nil, nil
	})

	// Test: Retrieve the list of registered backends
	expectedBackends := []string{"fs1", "s33"}
	actualBackends := Backends()

	// Assert: Check if the actual backends match the expected
	require.ElementsMatch(t, expectedBackends, actualBackends)
}

func TestNewExporter(t *testing.T) {
	// Setup: Register some backends
	Register("fs", 0, func(appCtx context.Context, opts *Options, name string, config map[string]string) (Exporter, error) {
		return MockedExporter{}, nil
	})
	Register("s3", 0, func(appCtx context.Context, opts *Options, name string, config map[string]string) (Exporter, error) {
		return MockedExporter{}, nil
	})

	tests := []struct {
		location        string
		expectedError   string
		expectedBackend string
	}{
		{location: "/", expectedError: "", expectedBackend: "fs"},
		{location: "fs://some/path", expectedError: "", expectedBackend: "fs"},
		{location: "s3://bucket/path", expectedError: "", expectedBackend: "s3"},
		{location: "http://unsupported", expectedError: "unsupported exporter protocol", expectedBackend: ""},
	}

	for _, test := range tests {
		t.Run(test.location, func(t *testing.T) {
			appCtx := kcontext.NewKContext()

			exporter, err := NewExporter(appCtx, map[string]string{"location": test.location})

			if test.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.expectedError)
			} else {
				require.NoError(t, err)
				require.NotNil(t, exporter)
			}
		})
	}
}
