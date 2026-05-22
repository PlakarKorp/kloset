package fileinfo_test

import (
	"io/fs"
	"testing"

	"github.com/PlakarKorp/kloset/testing/fileinfo"
	"github.com/stretchr/testify/require"
)

// TestMockFileInfoDefaults checks that New() returns sensible defaults.
func TestMockFileInfoDefaults(t *testing.T) {
	fi := fileinfo.New()

	require.Equal(t, "test", fi.Name())
	require.Equal(t, int64(100), fi.Size())
	require.Equal(t, fs.FileMode(0644), fi.Mode())
	require.False(t, fi.IsDir())
	require.False(t, fi.ModTime().IsZero())
	require.NotNil(t, fi.Sys())
}

// TestMockFileInfoImplementsInterface ensures MockFileInfo satisfies fs.FileInfo.
func TestMockFileInfoImplementsInterface(t *testing.T) {
	var fi fs.FileInfo = fileinfo.New()
	require.NotNil(t, fi)
}
