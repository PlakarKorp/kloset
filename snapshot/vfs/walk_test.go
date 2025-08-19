package vfs_test

import (
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/snapshot/vfs"
	"github.com/stretchr/testify/require"
)

func TestWalk(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	// search for the correct filepath as the path was mkdir temp we cannot hardcode it
	var filepath string
	for pathname, err := range fs.Pathnames() {
		require.NoError(t, err)
		if strings.Contains(pathname, "subdir") {
			filepath = pathname
			break
		}
	}
	require.NotEmpty(t, filepath)

	err = fs.WalkDir(filepath, func(path string, d *vfs.Entry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && path == filepath {
			return nil
		}

		if n := d.Name(); n != "dummy.txt" && n != "big" {
			t.Error("unexpected file", n)
		}

		return nil
	})
	require.NoError(t, err)
}
