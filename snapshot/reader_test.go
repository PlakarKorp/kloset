package snapshot_test

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewReader(t *testing.T) {
	_, snap := generateSnapshot(t)
	defer snap.Close()

	// search for the correct filepath as the path was mkdir temp we cannot hardcode it
	var filepath string
	fs, err := snap.Filesystem()
	require.NoError(t, err)
	for pathname, err := range fs.Pathnames() {
		require.NoError(t, err)
		if strings.Contains(pathname, "dummy.txt") {
			filepath = pathname
		}
	}
	require.NotEmpty(t, filepath)

	rd, err := snap.NewReader(filepath)
	require.NoError(t, err)
	content, err := io.ReadAll(rd)
	require.NoError(t, err)
	require.Equal(t, "hello", string(content))
}
