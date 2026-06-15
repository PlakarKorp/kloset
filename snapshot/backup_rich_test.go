package snapshot_test

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// generateRichBackup builds a snapshot whose import stream carries an xattr, a
// recorded error, and a symlink in addition to ordinary files and directories.
// This drives the recordXattr / recordError / symlink branches of processRecord
// and the corresponding buildXattrIndex / buildErrorIndex passes.
func generateRichBackup(t *testing.T) *snapshot.Snapshot {
	t.Helper()
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	return ptesting.GenerateSnapshot(t, repo, nil,
		ptesting.WithGenerator(func(ch chan<- *connectors.Record) {
			ch <- connectors.NewRecord("/", "", objects.FileInfo{
				Lname: "/", Lmode: os.ModeDir | 0755,
			}, nil, nil)
			ch <- connectors.NewRecord("/data", "", objects.FileInfo{
				Lname: "data", Lmode: os.ModeDir | 0755,
			}, nil, nil)
			ch <- connectors.NewRecord("/data/file.txt", "", objects.FileInfo{
				Lname: "file.txt", Lmode: 0644, Lsize: int64(len("payload here")),
			}, []string{"user.note"}, func() (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("payload here")), nil
			})
			ch <- connectors.NewXattr("/data/file.txt", "user.note", objects.AttributeExtended,
				func() (io.ReadCloser, error) {
					return io.NopCloser(strings.NewReader("note value")), nil
				})
			ch <- connectors.NewRecord("/data/link", "file.txt", objects.FileInfo{
				Lname: "link", Lmode: os.ModeSymlink | 0777,
			}, nil, nil)
			ch <- connectors.NewError("/data/denied", os.ErrPermission)
		}),
	)
}

// TestRichBackupCheck backs up a tree with an xattr, an error and a symlink,
// then runs Check over it so the corresponding index/walk branches execute.
func TestRichBackupCheck(t *testing.T) {
	snap := generateRichBackup(t)
	defer snap.Close()

	require.NoError(t, snap.Check("/", &snapshot.CheckOptions{}))
}

// TestRichBackupErrorsAndXattr confirms the recorded error and xattr survive
// into the snapshot's filesystem view.
func TestRichBackupErrorsAndXattr(t *testing.T) {
	snap := generateRichBackup(t)
	defer snap.Close()

	fs, err := snap.Filesystem()
	require.NoError(t, err)

	var errCount int
	for item, err := range fs.Errors("/") {
		require.NoError(t, err)
		require.NotEmpty(t, item.Name)
		errCount++
	}
	require.Greater(t, errCount, 0)

	e, err := fs.GetEntry("/data/file.txt")
	require.NoError(t, err)
	rd, err := e.Xattr(fs, "user.note")
	require.NoError(t, err)
	data, err := io.ReadAll(rd)
	require.NoError(t, err)
	require.Equal(t, "note value", string(data))
}
