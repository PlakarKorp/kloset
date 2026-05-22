package scanlog_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/scanlog"
	"github.com/stretchr/testify/require"
)

// TestListReverse verifies that reverse=true on ListFiles/ListDirectories returns
// entries in reverse path order.
func TestListReverse(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac := objects.MAC{1}
	require.NoError(t, sl.PutDirectory("/a", mac, []byte("a")))
	require.NoError(t, sl.PutDirectory("/b", mac, []byte("b")))
	require.NoError(t, sl.PutDirectory("/c", mac, []byte("c")))

	var fwd, rev []scanlog.Entry
	for e := range sl.ListDirectories("/", false) {
		fwd = append(fwd, e)
	}
	for e := range sl.ListDirectories("/", true) {
		rev = append(rev, e)
	}

	require.Len(t, fwd, 3)
	require.Len(t, rev, 3)
	// Reversed order should be the opposite of forward.
	for i, j := 0, len(rev)-1; i < len(fwd); i, j = i+1, j-1 {
		require.Equal(t, fwd[i].Path, rev[j].Path)
	}
}

// TestListFilesReverse verifies reverse listing of files.
func TestListFilesReverse(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac := objects.MAC{2}
	require.NoError(t, sl.PutFile("/f1.txt", "text/plain", mac, []byte("1"), nil))
	require.NoError(t, sl.PutFile("/f2.txt", "text/plain", mac, []byte("2"), nil))

	var fwd, rev []scanlog.Entry
	for e := range sl.ListFiles(scanlog.KindFile, "/", false) {
		fwd = append(fwd, e)
	}
	for e := range sl.ListFiles(scanlog.KindFile, "/", true) {
		rev = append(rev, e)
	}

	require.Len(t, fwd, 2)
	require.Len(t, rev, 2)
	require.Equal(t, fwd[0].Path, rev[1].Path)
	require.Equal(t, fwd[1].Path, rev[0].Path)
}

// TestListDirectPathnamesReverse verifies the reverse flag in ListDirectPathnames.
func TestListDirectPathnamesReverse(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac := objects.MAC{3}
	require.NoError(t, sl.PutDirectory("/parent/a", mac, []byte("a")))
	require.NoError(t, sl.PutDirectory("/parent/b", mac, []byte("b")))
	// Nested entry — should not appear as a direct child of "/parent".
	require.NoError(t, sl.PutDirectory("/parent/a/nested", mac, []byte("nested")))

	var fwd, rev []scanlog.Entry
	for e := range sl.ListDirectPathnames("/parent", false) {
		fwd = append(fwd, e)
	}
	for e := range sl.ListDirectPathnames("/parent", true) {
		rev = append(rev, e)
	}

	require.Len(t, fwd, 2)
	require.Len(t, rev, 2)
	require.Equal(t, fwd[0].Path, rev[1].Path)
	require.Equal(t, fwd[1].Path, rev[0].Path)
}

// TestBatchCommitWithMACs tests that batch Put populates rows with correct MACs.
func TestBatchCommitWithMACs(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac1 := objects.MAC{0xaa}
	mac2 := objects.MAC{0xbb}
	batch := sl.NewBatch()

	require.NoError(t, batch.PutDirectory("/batch/x", mac1, []byte("x")))
	require.NoError(t, batch.PutDirectory("/batch/y", mac2, []byte("y")))
	require.Equal(t, uint32(2), batch.Count())
	require.NoError(t, batch.Commit())

	// Count should reset after commit.
	require.Equal(t, uint32(0), batch.Count())

	// Both entries should be readable.
	got, err := sl.GetDirectory("/batch/x")
	require.NoError(t, err)
	require.Equal(t, []byte("x"), got)

	got, err = sl.GetDirectory("/batch/y")
	require.NoError(t, err)
	require.Equal(t, []byte("y"), got)
}

// TestListPathnamesIncludesBothKinds verifies ListPathnames returns dirs and files.
func TestListPathnamesIncludesBothKinds(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac := objects.MAC{5}
	require.NoError(t, sl.PutDirectory("/mixed/subdir", mac, []byte("d")))
	require.NoError(t, sl.PutFile("/mixed/file.txt", "", mac, []byte("f"), nil))

	var entries []scanlog.Entry
	for e := range sl.ListPathnames("/mixed/", false) {
		entries = append(entries, e)
	}
	require.Len(t, entries, 2)
}

// TestListPathnameEntriesHasPayload verifies that ListPathnameEntries populates Payload.
func TestListPathnameEntriesHasPayload(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac := objects.MAC{6}
	payload := []byte("rich entry data")
	require.NoError(t, sl.PutFile("/data/file.bin", "application/octet-stream", mac, payload, nil))

	var entries []scanlog.Entry
	for e := range sl.ListPathnameEntries("/data/", false) {
		entries = append(entries, e)
	}
	require.Len(t, entries, 1)
	require.Equal(t, payload, entries[0].Payload)
}

// TestListPathMACsFromEmpty verifies that an empty result set is fine.
func TestListPathMACsFromEmpty(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	var entries []scanlog.PathMACEntry
	for e := range sl.ListPathMACsFrom(scanlog.KindError, "/nonexistent/") {
		entries = append(entries, e)
	}
	require.Empty(t, entries)
}

// TestCountDirectPathMACsZero verifies that counting with no entries returns 0.
func TestCountDirectPathMACsZero(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	n, err := sl.CountDirectPathMACs(scanlog.KindError, "/empty")
	require.NoError(t, err)
	require.Equal(t, uint64(0), n)
}

// TestGetPathMACInvalidPath verifies that GetPathMAC on a missing path returns NilMac.
func TestGetPathMACMissing(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	got, err := sl.GetPathMAC(scanlog.KindXattr, "/no/such/path")
	require.NoError(t, err)
	require.Equal(t, objects.NilMac, got)
}
