package scanlog_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/scanlog"
	"github.com/stretchr/testify/require"
)

// TestGetDirectoryMissing covers the sql.ErrNoRows path in get: a missing entry
// yields (nil, nil).
func TestGetDirectoryMissing(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	got, err := sl.GetDirectory("/nope")
	require.NoError(t, err)
	require.Nil(t, got)

	got, err = sl.GetFile("/nope")
	require.NoError(t, err)
	require.Nil(t, got)
}

// TestCommitEmptyBatch covers the early return in Commit when no records are
// queued.
func TestCommitEmptyBatch(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	b := sl.NewBatch()
	require.Equal(t, uint32(0), b.Count())
	require.NoError(t, b.Commit())
}

// TestListEarlyStop covers the yield-returns-false branches of the list
// iterators by breaking after the first element.
func TestListEarlyStop(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac := objects.MAC{1}
	require.NoError(t, sl.PutDirectory("/x/a", mac, []byte("a")))
	require.NoError(t, sl.PutDirectory("/x/b", mac, []byte("b")))

	// list() with withEntry=false (ListDirectories).
	count := 0
	for range sl.ListDirectories("/x/", false) {
		count++
		break
	}
	require.Equal(t, 1, count)

	// list() with withEntry=true (ListPathnameEntries) — also exercises the
	// payload Decode path.
	count = 0
	for e := range sl.ListPathnameEntries("/x/", false) {
		require.NotNil(t, e.Payload)
		count++
		break
	}
	require.Equal(t, 1, count)
}

// TestListDirectPathnamesWithSummaryAndStop covers the summary-decode branch and
// the early-stop branch of ListDirectPathnames.
func TestListDirectPathnamesWithSummaryAndStop(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac := objects.MAC{2}
	// A file with a non-nil summary triggers the summary Decode branch.
	require.NoError(t, sl.PutFile("/parent/file1", "text/plain", mac, []byte("p1"), []byte("summary1")))
	require.NoError(t, sl.PutFile("/parent/file2", "text/plain", mac, []byte("p2"), []byte("summary2")))

	// Full iteration to hit the summary path on at least one entry.
	var withSummary int
	for e := range sl.ListDirectPathnames("/parent", false) {
		if e.Summary != nil {
			withSummary++
		}
	}
	require.Equal(t, 2, withSummary)

	// Early stop after the first entry.
	count := 0
	for range sl.ListDirectPathnames("/parent", false) {
		count++
		break
	}
	require.Equal(t, 1, count)
}

// TestListPathMACsFromWithDataAndStop covers ListPathMACsFrom yielding real rows
// and the early-stop branch.
func TestListPathMACsFromWithDataAndStop(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	require.NoError(t, sl.PutPathMAC(scanlog.KindError, "/e/one", objects.MAC{0x11}))
	require.NoError(t, sl.PutPathMAC(scanlog.KindError, "/e/two", objects.MAC{0x22}))

	var all []scanlog.PathMACEntry
	for e := range sl.ListPathMACsFrom(scanlog.KindError, "/e/") {
		all = append(all, e)
	}
	require.Len(t, all, 2)

	count := 0
	for range sl.ListPathMACsFrom(scanlog.KindError, "/e/") {
		count++
		break
	}
	require.Equal(t, 1, count)
}

// TestGetPathMACFound covers the success (non-error, found) path of GetPathMAC.
func TestGetPathMACFound(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	want := objects.MAC{0x33}
	require.NoError(t, sl.PutPathMAC(scanlog.KindXattr, "/x/attr", want))
	got, err := sl.GetPathMAC(scanlog.KindXattr, "/x/attr")
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// TestCountDirectPathMACsNonZero covers CountDirectPathMACs returning a count.
func TestCountDirectPathMACsNonZero(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	require.NoError(t, sl.PutPathMAC(scanlog.KindError, "/d/c1", objects.MAC{1}))
	require.NoError(t, sl.PutPathMAC(scanlog.KindError, "/d/c2", objects.MAC{2}))

	n, err := sl.CountDirectPathMACs(scanlog.KindError, "/d")
	require.NoError(t, err)
	require.Equal(t, uint64(2), n)
}

// TestQueryErrorsAfterClose closes the underlying database and then exercises
// the query-failure branches across the read/list methods, which all bail out
// early once the DB connection is gone.
func TestQueryErrorsAfterClose(t *testing.T) {
	sl := newScanLog(t)
	// Queue a batch with one record before closing, so Commit reaches the
	// Begin() error branch rather than the empty-batch early return.
	b := sl.NewBatch()
	require.NoError(t, b.PutDirectory("/q/d", objects.MAC{1}, []byte("d")))
	require.NoError(t, sl.Close())

	// get(): non-ErrNoRows error path.
	_, err := sl.GetDirectory("/q/d")
	require.Error(t, err)

	// GetPathMAC(): non-ErrNoRows error path.
	_, err = sl.GetPathMAC(scanlog.KindError, "/q/d")
	require.Error(t, err)

	// CountDirectPathMACs(): QueryRow error path.
	_, err = sl.CountDirectPathMACs(scanlog.KindError, "/q")
	require.Error(t, err)

	// Commit(): Begin() error path.
	require.Error(t, b.Commit())

	// list iterators: Query() error path => no entries yielded, no panic.
	for range sl.ListDirectories("/q/", false) {
		t.Fatal("expected no entries from a closed db")
	}
	for range sl.ListDirectPathnames("/q", false) {
		t.Fatal("expected no entries from a closed db")
	}
	for range sl.ListPathMACsFrom(scanlog.KindError, "/q/") {
		t.Fatal("expected no entries from a closed db")
	}
}
