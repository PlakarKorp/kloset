package scanlog_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/scanlog"
	"github.com/stretchr/testify/require"
)

func newScanLog(t *testing.T) *scanlog.ScanLog {
	t.Helper()
	dir := t.TempDir()
	sl, err := scanlog.New(dir)
	require.NoError(t, err)
	return sl
}

func TestNew(t *testing.T) {
	sl := newScanLog(t)
	require.NotNil(t, sl)
	require.NoError(t, sl.Close())
}

func TestPutAndGetDirectory(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac := objects.MAC{1, 2, 3}
	payload := []byte("directory payload")

	require.NoError(t, sl.PutDirectory("/foo/bar", mac, payload))

	got, err := sl.GetDirectory("/foo/bar")
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestPutAndGetFile(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac := objects.MAC{4, 5, 6}
	payload := []byte("file payload")
	summary := []byte("file summary")

	require.NoError(t, sl.PutFile("/foo/file.txt", "text/plain", mac, payload, summary))

	got, err := sl.GetFile("/foo/file.txt")
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestGetMissingDirectory(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	got, err := sl.GetDirectory("/does/not/exist")
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestGetMissingFile(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	got, err := sl.GetFile("/does/not/exist.txt")
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestListDirectories(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac := objects.MAC{10}
	require.NoError(t, sl.PutDirectory("/alpha", mac, []byte("a")))
	require.NoError(t, sl.PutDirectory("/beta", mac, []byte("b")))
	require.NoError(t, sl.PutDirectory("/gamma", mac, []byte("c")))

	var dirs []string
	for entry := range sl.ListDirectories("/", false) {
		dirs = append(dirs, entry.Path)
	}
	require.GreaterOrEqual(t, len(dirs), 3)
}

func TestListFiles(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac := objects.MAC{20}
	require.NoError(t, sl.PutFile("/a.txt", "text/plain", mac, []byte("a"), nil))
	require.NoError(t, sl.PutFile("/b.txt", "text/plain", mac, []byte("b"), nil))

	var files []string
	for entry := range sl.ListFiles(0, "/", false) {
		files = append(files, entry.Path)
	}
	require.GreaterOrEqual(t, len(files), 2)
}

func TestListPathnames(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac := objects.MAC{30}
	require.NoError(t, sl.PutDirectory("/dir1", mac, []byte("d")))
	require.NoError(t, sl.PutFile("/dir1/file.txt", "text/plain", mac, []byte("f"), nil))

	var paths []string
	for entry := range sl.ListPathnames("/dir1", false) {
		paths = append(paths, entry.Path)
	}
	require.GreaterOrEqual(t, len(paths), 2)
}

func TestListPathnameEntries(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac := objects.MAC{40}
	payload := []byte("entry payload")
	require.NoError(t, sl.PutFile("/myfile.txt", "text/plain", mac, payload, nil))

	for entry := range sl.ListPathnameEntries("/myfile.txt", false) {
		require.Equal(t, "/myfile.txt", entry.Path)
		require.Equal(t, payload, entry.Payload)
	}
}

func TestListDirectPathnames(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac := objects.MAC{50}
	require.NoError(t, sl.PutDirectory("/parent", mac, []byte("parent")))
	require.NoError(t, sl.PutDirectory("/parent/child1", mac, []byte("child1")))
	require.NoError(t, sl.PutFile("/parent/file.txt", "text/plain", mac, []byte("file"), []byte("summary")))
	require.NoError(t, sl.PutDirectory("/parent/child1/grandchild", mac, []byte("grandchild")))

	var names []string
	for entry := range sl.ListDirectPathnames("/parent", false) {
		names = append(names, entry.Path)
	}
	// Only direct children: child1 and file.txt
	require.GreaterOrEqual(t, len(names), 2)
	for _, n := range names {
		require.NotEqual(t, "/parent/child1/grandchild", n)
	}
}

func TestPutPathMACAndGet(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac := objects.MAC{60, 61, 62}
	require.NoError(t, sl.PutPathMAC(scanlog.KindError, "/foo/bar", mac))

	got, err := sl.GetPathMAC(scanlog.KindError, "/foo/bar")
	require.NoError(t, err)
	require.Equal(t, mac, got)
}

func TestGetMissingPathMAC(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	got, err := sl.GetPathMAC(scanlog.KindXattr, "/does/not/exist")
	require.NoError(t, err)
	require.Equal(t, objects.NilMac, got)
}

func TestListPathMACsFrom(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac1 := objects.MAC{70}
	mac2 := objects.MAC{71}
	require.NoError(t, sl.PutPathMAC(scanlog.KindXattr, "/xattr/a", mac1))
	require.NoError(t, sl.PutPathMAC(scanlog.KindXattr, "/xattr/b", mac2))

	var entries []scanlog.PathMACEntry
	for e := range sl.ListPathMACsFrom(scanlog.KindXattr, "/xattr/") {
		entries = append(entries, e)
	}
	require.Len(t, entries, 2)
}

func TestCountDirectPathMACs(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac := objects.MAC{80}
	require.NoError(t, sl.PutPathMAC(scanlog.KindError, "/errors/a", mac))
	require.NoError(t, sl.PutPathMAC(scanlog.KindError, "/errors/b", mac))
	require.NoError(t, sl.PutPathMAC(scanlog.KindError, "/errors/a/nested", mac))

	count, err := sl.CountDirectPathMACs(scanlog.KindError, "/errors")
	require.NoError(t, err)
	require.Equal(t, uint64(2), count)
}

func TestBatchCommit(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac := objects.MAC{90}
	batch := sl.NewBatch()
	require.NotNil(t, batch)

	require.NoError(t, batch.PutDirectory("/batch/dir", mac, []byte("d")))
	require.NoError(t, batch.PutFile("/batch/file.txt", "text/plain", mac, []byte("f"), []byte("s")))
	require.Equal(t, uint32(2), batch.Count())

	require.NoError(t, batch.Commit())

	// Verify committed data is readable
	got, err := sl.GetDirectory("/batch/dir")
	require.NoError(t, err)
	require.Equal(t, []byte("d"), got)

	got, err = sl.GetFile("/batch/file.txt")
	require.NoError(t, err)
	require.Equal(t, []byte("f"), got)
}

func TestBatchCommitEmpty(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	batch := sl.NewBatch()
	require.Equal(t, uint32(0), batch.Count())
	require.NoError(t, batch.Commit())
}

func TestBatchCommitMultipleTimes(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac := objects.MAC{91}
	batch := sl.NewBatch()

	require.NoError(t, batch.PutFile("/batch2/a.txt", "text/plain", mac, []byte("a"), nil))
	require.NoError(t, batch.Commit())
	require.Equal(t, uint32(0), batch.Count())

	require.NoError(t, batch.PutFile("/batch2/b.txt", "text/plain", mac, []byte("b"), nil))
	require.NoError(t, batch.Commit())

	got, err := sl.GetFile("/batch2/b.txt")
	require.NoError(t, err)
	require.Equal(t, []byte("b"), got)
}

func TestSetMaxConn(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()
	// Should not panic
	sl.SetMaxConn(4)
}

func TestPutFileWithNilSummary(t *testing.T) {
	sl := newScanLog(t)
	defer sl.Close()

	mac := objects.MAC{100}
	require.NoError(t, sl.PutFile("/nosummary.txt", "text/plain", mac, []byte("data"), nil))

	got, err := sl.GetFile("/nosummary.txt")
	require.NoError(t, err)
	require.Equal(t, []byte("data"), got)
}
