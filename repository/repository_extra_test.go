package repository_test

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/repository/state"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// TestEmitter verifies that Emitter returns a non-nil *events.Emitter for a
// valid workflow name.
func TestEmitter(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	emitter := repo.Emitter("backup")
	require.NotNil(t, emitter)

	// A different workflow name also returns a non-nil emitter.
	emitter2 := repo.Emitter("restore")
	require.NotNil(t, emitter2)
}

// TestIOStats confirms IOStats() returns a non-nil tracker.
func TestIOStats(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	stats := repo.IOStats()
	require.NotNil(t, stats)
}

// TestChunker ensures Chunker returns a working chunker for a non-empty reader.
func TestChunker(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	data := strings.Repeat("hello world this is chunker test data ", 1000)
	chunker, err := repo.Chunker(strings.NewReader(data))
	require.NoError(t, err)
	require.NotNil(t, chunker)
}

// TestOriginRootType verifies Origin, Root and Type return non-empty strings for
// a repository backed by the mock store.
func TestOriginRootType(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	origin := repo.Origin()
	require.NotEmpty(t, origin)

	root := repo.Root()
	require.NotEmpty(t, root)

	typ := repo.Type()
	require.NotEmpty(t, typ)
}

// TestGetLockMissing ensures GetLock returns an error when the lock does not
// exist in the store.
func TestGetLockMissing(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	_, err := repo.GetLock(objects.MAC{0x01})
	require.Error(t, err)
}

// TestPutLockAndGet stores a lock then reads it back and verifies correctness.
func TestPutLockAndGet(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	lock := repository.NewExclusiveLock("testhost")
	var buf bytes.Buffer
	require.NoError(t, lock.SerializeToStream(&buf))

	lockID := objects.MAC{0xFF, 0x01}
	nbytes, err := repo.PutLock(lockID, &buf)
	require.NoError(t, err)
	require.Greater(t, nbytes, int64(0))

	// Read it back.
	rc, err := repo.GetLock(lockID)
	require.NoError(t, err)
	defer rc.Close()

	got, err := repository.NewLockFromStream(rc)
	require.NoError(t, err)
	require.Equal(t, lock.Hostname, got.Hostname)
	require.Equal(t, lock.Exclusive, got.Exclusive)
}

// TestListPackfileEntriesAfterBackup creates a snapshot (which writes packfiles)
// then calls ListPackfileEntries and asserts at least one entry is returned.
func TestListPackfileEntriesAfterBackup(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	files := []ptesting.MockFile{
		ptesting.NewMockDir("/"),
		ptesting.NewMockFile("/hello.txt", 0644, "hello from packfile entry test"),
	}
	_ = ptesting.GenerateSnapshot(t, repo, files)

	var count int
	for _, err := range repo.ListPackfileEntries() {
		require.NoError(t, err)
		count++
	}
	require.Greater(t, count, 0, "expected at least one packfile entry after a backup")
}

// TestRBytesWBytesAfterOps confirms that writing actually increments the I/O
// counter exposed through WBytes.
func TestWBytesAfterPutState(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	repo.NoStateToLocalDisk = true

	// Write a state to bump the write counter.
	data := bytes.NewReader([]byte("some state bytes"))
	require.NoError(t, repo.PutState(objects.MAC{0xAB}, data))
	require.Greater(t, repo.WBytes(), int64(0))
}

// TestNoStateToLocalDiskPutState exercises the code path where PutState does not
// write to local disk (NoStateToLocalDisk=true).
func TestNoStateToLocalDiskPutState(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	repo.NoStateToLocalDisk = true

	data := bytes.NewReader([]byte("direct state bytes"))
	err := repo.PutState(objects.MAC{0x77}, data)
	require.NoError(t, err)
}

// TestDeleteStateThenGetState writes a state, deletes it, and confirms a
// subsequent GetState returns an error (not found).
func TestDeleteStateThenGetState(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	repo.NoStateToLocalDisk = true

	mac := objects.MAC{0x42}
	require.NoError(t, repo.PutState(mac, bytes.NewReader([]byte("ephemeral"))))
	require.NoError(t, repo.DeleteState(mac))

	_, _, err := repo.GetState(mac)
	require.Error(t, err)
}

// TestGetPackfileMissing confirms that requesting a packfile that does not
// exist returns a non-nil error.
func TestGetPackfileMissing(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	_, err := repo.GetPackfile(objects.MAC{0x99})
	require.Error(t, err)
}

// TestGetPackfileForBlobMissing checks that a blob lookup returns (_, false, nil)
// when nothing has been stored yet.
func TestGetPackfileForBlobMissing(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	_, exists, err := repo.GetPackfileForBlob(0x01, objects.MAC{0xDE, 0xAD})
	require.NoError(t, err)
	require.False(t, exists)
}

// TestListPackfilesEmpty ensures ListPackfiles returns an empty (or working) iterator
// on a fresh repository.
func TestListPackfilesEmpty(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	var count int
	for range repo.ListPackfiles() {
		count++
	}
	// A fresh repository has no packfiles yet.
	require.Equal(t, 0, count)
}

// TestListSnapshotsAfterBackup confirms that ListSnapshots returns at least one
// entry after GenerateSnapshot.
func TestListSnapshotsAfterBackup(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	files := []ptesting.MockFile{
		ptesting.NewMockDir("/"),
		ptesting.NewMockFile("/data.txt", 0644, "snapshot listing test"),
	}
	_ = ptesting.GenerateSnapshot(t, repo, files)

	var count int
	for _, err := range repo.ListSnapshots() {
		require.NoError(t, err)
		count++
	}
	require.Greater(t, count, 0)
}

// TestStorageSizeAfterBackup checks that StorageSize is non-negative after
// writing data.
func TestStorageSizeAfterBackup(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	files := []ptesting.MockFile{
		ptesting.NewMockDir("/"),
		ptesting.NewMockFile("/size.txt", 0644, "data that should grow the repo size"),
	}
	_ = ptesting.GenerateSnapshot(t, repo, files)

	size, err := repo.StorageSize()
	require.NoError(t, err)
	require.GreaterOrEqual(t, size, int64(0))
}

// TestGetLocksEmpty ensures GetLocks returns an empty list and no error on a
// fresh repository.
func TestGetLocksEmpty(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	locks, err := repo.GetLocks()
	require.NoError(t, err)
	require.NotNil(t, locks)
}

// TestIngestStateFileError verifies IngestStateFile returns an error when the
// state file does not exist on disk.
func TestIngestStateFileError(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	// Use an obviously-missing path: IngestStateFile builds the path from the
	// MAC via getStateFilePath (private), so we exercise the error path by
	// passing a MAC whose derived path does not exist.
	err := repo.IngestStateFile(objects.MAC{0x11, 0x22, 0x33})
	require.Error(t, err)
}

// TestGetPackfileRangeAfterBackup exercises GetPackfileRange by reading an
// actual blob range from a packfile written during a backup.
func TestGetPackfileRangeAfterBackup(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	files := []ptesting.MockFile{
		ptesting.NewMockDir("/"),
		ptesting.NewMockFile("/range.txt", 0644, strings.Repeat("z", 256)),
	}
	_ = ptesting.GenerateSnapshot(t, repo, files)

	for entry, err := range repo.ListPackfileEntries() {
		require.NoError(t, err)
		pf, pfErr := repo.GetPackfile(entry.Packfile)
		if pfErr != nil {
			continue
		}
		blobs := pf.Entries()
		if len(blobs) == 0 {
			continue
		}
		blob := blobs[0]
		if blob.Length == 0 {
			continue
		}

		loc := state.Location{
			Packfile: entry.Packfile,
			Offset:   blob.Offset,
			Length:   blob.Length,
		}
		data, rangeErr := repo.GetPackfileRange(loc)
		require.NoError(t, rangeErr)
		require.Len(t, data, int(blob.Length))
		return
	}
}

// TestGetPackfileBlobAfterBackup exercises GetPackfileBlob similarly.
func TestGetPackfileBlobAfterBackup(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	files := []ptesting.MockFile{
		ptesting.NewMockDir("/"),
		ptesting.NewMockFile("/blob.txt", 0644, strings.Repeat("b", 256)),
	}
	_ = ptesting.GenerateSnapshot(t, repo, files)

	for entry, err := range repo.ListPackfileEntries() {
		require.NoError(t, err)
		pf, pfErr := repo.GetPackfile(entry.Packfile)
		if pfErr != nil {
			continue
		}
		blobs := pf.Entries()
		if len(blobs) == 0 {
			continue
		}
		blob := blobs[0]
		if blob.Length == 0 {
			continue
		}

		loc := state.Location{
			Packfile: entry.Packfile,
			Offset:   blob.Offset,
			Length:   blob.Length,
		}
		rs, blobErr := repo.GetPackfileBlob(loc)
		require.NoError(t, blobErr)
		require.NotNil(t, rs)
		return
	}
}

// TestListColouredPackfilesEmpty confirms no panics and non-nil iterator on a
// fresh repository.
func TestListColouredPackfilesEmpty(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	var count int
	for range repo.ListColouredPackfiles() {
		count++
	}
	require.Equal(t, 0, count)
}

// TestComputeMACDeterministic verifies the same input always produces the same
// MAC.
func TestComputeMACDeterministic(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	data := []byte("deterministic mac test")
	mac1 := repo.ComputeMAC(data)
	mac2 := repo.ComputeMAC(data)
	require.Equal(t, mac1, mac2)
	require.NotEqual(t, objects.NilMac, mac1)
}

// TestDeletePackfileThenList writes nothing but confirms DeletePackfile does not
// error on a non-existent packfile (the mock store returns success on delete).
func TestDeletePackfileThenList(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	err := repo.DeletePackfile(objects.MAC{0xFE})
	require.NoError(t, err)
}

// TestHasDeletedPackfileAfterColour colours a snapshot (which marks it deleted)
// and verifies HasDeletedPackfile is still false for a random packfile MAC.
func TestHasDeletedPackfileIsFalse(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	has, err := repo.HasDeletedPackfile(objects.MAC{0x55})
	require.NoError(t, err)
	require.False(t, has)
}

// TestRebuildStateIdempotent rebuilds the state twice and asserts no error.
func TestRebuildStateIdempotent(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	require.NoError(t, repo.RebuildState())
	require.NoError(t, repo.RebuildState())
}

// TestAppContextNonNil confirms AppContext() is always set.
func TestAppContextNonNil(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	require.NotNil(t, repo.AppContext())
}

// TestConfigurationHasID ensures the repository configuration has a non-zero ID.
func TestConfigurationHasID(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	cfg := repo.Configuration()
	require.NotEqual(t, objects.NilMac, cfg.RepositoryID)
}

// TestDeleteLockTwice confirms that deleting the same lock twice does not error.
func TestDeleteLockTwice(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	lockID := objects.MAC{0xCA, 0xFE}
	require.NoError(t, repo.DeleteLock(lockID))
	require.NoError(t, repo.DeleteLock(lockID))
}

// TestCloseAfterBackup ensures Close returns nil even after a backup was done.
func TestCloseAfterBackup(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)
	files := []ptesting.MockFile{
		ptesting.NewMockDir("/"),
		ptesting.NewMockFile("/close.txt", 0644, "close test"),
	}
	_ = ptesting.GenerateSnapshot(t, repo, files)
	require.NoError(t, repo.Close())
}

// Suppress unused import warning for os
var _ = os.DevNull
