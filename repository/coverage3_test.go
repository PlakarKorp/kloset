package repository_test

import (
	"bytes"
	"testing"

	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/stretchr/testify/require"
)

// TestGetLockCorrupt writes raw garbage to a lock slot and confirms GetLock
// returns the deserialize error rather than a valid lock.
func TestGetLockCorrupt(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	lockID := objects.MAC{0x5A, 0x5A}
	_, err := repo.Store().Put(repo.AppContext(), storage.StorageResourceLock, lockID, bytes.NewReader([]byte("not a lock")))
	require.NoError(t, err)

	_, err = repo.GetLock(lockID)
	require.Error(t, err)
}

// TestPutBlobDedup writes the same blob twice through PutBlob. The second call
// hits the InsertIfNotPresent "already present" short-circuit.
func TestPutBlobDedup(t *testing.T) {
	_, writer, _, _ := makeWriter(t)

	mac := objects.MAC{0x01, 0x02}
	require.NoError(t, writer.PutBlob(resources.RT_CHUNK, mac, []byte("payload"), false))
	// Same MAC again -> dedup branch.
	require.NoError(t, writer.PutBlob(resources.RT_CHUNK, mac, []byte("payload"), false))
}

// TestPutBlobWithHintDedup is the hinted variant of TestPutBlobDedup.
func TestPutBlobWithHintDedup(t *testing.T) {
	_, writer, _, _ := makeWriter(t)

	mac := objects.MAC{0x03, 0x04}
	require.NoError(t, writer.PutBlobWithHint(0, resources.RT_CHUNK, mac, []byte("hinted"), true))
	require.NoError(t, writer.PutBlobWithHint(0, resources.RT_CHUNK, mac, []byte("hinted"), true))
}

// TestPutBlobIfNotExistsWithHintExisting exercises the BlobExists()==true
// short-circuit of PutBlobIfNotExistsWithHint: after the blob is committed to
// the repository state, a second insert-if-not-exists must early-return.
func TestPutBlobIfNotExistsWithHintExisting(t *testing.T) {
	_, writer, _, snapshotID := makeWriter(t)

	mac := objects.MAC{0x05, 0x06}
	require.NoError(t, writer.PutBlobIfNotExistsWithHint(0, resources.RT_CHUNK, mac, []byte("data"), true))
	// Calling again while still in the packer/delta state takes the
	// already-exists path.
	require.NoError(t, writer.PutBlobIfNotExistsWithHint(0, resources.RT_CHUNK, mac, []byte("data"), true))

	require.NoError(t, writer.CommitTransaction(snapshotID))
}

// TestNewRepositoryWriterOnDiskPackfiles exercises the on-disk packfile
// constructor branch of newRepositoryWriter by passing a non-empty
// packfileTmpDir.
func TestNewRepositoryWriterOnDiskPackfiles(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	tmpDir := t.TempDir()
	snapshotID := objects.MAC{0x70}
	scanCache, err := repo.AppContext().GetCache().Scan(snapshotID)
	require.NoError(t, err)

	writer := repo.NewRepositoryWriter(scanCache, snapshotID, repository.DefaultType, tmpDir)
	require.NotNil(t, writer)

	// Write a blob so the on-disk packfile machinery actually runs.
	require.NoError(t, writer.PutBlob(resources.RT_CHUNK, objects.MAC{0x71}, []byte("on-disk packfile data"), false))
	require.NoError(t, writer.CommitTransaction(snapshotID))
}

// TestNewRepositoryWriterPtarType exercises the PtarType branch of
// newRepositoryWriter.
func TestNewRepositoryWriterPtarType(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	snapshotID := objects.MAC{0x80}
	scanCache, err := repo.AppContext().GetCache().Scan(snapshotID)
	require.NoError(t, err)

	writer := repo.NewRepositoryWriter(scanCache, snapshotID, repository.PtarType, "")
	require.NotNil(t, writer)
}
