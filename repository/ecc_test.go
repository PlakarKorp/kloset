package repository_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/caching/pebble"
	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/ecc"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/packfile"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

// corruptShards flips a byte inside each of the first n data shards of a stored
// primary object, simulating corruption spread across n shards.
func corruptShards(t *testing.T, backend *ptesting.MockBackend, res storage.StorageResource, mac objects.MAC, primaryLen, dataShards, n int) {
	t.Helper()
	shardSize := (primaryLen + dataShards - 1) / dataShards
	if shardSize == 0 {
		shardSize = 1
	}
	for i := 0; i < n; i++ {
		off := i * shardSize
		require.NoError(t, backend.Corrupt(res, mac, off))
	}
}

func primaryLen(t *testing.T, backend *ptesting.MockBackend, res storage.StorageResource, mac objects.MAC) int {
	t.Helper()
	rd, err := backend.Get(t.Context(), res, mac, nil)
	require.NoError(t, err)
	defer rd.Close()
	b, err := io.ReadAll(rd)
	require.NoError(t, err)
	return len(b)
}

func TestECCStateRoundTripAndParity(t *testing.T) {
	repo, backend := ptesting.GenerateRepositoryWithECC(t, nil, nil, nil, ecc.NewDefaultConfiguration())

	mac := objects.MAC{0xAA}
	payload := bytes.Repeat([]byte("state-payload-"), 500)

	require.NoError(t, repo.PutState(mac, bytes.NewReader(payload)))

	// A parity object must have been written under the ECC state resource.
	require.True(t, backend.Has(storage.StorageResourceECCState, mac), "expected ECC parity object for state")

	rd, _, err := repo.GetState(mac)
	require.NoError(t, err)
	got, err := io.ReadAll(rd)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestECCDisabledWritesNoParity(t *testing.T) {
	repo, backend := ptesting.GenerateRepositoryWithECC(t, nil, nil, nil, nil) // ECC disabled

	mac := objects.MAC{0xBB}
	require.NoError(t, repo.PutState(mac, bytes.NewReader([]byte("hello"))))

	require.False(t, backend.Has(storage.StorageResourceECCState, mac), "no parity object expected when ECC disabled")
	require.True(t, backend.Has(storage.StorageResourceState, mac))
}

func TestECCStateRepairWithinTolerance(t *testing.T) {
	cfg := ecc.NewDefaultConfiguration() // 10+3, tolerates 3
	repo, backend := ptesting.GenerateRepositoryWithECC(t, nil, nil, nil, cfg)

	mac := objects.MAC{0xCC}
	payload := bytes.Repeat([]byte("repairable-state!"), 1000)
	require.NoError(t, repo.PutState(mac, bytes.NewReader(payload)))

	plen := primaryLen(t, backend, storage.StorageResourceState, mac)

	// Corrupt exactly ParityShards data shards: at the tolerance limit.
	corruptShards(t, backend, storage.StorageResourceState, mac, plen, cfg.DataShards, cfg.ParityShards)

	// Read must transparently repair and return the original payload.
	rd, _, err := repo.GetState(mac)
	require.NoError(t, err)
	got, err := io.ReadAll(rd)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestECCStateRepairBeyondToleranceFails(t *testing.T) {
	cfg := ecc.NewDefaultConfiguration() // tolerates 3
	repo, backend := ptesting.GenerateRepositoryWithECC(t, nil, nil, nil, cfg)

	mac := objects.MAC{0xDD}
	payload := bytes.Repeat([]byte("unrecoverable-state"), 1000)
	require.NoError(t, repo.PutState(mac, bytes.NewReader(payload)))

	plen := primaryLen(t, backend, storage.StorageResourceState, mac)

	// Corrupt ParityShards+1 data shards: beyond what parity can repair.
	corruptShards(t, backend, storage.StorageResourceState, mac, plen, cfg.DataShards, cfg.ParityShards+1)

	_, _, err := repo.GetState(mac)
	require.Error(t, err)
}

func TestECCStateRepairFromMissingPrimary(t *testing.T) {
	// With ParityShards >= DataShards the object survives total primary loss.
	cfg := &ecc.Configuration{DataShards: 3, ParityShards: 3}
	repo, backend := ptesting.GenerateRepositoryWithECC(t, nil, nil, nil, cfg)

	mac := objects.MAC{0xEE}
	payload := bytes.Repeat([]byte("survives-primary-loss"), 300)
	require.NoError(t, repo.PutState(mac, bytes.NewReader(payload)))

	// Delete the primary entirely; only parity remains.
	require.NoError(t, backend.Delete(t.Context(), storage.StorageResourceState, mac))
	require.False(t, backend.Has(storage.StorageResourceState, mac))

	rd, _, err := repo.GetState(mac)
	require.NoError(t, err)
	got, err := io.ReadAll(rd)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestECCReadsObjectWithoutParity(t *testing.T) {
	// Models a repository where ECC was enabled after some objects were already
	// written: those objects have no parity, and reads must still work.
	repo, backend := ptesting.GenerateRepositoryWithECC(t, nil, nil, nil, ecc.NewDefaultConfiguration())

	mac := objects.MAC{0x77}
	payload := []byte("pre-existing state without parity")
	require.NoError(t, repo.PutState(mac, bytes.NewReader(payload)))

	// Drop the parity object to simulate a pre-ECC write.
	require.NoError(t, backend.Delete(t.Context(), storage.StorageResourceECCState, mac))
	require.False(t, backend.Has(storage.StorageResourceECCState, mac))

	rd, _, err := repo.GetState(mac)
	require.NoError(t, err)
	got, err := io.ReadAll(rd)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

// --- packfile coverage ---

func newWriter(t *testing.T, repo *repository.Repository) *repository.RepositoryWriter {
	t.Helper()
	tmpCacheDir, err := os.MkdirTemp("", "kloset-ecc-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmpCacheDir) })

	cacheManager := caching.NewManager(pebble.Constructor(tmpCacheDir))
	snapshotID := objects.MAC{}
	scanCache, err := cacheManager.Scan(snapshotID)
	require.NoError(t, err)

	return repo.NewRepositoryWriter(scanCache, snapshotID, repository.DefaultType, "")
}

// putTestPackfile writes a packfile containing a single blob and returns its MAC.
func putTestPackfile(t *testing.T, writer *repository.RepositoryWriter, blob []byte) objects.MAC {
	t.Helper()
	pfile, err := packfile.NewPackfileInMemory(writer.GetMACHasher)
	require.NoError(t, err)

	mac := writer.ComputeMAC(blob)
	require.NoError(t, pfile.AddBlob(resources.RT_CHUNK, versioning.GetCurrentVersion(resources.RT_CHUNK), mac, blob, 0))
	require.NoError(t, writer.PutPackfile(pfile))

	// The packfile is keyed by the MAC of its serialized form; recover it from
	// the backend listing.
	macs, err := writer.GetPackfiles()
	require.NoError(t, err)
	require.Len(t, macs, 1)
	return macs[0]
}

func TestECCPackfileRoundTripAndRepair(t *testing.T) {
	cfg := ecc.NewDefaultConfiguration()
	repo, backend := ptesting.GenerateRepositoryWithECC(t, nil, nil, nil, cfg)
	writer := newWriter(t, repo)

	blob := bytes.Repeat([]byte("packfile-blob-content"), 800)
	pfMAC := putTestPackfile(t, writer, blob)

	// Parity object must exist for the packfile.
	require.True(t, backend.Has(storage.StorageResourceECCPackfile, pfMAC), "expected ECC parity for packfile")

	// Clean read works.
	pf, err := repo.GetPackfile(pfMAC)
	require.NoError(t, err)
	require.NotNil(t, pf)

	// Corrupt within tolerance, then read must repair.
	plen := primaryLen(t, backend, storage.StorageResourcePackfile, pfMAC)
	corruptShards(t, backend, storage.StorageResourcePackfile, pfMAC, plen, cfg.DataShards, cfg.ParityShards)

	pf2, err := repo.GetPackfile(pfMAC)
	require.NoError(t, err, "GetPackfile should repair corruption within tolerance")
	require.NotNil(t, pf2)
}

func TestECCExplicitRepairObjectHeals(t *testing.T) {
	cfg := ecc.NewDefaultConfiguration()
	repo, backend := ptesting.GenerateRepositoryWithECC(t, nil, nil, nil, cfg)
	writer := newWriter(t, repo)

	mac := objects.MAC{0x42}
	payload := bytes.Repeat([]byte("heal-me"), 2000)
	require.NoError(t, repo.PutState(mac, bytes.NewReader(payload)))

	plen := primaryLen(t, backend, storage.StorageResourceState, mac)

	// Healthy object: RepairObject reports no repair needed.
	healed, err := writer.RepairObject(storage.StorageResourceState, mac)
	require.NoError(t, err)
	require.False(t, healed)

	// Corrupt within tolerance, then RepairObject must rewrite the primary.
	corruptShards(t, backend, storage.StorageResourceState, mac, plen, cfg.DataShards, cfg.ParityShards)

	healed, err = writer.RepairObject(storage.StorageResourceState, mac)
	require.NoError(t, err)
	require.True(t, healed)

	// After healing, the primary is intact on its own (verify passes).
	parity, err := backend.Get(t.Context(), storage.StorageResourceECCState, mac, nil)
	require.NoError(t, err)
	parityBytes, err := io.ReadAll(parity)
	require.NoError(t, err)
	primary, err := backend.Get(t.Context(), storage.StorageResourceState, mac, nil)
	require.NoError(t, err)
	primaryBytes, err := io.ReadAll(primary)
	require.NoError(t, err)
	ok, err := ecc.Verify(primaryBytes, parityBytes)
	require.NoError(t, err)
	require.True(t, ok, "primary should verify clean after healing")
}
