package repository_test

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/caching/pebble"
	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/packfile"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
	ptesting "github.com/PlakarKorp/kloset/testing"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

// putRawPackfile stores body under mac wrapped in the storage framing
// GetPackfile expects, so the bytes reach the packfile parsing untouched.
// The test repository has neither compression nor encryption, so body is
// exactly what GetPackfile slices.
func putRawPackfile(t *testing.T, repo *repository.Repository, mac objects.MAC, body []byte) {
	t.Helper()

	rd, err := storage.Serialize(repo.GetMACHasher(), resources.RT_PACKFILE, versioning.GetCurrentVersion(resources.RT_PACKFILE), bytes.NewReader(body))
	require.NoError(t, err)

	_, err = repo.Store().Put(repo.AppContext(), storage.StorageResourcePackfile, mac, rd)
	require.NoError(t, err)
}

// packfileFooter encodes a footer claiming indexOffset, followed by the
// trailing footer length, as the in-memory packfile serializer writes them.
func packfileFooter(t *testing.T, indexOffset uint64) []byte {
	t.Helper()

	buf := &bytes.Buffer{}
	require.NoError(t, binary.Write(buf, binary.LittleEndian, int64(0)))          // Timestamp
	require.NoError(t, binary.Write(buf, binary.LittleEndian, uint32(0)))         // Count
	require.NoError(t, binary.Write(buf, binary.LittleEndian, indexOffset))       // IndexOffset
	require.NoError(t, binary.Write(buf, binary.LittleEndian, objects.MAC{}))     // IndexMAC
	require.NoError(t, binary.Write(buf, binary.LittleEndian, uint32(0)))         // Flags
	require.NoError(t, binary.Write(buf, binary.LittleEndian, uint32(buf.Len()))) // footer length
	return buf.Bytes()
}

// TestGetPackfileCorruptFooter feeds GetPackfile packfiles whose trailing
// footer length or footer index offset do not fit in the bytes stored and
// checks it returns an error instead of slicing past the buffer.
func TestGetPackfileCorruptFooter(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	tests := []struct {
		name string
		body []byte
	}{
		{"empty", nil},
		{"three bytes", []byte{0x01, 0x02, 0x03}},
		{"footer length larger than file", append(make([]byte, 16), 0x64, 0x00, 0x00, 0x00)},
		{"footer length wraps", bytes.Repeat([]byte{0xff}, 7)},
		{"index offset past end", append([]byte("abc"), packfileFooter(t, 1000)...)},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mac := objects.MAC{byte(i + 1)}
			putRawPackfile(t, repo, mac, tc.body)

			require.NotPanics(t, func() {
				_, err := repo.GetPackfile(mac)
				require.Error(t, err)
			})
		})
	}
}

// TestGetPackfileRoundTrip checks a packfile written through PutPackfile is
// still read back whole once the footer bounds are checked.
func TestGetPackfileRoundTrip(t *testing.T) {
	repo := ptesting.GenerateRepository(t, nil, nil, nil)

	tmpCacheDir, err := os.MkdirTemp("", "kloset-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpCacheDir)

	cacheManager := caching.NewManager(pebble.Constructor(tmpCacheDir))
	snapshotID := objects.MAC{}
	scanCache, err := cacheManager.Scan(snapshotID)
	require.NoError(t, err)

	writer := repo.NewRepositoryWriter(scanCache, snapshotID, repository.DefaultType, "")

	pfile, err := packfile.NewPackfileInMemory(writer.GetMACHasher)
	require.NoError(t, err)
	require.NoError(t, pfile.AddBlob(resources.RT_CHUNK, versioning.GetCurrentVersion(resources.RT_CHUNK), objects.MAC{0x42}, []byte("hello"), 0))
	require.NoError(t, writer.PutPackfile(pfile))

	macs, err := repo.GetPackfiles()
	require.NoError(t, err)
	require.Len(t, macs, 1)

	got, err := repo.GetPackfile(macs[0])
	require.NoError(t, err)
	require.Len(t, got.Entries(), 1)
	require.Equal(t, objects.MAC{0x42}, got.Entries()[0].MAC)
	require.Equal(t, []byte("hello"), got.Blobs[:got.Entries()[0].Length])
}
