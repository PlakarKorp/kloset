package packfile

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

// --- SetHot / Hot for both implementations -------------------------------

func TestPackfileOnDiskHot(t *testing.T) {
	tmp := t.TempDir()
	p, err := NewPackfileOnDisk(tmp, sha256Factory)
	require.NoError(t, err)
	defer p.Cleanup()

	require.False(t, p.Hot())
	p.SetHot()
	require.True(t, p.Hot())
}

func TestPackfileInMemoryHot(t *testing.T) {
	p, err := NewPackfileInMemory(sha256Factory)
	require.NoError(t, err)
	defer p.Cleanup()

	require.False(t, p.Hot())
	p.SetHot()
	require.True(t, p.Hot())
}

// --- NewPackfileOnDisk error path ----------------------------------------

// TestNewPackfileOnDiskCreateTempError points the temp dir at a path that is a
// regular file (not a directory), so os.CreateTemp fails.
func TestNewPackfileOnDiskCreateTempError(t *testing.T) {
	tmp := t.TempDir()
	notADir := filepath.Join(tmp, "afile")
	require.NoError(t, os.WriteFile(notADir, []byte("x"), 0o644))

	_, err := NewPackfileOnDisk(notADir, sha256Factory)
	require.Error(t, err)
}

// --- PackfileOnDisk.AddBlob write error -----------------------------------

// TestPackfileOnDiskAddBlobWriteError closes the underlying file and writes
// enough data to overflow the 1MB bufio buffer, forcing the buffered writer to
// flush to the now-closed file and return an error from AddBlob.
func TestPackfileOnDiskAddBlobWriteError(t *testing.T) {
	tmp := t.TempDir()
	pi, err := NewPackfileOnDisk(tmp, sha256Factory)
	require.NoError(t, err)
	defer pi.Cleanup()

	p := pi.(*PackfileOnDisk)
	require.NoError(t, p.f.Close())

	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)
	// > 1MB so bufio.Writer flushes through to the closed fd during Write.
	big := make([]byte, 2<<20)
	err = p.AddBlob(resources.RT_CHUNK, ver, objects.MAC{1}, big, 0)
	require.Error(t, err)
}

// --- NewPackfileInMemoryFromBytes footer-parse branches -------------------
//
// A valid packfile always has a full FOOTER_SIZE-byte footer (Seek(-FOOTER_SIZE)
// must succeed before any field is read), so the individual footer-field reads
// cannot fail on real input. The reachable failure after a clean footer parse
// is the IndexMAC check: a buffer of exactly FOOTER_SIZE with IndexOffset==0
// parses the footer fully, then the recomputed (empty) index MAC won't match a
// nonzero stored IndexMAC.
func TestNewPackfileInMemoryFromBytesFooterOnlyMACMismatch(t *testing.T) {
	footerBuf := &bytes.Buffer{}
	_ = binary.Write(footerBuf, binary.LittleEndian, int64(0))       // Timestamp
	_ = binary.Write(footerBuf, binary.LittleEndian, uint32(0))      // Count
	_ = binary.Write(footerBuf, binary.LittleEndian, uint64(0))      // IndexOffset
	_ = binary.Write(footerBuf, binary.LittleEndian, objects.MAC{1}) // IndexMAC (nonzero)
	_ = binary.Write(footerBuf, binary.LittleEndian, uint32(0))      // Flags
	require.Equal(t, FOOTER_SIZE, footerBuf.Len())

	// Empty index + nonzero IndexMAC => the recomputed MAC (of nothing) won't
	// match, so we hit the "index mac mismatch" error after a clean footer parse.
	_, err := NewPackfileInMemoryFromBytes(sha256.New(), versioning.FromString(VERSION), footerBuf.Bytes())
	require.Error(t, err)
}

// TestNewPackfileInMemoryFromBytesEmptyValid round-trips an empty packfile so
// the footer parse + zero-length data read + empty index loop + matching MAC
// all succeed.
func TestNewPackfileInMemoryFromBytesEmptyValid(t *testing.T) {
	hasher := sha256.New()
	p := newPackfileInMemory(hasher).(*PackfileInMemory)

	reader, _, err := p.Serialize(func(r io.Reader) (io.Reader, error) { return r, nil })
	require.NoError(t, err)
	serialized, err := io.ReadAll(reader)
	require.NoError(t, err)

	unwrapped := unwrapSerializedPackfile(t, serialized, p.Footer)
	p2, err := NewPackfileInMemoryFromBytes(sha256.New(), versioning.FromString(VERSION), unwrapped)
	require.NoError(t, err)
	require.Equal(t, uint32(0), p2.Footer.Count)
}
