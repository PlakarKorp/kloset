package packfile

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

// errReader returns an error on every Read.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read fail") }

// failingIndexEncoder returns an error the first time it's invoked (when the
// index is being encoded) and passes through on subsequent calls.
type failingIndexEncoder struct{ called int }

func (e *failingIndexEncoder) encode(r io.Reader) (io.Reader, error) {
	e.called++
	if e.called == 1 {
		return nil, errors.New("encoder index fail")
	}
	return r, nil
}

// failingFooterEncoder errors only on the second call (when the footer is
// being encoded).
type failingFooterEncoder struct{ called int }

func (e *failingFooterEncoder) encode(r io.Reader) (io.Reader, error) {
	e.called++
	if e.called == 2 {
		return nil, errors.New("encoder footer fail")
	}
	return r, nil
}

// errReaderEncoder returns a Reader that fails on Read, only on the requested call.
type errReaderEncoder struct {
	called int
	failAt int
}

func (e *errReaderEncoder) encode(r io.Reader) (io.Reader, error) {
	e.called++
	if e.called == e.failAt {
		return errReader{}, nil
	}
	return r, nil
}

// --- packfile_disk.go error paths ---------------------------------------

func TestPackfileOnDiskSerializeIndexEncoderError(t *testing.T) {
	tmp := t.TempDir()
	p, err := NewPackfileOnDisk(tmp, sha256Factory)
	require.NoError(t, err)
	defer p.Cleanup()

	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)
	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, objects.MAC{1}, []byte("data"), 0))

	enc := &failingIndexEncoder{}
	_, _, err = p.Serialize(enc.encode)
	require.Error(t, err)
}

func TestPackfileOnDiskSerializeFooterEncoderError(t *testing.T) {
	tmp := t.TempDir()
	p, err := NewPackfileOnDisk(tmp, sha256Factory)
	require.NoError(t, err)
	defer p.Cleanup()

	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)
	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, objects.MAC{1}, []byte("data"), 0))

	enc := &failingFooterEncoder{}
	_, _, err = p.Serialize(enc.encode)
	require.Error(t, err)
}

func TestPackfileOnDiskSerializeIndexCopyError(t *testing.T) {
	tmp := t.TempDir()
	p, err := NewPackfileOnDisk(tmp, sha256Factory)
	require.NoError(t, err)
	defer p.Cleanup()

	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)
	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, objects.MAC{1}, []byte("data"), 0))

	enc := &errReaderEncoder{failAt: 1}
	_, _, err = p.Serialize(enc.encode)
	require.Error(t, err)
}

func TestPackfileOnDiskSerializeFooterCopyError(t *testing.T) {
	tmp := t.TempDir()
	p, err := NewPackfileOnDisk(tmp, sha256Factory)
	require.NoError(t, err)
	defer p.Cleanup()

	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)
	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, objects.MAC{1}, []byte("data"), 0))

	enc := &errReaderEncoder{failAt: 2}
	_, _, err = p.Serialize(enc.encode)
	require.Error(t, err)
}

// TestPackfileOnDiskSerializeFlushSyncSeekErrors closes the underlying file
// before calling Serialize, so all of Flush / Sync / Seek will return errors
// when the buffered writer / file descriptor reach them.
func TestPackfileOnDiskSerializeWriteAfterClose(t *testing.T) {
	tmp := t.TempDir()
	pi, err := NewPackfileOnDisk(tmp, sha256Factory)
	require.NoError(t, err)
	defer pi.Cleanup()

	p := pi.(*PackfileOnDisk)
	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)
	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, objects.MAC{1}, []byte("data"), 0))

	// Fill the buffered writer with enough data that the next encoder Copy
	// is forced to actually push bytes through the underlying file. Then
	// close the underlying file so future writes/flush/sync fail.
	// Closing the os.File causes Flush() at the end of Serialize to fail.
	require.NoError(t, p.f.Close())

	// Use an encoder that returns a Reader producing enough bytes to overflow
	// the bufio.Writer's 1MB internal buffer, forcing an immediate
	// write-through to the closed file and thus an io.Copy error.
	bigEnc := func(r io.Reader) (io.Reader, error) {
		return bytes.NewReader(make([]byte, 2<<20)), nil
	}
	_, _, err = p.Serialize(bigEnc)
	require.Error(t, err)
}

// --- packfile_mem.go error paths ---------------------------------------

func TestNewInMemoryFooterFromBytesPartialIndexOffset(t *testing.T) {
	// 12 bytes: Timestamp (8) ok, Count (4) ok, IndexOffset (8) missing
	buf := &bytes.Buffer{}
	_ = binary.Write(buf, binary.LittleEndian, int64(1))
	_ = binary.Write(buf, binary.LittleEndian, uint32(1))
	_, err := NewInMemoryFooterFromBytes(versioning.FromString(VERSION), buf.Bytes())
	require.Error(t, err)
}

func TestNewInMemoryFooterFromBytesPartialIndexMAC(t *testing.T) {
	// 20 bytes: Timestamp ok, Count ok, IndexOffset ok, MAC (32) missing
	buf := &bytes.Buffer{}
	_ = binary.Write(buf, binary.LittleEndian, int64(1))
	_ = binary.Write(buf, binary.LittleEndian, uint32(1))
	_ = binary.Write(buf, binary.LittleEndian, uint64(1))
	_, err := NewInMemoryFooterFromBytes(versioning.FromString(VERSION), buf.Bytes())
	require.Error(t, err)
}

func TestNewInMemoryFooterFromBytesPartialFlags(t *testing.T) {
	// 52 bytes: everything but Flags (4)
	buf := &bytes.Buffer{}
	_ = binary.Write(buf, binary.LittleEndian, int64(1))
	_ = binary.Write(buf, binary.LittleEndian, uint32(1))
	_ = binary.Write(buf, binary.LittleEndian, uint64(1))
	_ = binary.Write(buf, binary.LittleEndian, objects.MAC{})
	_, err := NewInMemoryFooterFromBytes(versioning.FromString(VERSION), buf.Bytes())
	require.Error(t, err)
}

// TestNewInMemoryIndexFromBytesTruncated exercises each early-exit binary.Read
// error path in NewInMemoryIndexFromBytes by truncating at different offsets.
func TestNewInMemoryIndexFromBytesTruncated(t *testing.T) {
	// A full Blob record is BLOB_RECORD_SIZE bytes. Truncate to a partial
	// record so binary.Read fails partway through.
	// Each size truncates at a different field boundary within a single 56-byte
	// record: Type(4) Version(4) MAC(32) Offset(8) Length(4) Flags(4).
	//   1  -> Type, 5 -> Version, 12 -> MAC, 44 -> Offset, 50 -> Length, 55 -> Flags.
	for _, n := range []int{1, 5, 12, 30, 44, 50, 55} {
		_, err := NewInMemoryIndexFromBytes(versioning.FromString(VERSION), make([]byte, n))
		require.Error(t, err)
	}
}

// TestNewPackfileInMemoryFromBytesIndexMACMismatch exercises the IndexMAC check
// in NewPackfileInMemoryFromBytes by constructing valid bytes and corrupting
// the IndexMAC in the footer.
func TestNewPackfileInMemoryFromBytesIndexMACMismatch(t *testing.T) {
	hasher := sha256.New()
	p := newPackfileInMemory(hasher).(*PackfileInMemory)

	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)
	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, objects.MAC{1}, []byte("hello"), 0))

	reader, _, err := p.Serialize(identityEncoder)
	require.NoError(t, err)
	serialized, err := io.ReadAll(reader)
	require.NoError(t, err)

	unwrapped := unwrapSerializedPackfile(t, serialized, p.Footer)

	// The raw footer layout (without the trailing length): Timestamp(8) +
	// Count(4) + IndexOffset(8) + IndexMAC(32) + Flags(4). The IndexMAC
	// starts at byte 20 within the footer. The footer starts at
	// len(unwrapped) - FOOTER_SIZE.
	footerStart := len(unwrapped) - FOOTER_SIZE
	unwrapped[footerStart+20] ^= 0xFF // flip a bit inside IndexMAC

	_, err = NewPackfileInMemoryFromBytes(sha256.New(), versioning.FromString(VERSION), unwrapped)
	require.Error(t, err)
}

// TestNewPackfileInMemoryFromBytesBlobOffsetOverflow constructs a packfile
// where an index entry claims a blob extends beyond IndexOffset.
func TestNewPackfileInMemoryFromBytesBlobOffsetOverflow(t *testing.T) {
	// Build a packfile manually: blob data + a single index entry whose
	// Offset+Length exceeds the IndexOffset, then a footer.
	blobData := []byte("small")
	indexOff := uint64(len(blobData))

	// One blob record claiming Offset=0, Length=1000 (well beyond indexOff=5).
	idxBuf := &bytes.Buffer{}
	_ = binary.Write(idxBuf, binary.LittleEndian, resources.RT_CHUNK)
	_ = binary.Write(idxBuf, binary.LittleEndian, versioning.GetCurrentVersion(resources.RT_CHUNK))
	_ = binary.Write(idxBuf, binary.LittleEndian, objects.MAC{0xCC})
	_ = binary.Write(idxBuf, binary.LittleEndian, uint64(0))     // Offset
	_ = binary.Write(idxBuf, binary.LittleEndian, uint32(1_000)) // Length (overflow)
	_ = binary.Write(idxBuf, binary.LittleEndian, uint32(0))

	footerBuf := &bytes.Buffer{}
	_ = binary.Write(footerBuf, binary.LittleEndian, int64(0))    // Timestamp
	_ = binary.Write(footerBuf, binary.LittleEndian, uint32(1))   // Count
	_ = binary.Write(footerBuf, binary.LittleEndian, indexOff)    // IndexOffset
	_ = binary.Write(footerBuf, binary.LittleEndian, objects.MAC{}) // IndexMAC (won't be reached)
	_ = binary.Write(footerBuf, binary.LittleEndian, uint32(0))   // Flags

	all := append([]byte{}, blobData...)
	all = append(all, idxBuf.Bytes()...)
	all = append(all, footerBuf.Bytes()...)

	_, err := NewPackfileInMemoryFromBytes(sha256.New(), versioning.FromString(VERSION), all)
	require.Error(t, err)
}

// TestNewPackfileInMemoryFromBytesTruncatedIndex exercises the binary.Read
// failures inside the per-record index loop (lines 167-184) by truncating
// the input partway through a blob record.
func TestNewPackfileInMemoryFromBytesTruncatedIndex(t *testing.T) {
	blobData := []byte("blob")
	indexOff := uint64(len(blobData))

	// Start a blob record but truncate it mid-way (only the Type field).
	idxBuf := &bytes.Buffer{}
	_ = binary.Write(idxBuf, binary.LittleEndian, resources.RT_CHUNK)
	// Stop here: missing version, mac, offset, length, flags.

	// Footer says Count=1 so the loop will try to read one record but the
	// partial bytes will fail. We pad the partial record up to BLOB_RECORD_SIZE
	// minus 1 byte to be sure binary.Read fails somewhere in the middle, but
	// it's simpler to provide just the 4 bytes of Type and let `remaining`
	// stay at BLOB_RECORD_SIZE so the loop runs.
	footerBuf := &bytes.Buffer{}
	_ = binary.Write(footerBuf, binary.LittleEndian, int64(0))
	_ = binary.Write(footerBuf, binary.LittleEndian, uint32(1))
	_ = binary.Write(footerBuf, binary.LittleEndian, indexOff)
	_ = binary.Write(footerBuf, binary.LittleEndian, objects.MAC{})
	_ = binary.Write(footerBuf, binary.LittleEndian, uint32(0))

	all := append([]byte{}, blobData...)
	// Add only 4 bytes of partial index — Type only, no version/mac/etc.
	all = append(all, idxBuf.Bytes()...)
	all = append(all, footerBuf.Bytes()...)

	_, err := NewPackfileInMemoryFromBytes(sha256.New(), versioning.FromString(VERSION), all)
	require.Error(t, err)
}

// TestNewPackfileInMemoryFromBytesShortDataSection sets IndexOffset larger
// than the actual buffer minus footer/index so binary.Read(reader, ..., data)
// at line 147-149 fails.
func TestNewPackfileInMemoryFromBytesShortDataSection(t *testing.T) {
	// Footer claims IndexOffset=1000 but the buffer is only FOOTER_SIZE
	// bytes plus a tiny bit of padding.
	footerBuf := &bytes.Buffer{}
	_ = binary.Write(footerBuf, binary.LittleEndian, int64(0))
	_ = binary.Write(footerBuf, binary.LittleEndian, uint32(0))
	_ = binary.Write(footerBuf, binary.LittleEndian, uint64(1_000_000)) // huge IndexOffset
	_ = binary.Write(footerBuf, binary.LittleEndian, objects.MAC{})
	_ = binary.Write(footerBuf, binary.LittleEndian, uint32(0))

	// Buffer: 0 bytes of blob data + footer. IndexOffset claims 1M bytes
	// of blobs which can't be there.
	all := footerBuf.Bytes()

	_, err := NewPackfileInMemoryFromBytes(sha256.New(), versioning.FromString(VERSION), all)
	require.Error(t, err)
}

// TestPackfileInMemorySerializeIndexEncoderError makes the encoder fail on
// its first call (when the index is being encoded).
func TestPackfileInMemorySerializeIndexEncoderError(t *testing.T) {
	hasher := sha256.New()
	p := newPackfileInMemory(hasher).(*PackfileInMemory)

	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)
	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, objects.MAC{1}, []byte("x"), 0))

	enc := &failingIndexEncoder{}
	_, _, err := p.Serialize(enc.encode)
	require.Error(t, err)
}

// TestPackfileInMemorySerializeFooterEncoderError makes the encoder fail on
// its second call (when the footer is being encoded).
func TestPackfileInMemorySerializeFooterEncoderError(t *testing.T) {
	hasher := sha256.New()
	p := newPackfileInMemory(hasher).(*PackfileInMemory)

	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)
	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, objects.MAC{1}, []byte("x"), 0))

	enc := &failingFooterEncoder{}
	_, _, err := p.Serialize(enc.encode)
	require.Error(t, err)
}

// TestPackfileInMemorySerializeIndexCopyError makes the encoder return a
// Reader that fails on Read, so io.CopyBuffer hits an error.
func TestPackfileInMemorySerializeIndexCopyError(t *testing.T) {
	hasher := sha256.New()
	p := newPackfileInMemory(hasher).(*PackfileInMemory)

	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)
	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, objects.MAC{1}, []byte("x"), 0))

	enc := &errReaderEncoder{failAt: 1}
	_, _, err := p.Serialize(enc.encode)
	require.Error(t, err)
}

func TestPackfileInMemorySerializeFooterCopyError(t *testing.T) {
	hasher := sha256.New()
	p := newPackfileInMemory(hasher).(*PackfileInMemory)

	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)
	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, objects.MAC{1}, []byte("x"), 0))

	enc := &errReaderEncoder{failAt: 2}
	_, _, err := p.Serialize(enc.encode)
	require.Error(t, err)
}
