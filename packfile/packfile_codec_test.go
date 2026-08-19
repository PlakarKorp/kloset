package packfile

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/stretchr/testify/require"
)

// failAfterNWriter writes successfully for the first n Write calls, then
// returns an error on every subsequent Write. With n==0 the very first Write
// fails.
type failAfterNWriter struct {
	n   int
	cnt int
}

func (w *failAfterNWriter) Write(p []byte) (int, error) {
	if w.cnt >= w.n {
		return 0, errors.New("write fail")
	}
	w.cnt++
	return len(p), nil
}

// failAfterNReader returns valid bytes for the first n Read calls, then errors.
type failAfterNReader struct {
	src *bytes.Reader
	n   int
	cnt int
}

func (r *failAfterNReader) Read(p []byte) (int, error) {
	if r.cnt >= r.n {
		return 0, errors.New("read fail")
	}
	r.cnt++
	return r.src.Read(p)
}

// --- writeBlobRecord: each of the 6 field writes -------------------------

func TestWriteBlobRecordFieldErrors(t *testing.T) {
	b := Blob{
		Type:    resources.RT_CHUNK,
		Version: versioning.GetCurrentVersion(resources.RT_CHUNK),
		MAC:     objects.MAC{1},
		Offset:  10,
		Length:  20,
		Flags:   0,
	}
	// 6 fields => failing at write index 0..5 exercises each return err.
	for n := 0; n < 6; n++ {
		w := &failAfterNWriter{n: n}
		err := writeBlobRecord(w, b)
		require.Error(t, err, "expected error when write #%d fails", n)
	}
	// All writes succeed.
	var ok bytes.Buffer
	require.NoError(t, writeBlobRecord(&ok, b))
}

// --- readBlobRecord: each of the 6 field reads ---------------------------

func TestReadBlobRecordFieldErrors(t *testing.T) {
	var full bytes.Buffer
	require.NoError(t, writeBlobRecord(&full, Blob{Type: resources.RT_CHUNK}))
	require.Equal(t, BLOB_RECORD_SIZE, full.Len())

	// Failing at read index 0..5 exercises each return err.
	for n := 0; n < 6; n++ {
		r := &failAfterNReader{src: bytes.NewReader(full.Bytes()), n: n}
		_, err := readBlobRecord(r)
		require.Error(t, err, "expected error when read #%d fails", n)
	}
	// Clean read round-trips.
	got, err := readBlobRecord(bytes.NewReader(full.Bytes()))
	require.NoError(t, err)
	require.Equal(t, resources.RT_CHUNK, got.Type)
}

// --- writeFooterFields: each of the 5 field writes -----------------------

func TestWriteFooterFieldsErrors(t *testing.T) {
	for n := 0; n < 5; n++ {
		w := &failAfterNWriter{n: n}
		err := writeFooterFields(w, 1, 2, 3, objects.MAC{4}, 5)
		require.Error(t, err, "expected error when write #%d fails", n)
	}
	var ok bytes.Buffer
	require.NoError(t, writeFooterFields(&ok, 1, 2, 3, objects.MAC{4}, 5))
	require.Equal(t, FOOTER_SIZE, ok.Len())
}

// --- readFooterFields: each of the 5 field reads -------------------------

func TestReadFooterFieldsErrors(t *testing.T) {
	var full bytes.Buffer
	require.NoError(t, writeFooterFields(&full, 1, 2, 3, objects.MAC{4}, 5))

	for n := 0; n < 5; n++ {
		r := &failAfterNReader{src: bytes.NewReader(full.Bytes()), n: n}
		var f PackfileInMemoryFooter
		err := readFooterFields(r, &f)
		require.Error(t, err, "expected error when read #%d fails", n)
	}
	var f PackfileInMemoryFooter
	require.NoError(t, readFooterFields(bytes.NewReader(full.Bytes()), &f))
	require.Equal(t, int64(1), f.Timestamp)
	require.Equal(t, uint32(2), f.Count)
	require.Equal(t, uint64(3), f.IndexOffset)
	require.Equal(t, objects.MAC{4}, f.IndexMAC)
	require.Equal(t, uint32(5), f.Flags)
}

// --- PackfileInMemory.serializeTo destination-write errors ---------------

// buildPackWithBlob returns an in-memory packfile holding one blob.
func buildPackWithBlob(t *testing.T) *PackfileInMemory {
	t.Helper()
	p := newPackfileInMemory(sha256.New()).(*PackfileInMemory)
	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)
	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, objects.MAC{1}, []byte("payload"), 0))
	return p
}

// TestSerializeToBlobsWriteError fails the first write to the destination (the
// p.Blobs write).
func TestSerializeToBlobsWriteError(t *testing.T) {
	p := buildPackWithBlob(t)
	_, err := p.serializeTo(&failAfterNWriter{n: 0}, identityEncoder)
	require.Error(t, err)
}

// TestSerializeToIndexCopyError fails the index io.CopyBuffer to the
// destination. The blobs write (1 Write) must succeed first, so n=1.
func TestSerializeToIndexCopyError(t *testing.T) {
	p := buildPackWithBlob(t)
	_, err := p.serializeTo(&failAfterNWriter{n: 1}, identityEncoder)
	require.Error(t, err)
}

// TestSerializeToFooterLenWriteError lets everything through except the final
// footer-length write. The destination writes in order are: blobs(1),
// index copy(1+), footer copy(1+), footer-len(1). Use a generous n so only the
// last small writes can still trip — simplest is to fail very late.
func TestSerializeToFooterLenWriteError(t *testing.T) {
	p := buildPackWithBlob(t)
	// Count the writes a full successful serialize performs to the dst, then
	// fail on the last one.
	counter := &countingWriter{}
	_, err := p.serializeTo(counter, identityEncoder)
	require.NoError(t, err)
	total := counter.cnt

	p2 := buildPackWithBlob(t)
	_, err = p2.serializeTo(&failAfterNWriter{n: total - 1}, identityEncoder)
	require.Error(t, err)
}

// countingWriter counts Write calls and discards data.
type countingWriter struct{ cnt int }

func (c *countingWriter) Write(p []byte) (int, error) { c.cnt++; return len(p), nil }

// --- PackfileOnDisk.Serialize backend Flush/Sync/Seek errors -------------

// fakeBackend is a diskBackend whose Flush/Sync/Seek can be made to fail.
type fakeBackend struct {
	buf       bytes.Buffer
	failFlush bool
	failSync  bool
	failSeek  bool
}

func (b *fakeBackend) Write(p []byte) (int, error) { return b.buf.Write(p) }
func (b *fakeBackend) Flush() error {
	if b.failFlush {
		return errors.New("flush fail")
	}
	return nil
}
func (b *fakeBackend) Sync() error {
	if b.failSync {
		return errors.New("sync fail")
	}
	return nil
}
func (b *fakeBackend) Seek(int64, int) (int64, error) {
	if b.failSeek {
		return 0, errors.New("seek fail")
	}
	return 0, nil
}
func (b *fakeBackend) reader() io.Reader { return bytes.NewReader(b.buf.Bytes()) }

// newDiskPackWithFakeBackend builds a PackfileOnDisk wired to a fake backend
// and one blob, so Serialize reaches the publish tail.
func newDiskPackWithFakeBackend(t *testing.T, fb *fakeBackend) *PackfileOnDisk {
	t.Helper()
	pi, err := NewPackfileOnDisk(t.TempDir(), sha256Factory)
	require.NoError(t, err)
	p := pi.(*PackfileOnDisk)
	// Swap the backend and rewire combinedWriter to point at the fake.
	p.backend = fb
	p.combinedWriter = io.MultiWriter(fb, p.totalHash)

	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)
	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, objects.MAC{1}, []byte("data"), 0))
	return p
}

func TestPackfileOnDiskSerializeFlushError(t *testing.T) {
	p := newDiskPackWithFakeBackend(t, &fakeBackend{failFlush: true})
	_, _, err := p.Serialize(identityEncoder)
	require.Error(t, err)
}

func TestPackfileOnDiskSerializeSyncError(t *testing.T) {
	p := newDiskPackWithFakeBackend(t, &fakeBackend{failSync: true})
	_, _, err := p.Serialize(identityEncoder)
	require.Error(t, err)
}

func TestPackfileOnDiskSerializeSeekError(t *testing.T) {
	p := newDiskPackWithFakeBackend(t, &fakeBackend{failSeek: true})
	_, _, err := p.Serialize(identityEncoder)
	require.Error(t, err)
}

func TestPackfileOnDiskSerializeFooterLenWriteError(t *testing.T) {
	// A backend that fails after the index+footer copies, on the small
	// footer-length write. The combinedWriter is a MultiWriter(fake, hash); a
	// failAfterNWriter as the backend lets us fail the Nth write precisely.
	pi, err := NewPackfileOnDisk(t.TempDir(), sha256Factory)
	require.NoError(t, err)
	p := pi.(*PackfileOnDisk)

	ver := versioning.GetCurrentVersion(resources.RT_CHUNK)
	// The only fixed-size 4-byte write Serialize makes against the backend is
	// the trailing footer-length (a uint32). The index and footer are streamed
	// via io.Copy of encoded buffers that are larger than 4 bytes, so failing
	// exclusively on a 4-byte write isolates the footer-length write branch.
	p.backend = &failOnSizeBackend{failSize: 4}
	p.combinedWriter = io.MultiWriter(p.backend, p.totalHash)
	// Use a payload whose length is not 4, so the only 4-byte backend write is
	// the footer length itself.
	require.NoError(t, p.AddBlob(resources.RT_CHUNK, ver, objects.MAC{1}, []byte("payload"), 0))

	_, _, err = p.Serialize(identityEncoder)
	require.Error(t, err)
}

// failOnSizeBackend is a diskBackend that fails any Write whose length equals
// failSize, and succeeds otherwise. Flush/Sync/Seek always succeed.
type failOnSizeBackend struct {
	buf      bytes.Buffer
	failSize int
}

func (b *failOnSizeBackend) Write(p []byte) (int, error) {
	if len(p) == b.failSize {
		return 0, errors.New("sized write fail")
	}
	return b.buf.Write(p)
}
func (b *failOnSizeBackend) Flush() error                   { return nil }
func (b *failOnSizeBackend) Sync() error                    { return nil }
func (b *failOnSizeBackend) Seek(int64, int) (int64, error) { return 0, nil }
func (b *failOnSizeBackend) reader() io.Reader              { return bytes.NewReader(b.buf.Bytes()) }
