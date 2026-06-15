package packfile

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"hash"
	"io"
	"os"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
)

// diskBackend abstracts the buffered-file operations performed at the end of
// Serialize (flush the buffer, fsync, rewind). It exists so tests can inject a
// backend whose Flush/Sync/Seek fail independently — a real *os.File cannot be
// made to fail those at will once writing has succeeded.
type diskBackend interface {
	io.Writer
	Flush() error
	Sync() error
	Seek(offset int64, whence int) (int64, error)
	// reader returns the io.Reader handed back from Serialize after a rewind.
	reader() io.Reader
}

// fileBackend is the production diskBackend, pairing a bufio.Writer with the
// *os.File it wraps.
type fileBackend struct {
	f  *os.File
	bw *bufio.Writer
}

func (b *fileBackend) Write(p []byte) (int, error)               { return b.bw.Write(p) }
func (b *fileBackend) Flush() error                              { return b.bw.Flush() }
func (b *fileBackend) Sync() error                               { return b.f.Sync() }
func (b *fileBackend) Seek(off int64, whence int) (int64, error) { return b.f.Seek(off, whence) }
func (b *fileBackend) reader() io.Reader                         { return b.f }

type PackfileOnDisk struct {
	f              *os.File
	backend        diskBackend
	totalHash      hash.Hash
	combinedWriter io.Writer
	hf             HashFactory

	records []Blob
	curOff  uint64

	footerTimestamp int64
	footerFlags     uint32
	indexMAC        objects.MAC

	hot bool
}

func NewPackfileOnDisk(tempDir string, hf HashFactory) (Packfile, error) {
	f, err := os.CreateTemp(tempDir, "plakar-pack-*")
	if err != nil {
		return nil, err
	}

	p := &PackfileOnDisk{
		f:               f,
		backend:         &fileBackend{f: f, bw: bufio.NewWriterSize(f, 1<<20)},
		totalHash:       hf(),
		hf:              hf,
		footerTimestamp: time.Now().UnixNano(),
	}
	p.combinedWriter = io.MultiWriter(p.backend, p.totalHash)

	return p, nil
}

func (w *PackfileOnDisk) AddBlob(t resources.Type, v versioning.Version, mac objects.MAC, data []byte, flags uint32) error {
	_, err := w.combinedWriter.Write(data)
	if err != nil {
		return err
	}

	w.records = append(w.records, Blob{
		Type: t, Version: v, MAC: mac,
		Offset: w.curOff, Length: uint32(len(data)), Flags: flags,
	})
	w.curOff += uint64(len(data))

	return nil
}

func (w *PackfileOnDisk) Size() uint64    { return w.curOff }
func (w *PackfileOnDisk) Entries() []Blob { return w.records }
func (w *PackfileOnDisk) Cleanup() error  { _ = w.f.Close(); return os.Remove(w.f.Name()) }

func (w *PackfileOnDisk) Serialize(encoder EncodingFn) (io.Reader, objects.MAC, error) {
	rawIdx := &bytes.Buffer{}
	idxHash := w.hf()

	ciw := io.MultiWriter(rawIdx, idxHash)
	for _, r := range w.records {
		// ciw only wraps in-memory writers (bytes.Buffer + hash.Hash), so
		// these writes cannot fail; the error is intentionally ignored, as in
		// the pre-helper version of this loop.
		_ = writeBlobRecord(ciw, r)
	}

	copy(w.indexMAC[:], idxHash.Sum(nil))
	encIdx, err := encoder(rawIdx)
	if err != nil {
		return nil, objects.NilMac, err
	}
	if _, err := io.Copy(w.combinedWriter, encIdx); err != nil {
		return nil, objects.NilMac, err
	}

	var rawFooter bytes.Buffer
	// rawFooter is an in-memory buffer; this write cannot fail.
	_ = writeFooterFields(&rawFooter, w.footerTimestamp, uint32(len(w.records)), w.curOff, w.indexMAC, w.footerFlags)

	encFooter, err := encoder(bytes.NewReader(rawFooter.Bytes()))
	if err != nil {
		return nil, objects.NilMac, err
	}
	n, err := io.Copy(w.combinedWriter, encFooter)
	if err != nil {
		return nil, objects.NilMac, err
	}

	if err := binary.Write(w.combinedWriter, binary.LittleEndian, uint32(n)); err != nil {
		return nil, objects.NilMac, err
	}

	// Flush and sync before publishing the file.
	if err := w.backend.Flush(); err != nil {
		return nil, objects.NilMac, err
	}

	if err := w.backend.Sync(); err != nil {
		return nil, objects.NilMac, err
	}

	if _, err := w.backend.Seek(0, io.SeekStart); err != nil {
		return nil, objects.NilMac, err
	}

	return w.backend.reader(), objects.MAC(w.totalHash.Sum(nil)), nil
}

func (p *PackfileOnDisk) SetHot() {
	p.hot = true
}

func (p *PackfileOnDisk) Hot() bool {
	return p.hot
}
