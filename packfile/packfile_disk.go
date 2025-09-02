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

type PackfileOnDisk struct {
	f              *os.File
	bufferedWriter *bufio.Writer
	totalHash      hash.Hash
	combinedWriter io.Writer
	hf             HashFactory

	records []Blob
	curOff  uint64

	footerTimestamp int64
	footerFlags     uint32
	indexMAC        objects.MAC
}

func NewPackfileOnDisk(tempDir string, hf HashFactory) (Packfile, error) {
	f, err := os.CreateTemp(tempDir, "plakar-pack-*")
	if err != nil {
		return nil, err
	}

	p := &PackfileOnDisk{
		f:               f,
		bufferedWriter:  bufio.NewWriterSize(f, 1<<20),
		totalHash:       hf(),
		hf:              hf,
		footerTimestamp: time.Now().Unix(),
	}
	p.combinedWriter = io.MultiWriter(p.bufferedWriter, p.totalHash)

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
		_ = binary.Write(ciw, binary.LittleEndian, r.Type)
		_ = binary.Write(ciw, binary.LittleEndian, r.Version)
		_ = binary.Write(ciw, binary.LittleEndian, r.MAC)
		_ = binary.Write(ciw, binary.LittleEndian, r.Offset)
		_ = binary.Write(ciw, binary.LittleEndian, r.Length)
		_ = binary.Write(ciw, binary.LittleEndian, r.Flags)
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
	_ = binary.Write(&rawFooter, binary.LittleEndian, w.footerTimestamp)
	_ = binary.Write(&rawFooter, binary.LittleEndian, uint32(len(w.records)))
	_ = binary.Write(&rawFooter, binary.LittleEndian, w.curOff)
	_ = binary.Write(&rawFooter, binary.LittleEndian, w.indexMAC)
	_ = binary.Write(&rawFooter, binary.LittleEndian, w.footerFlags)

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
	if err := w.bufferedWriter.Flush(); err != nil {
		return nil, objects.NilMac, err
	}

	if err := w.f.Sync(); err != nil {
		return nil, objects.NilMac, err
	}

	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, objects.NilMac, err
	}

	return w.f, objects.MAC(w.totalHash.Sum(nil)), nil
}
