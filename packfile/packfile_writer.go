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

type Record struct {
	Type    resources.Type
	Version versioning.Version
	MAC     objects.MAC
	Offset  uint64
	Length  uint32
	Flags   uint32
}

type Writer interface {
	AddBlob(t resources.Type, v versioning.Version, mac objects.MAC, data []byte, flags uint32) error
	Size() uint32
	Records() []Record
	Finalize() error
	Path() string
	Cleanup() error
}

type FileWriter struct {
	f       *os.File
	bw      *bufio.Writer
	encoder func(io.Reader) (io.Reader, error)
	hasher  hash.Hash

	records []Record
	curOff  uint64

	footerTimestamp int64
	footerFlags     uint32
	indexOffset     uint64
	indexMAC        objects.MAC
}

func NewFileWriter(tempDir string, hf func() hash.Hash, encoder func(io.Reader) (io.Reader, error)) (*FileWriter, error) {
	f, err := os.CreateTemp(tempDir, "plakar-pack-*")
	if err != nil {
		return nil, err
	}
	return &FileWriter{
		f:               f,
		bw:              bufio.NewWriterSize(f, 1<<20),
		encoder:         encoder,
		hasher:          hf(),
		footerTimestamp: time.Now().Unix(),
	}, nil
}

func (w *FileWriter) AddBlob(t resources.Type, v versioning.Version, mac objects.MAC, data []byte, flags uint32) error {
	encR, err := w.encoder(bytes.NewReader(data))
	if err != nil {
		return err
	}
	n, err := io.Copy(w.bw, encR)
	if err != nil {
		return err
	}
	w.records = append(w.records, Record{
		Type: t, Version: v, MAC: mac,
		Offset: w.curOff, Length: uint32(n), Flags: flags,
	})
	w.curOff += uint64(n)
	w.indexOffset = w.curOff
	return nil
}

func (w *FileWriter) Size() uint32      { return uint32(w.curOff) }
func (w *FileWriter) Records() []Record { return w.records }
func (w *FileWriter) Path() string      { return w.f.Name() }
func (w *FileWriter) Cleanup() error    { _ = w.f.Close(); return os.Remove(w.f.Name()) }

func (w *FileWriter) Finalize() error {
	var rawIdx bytes.Buffer
	for i := range w.records {
		r := &w.records[i]
		_ = binary.Write(&rawIdx, binary.LittleEndian, r.Type)
		_ = binary.Write(&rawIdx, binary.LittleEndian, r.Version)
		_ = binary.Write(&rawIdx, binary.LittleEndian, r.MAC)
		_ = binary.Write(&rawIdx, binary.LittleEndian, r.Offset)
		_ = binary.Write(&rawIdx, binary.LittleEndian, r.Length)
		_ = binary.Write(&rawIdx, binary.LittleEndian, r.Flags)
	}
	w.hasher.Reset()
	if _, err := w.hasher.Write(rawIdx.Bytes()); err != nil {
		return err
	}
	copy(w.indexMAC[:], w.hasher.Sum(nil))

	encIdx, err := w.encoder(bytes.NewReader(rawIdx.Bytes()))
	if err != nil {
		return err
	}
	if _, err := io.Copy(w.bw, encIdx); err != nil {
		return err
	}

	var rawFooter bytes.Buffer
	_ = binary.Write(&rawFooter, binary.LittleEndian, w.footerTimestamp)
	_ = binary.Write(&rawFooter, binary.LittleEndian, uint32(len(w.records)))
	_ = binary.Write(&rawFooter, binary.LittleEndian, w.indexOffset)
	_ = binary.Write(&rawFooter, binary.LittleEndian, w.indexMAC)
	_ = binary.Write(&rawFooter, binary.LittleEndian, w.footerFlags)

	encFooter, err := w.encoder(bytes.NewReader(rawFooter.Bytes()))
	if err != nil {
		return err
	}
	n, err := io.Copy(w.bw, encFooter)
	if err != nil {
		return err
	}

	var lenLE [4]byte
	binary.LittleEndian.PutUint32(lenLE[:], uint32(n))
	if _, err := w.bw.Write(lenLE[:]); err != nil {
		return err
	}

	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}
