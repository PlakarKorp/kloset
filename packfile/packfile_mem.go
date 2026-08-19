package packfile

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
)

type PackfileInMemory struct {
	hasher hash.Hash
	Blobs  []byte
	Index  []Blob
	Footer PackfileInMemoryFooter

	hot bool
}

type PackfileInMemoryFooter struct {
	Version     versioning.Version `msgpack:"-"`
	Timestamp   int64
	Count       uint32
	IndexOffset uint64
	IndexMAC    objects.MAC
	Flags       uint32
}

const FOOTER_SIZE = 56

func NewInMemoryFooterFromBytes(version versioning.Version, serialized []byte) (PackfileInMemoryFooter, error) {
	var footer PackfileInMemoryFooter

	reader := bytes.NewReader(serialized)
	footer.Version = version
	if err := readFooterFields(reader, &footer); err != nil {
		return footer, err
	}
	return footer, nil
}

func NewInMemoryIndexFromBytes(version versioning.Version, serialized []byte) ([]Blob, error) {
	reader := bytes.NewReader(serialized)
	index := make([]Blob, 0)
	for reader.Len() > 0 {
		blob, err := readBlobRecord(reader)
		if err != nil {
			return nil, err
		}
		index = append(index, blob)
	}
	return index, nil
}

func NewPackfileInMemory(hf HashFactory) (Packfile, error) {
	return newPackfileInMemory(hf()), nil
}

// Compat constructor, the above one is the public API.
func newPackfileInMemory(hasher hash.Hash) Packfile {
	return &PackfileInMemory{
		hasher: hasher,
		Blobs:  make([]byte, 0),
		Index:  make([]Blob, 0),
		Footer: PackfileInMemoryFooter{
			Version:   versioning.FromString(VERSION),
			Timestamp: time.Now().UnixNano(),
			Count:     0,
		},
	}
}

func NewPackfileInMemoryFromBytes(hasher hash.Hash, version versioning.Version, serialized []byte) (*PackfileInMemory, error) {
	reader := bytes.NewReader(serialized)
	var footer PackfileInMemoryFooter
	_, err := reader.Seek(-FOOTER_SIZE, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	footer.Version = version

	if err := readFooterFields(reader, &footer); err != nil {
		return nil, err
	}

	_, err = reader.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	data := make([]byte, footer.IndexOffset)
	if err := binary.Read(reader, binary.LittleEndian, data); err != nil {
		return nil, err
	}

	// we won't read the totalLength again
	remaining := reader.Len() - FOOTER_SIZE

	// Fake factory, we know it's safe.
	p := newPackfileInMemory(hasher).(*PackfileInMemory)
	p.Footer = footer
	p.Blobs = data
	p.hasher.Reset()
	for remaining > 0 {
		blob, err := readBlobRecord(reader)
		if err != nil {
			return nil, err
		}

		if blob.Offset+uint64(blob.Length) > p.Footer.IndexOffset {
			return nil, fmt.Errorf("blob offset + blob length exceeds total length of packfile")
		}

		// p.hasher is a hash.Hash; writing to it cannot fail.
		_ = writeBlobRecord(p.hasher, blob)
		p.Index = append(p.Index, blob)
		remaining -= BLOB_RECORD_SIZE
	}
	mac := objects.MAC(p.hasher.Sum(nil))
	if mac != p.Footer.IndexMAC {
		return nil, fmt.Errorf("index mac mismatch")
	}

	return p, nil
}

func (p *PackfileInMemory) Serialize(encoder EncodingFn) (io.Reader, objects.MAC, error) {
	buffer := &bytes.Buffer{}
	mac, err := p.serializeTo(buffer, encoder)
	if err != nil {
		return nil, objects.NilMac, err
	}
	return buffer, mac, nil
}

// serializeTo writes the full packfile byte stream to dst and returns the
// total MAC. It is split out from Serialize so the destination writer can be
// substituted in tests to exercise the write-error branches that a
// bytes.Buffer can never trigger on its own.
func (p *PackfileInMemory) serializeTo(dst io.Writer, encoder EncodingFn) (objects.MAC, error) {
	// We need an hash of the index, so let's first calculate that.
	idxBuffer := &bytes.Buffer{}
	p.hasher.Reset()
	idxWriter := io.MultiWriter(idxBuffer, p.hasher)
	for _, blob := range p.Index {
		// idxWriter wraps only in-memory writers (bytes.Buffer + hash.Hash),
		// so this cannot fail.
		_ = writeBlobRecord(idxWriter, blob)
	}
	p.Footer.IndexMAC = objects.MAC(p.hasher.Sum(nil))

	// Now reset hasher and start constructing the final file format.
	// First the data.
	teeWriter := io.MultiWriter(dst, p.hasher)

	if err := binary.Write(teeWriter, binary.LittleEndian, p.Blobs); err != nil {
		return objects.NilMac, err
	}

	// Second the encoded index.
	encodedIdx, err := encoder(idxBuffer)
	if err != nil {
		return objects.NilMac, err
	}

	scratchPad := make([]byte, 1024)
	if _, err := io.CopyBuffer(teeWriter, encodedIdx, scratchPad); err != nil {
		return objects.NilMac, err
	}

	footerBuffer := &bytes.Buffer{}
	// footerBuffer is an in-memory buffer; this write cannot fail.
	_ = writeFooterFields(footerBuffer, p.Footer.Timestamp, p.Footer.Count, p.Footer.IndexOffset, p.Footer.IndexMAC, p.Footer.Flags)

	// Third the encoded footer
	encodedFooter, err := encoder(footerBuffer)
	if err != nil {
		return objects.NilMac, err
	}

	footerLen, err := io.CopyBuffer(teeWriter, encodedFooter, scratchPad)
	if err != nil {
		return objects.NilMac, err
	}

	// Finally write the encoded footer length.
	if err := binary.Write(teeWriter, binary.LittleEndian, uint32(footerLen)); err != nil {
		return objects.NilMac, err
	}

	return objects.MAC(p.hasher.Sum(nil)), nil
}

func (p *PackfileInMemory) AddBlob(resourceType resources.Type, version versioning.Version, mac objects.MAC, data []byte, flags uint32) error {
	p.Index = append(p.Index, Blob{
		Type:    resourceType,
		Version: version,
		MAC:     mac,
		Offset:  uint64(len(p.Blobs)),
		Length:  uint32(len(data)),
		Flags:   flags,
	})
	p.Blobs = append(p.Blobs, data...)
	p.Footer.Count++
	p.Footer.IndexOffset = uint64(len(p.Blobs))

	return nil
}

func (p *PackfileInMemory) Entries() []Blob {
	return p.Index
}

func (p *PackfileInMemory) Size() uint64 {
	return uint64(len(p.Blobs))
}

func (p *PackfileInMemory) Cleanup() error {
	return nil
}

// Keeping this one only for testing purpose...
func (p *PackfileInMemory) getBlob(mac objects.MAC) ([]byte, bool) {
	for _, blob := range p.Index {
		if blob.MAC == mac {
			return p.Blobs[blob.Offset : blob.Offset+uint64(blob.Length)], true
		}
	}
	return nil, false
}

func (p *PackfileInMemory) SetHot() {
	p.hot = true
}

func (p *PackfileInMemory) Hot() bool {
	return p.hot
}
