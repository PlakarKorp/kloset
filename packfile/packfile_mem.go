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
	if err := binary.Read(reader, binary.LittleEndian, &footer.Timestamp); err != nil {
		return footer, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.Count); err != nil {
		return footer, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.IndexOffset); err != nil {
		return footer, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.IndexMAC); err != nil {
		return footer, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.Flags); err != nil {
		return footer, err
	}
	return footer, nil
}

func NewInMemoryIndexFromBytes(version versioning.Version, serialized []byte) ([]Blob, error) {
	reader := bytes.NewReader(serialized)
	index := make([]Blob, 0)
	for reader.Len() > 0 {
		var resourceType resources.Type
		var resourceVersion versioning.Version
		var mac objects.MAC
		var blobOffset uint64
		var blobLength uint32
		var blobFlags uint32

		if err := binary.Read(reader, binary.LittleEndian, &resourceType); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &resourceVersion); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &mac); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &blobOffset); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &blobLength); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &blobFlags); err != nil {
			return nil, err
		}
		index = append(index, Blob{
			Type:    resourceType,
			Version: resourceVersion,
			MAC:     mac,
			Offset:  blobOffset,
			Length:  blobLength,
			Flags:   blobFlags,
		})
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

	if err := binary.Read(reader, binary.LittleEndian, &footer.Timestamp); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.Count); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.IndexOffset); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.IndexMAC); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, binary.LittleEndian, &footer.Flags); err != nil {
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
		var resourceType resources.Type
		var resourceVersion versioning.Version
		var mac objects.MAC
		var blobOffset uint64
		var blobLength uint32
		var blobFlags uint32

		if err := binary.Read(reader, binary.LittleEndian, &resourceType); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &resourceVersion); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &mac); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &blobOffset); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &blobLength); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.LittleEndian, &blobFlags); err != nil {
			return nil, err
		}

		if blobOffset+uint64(blobLength) > p.Footer.IndexOffset {
			return nil, fmt.Errorf("blob offset + blob length exceeds total length of packfile")
		}

		if err := binary.Write(p.hasher, binary.LittleEndian, resourceType); err != nil {
			return nil, err
		}
		if err := binary.Write(p.hasher, binary.LittleEndian, resourceVersion); err != nil {
			return nil, err
		}
		if err := binary.Write(p.hasher, binary.LittleEndian, mac); err != nil {
			return nil, err
		}
		if err := binary.Write(p.hasher, binary.LittleEndian, blobOffset); err != nil {
			return nil, err
		}
		if err := binary.Write(p.hasher, binary.LittleEndian, blobLength); err != nil {
			return nil, err
		}
		if err := binary.Write(p.hasher, binary.LittleEndian, blobFlags); err != nil {
			return nil, err
		}
		p.Index = append(p.Index, Blob{
			Type:    resourceType,
			Version: resourceVersion,
			MAC:     mac,
			Offset:  blobOffset,
			Length:  blobLength,
			Flags:   blobFlags,
		})
		remaining -= BLOB_RECORD_SIZE
	}
	mac := objects.MAC(p.hasher.Sum(nil))
	if mac != p.Footer.IndexMAC {
		return nil, fmt.Errorf("index mac mismatch")
	}

	return p, nil
}

func (p *PackfileInMemory) Serialize(encoder EncodingFn) (io.Reader, objects.MAC, error) {
	// We need an hash of the index, so let's first calculate that.
	idxBuffer := &bytes.Buffer{}
	p.hasher.Reset()
	for _, blob := range p.Index {
		if err := binary.Write(idxBuffer, binary.LittleEndian, blob.Type); err != nil {
			return nil, objects.NilMac, err
		}
		if err := binary.Write(idxBuffer, binary.LittleEndian, blob.Version); err != nil {
			return nil, objects.NilMac, err
		}
		if err := binary.Write(idxBuffer, binary.LittleEndian, blob.MAC); err != nil {
			return nil, objects.NilMac, err
		}
		if err := binary.Write(idxBuffer, binary.LittleEndian, blob.Offset); err != nil {
			return nil, objects.NilMac, err
		}
		if err := binary.Write(idxBuffer, binary.LittleEndian, blob.Length); err != nil {
			return nil, objects.NilMac, err
		}
		if err := binary.Write(idxBuffer, binary.LittleEndian, blob.Flags); err != nil {
			return nil, objects.NilMac, err
		}

		if err := binary.Write(p.hasher, binary.LittleEndian, blob.Type); err != nil {
			return nil, objects.NilMac, err
		}
		if err := binary.Write(p.hasher, binary.LittleEndian, blob.Version); err != nil {
			return nil, objects.NilMac, err
		}
		if err := binary.Write(p.hasher, binary.LittleEndian, blob.MAC); err != nil {
			return nil, objects.NilMac, err
		}
		if err := binary.Write(p.hasher, binary.LittleEndian, blob.Offset); err != nil {
			return nil, objects.NilMac, err
		}
		if err := binary.Write(p.hasher, binary.LittleEndian, blob.Length); err != nil {
			return nil, objects.NilMac, err
		}
		if err := binary.Write(p.hasher, binary.LittleEndian, blob.Flags); err != nil {
			return nil, objects.NilMac, err
		}
	}
	p.Footer.IndexMAC = objects.MAC(p.hasher.Sum(nil))

	// Now reset hasher and start constructing the final file format.
	// First the data.
	buffer := &bytes.Buffer{}
	teeWriter := io.MultiWriter(buffer, p.hasher)

	if err := binary.Write(teeWriter, binary.LittleEndian, p.Blobs); err != nil {
		return nil, objects.NilMac, err
	}

	// Second the encoded index.
	encodedIdx, err := encoder(idxBuffer)
	if err != nil {
		return nil, objects.NilMac, err
	}

	scratchPad := make([]byte, 1024)
	if _, err := io.CopyBuffer(teeWriter, encodedIdx, scratchPad); err != nil {
		return nil, objects.NilMac, err
	}

	footerBuffer := &bytes.Buffer{}
	if err := binary.Write(footerBuffer, binary.LittleEndian, p.Footer.Timestamp); err != nil {
		return nil, objects.NilMac, err
	}
	if err := binary.Write(footerBuffer, binary.LittleEndian, p.Footer.Count); err != nil {
		return nil, objects.NilMac, err
	}
	if err := binary.Write(footerBuffer, binary.LittleEndian, p.Footer.IndexOffset); err != nil {
		return nil, objects.NilMac, err
	}
	if err := binary.Write(footerBuffer, binary.LittleEndian, p.Footer.IndexMAC); err != nil {
		return nil, objects.NilMac, err
	}
	if err := binary.Write(footerBuffer, binary.LittleEndian, p.Footer.Flags); err != nil {
		return nil, objects.NilMac, err
	}

	// Third the encoded footer
	encodedFooter, err := encoder(footerBuffer)
	if err != nil {
		return nil, objects.NilMac, err
	}

	footerLen, err := io.CopyBuffer(teeWriter, encodedFooter, scratchPad)
	if err != nil {
		return nil, objects.NilMac, err
	}

	// Finally write the encoded footer length.
	if err := binary.Write(teeWriter, binary.LittleEndian, uint32(footerLen)); err != nil {
		return nil, objects.NilMac, err
	}

	return buffer, objects.MAC(p.hasher.Sum(nil)), nil
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
