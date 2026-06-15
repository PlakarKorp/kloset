package packfile

import (
	"encoding/binary"
	"io"

	"github.com/PlakarKorp/kloset/objects"
)

// This file holds the shared, low-level (de)serialization helpers used by both
// the in-memory and on-disk packfile implementations. Keeping the per-field
// binary.Write / binary.Read calls in one place removes the large blocks of
// duplicated marshalling logic that previously lived in each Serialize /
// FromBytes function, and gives those error returns a single testable seam.

// writeBlobRecord serializes a single index entry to w in the canonical
// little-endian packfile layout: Type, Version, MAC, Offset, Length, Flags.
func writeBlobRecord(w io.Writer, b Blob) error {
	if err := binary.Write(w, binary.LittleEndian, b.Type); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, b.Version); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, b.MAC); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, b.Offset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, b.Length); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, b.Flags); err != nil {
		return err
	}
	return nil
}

// readBlobRecord reads a single index entry from r, the inverse of
// writeBlobRecord.
func readBlobRecord(r io.Reader) (Blob, error) {
	var b Blob
	if err := binary.Read(r, binary.LittleEndian, &b.Type); err != nil {
		return b, err
	}
	if err := binary.Read(r, binary.LittleEndian, &b.Version); err != nil {
		return b, err
	}
	if err := binary.Read(r, binary.LittleEndian, &b.MAC); err != nil {
		return b, err
	}
	if err := binary.Read(r, binary.LittleEndian, &b.Offset); err != nil {
		return b, err
	}
	if err := binary.Read(r, binary.LittleEndian, &b.Length); err != nil {
		return b, err
	}
	if err := binary.Read(r, binary.LittleEndian, &b.Flags); err != nil {
		return b, err
	}
	return b, nil
}

// writeFooterFields serializes the footer payload (everything except the
// trailing encoded-footer-length) to w: Timestamp, Count, IndexOffset,
// IndexMAC, Flags.
func writeFooterFields(w io.Writer, timestamp int64, count uint32, indexOffset uint64, indexMAC objects.MAC, flags uint32) error {
	if err := binary.Write(w, binary.LittleEndian, timestamp); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, count); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, indexOffset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, indexMAC); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, flags); err != nil {
		return err
	}
	return nil
}

// readFooterFields reads the footer payload into f, the inverse of
// writeFooterFields. The caller is responsible for setting f.Version.
func readFooterFields(r io.Reader, f *PackfileInMemoryFooter) error {
	if err := binary.Read(r, binary.LittleEndian, &f.Timestamp); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &f.Count); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &f.IndexOffset); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &f.IndexMAC); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &f.Flags); err != nil {
		return err
	}
	return nil
}
