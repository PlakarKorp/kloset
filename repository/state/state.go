/*
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
)

const VERSION = "1.1.0"

func init() {
	versioning.Register(resources.RT_STATE, versioning.FromString(VERSION))
}

type EntryType uint8

const (
	ET_METADATA      EntryType = 1
	ET_LOCATIONS     EntryType = 2
	ET_COLOURED      EntryType = 3
	ET_PACKFILE      EntryType = 4
	ET_CONFIGURATION EntryType = 5
	ET_DELETE        EntryType = 6
)

// In the loaded format, both header and trailer are loaded inside the same
// byte array and same column.
// On disk they are split.
type Metadata struct {
	// Header part of the on disk format.
	Parent objects.MAC `msgpack:"parent"`

	// Prelude part of the on disk format.
	Version   versioning.Version `msgpack:"version"`
	Timestamp time.Time          `msgpack:"timestamp"`
	Serial    uuid.UUID          `msgpack:"serial"`
}

type Location struct {
	Packfile objects.MAC
	Offset   uint64
	Length   uint32
}

const LocationSerializedSize = 32 + 8 + 4

type DeltaEntry struct {
	Type     resources.Type
	Version  versioning.Version
	Blob     objects.MAC
	Location Location
	Flags    uint32
}

const DeltaEntrySerializedSize = 1 + 4 + 32 + LocationSerializedSize + 4

type ColouredEntry struct {
	Type resources.Type
	Blob objects.MAC
	When time.Time
}

const ColouredEntrySerializedSize = 1 + 32 + 8

type PackfileEntry struct {
	Packfile  objects.MAC
	StateID   objects.MAC
	Timestamp time.Time
}

const PackfileEntrySerializedSize = 32 + 32 + 8

type ConfigurationEntry struct {
	Key       string
	Value     []byte
	CreatedAt time.Time
}

type DeleteEntry struct {
	Type     EntryType
	BlobType resources.Type
	Blob     objects.MAC
	Packfile objects.MAC
}

const DeleteEntrySerializedSize = 1 + 32 + 1 + 32

/* This needs some more thoughts
func FromStream(rd io.Reader, ver versioning.Version, cache caching.StateCache) (*LocalState, error) {
	st := &LocalState{cache: cache}

	var err error
	if ver.Equals(versioning.FromString("1.1.0")) {
		err = st.deserializeFromStream(rd)
	} else {
		err = st.deserializeFromStreamv100(rd)
	}

	if err != nil {
		return nil, err
	} else {
		return st, nil
	}
}
*/

// Reads only the Header part of the Metadata structure, the rest will be
// uninitialized.
func ReadHeader(rd io.Reader, ver versioning.Version) (*Metadata, error) {
	hdr := &Metadata{}

	if ver.Equals(versioning.FromString("1.1.0")) {
		n, err := rd.Read(hdr.Parent[:])
		if err != nil || n != len(objects.MAC{}) {
			return nil, fmt.Errorf("failed to read header %w", err)
		}
	}

	return hdr, nil
}

func DeleteEntryFromBytes(buf []byte) (del DeleteEntry, err error) {
	if len(buf) < DeleteEntrySerializedSize {
		return del, fmt.Errorf("short read while deserializing delete entry: have %d, want %d", len(buf), DeleteEntrySerializedSize)
	}

	bbuf := bytes.NewBuffer(buf)

	typ, err := bbuf.ReadByte()
	if err != nil {
		return
	}
	del.Type = EntryType(typ)

	typ, err = bbuf.ReadByte()
	if err != nil {
		return
	}
	del.BlobType = resources.Type(typ)

	n, err := bbuf.Read(del.Blob[:])
	if err != nil {
		return
	}
	if n < len(objects.MAC{}) {
		return del, fmt.Errorf("short read while deserializing delete entry")
	}

	n, err = bbuf.Read(del.Packfile[:])
	if err != nil {
		return
	}
	if n < len(objects.MAC{}) {
		return del, fmt.Errorf("short read while deserializing delete entry")
	}

	return
}

func (del *DeleteEntry) _toBytes(buf []byte) {
	pos := 0
	buf[pos] = byte(del.Type)
	pos++

	buf[pos] = byte(del.BlobType)
	pos++

	pos += copy(buf[pos:], del.Blob[:])
	pos += copy(buf[pos:], del.Packfile[:])
}

func (de *DeleteEntry) ToBytes() (ret []byte) {
	ret = make([]byte, DeleteEntrySerializedSize)
	de._toBytes(ret)
	return
}

func DeltaEntryFromBytes(buf []byte) (de DeltaEntry, err error) {
	if len(buf) < DeltaEntrySerializedSize {
		return de, fmt.Errorf("short read while deserializing delta entry: have %d, want %d", len(buf), DeltaEntrySerializedSize)
	}

	bbuf := bytes.NewBuffer(buf)

	typ, err := bbuf.ReadByte()
	if err != nil {
		return
	}

	de.Type = resources.Type(typ)
	de.Version = versioning.Version(binary.LittleEndian.Uint32(bbuf.Next(4)))

	n, err := bbuf.Read(de.Blob[:])
	if err != nil {
		return
	}
	if n < len(objects.MAC{}) {
		return de, fmt.Errorf("short read while deserializing delta entry")
	}

	n, err = bbuf.Read(de.Location.Packfile[:])
	if err != nil {
		return
	}
	if n < len(objects.MAC{}) {
		return de, fmt.Errorf("short read while deserializing delta entry")
	}

	de.Location.Offset = binary.LittleEndian.Uint64(bbuf.Next(8))
	de.Location.Length = binary.LittleEndian.Uint32(bbuf.Next(4))
	de.Flags = binary.LittleEndian.Uint32(bbuf.Next(4))

	return
}

func (de *DeltaEntry) _toBytes(buf []byte) {
	pos := 0
	buf[pos] = byte(de.Type)
	pos++
	binary.LittleEndian.PutUint32(buf[pos:], uint32(de.Version))
	pos += 4

	pos += copy(buf[pos:], de.Blob[:])
	pos += copy(buf[pos:], de.Location.Packfile[:])
	binary.LittleEndian.PutUint64(buf[pos:], de.Location.Offset)
	pos += 8
	binary.LittleEndian.PutUint32(buf[pos:], de.Location.Length)
	pos += 4
	binary.LittleEndian.PutUint32(buf[pos:], de.Flags)
}

func (de *DeltaEntry) ToBytes() (ret []byte) {
	ret = make([]byte, DeltaEntrySerializedSize)
	de._toBytes(ret)
	return
}

func PackfileEntryFromBytes(buf []byte) (pe PackfileEntry, err error) {
	if len(buf) < PackfileEntrySerializedSize {
		return pe, fmt.Errorf("short read while deserializing packfile entry: have %d, want %d", len(buf), PackfileEntrySerializedSize)
	}

	bbuf := bytes.NewBuffer(buf)

	n, err := bbuf.Read(pe.Packfile[:])
	if err != nil {
		return
	}
	if n < len(objects.MAC{}) {
		return pe, fmt.Errorf("Short read while deserializing packfile entry")
	}

	n, err = bbuf.Read(pe.StateID[:])
	if err != nil {
		return
	}
	if n < len(objects.MAC{}) {
		return pe, fmt.Errorf("Short read while deserializing packfile entry")
	}

	timestamp := binary.LittleEndian.Uint64(bbuf.Next(8))
	pe.Timestamp = time.Unix(0, int64(timestamp))

	return
}

func (pe *PackfileEntry) _toBytes(buf []byte) {
	pos := 0
	pos += copy(buf[pos:], pe.Packfile[:])
	pos += copy(buf[pos:], pe.StateID[:])
	binary.LittleEndian.PutUint64(buf[pos:], uint64(pe.Timestamp.UnixNano()))
}

func (pe *PackfileEntry) ToBytes() (ret []byte) {
	ret = make([]byte, PackfileEntrySerializedSize)
	pe._toBytes(ret)
	return
}

func ColouredEntryFromBytes(buf []byte) (de ColouredEntry, err error) {
	if len(buf) < ColouredEntrySerializedSize {
		return de, fmt.Errorf("short read while deserializing coloured entry: have %d, want %d", len(buf), ColouredEntrySerializedSize)
	}
	bbuf := bytes.NewBuffer(buf)

	typ, err := bbuf.ReadByte()
	if err != nil {
		return
	}

	de.Type = resources.Type(typ)

	n, err := bbuf.Read(de.Blob[:])
	if err != nil {
		return
	}
	if n < len(objects.MAC{}) {
		return de, fmt.Errorf("Short read while deserializing coloured entry")
	}

	timestamp := binary.LittleEndian.Uint64(bbuf.Next(8))
	de.When = time.Unix(0, int64(timestamp))

	return
}

func (de *ColouredEntry) _toBytes(buf []byte) {
	pos := 0
	buf[pos] = byte(de.Type)
	pos++

	pos += copy(buf[pos:], de.Blob[:])
	binary.LittleEndian.PutUint64(buf[pos:], uint64(de.When.UnixNano()))
}

func (de *ColouredEntry) ToBytes() (ret []byte) {
	ret = make([]byte, ColouredEntrySerializedSize)
	de._toBytes(ret)
	return
}

// Because it's a variable sized struct we encode it this way:
// - keyLen uint8
// - key [keylen]byte
// - valueLen uint16
// - value [valueLen]byte
// - createdAt uint64
func ConfigurationEntryFromBytes(buf []byte) (ce ConfigurationEntry, err error) {
	const minSize = 1 + 2 + 8
	if len(buf) < minSize {
		return ce, fmt.Errorf("short read while deserializing configuration entry: have %d, want at least %d", len(buf), minSize)
	}

	bbuf := bytes.NewBuffer(buf)

	keyLen, err := bbuf.ReadByte()
	if err != nil {
		return ce, fmt.Errorf("Short read while deserializing keyLen ConfigurationEntry")
	}
	ce.Key = string(bbuf.Next(int(keyLen)))

	valueLen := binary.LittleEndian.Uint16(bbuf.Next(2))
	ce.Value = bbuf.Next(int(valueLen))

	timestamp := binary.LittleEndian.Uint64(bbuf.Next(8))
	ce.CreatedAt = time.Unix(0, int64(timestamp))

	return
}

func (ce *ConfigurationEntry) ToBytes() []byte {
	buf := make([]byte, 1+len(ce.Key)+2+len(ce.Value)+8)
	pos := 0

	buf[pos] = byte(len(ce.Key))
	pos += 1
	pos += copy(buf[pos:], ce.Key)

	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(ce.Value)))
	pos += 2
	pos += copy(buf[pos:], ce.Value)

	binary.LittleEndian.PutUint64(buf[pos:], uint64(ce.CreatedAt.UnixNano()))

	return buf
}

func (mt *Metadata) ToBytes() ([]byte, error) {
	return msgpack.Marshal(mt)
}

func MetadataFromBytes(data []byte) (*Metadata, error) {
	var mt Metadata
	if err := msgpack.Unmarshal(data, &mt); err != nil {
		return nil, err
	}
	return &mt, nil
}

type State struct {
	Metadata Metadata

	configuration map[string]ConfigurationEntry

	cache *caching.ScanCache
}

func (ls *State) PutDelta(de *DeltaEntry) error {
	return ls.cache.PutDelta(de.Type, de.Blob, de.Location.Packfile, de.ToBytes())
}

func (ls *State) DelDelta(Type resources.Type, blobMAC, packfileMAC objects.MAC) error {
	del := DeleteEntry{
		Type:     ET_LOCATIONS,
		BlobType: Type,
		Blob:     blobMAC,
		Packfile: packfileMAC,
	}

	return ls.cache.PutDeleted(uint8(ET_LOCATIONS), blobMAC, del.ToBytes())
}

func (ls *State) PutPackfile(stateId, packfile objects.MAC) error {
	pe := PackfileEntry{
		StateID:   stateId,
		Packfile:  packfile,
		Timestamp: time.Now(),
	}

	return ls.cache.PutPackfile(pe.Packfile, pe.ToBytes())
}

func (ls *State) DelPackfile(packfile objects.MAC) error {
	del := DeleteEntry{
		Type:     ET_PACKFILE,
		BlobType: 0,
		Blob:     objects.NilMac,
		Packfile: packfile,
	}

	return ls.cache.PutDeleted(uint8(ET_PACKFILE), packfile, del.ToBytes())
}

func (ls *State) ColourResource(rtype resources.Type, resource objects.MAC) error {
	de := ColouredEntry{
		Type: rtype,
		Blob: resource,
		When: time.Now(),
	}
	return ls.cache.PutColoured(de.Type, de.Blob, de.ToBytes())
}

func (ls *State) HasColouredResource(rtype resources.Type, resource objects.MAC) (bool, error) {
	return ls.cache.HasColoured(rtype, resource)
}

func (ls *State) DelColouredResource(rtype resources.Type, resourceMAC objects.MAC) error {
	del := DeleteEntry{
		Type:     ET_COLOURED,
		BlobType: rtype,
		Blob:     resourceMAC,
		Packfile: objects.NilMac,
	}

	return ls.cache.PutDeleted(uint8(ET_COLOURED), resourceMAC, del.ToBytes())
}

func (ls *State) NewBatch() caching.StateBatch {
	return ls.cache.NewBatch()
}

func (ls *State) BlobExists(Type resources.Type, blobMAC objects.MAC) bool {
	for _, buf := range ls.cache.GetDelta(Type, blobMAC) {
		de, err := DeltaEntryFromBytes(buf)
		if err != nil {
			continue
		}

		ok, err := ls.cache.HasPackfile(de.Location.Packfile)
		if err != nil {
			continue
		}

		coloured, _ := ls.HasColouredResource(resources.RT_PACKFILE, de.Location.Packfile)
		if ok && !coloured {
			return true
		}
	}

	return false
}

/* On disk format is <Header><EntryType><EntryLength><Entry>...N<Metadata>
 * Counting keys would mean iterating twice so we reverse the format and add a
 * type.
 */
func (ls *State) SerializeToStream(w io.Writer) error {
	writeUint64 := func(value uint64) error {
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, value)
		_, err := w.Write(buf)
		return err
	}

	writeUint32 := func(value uint32) error {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, value)
		_, err := w.Write(buf)
		return err
	}

	// First put the header
	if _, err := w.Write(ls.Metadata.Parent[:]); err != nil {
		return fmt.Errorf("failed to write header parent %w", err)
	}

	for entry := range ls.cache.GetDeletedEntries() {
		if _, err := w.Write([]byte{byte(ET_DELETE)}); err != nil {
			return fmt.Errorf("failed to write delete entry type: %w", err)
		}

		if err := writeUint32(DeleteEntrySerializedSize); err != nil {
			return fmt.Errorf("failed to write delete entry length: %w", err)
		}

		if _, err := w.Write(entry); err != nil {
			return fmt.Errorf("failed to write delete entry: %w", err)
		}
	}

	for _, entry := range ls.cache.GetDeltas() {
		if _, err := w.Write([]byte{byte(ET_LOCATIONS)}); err != nil {
			return fmt.Errorf("failed to write delta entry type: %w", err)
		}

		if err := writeUint32(DeltaEntrySerializedSize); err != nil {
			return fmt.Errorf("failed to write delta entry length: %w", err)
		}

		if _, err := w.Write(entry); err != nil {
			return fmt.Errorf("failed to write delta entry: %w", err)
		}
	}

	for _, entry := range ls.cache.GetColouredEntries() {
		if _, err := w.Write([]byte{byte(ET_COLOURED)}); err != nil {
			return fmt.Errorf("failed to write coloured entry type: %w", err)
		}

		if err := writeUint32(ColouredEntrySerializedSize); err != nil {
			return fmt.Errorf("failed to write coloured entry length: %w", err)
		}

		if _, err := w.Write(entry); err != nil {
			return fmt.Errorf("failed to write coloured entry: %w", err)
		}
	}

	for _, entry := range ls.cache.GetPackfiles() {
		if _, err := w.Write([]byte{byte(ET_PACKFILE)}); err != nil {
			return fmt.Errorf("failed to write packfile entry type: %w", err)
		}

		if err := writeUint32(PackfileEntrySerializedSize); err != nil {
			return fmt.Errorf("failed to write packfile entry length: %w", err)
		}

		if _, err := w.Write(entry); err != nil {
			return fmt.Errorf("failed to write packfile entry: %w", err)
		}
	}

	for entry := range ls.cache.GetConfigurations() {
		if _, err := w.Write([]byte{byte(ET_CONFIGURATION)}); err != nil {
			return fmt.Errorf("failed to write configuration entry type: %w", err)
		}

		if err := writeUint32(uint32(len(entry))); err != nil {
			return fmt.Errorf("failed to write configuration entry length: %w", err)
		}

		if _, err := w.Write(entry); err != nil {
			return fmt.Errorf("failed to write configuration entry: %w", err)
		}
	}

	/* Finally we serialize the Metadata */
	if _, err := w.Write([]byte{byte(ET_METADATA)}); err != nil {
		return fmt.Errorf("failed to write metadata type %w", err)
	}
	if err := writeUint32(uint32(ls.Metadata.Version)); err != nil {
		return fmt.Errorf("failed to write version: %w", err)
	}
	timestamp := ls.Metadata.Timestamp.UnixNano()
	if err := writeUint64(uint64(timestamp)); err != nil {
		return fmt.Errorf("failed to write timestamp: %w", err)
	}
	if _, err := w.Write(ls.Metadata.Serial[:]); err != nil {
		return fmt.Errorf("failed to write serial flag: %w", err)
	}

	return nil

}
