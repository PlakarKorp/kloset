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
	"encoding/binary"
	"fmt"
	"io"
	"iter"
	"time"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/google/uuid"
)

// A local version of the state, possibly aggregated, that uses on-disk storage.
//   - States are stored under a dedicated prefix key, with their data being the
//     state's metadata.
//   - Delta entries are stored under another dedicated prefix and are keyed by
//     their issuing state.
type LocalState struct {
	Metadata Metadata

	// Contains live configuration values (most up to date loaded from
	// repository state), or when in a derived State contains configurations
	// about to be pushed to the repository.
	configuration map[string]ConfigurationEntry

	// DeltaEntries are keyed by <EntryType>:<EntryCsum>:<StateID> in the cache.
	// This allows:
	//  - Grouping and iterating on them by Type.
	//  - Finding a particular Csum efficiently if you know the type.
	//  - Somewhat fast key retrieval if you only know the Csum (something we
	//    don't need right now).
	//  - StateID is there at the end because we don't need to query by it but
	//    we need it to avoid concurrent insert of the same entry by two
	//    different backup processes.
	cache *caching.SQLState
}

// XXX: Needs a big refactoring to split this into three different concepts:
// 1- A "LocalState" representing an unitary state but loaded in cache.
// 2- The local aggregated state (aka the collection of "LocalState")
// 3- A delta state, which is a special version of the LocalState that is being
// mutated in order to be serialized.
func NewLocalState(cache *caching.SQLState) (*LocalState, error) {
	// Sadly we have to ignore the error here because:
	// 1- If we are on a new repository, the database schema hasn't been created
	// yet, leading to an error.
	// 2- Sadly the error used is generic (SQL logic error) with a custom string,
	// so we just can't match on a specific error, hence we ignore everything.
	// It's safe to ignore everything here, worst case we have no parent, and
	// it'll fail right after on the first usage of sqlite.
	parentState, _ := cache.GetLatestState()

	return &LocalState{
		Metadata: Metadata{
			Parent:    parentState,
			Version:   versioning.FromString(VERSION),
			Timestamp: time.Now(),
		},
		configuration: make(map[string]ConfigurationEntry),
		cache:         cache,
	}, nil
}

// Derive constructs a new state backed by *cache*, keeping the same serial as previous one.
// Mainly used to construct Delta states when backing up.
func (ls *LocalState) Derive(cache *caching.ScanCache) *State {
	return &State{
		Metadata: Metadata{
			Parent:    ls.Metadata.Parent,
			Version:   versioning.FromString(VERSION),
			Timestamp: time.Now(),
			Serial:    ls.Metadata.Serial,
		},
		configuration: make(map[string]ConfigurationEntry),
		cache:         cache,
	}
}

// Finds the latest (current) serial in the aggregate state, and if none sets
// it to the provided one.
func (ls *LocalState) UpdateSerialOr(serial uuid.UUID) error {
	var latestID *objects.MAC = nil
	var latestMT *Metadata = nil

	states, err := ls.cache.GetStates()
	if err != nil {
		return err
	}

	for stateID, buf := range states {
		mt, err := MetadataFromBytes(buf)

		if err != nil {
			return err
		}

		if latestID == nil || latestMT.Timestamp.Before(mt.Timestamp) {
			latestID = &stateID
			latestMT = mt
		}
	}

	if latestMT != nil {
		ls.Metadata.Serial = latestMT.Serial
	} else {
		ls.Metadata.Serial = serial
	}

	return nil
}

/* Insert the state denotated by stateID and its associated delta entries read
 * from rd into the local aggregated version of the state. */
func (ls *LocalState) MergeState(stateID objects.MAC, rd io.Reader, ver versioning.Version) error {
	has, err := ls.HasState(stateID)
	if err != nil {
		return err
	}

	if has {
		return nil
	}

	// This implicitly sets the parent, see the note about refactoring, and
	// since we Derive() to construct Delta streams this will set the correct
	// parent. This is all way too intricated and will be fixed by a refacto.
	if ver.Equals(versioning.FromString("1.1.0")) {
		err = ls.deserializeFromStream(rd)
	} else {
		err = ls.deserializeFromStreamv100(rd)
	}
	if err != nil {
		return err
	}

	/* We merged the state deltas, we can now publish it */
	return ls.PutState(stateID)
}

func (ls *LocalState) MergeStateFromCache(stateID objects.MAC, from caching.StateCache) error {
	has, err := ls.HasState(stateID)
	if err != nil {
		return err
	}

	if has {
		return nil
	}

	err = ls.mergeFromCache(from)
	if err != nil {
		return err
	}

	/* We merged the state deltas, we can now publish it */
	return ls.PutState(stateID)
}

/* Publishes the current state, by saving the stateID with the current Metadata. */
func (ls *LocalState) PutState(stateID objects.MAC) error {
	mt, err := ls.Metadata.ToBytes()
	if err != nil {
		return err
	}

	err = ls.cache.PutState(stateID, mt)
	if err != nil {
		return err
	}

	return nil
}

func (ls *LocalState) GetStates() (map[objects.MAC][]byte, error) {
	return ls.cache.GetStates()
}

/* On disk format is <Header><EntryType><EntryLength><Entry>...N<Metadata>
 * Counting keys would mean iterating twice so we reverse the format and add a
 * type.
 */
func (ls *LocalState) SerializeToStream(w io.Writer) error {
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

func (ls *LocalState) deserializeFromStream(r io.Reader) error {
	readUint64 := func() (uint64, error) {
		buf := make([]byte, 8)
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint64(buf), nil
	}

	readUint32 := func() (uint32, error) {
		buf := make([]byte, 4)
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint32(buf), nil
	}

	n, err := r.Read(ls.Metadata.Parent[:])
	if err != nil || n != len(objects.MAC{}) {
		return fmt.Errorf("failed to read header %w", err)
	}

	/* Deserialize LOCATIONS */
	et_buf := make([]byte, 1)
	de_buf := make([]byte, DeltaEntrySerializedSize)
	del_buf := make([]byte, DeleteEntrySerializedSize)
	coloured_buf := make([]byte, ColouredEntrySerializedSize)
	pe_buf := make([]byte, PackfileEntrySerializedSize)
	for {
		n, err := r.Read(et_buf)
		if err != nil || n != len(et_buf) {
			return fmt.Errorf("failed to read entry type %w", err)
		}

		entryType := EntryType(et_buf[0])
		if entryType == ET_METADATA {
			break
		}

		length, err := readUint32()
		if err != nil {
			return fmt.Errorf("failed to read entry length %w", err)
		}

		//XXX: This is screaming refactorization, but is a bit subtil.
		switch entryType {
		case ET_DELETE:
			if length != DeleteEntrySerializedSize {
				return fmt.Errorf("failed to read delete entry wrong length got(%d)/expected(%d)", length, DeltaEntrySerializedSize)
			}

			if n, err := io.ReadFull(r, del_buf); err != nil {
				return fmt.Errorf("failed to read delete entry %w, read(%d)/expected(%d)", err, n, length)
			}

			toDel, err := DeleteEntryFromBytes(del_buf)
			if err != nil {
				return fmt.Errorf("failed to deserialize delta entry %w", err)
			}

			switch toDel.Type {
			case ET_LOCATIONS:
				ls.cache.DelDelta(toDel.BlobType, toDel.Blob, toDel.Packfile)
			case ET_COLOURED:
				ls.cache.DelColoured(toDel.BlobType, toDel.Blob)
			case ET_PACKFILE:
				ls.cache.DelPackfile(toDel.Packfile)
			default:
				return fmt.Errorf("invalid delete Type %d", toDel.Type)
			}
		case ET_LOCATIONS:
			if length != DeltaEntrySerializedSize {
				return fmt.Errorf("failed to read delta entry wrong length got(%d)/expected(%d)", length, DeltaEntrySerializedSize)
			}

			if n, err := io.ReadFull(r, de_buf); err != nil {
				return fmt.Errorf("failed to read delta entry %w, read(%d)/expected(%d)", err, n, length)
			}

			// We need to decode just to make the key, but we can reuse the buffer
			// to put inside the data part of the cache.
			delta, err := DeltaEntryFromBytes(de_buf)
			if err != nil {
				return fmt.Errorf("failed to deserialize delta entry %w", err)
			}

			if err := ls.cache.PutDelta(delta.Type, delta.Blob, delta.Location.Packfile, de_buf); err != nil {
				return err
			}

		case ET_COLOURED:
			if length != ColouredEntrySerializedSize {
				return fmt.Errorf("failed to read coloured entry wrong length got(%d)/expected(%d)", length, ColouredEntrySerializedSize)
			}

			if n, err := io.ReadFull(r, coloured_buf); err != nil {
				return fmt.Errorf("failed to read coloured entry %w, read(%d)/expected(%d)", err, n, length)
			}

			coloured, err := ColouredEntryFromBytes(coloured_buf)
			if err != nil {
				return fmt.Errorf("failed to deserialize coloured entry %w", err)
			}

			if err := ls.cache.PutColoured(coloured.Type, coloured.Blob, coloured_buf); err != nil {
				return err
			}
		case ET_PACKFILE:
			if length != PackfileEntrySerializedSize {
				return fmt.Errorf("failed to read packfile entry wrong length got(%d)/expected(%d)", length, PackfileEntrySerializedSize)
			}

			if n, err := io.ReadFull(r, pe_buf); err != nil {
				return fmt.Errorf("failed to read packfile entry %w, read(%d)/expected(%d)", err, n, length)
			}

			pe, err := PackfileEntryFromBytes(pe_buf)
			if err != nil {
				return fmt.Errorf("failed to deserialize packfile entry %w", err)
			}

			if err := ls.cache.PutPackfile(pe.Packfile, pe_buf); err != nil {
				return err
			}

		case ET_CONFIGURATION:
			ce_buf := make([]byte, length)

			if n, err := io.ReadFull(r, ce_buf); err != nil {
				return fmt.Errorf("failed to read configuration entry %w, read(%d)/expected(%d)", err, n, length)
			}

			ce, err := ConfigurationEntryFromBytes(ce_buf)
			if err != nil {
				return fmt.Errorf("failed to deserialize configuration entry %w", err)
			}

			err = ls.insertOrUpdateConfiguration(ce)
			if err != nil {
				return fmt.Errorf("failed to insert/update configuration entry %w", err)
			}
		default:
			// Our version doesn't know this entry type, just skip it.
			io.CopyN(io.Discard, r, int64(length))
		}
	}

	/* Deserialize Metadata */
	version, err := readUint32()
	if err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}
	ls.Metadata.Version = versioning.Version(version)

	timestamp, err := readUint64()
	if err != nil {
		return fmt.Errorf("failed to read timestamp: %w", err)
	}
	ls.Metadata.Timestamp = time.Unix(0, int64(timestamp))

	serial := make([]byte, len(uuid.UUID{}))
	if _, err := io.ReadFull(r, serial); err != nil {
		return fmt.Errorf("failed to read serial: %w", err)
	}
	ls.Metadata.Serial = uuid.UUID(serial)

	return nil
}

func (ls *LocalState) deserializeFromStreamv100(r io.Reader) error {
	readUint64 := func() (uint64, error) {
		buf := make([]byte, 8)
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint64(buf), nil
	}

	readUint32 := func() (uint32, error) {
		buf := make([]byte, 4)
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		return binary.LittleEndian.Uint32(buf), nil
	}

	/* Deserialize LOCATIONS */
	et_buf := make([]byte, 1)
	de_buf := make([]byte, DeltaEntrySerializedSize)
	coloured_buf := make([]byte, ColouredEntrySerializedSize)
	pe_buf := make([]byte, PackfileEntrySerializedSize)
	for {
		n, err := r.Read(et_buf)
		if err != nil || n != len(et_buf) {
			return fmt.Errorf("failed to read entry type %w", err)
		}

		entryType := EntryType(et_buf[0])
		if entryType == ET_METADATA {
			break
		}

		length, err := readUint32()
		if err != nil {
			return fmt.Errorf("failed to read entry length %w", err)
		}

		//XXX: This is screaming refactorization, but is a bit subtil.
		switch entryType {
		case ET_LOCATIONS:
			if length != DeltaEntrySerializedSize {
				return fmt.Errorf("failed to read delta entry wrong length got(%d)/expected(%d)", length, DeltaEntrySerializedSize)
			}

			if n, err := io.ReadFull(r, de_buf); err != nil {
				return fmt.Errorf("failed to read delta entry %w, read(%d)/expected(%d)", err, n, length)
			}

			// We need to decode just to make the key, but we can reuse the buffer
			// to put inside the data part of the cache.
			delta, err := DeltaEntryFromBytes(de_buf)
			if err != nil {
				return fmt.Errorf("failed to deserialize delta entry %w", err)
			}

			if err := ls.cache.PutDelta(delta.Type, delta.Blob, delta.Location.Packfile, de_buf); err != nil {
				return err
			}

		case ET_COLOURED:
			if length != ColouredEntrySerializedSize {
				return fmt.Errorf("failed to read coloured entry wrong length got(%d)/expected(%d)", length, ColouredEntrySerializedSize)
			}

			if n, err := io.ReadFull(r, coloured_buf); err != nil {
				return fmt.Errorf("failed to read coloured entry %w, read(%d)/expected(%d)", err, n, length)
			}

			coloured, err := ColouredEntryFromBytes(coloured_buf)
			if err != nil {
				return fmt.Errorf("failed to deserialize coloured entry %w", err)
			}

			if err := ls.cache.PutColoured(coloured.Type, coloured.Blob, coloured_buf); err != nil {
				return err
			}
		case ET_PACKFILE:
			if length != PackfileEntrySerializedSize {
				return fmt.Errorf("failed to read packfile entry wrong length got(%d)/expected(%d)", length, PackfileEntrySerializedSize)
			}

			if n, err := io.ReadFull(r, pe_buf); err != nil {
				return fmt.Errorf("failed to read packfile entry %w, read(%d)/expected(%d)", err, n, length)
			}

			pe, err := PackfileEntryFromBytes(pe_buf)
			if err != nil {
				return fmt.Errorf("failed to deserialize packfile entry %w", err)
			}

			if err := ls.cache.PutPackfile(pe.Packfile, pe_buf); err != nil {
				return err
			}

		case ET_CONFIGURATION:
			ce_buf := make([]byte, length)

			if n, err := io.ReadFull(r, ce_buf); err != nil {
				return fmt.Errorf("failed to read configuration entry %w, read(%d)/expected(%d)", err, n, length)
			}

			ce, err := ConfigurationEntryFromBytes(ce_buf)
			if err != nil {
				return fmt.Errorf("failed to deserialize configuration entry %w", err)
			}

			err = ls.insertOrUpdateConfiguration(ce)
			if err != nil {
				return fmt.Errorf("failed to insert/update configuration entry %w", err)
			}
		default:
			// Our version doesn't know this entry type, just skip it.
			io.CopyN(io.Discard, r, int64(length))
		}
	}

	/* Deserialize Metadata */
	version, err := readUint32()
	if err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}
	ls.Metadata.Version = versioning.Version(version)

	timestamp, err := readUint64()
	if err != nil {
		return fmt.Errorf("failed to read timestamp: %w", err)
	}
	ls.Metadata.Timestamp = time.Unix(0, int64(timestamp))

	serial := make([]byte, len(uuid.UUID{}))
	if _, err := io.ReadFull(r, serial); err != nil {
		return fmt.Errorf("failed to read serial: %w", err)
	}
	ls.Metadata.Serial = uuid.UUID(serial)

	return nil
}

func (ls *LocalState) mergeFromCache(from caching.StateCache) error {
	for _, entry := range from.GetDeltas() {
		delta, err := DeltaEntryFromBytes(entry)
		if err != nil {
			return fmt.Errorf("failed to deserialize delta entry %w", err)
		}

		if err := ls.cache.PutDelta(delta.Type, delta.Blob, delta.Location.Packfile, entry); err != nil {
			return err
		}
	}

	for _, coloured_buf := range from.GetColouredEntries() {
		coloured, err := ColouredEntryFromBytes(coloured_buf)
		if err != nil {
			return fmt.Errorf("failed to deserialize coloured entry %w", err)
		}

		if err := ls.cache.PutColoured(coloured.Type, coloured.Blob, coloured_buf); err != nil {
			return err
		}
	}

	for _, pe_buf := range from.GetPackfiles() {
		pe, err := PackfileEntryFromBytes(pe_buf)
		if err != nil {
			return fmt.Errorf("failed to deserialize packfile entry %w", err)
		}

		if err := ls.cache.PutPackfile(pe.Packfile, pe_buf); err != nil {
			return err
		}
	}

	for ce_buf := range from.GetConfigurations() {
		ce, err := ConfigurationEntryFromBytes(ce_buf)
		if err != nil {
			return fmt.Errorf("failed to deserialize configuration entry %w", err)
		}

		err = ls.insertOrUpdateConfiguration(ce)
		if err != nil {
			return fmt.Errorf("failed to insert/update configuration entry %w", err)
		}
	}

	return nil
}

func (ls *LocalState) HasState(stateID objects.MAC) (bool, error) {
	return ls.cache.HasState(stateID)
}

func (ls *LocalState) DelState(stateID objects.MAC) error {
	return ls.cache.DelState(stateID)
}

func (ls *LocalState) NewBatch() caching.StateBatch {
	return ls.cache.NewBatch()
}

func (ls *LocalState) PutDelta(de *DeltaEntry) error {
	return ls.cache.PutDelta(de.Type, de.Blob, de.Location.Packfile, de.ToBytes())
}

func (ls *LocalState) DelDelta(Type resources.Type, blobMAC, packfileMAC objects.MAC) error {
	del := DeleteEntry{
		Type:     ET_LOCATIONS,
		BlobType: Type,
		Blob:     blobMAC,
		Packfile: packfileMAC,
	}

	return ls.cache.PutDeleted(uint8(ET_LOCATIONS), blobMAC, del.ToBytes())
}

func (ls *LocalState) BlobExists(Type resources.Type, blobMAC objects.MAC) bool {
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

func (ls *LocalState) GetSubpartForBlob(Type resources.Type, blobMAC objects.MAC) (Location, bool, error) {
	var delta *DeltaEntry
	for _, buf := range ls.cache.GetDelta(Type, blobMAC) {
		de, err := DeltaEntryFromBytes(buf)

		if err != nil {
			return Location{}, false, err
		}

		ok, err := ls.cache.HasPackfile(de.Location.Packfile)
		if err != nil {
			return Location{}, false, err
		}

		coloured, _ := ls.HasColouredResource(resources.RT_PACKFILE, de.Location.Packfile)
		if ok && !coloured {
			delta = &de
			break
		}
	}

	if delta == nil {
		return Location{}, false, nil
	} else {
		return delta.Location, true, nil
	}
}

func (ls *LocalState) PutPackfile(stateId, packfile objects.MAC) error {
	pe := PackfileEntry{
		StateID:   stateId,
		Packfile:  packfile,
		Timestamp: time.Now(),
	}

	return ls.cache.PutPackfile(pe.Packfile, pe.ToBytes())
}

func (ls *LocalState) DelPackfile(packfile objects.MAC) error {
	del := DeleteEntry{
		Type:     ET_PACKFILE,
		BlobType: 0,
		Blob:     objects.NilMac,
		Packfile: packfile,
	}

	return ls.cache.PutDeleted(uint8(ET_PACKFILE), packfile, del.ToBytes())
}

func (ls *LocalState) ListPackfiles() iter.Seq[objects.MAC] {
	return func(yield func(objects.MAC) bool) {
		for st := range ls.cache.GetPackfiles() {
			if !yield(st) {
				return
			}
		}
	}
}

func (ls *LocalState) ListPackfileEntries() iter.Seq2[PackfileEntry, error] {
	return func(yield func(PackfileEntry, error) bool) {
		for _, buf := range ls.cache.GetPackfiles() {
			pe, err := PackfileEntryFromBytes(buf)
			if !yield(pe, err) {
				return
			}
		}
	}
}

func (ls *LocalState) ListSnapshots() iter.Seq2[objects.MAC, error] {
	return func(yield func(objects.MAC, error) bool) {
		for _, buf := range ls.cache.GetDeltasByType(resources.RT_SNAPSHOT) {
			de, err := DeltaEntryFromBytes(buf)
			if err != nil {
				if !yield(objects.NilMac, err) {
					return
				}
			}

			ok, err := ls.cache.HasPackfile(de.Location.Packfile)
			if err != nil {
				if !yield(objects.NilMac, err) {
					return
				}
			}
			if !ok {
				continue
			}

			has, err := ls.cache.HasColoured(resources.RT_SNAPSHOT, de.Blob)
			if err != nil {
				if !yield(objects.NilMac, err) {
					return
				}
			}
			if has {
				continue
			}

			if !yield(de.Blob, nil) {
				return
			}
		}
	}
}

func (ls *LocalState) ListObjectsOfType(Type resources.Type) iter.Seq2[DeltaEntry, error] {
	return func(yield func(DeltaEntry, error) bool) {
		for _, buf := range ls.cache.GetDeltasByType(Type) {
			de, err := DeltaEntryFromBytes(buf)
			if err != nil {
				if !yield(DeltaEntry{}, err) {
					return
				}
			}

			ok, err := ls.cache.HasPackfile(de.Location.Packfile)
			if err != nil {
				if !yield(DeltaEntry{}, err) {
					return
				}
			}

			if !ok {
				continue
			}

			if !yield(de, err) {
				return
			}
		}
	}
}

func (ls *LocalState) ListOrphanDeltas() iter.Seq2[DeltaEntry, error] {
	return func(yield func(DeltaEntry, error) bool) {
		for _, buf := range ls.cache.GetDeltas() {
			de, err := DeltaEntryFromBytes(buf)

			if err != nil {
				if !yield(DeltaEntry{}, err) {
					return
				}
			}

			ok, err := ls.cache.HasPackfile(de.Location.Packfile)
			if err != nil {
				if !yield(DeltaEntry{}, err) {
					return
				}
			}

			if !ok {
				if !yield(de, nil) {
					return
				}
			}
		}
	}
}

func (ls *LocalState) ColourResource(rtype resources.Type, resource objects.MAC) error {
	de := ColouredEntry{
		Type: rtype,
		Blob: resource,
		When: time.Now(),
	}
	return ls.cache.PutColoured(de.Type, de.Blob, de.ToBytes())
}

func (ls *LocalState) HasColouredResource(rtype resources.Type, resource objects.MAC) (bool, error) {
	return ls.cache.HasColoured(rtype, resource)
}

// Public function to insert a new configuration, beware this is to be
// serialized and pushed to repository, in order to do so most of the time you
// want to do it on a Derive'd State (in order to not repush existing
// configuration entries)
func (ls *LocalState) SetConfiguration(key string, value []byte) error {
	ce := ConfigurationEntry{
		Key:       key,
		Value:     value,
		CreatedAt: time.Now(),
	}

	return ls.insertOrUpdateConfiguration(ce)
}

// Internal function used by deserialization that only updates our local on
// disk state if the provided configuration is more recent than the stored one
func (ls *LocalState) insertOrUpdateConfiguration(ce ConfigurationEntry) error {
	value, err := ls.cache.GetConfiguration(ce.Key)
	if err != nil {
		return err
	}

	if value == nil {
		// not found, just insert it
		return ls.cache.PutConfiguration(ce.Key, ce.ToBytes())
	}

	oldCe, err := ConfigurationEntryFromBytes(value)
	if err != nil {
		return err
	}

	if oldCe.CreatedAt.Before(ce.CreatedAt) {
		if err := ls.cache.PutConfiguration(ce.Key, ce.ToBytes()); err != nil {
			return err
		}
	}

	return nil
}

func (ls *LocalState) ListColouredResources(rtype resources.Type) iter.Seq2[ColouredEntry, error] {
	return func(yield func(ColouredEntry, error) bool) {
		for _, buf := range ls.cache.GetColouredEntriesByType(rtype) {
			de, err := ColouredEntryFromBytes(buf)

			if !yield(de, err) {
				return
			}
		}
	}
}

func (ls *LocalState) DelColouredResource(rtype resources.Type, resourceMAC objects.MAC) error {
	del := DeleteEntry{
		Type:     ET_COLOURED,
		BlobType: rtype,
		Blob:     resourceMAC,
		Packfile: objects.NilMac,
	}

	return ls.cache.PutDeleted(uint8(ET_COLOURED), resourceMAC, del.ToBytes())
}
