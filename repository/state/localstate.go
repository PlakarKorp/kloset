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
	"bufio"
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
	parentState, err := cache.GetLatestState()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch latest state: %w", err)
	}

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
	tx, err := ls.cache.Begin()
	if err != nil {
		return err
	}

	has, err := tx.HasState(stateID)
	if err != nil {
		tx.Rollback()
		return err
	}

	if has {
		tx.Rollback()
		return nil
	}

	// This implicitly sets the parent, see the note about refactoring, and
	// since we Derive() to construct Delta streams this will set the correct
	// parent. This is all way too intricated and will be fixed by a refacto.
	var mt *Metadata
	if ver.Equals(versioning.FromString("1.1.0")) {
		mt, err = deserializeFromStream(ls, tx, bufio.NewReaderSize(rd, 256 << 10))
	} else {
		mt, err = deserializeFromStreamv100(ls, tx, bufio.NewReaderSize(rd, 256 << 10))
	}

	if err != nil {
		tx.Rollback()
		return err
	}

	metadata, err := mt.ToBytes()
	if err != nil {
		tx.Rollback()
		return err
	}

	/* We merged the state deltas, we can now publish it */
	if err := tx.PutState(stateID, metadata); err != nil {
		tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return err
	}

	ls.Metadata = *mt
	return nil
}

func (ls *LocalState) MergeStateFromCache(stateID objects.MAC, from *caching.ScanCache) error {
	tx, err := ls.cache.Begin()
	if err != nil {
		return err
	}

	has, err := tx.HasState(stateID)
	if err != nil {
		tx.Rollback()
		return err
	}

	if has {
		tx.Rollback()
		return nil
	}

	err = mergeFromCache(ls, tx, from)
	if err != nil {
		tx.Rollback()
		return err
	}

	mt, err := ls.Metadata.ToBytes()
	if err != nil {
		tx.Rollback()
		return err
	}

	/* We merged the state deltas, we can now publish it */
	if err := tx.PutState(stateID, mt); err != nil {
		tx.Rollback()
		return err
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return err
	}

	return nil
}

func (ls *LocalState) GetStates() (map[objects.MAC][]byte, error) {
	return ls.cache.GetStates()
}

func deserializeFromStream(l *LocalState, tx *caching.SQLStateTx, r io.Reader) (*Metadata, error) {
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

	mt := &Metadata{}
	n, err := r.Read(mt.Parent[:])
	if err != nil || n != len(objects.MAC{}) {
		return nil, fmt.Errorf("failed to read header %w", err)
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
			return nil, fmt.Errorf("failed to read entry type %w", err)
		}

		entryType := EntryType(et_buf[0])
		if entryType == ET_METADATA {
			break
		}

		length, err := readUint32()
		if err != nil {
			return nil, fmt.Errorf("failed to read entry length %w", err)
		}

		//XXX: This is screaming refactorization, but is a bit subtil.
		switch entryType {
		case ET_DELETE:
			if length != DeleteEntrySerializedSize {
				return nil, fmt.Errorf("failed to read delete entry wrong length got(%d)/expected(%d)", length, DeltaEntrySerializedSize)
			}

			if n, err := io.ReadFull(r, del_buf); err != nil {
				return nil, fmt.Errorf("failed to read delete entry %w, read(%d)/expected(%d)", err, n, length)
			}

			toDel, err := DeleteEntryFromBytes(del_buf)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize delta entry %w", err)
			}

			switch toDel.Type {
			case ET_LOCATIONS:
				tx.DelDelta(toDel.BlobType, toDel.Blob, toDel.Packfile)
			case ET_COLOURED:
				tx.DelColoured(toDel.BlobType, toDel.Blob)
			case ET_PACKFILE:
				tx.DelPackfile(toDel.Packfile)
			default:
				return nil, fmt.Errorf("invalid delete Type %d", toDel.Type)
			}
		case ET_LOCATIONS:
			if length != DeltaEntrySerializedSize {
				return nil, fmt.Errorf("failed to read delta entry wrong length got(%d)/expected(%d)", length, DeltaEntrySerializedSize)
			}

			if n, err := io.ReadFull(r, de_buf); err != nil {
				return nil, fmt.Errorf("failed to read delta entry %w, read(%d)/expected(%d)", err, n, length)
			}

			// We need to decode just to make the key, but we can reuse the buffer
			// to put inside the data part of the cache.
			delta, err := DeltaEntryFromBytes(de_buf)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize delta entry %w", err)
			}

			if err := tx.PutDelta(delta.Type, delta.Blob, delta.Location.Packfile, delta.Location.Offset, delta.Location.Length); err != nil {
				return nil, err
			}

		case ET_COLOURED:
			if length != ColouredEntrySerializedSize {
				return nil, fmt.Errorf("failed to read coloured entry wrong length got(%d)/expected(%d)", length, ColouredEntrySerializedSize)
			}

			if n, err := io.ReadFull(r, coloured_buf); err != nil {
				return nil, fmt.Errorf("failed to read coloured entry %w, read(%d)/expected(%d)", err, n, length)
			}

			coloured, err := ColouredEntryFromBytes(coloured_buf)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize coloured entry %w", err)
			}

			if err := tx.PutColoured(coloured.Type, coloured.Blob, coloured.When); err != nil {
				return nil, err
			}
		case ET_PACKFILE:
			if length != PackfileEntrySerializedSize {
				return nil, fmt.Errorf("failed to read packfile entry wrong length got(%d)/expected(%d)", length, PackfileEntrySerializedSize)
			}

			if n, err := io.ReadFull(r, pe_buf); err != nil {
				return nil, fmt.Errorf("failed to read packfile entry %w, read(%d)/expected(%d)", err, n, length)
			}

			pe, err := PackfileEntryFromBytes(pe_buf)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize packfile entry %w", err)
			}

			if err := tx.PutPackfile(pe.Packfile, pe.StateID, pe.Timestamp); err != nil {
				return nil, err
			}

		case ET_CONFIGURATION:
			ce_buf := make([]byte, length)

			if n, err := io.ReadFull(r, ce_buf); err != nil {
				return nil, fmt.Errorf("failed to read configuration entry %w, read(%d)/expected(%d)", err, n, length)
			}

			ce, err := ConfigurationEntryFromBytes(ce_buf)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize configuration entry %w", err)
			}

			err = l.insertOrUpdateConfiguration(ce)
			if err != nil {
				return nil, fmt.Errorf("failed to insert/update configuration entry %w", err)
			}
		default:
			// Our version doesn't know this entry type, just skip it.
			io.CopyN(io.Discard, r, int64(length))
		}
	}

	/* Deserialize Metadata */
	version, err := readUint32()
	if err != nil {
		return nil, fmt.Errorf("failed to read version: %w", err)
	}
	mt.Version = versioning.Version(version)

	timestamp, err := readUint64()
	if err != nil {
		return nil, fmt.Errorf("failed to read timestamp: %w", err)
	}
	mt.Timestamp = time.Unix(0, int64(timestamp))

	serial := make([]byte, len(uuid.UUID{}))
	if _, err := io.ReadFull(r, serial); err != nil {
		return nil, fmt.Errorf("failed to read serial: %w", err)
	}
	mt.Serial = uuid.UUID(serial)

	return mt, nil
}

func deserializeFromStreamv100(l *LocalState, tx *caching.SQLStateTx, r io.Reader) (*Metadata, error) {
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

	mt := &Metadata{}

	/* Deserialize LOCATIONS */
	et_buf := make([]byte, 1)
	de_buf := make([]byte, DeltaEntrySerializedSize)
	coloured_buf := make([]byte, ColouredEntrySerializedSize)
	pe_buf := make([]byte, PackfileEntrySerializedSize)
	for {
		n, err := r.Read(et_buf)
		if err != nil || n != len(et_buf) {
			return nil, fmt.Errorf("failed to read entry type %w", err)
		}

		entryType := EntryType(et_buf[0])
		if entryType == ET_METADATA {
			break
		}

		length, err := readUint32()
		if err != nil {
			return nil, fmt.Errorf("failed to read entry length %w", err)
		}

		//XXX: This is screaming refactorization, but is a bit subtil.
		switch entryType {
		case ET_LOCATIONS:
			if length != DeltaEntrySerializedSize {
				return nil, fmt.Errorf("failed to read delta entry wrong length got(%d)/expected(%d)", length, DeltaEntrySerializedSize)
			}

			if n, err := io.ReadFull(r, de_buf); err != nil {
				return nil, fmt.Errorf("failed to read delta entry %w, read(%d)/expected(%d)", err, n, length)
			}

			// We need to decode just to make the key, but we can reuse the buffer
			// to put inside the data part of the cache.
			delta, err := DeltaEntryFromBytes(de_buf)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize delta entry %w", err)
			}

			if err := tx.PutDelta(delta.Type, delta.Blob, delta.Location.Packfile, delta.Location.Offset, delta.Location.Length); err != nil {
				return nil, err
			}

		case ET_COLOURED:
			if length != ColouredEntrySerializedSize {
				return nil, fmt.Errorf("failed to read coloured entry wrong length got(%d)/expected(%d)", length, ColouredEntrySerializedSize)
			}

			if n, err := io.ReadFull(r, coloured_buf); err != nil {
				return nil, fmt.Errorf("failed to read coloured entry %w, read(%d)/expected(%d)", err, n, length)
			}

			coloured, err := ColouredEntryFromBytes(coloured_buf)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize coloured entry %w", err)
			}

			if err := tx.PutColoured(coloured.Type, coloured.Blob, coloured.When); err != nil {
				return nil, err
			}
		case ET_PACKFILE:
			if length != PackfileEntrySerializedSize {
				return nil, fmt.Errorf("failed to read packfile entry wrong length got(%d)/expected(%d)", length, PackfileEntrySerializedSize)
			}

			if n, err := io.ReadFull(r, pe_buf); err != nil {
				return nil, fmt.Errorf("failed to read packfile entry %w, read(%d)/expected(%d)", err, n, length)
			}

			pe, err := PackfileEntryFromBytes(pe_buf)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize packfile entry %w", err)
			}

			if err := tx.PutPackfile(pe.Packfile, pe.StateID, pe.Timestamp); err != nil {
				return nil, err
			}

		case ET_CONFIGURATION:
			ce_buf := make([]byte, length)

			if n, err := io.ReadFull(r, ce_buf); err != nil {
				return nil, fmt.Errorf("failed to read configuration entry %w, read(%d)/expected(%d)", err, n, length)
			}

			ce, err := ConfigurationEntryFromBytes(ce_buf)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize configuration entry %w", err)
			}

			err = l.insertOrUpdateConfiguration(ce)
			if err != nil {
				return nil, fmt.Errorf("failed to insert/update configuration entry %w", err)
			}
		default:
			// Our version doesn't know this entry type, just skip it.
			io.CopyN(io.Discard, r, int64(length))
		}
	}

	/* Deserialize Metadata */
	version, err := readUint32()
	if err != nil {
		return nil, fmt.Errorf("failed to read version: %w", err)
	}
	mt.Version = versioning.Version(version)

	timestamp, err := readUint64()
	if err != nil {
		return nil, fmt.Errorf("failed to read timestamp: %w", err)
	}
	mt.Timestamp = time.Unix(0, int64(timestamp))

	serial := make([]byte, len(uuid.UUID{}))
	if _, err := io.ReadFull(r, serial); err != nil {
		return nil, fmt.Errorf("failed to read serial: %w", err)
	}
	mt.Serial = uuid.UUID(serial)

	return mt, nil
}

func mergeFromCache(l *LocalState, tx *caching.SQLStateTx, from *caching.ScanCache) error {
	for _, entry := range from.GetDeltas() {
		delta, err := DeltaEntryFromBytes(entry)
		if err != nil {
			return fmt.Errorf("failed to deserialize delta entry %w", err)
		}

		if err := tx.PutDelta(delta.Type, delta.Blob, delta.Location.Packfile, delta.Location.Offset, delta.Location.Length); err != nil {
			return err
		}
	}

	for _, coloured_buf := range from.GetColouredEntries() {
		coloured, err := ColouredEntryFromBytes(coloured_buf)
		if err != nil {
			return fmt.Errorf("failed to deserialize coloured entry %w", err)
		}

		if err := tx.PutColoured(coloured.Type, coloured.Blob, coloured.When); err != nil {
			return err
		}
	}

	for _, pe_buf := range from.GetPackfiles() {
		pe, err := PackfileEntryFromBytes(pe_buf)
		if err != nil {
			return fmt.Errorf("failed to deserialize packfile entry %w", err)
		}

		if err := tx.PutPackfile(pe.Packfile, pe.StateID, pe.Timestamp); err != nil {
			return err
		}
	}

	for ce_buf := range from.GetConfigurations() {
		ce, err := ConfigurationEntryFromBytes(ce_buf)
		if err != nil {
			return fmt.Errorf("failed to deserialize configuration entry %w", err)
		}

		err = l.insertOrUpdateConfiguration(ce)
		if err != nil {
			return fmt.Errorf("failed to insert/update configuration entry %w", err)
		}
	}

	return nil
}

func (ls *LocalState) DelState(stateID objects.MAC) error {
	return ls.cache.DelState(stateID)
}

func (ls *LocalState) BlobExists(Type resources.Type, blobMAC objects.MAC) bool {
	for b, err := range ls.cache.GetDelta(Type, blobMAC) {
		if err != nil {
			continue
		}

		ok, err := ls.cache.HasPackfile(b.Packfile)
		if err != nil {
			continue
		}

		coloured, _ := ls.HasColouredResource(resources.RT_PACKFILE, b.Packfile)
		if ok && !coloured {
			return true
		}
	}

	return false
}

func (ls *LocalState) GetSubpartForBlob(Type resources.Type, blobMAC objects.MAC) (Location, bool, error) {
	var blob *caching.Blob
	for b, err := range ls.cache.GetDelta(Type, blobMAC) {
		if err != nil {
			return Location{}, false, err
		}

		ok, err := ls.cache.HasPackfile(b.Packfile)
		if err != nil {
			return Location{}, false, err
		}

		coloured, _ := ls.HasColouredResource(resources.RT_PACKFILE, b.Packfile)
		if ok && !coloured {
			blob = &b
			break
		}
	}

	if blob == nil {
		return Location{}, false, nil
	} else {
		l := Location{
			Packfile: blob.Packfile,
			Offset:   blob.Offset,
			Length:   blob.Length,
		}
		return l, true, nil
	}
}

func (ls *LocalState) ListPackfiles() iter.Seq2[objects.MAC, error] {
	return func(yield func(objects.MAC, error) bool) {
		for p, err := range ls.cache.GetPackfiles() {
			if err != nil {
				if !yield(objects.NilMac, err) {
					return
				}

				continue
			}

			if !yield(p.MAC, nil) {
				return
			}
		}
	}
}

func (ls *LocalState) ListPackfileEntries() iter.Seq2[PackfileEntry, error] {
	return func(yield func(PackfileEntry, error) bool) {
		for p, err := range ls.cache.GetPackfiles() {
			if err != nil {
				if !yield(PackfileEntry{}, err) {
					return
				}

				continue
			}

			pe := PackfileEntry{
				Packfile:  p.MAC,
				StateID:   p.State,
				Timestamp: p.Timestamp,
			}
			if !yield(pe, nil) {
				return
			}
		}
	}
}

func (ls *LocalState) ListSnapshots() iter.Seq2[objects.MAC, error] {
	return func(yield func(objects.MAC, error) bool) {
		for b, err := range ls.cache.GetDeltasByType(resources.RT_SNAPSHOT) {
			if err != nil {
				if !yield(objects.NilMac, err) {
					return
				}

				continue
			}

			ok, err := ls.cache.HasPackfile(b.Packfile)
			if err != nil {
				if !yield(objects.NilMac, err) {
					return
				}

				continue
			}
			if !ok {
				continue
			}

			has, err := ls.cache.HasColoured(resources.RT_SNAPSHOT, b.MAC)
			if err != nil {
				if !yield(objects.NilMac, err) {
					return
				}

				continue
			}
			if has {
				continue
			}

			if !yield(b.MAC, nil) {
				return
			}
		}
	}
}

func (ls *LocalState) ListOrphanDeltas() iter.Seq2[DeltaEntry, error] {
	return func(yield func(DeltaEntry, error) bool) {
		for b, err := range ls.cache.GetDeltas() {
			if err != nil {
				if !yield(DeltaEntry{}, err) {
					return
				}

				continue
			}

			ok, err := ls.cache.HasPackfile(b.Packfile)
			if err != nil {
				if !yield(DeltaEntry{}, err) {
					return
				}

				continue
			}

			if !ok {
				de := DeltaEntry{
					Type: b.Type,
					Blob: b.MAC,
					Location: Location{
						Packfile: b.Packfile,
						Offset:   b.Offset,
						Length:   b.Length,
					},
				}

				if !yield(de, nil) {
					return
				}
			}
		}
	}
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
		for ce, err := range ls.cache.GetColouredEntriesByType(rtype) {
			if err != nil {
				if !yield(ColouredEntry{}, err) {
					return
				}

				continue
			}

			de := ColouredEntry{
				Type: ce.Type,
				Blob: ce.MAC,
				When: ce.When,
			}
			if !yield(de, nil) {
				return
			}
		}
	}
}

func (ls *LocalState) ListObjectsOfType(Type resources.Type) iter.Seq2[DeltaEntry, error] {
	return func(yield func(DeltaEntry, error) bool) {
		for b, err := range ls.cache.GetDeltasByType(Type) {
			if err != nil {
				if !yield(DeltaEntry{}, err) {
					return
				}

				continue
			}

			ok, err := ls.cache.HasPackfile(b.Packfile)
			if err != nil {
				if !yield(DeltaEntry{}, err) {
					return
				}

				continue
			}

			if !ok {
				continue
			}

			de := DeltaEntry{
				Type: b.Type,
				Blob: b.MAC,
				Location: Location{
					Packfile: b.Packfile,
					Offset:   b.Offset,
					Length:   b.Length,
				},
			}
			if !yield(de, nil) {
				return
			}
		}
	}
}
