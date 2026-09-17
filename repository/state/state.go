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
	"time"

	"github.com/PlakarKorp/kloset/caching"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
)

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

func (ls *State) NewBatch() *caching.ScanBatch {
	return ls.cache.NewScanBatch()
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
