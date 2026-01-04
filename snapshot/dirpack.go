package snapshot

import (
	"encoding/binary"
	"errors"
	"io"
	"io/fs"
	"sync"

	"github.com/PlakarKorp/kloset/snapshot/vfs"
	"github.com/vmihailenco/msgpack/v5"
)

type Dirpack struct {
	mu      sync.Mutex
	entries map[string]vfs.Entry
}

func readDirPackHdr(rd io.Reader) (typ uint8, siz uint32, err error) {
	endian := binary.LittleEndian
	if err = binary.Read(rd, endian, &typ); err != nil {
		return
	}
	if err = binary.Read(rd, endian, &siz); err != nil {
		return
	}
	return
}

func NewDirpackFromReader(rd io.Reader) (*Dirpack, error) {
	d := &Dirpack{
		entries: make(map[string]vfs.Entry),
	}

	for {
		_, siz, err := readDirPackHdr(rd)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		var entry vfs.Entry
		lrd := io.LimitReader(rd, int64(siz-uint32(len(entry.MAC))))
		err = msgpack.NewDecoder(lrd).Decode(&entry)
		if err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(rd, entry.MAC[:]); err != nil {
			return nil, err
		}
		d.entries[entry.Name()] = entry
	}
	return d, nil
}

func (d *Dirpack) GetEntry(name string) (*vfs.Entry, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if entry, ok := d.entries[name]; !ok {
		return nil, fs.ErrNotExist
	} else {
		return &entry, nil
	}
}
