// Package filestore provides an on-disk [btree.Storer] meant for backup
// operations (i.e. heavy-write, almost no reads or updates).  It is
// backed by a file containing only the concatenated msgpack-encoded
// nodes.  An in-memory indexs maps the IDs to the record on disk.
package filestore

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/vmihailenco/msgpack/v5"
)

type record struct {
	offset uint64
	length uint32
}

// FileStore implements btree.Storer[K, int, V] as a flat, append-only
// heap file.
type FileStore[K any, V any] struct {
	file *os.File
	path string

	mu     sync.Mutex
	offset uint64
	index  []record
	free   []record // holes left behind by Update, reusable by write
}

// New creates a new FileStore backed by a file named name inside dir.
// dir is created if it doesn't exist.
func New[K any, V any](dir, name string) (*FileStore[K, V], error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}

	path := filepath.Join(dir, name)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}

	return &FileStore[K, V]{
		file: f,
		path: path,
	}, nil
}

func (s *FileStore[K, V]) Get(ptr int) (*btree.Node[K, int, V], error) {
	s.mu.Lock()
	if ptr < 0 || ptr >= len(s.index) {
		s.mu.Unlock()
		return nil, fmt.Errorf("storage: no such node %d", ptr)
	}
	rec := s.index[ptr]
	s.mu.Unlock()

	buf := make([]byte, rec.length)
	if _, err := s.file.ReadAt(buf, int64(rec.offset)); err != nil {
		return nil, err
	}

	node := &btree.Node[K, int, V]{}
	if err := msgpack.Unmarshal(buf, node); err != nil {
		return nil, err
	}
	return node, nil
}

// alloc does a first-fit scan of the free list for a hole that fits
// length bytes, otherwise reserves some space at the end of the file.
func (s *FileStore[K, V]) alloc(length uint32) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, r := range s.free {
		if r.length >= length {
			s.free[i] = s.free[len(s.free)-1]
			s.free = s.free[:len(s.free)-1]
			return r.offset
		}
	}

	// cannot reuse a free block, append to the end of the file.
	off := s.offset
	s.offset += uint64(length)
	return off
}

// write encodes node and stores it at a reused hole if the free list
// has one big enough, or else at a freshly reserved offset, and
// returns where it landed.
func (s *FileStore[K, V]) write(node *btree.Node[K, int, V]) (record, error) {
	buf, err := msgpack.Marshal(node)
	if err != nil {
		return record{}, err
	}
	length := uint32(len(buf))

	off := s.alloc(length)
	if _, err := s.file.WriteAt(buf, int64(off)); err != nil {
		return record{}, err
	}

	return record{offset: off, length: length}, nil
}

func (s *FileStore[K, V]) Update(ptr int, node *btree.Node[K, int, V]) error {
	rec, err := s.write(node)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if ptr < 0 || ptr >= len(s.index) {
		return fmt.Errorf("storage: no such node %d", ptr)
	}
	s.free = append(s.free, s.index[ptr])
	s.index[ptr] = rec
	return nil
}

func (s *FileStore[K, V]) Put(node *btree.Node[K, int, V]) (int, error) {
	rec, err := s.write(node)
	if err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	ptr := len(s.index)
	s.index = append(s.index, rec)
	return ptr, nil
}

// Close closes and removes the backing file.
func (s *FileStore[K, V]) Close() error {
	err := s.file.Close()
	if path := s.file.Name(); filepath.IsAbs(path) {
		if rerr := os.Remove(s.file.Name()); err == nil {
			err = rerr
		}
	}
	return err
}
