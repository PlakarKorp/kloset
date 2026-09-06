// Package storage provides an on-disk [btree.Storer] meant for backup
// operations (i.e. heavy-write, almost no reads or updates).  It is
// backed by a file containing only the concatenated msgpack-encoded
// nodes.  An in-memory indexs maps the IDs to the record on disk.
package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

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

	// next free byte in the file, bump-allocated so that concurrent
	// Put/Update calls can write in parallel without serializing on
	// the mutex below.
	offset atomic.Uint64

	mu    sync.Mutex
	index []record
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

// write appends the encoded node to the file at a freshly reserved
// offset and returns where it landed.
func (s *FileStore[K, V]) write(node *btree.Node[K, int, V]) (record, error) {
	buf, err := msgpack.Marshal(node)
	if err != nil {
		return record{}, err
	}

	off := s.offset.Add(uint64(len(buf))) - uint64(len(buf))
	if _, err := s.file.WriteAt(buf, int64(off)); err != nil {
		return record{}, err
	}

	return record{offset: off, length: uint32(len(buf))}, nil
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
