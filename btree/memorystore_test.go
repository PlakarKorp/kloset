package btree

import (
	"errors"
	"slices"
)

var (
	notfound = errors.New("item not found")
)

type InMemoryStore[K any, V any] struct {
	store []Node[K, int, V]
	getFn func(int) (*Node[K, int, V], error)
}

func cloneNode[K any, P comparable, V any](n *Node[K, P, V]) *Node[K, P, V] {
	if n == nil {
		return nil
	}

	return &Node[K, P, V]{
		Version:  n.Version,
		Keys:     slices.Clone(n.Keys),
		Pointers: slices.Clone(n.Pointers),
		Values:   slices.Clone(n.Values),
		Prev:     n.Prev,
		Next:     n.Next,
	}
}

func (s *InMemoryStore[K, V]) Get(ptr int) (n *Node[K, int, V], err error) {
	if s.getFn != nil {
		return s.getFn(ptr)
	}

	if ptr >= len(s.store) {
		return nil, notfound
	}

	return &s.store[ptr], nil
}

func (s *InMemoryStore[K, V]) Update(ptr int, n *Node[K, int, V]) error {
	_, err := s.Get(ptr)
	if err != nil {
		return err
	}

	dup := cloneNode(n)
	dup.Next = n.Next
	s.store[ptr] = *dup
	return nil
}

func (s *InMemoryStore[K, V]) Put(n *Node[K, int, V]) (int, error) {
	dup := cloneNode(n)
	dup.Next = n.Next
	s.store = append(s.store, *dup)
	return len(s.store) - 1, nil
}

func (s *InMemoryStore[K, V]) Close() error {
	return nil
}
