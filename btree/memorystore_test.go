package btree

type InMemoryStore_t[K any, V any] struct {
	InMemoryStore[K, V]
	getFn func(int) (*Node[K, int, V], error)
	PutFn func(*Node[K, int, V]) (int, error)
}

func (s *InMemoryStore_t[K, V]) Get(ptr int) (n *Node[K, int, V], err error) {
	if s.getFn != nil {
		return s.getFn(ptr)
	}

	if ptr >= len(s.store) {
		return nil, notfound
	}

	return &s.store[ptr], nil
}

func (s *InMemoryStore_t[K, V]) Put(n *Node[K, int, V]) (int, error) {
	if s.PutFn != nil {
		return s.PutFn(n)
	}

	dup := cloneNode(n)
	dup.Next = n.Next
	s.store = append(s.store, *dup)
	return len(s.store) - 1, nil
}
