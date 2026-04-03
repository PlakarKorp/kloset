package btree

type InMemoryStore_t[K any, V any] struct {
	InMemoryStore[K, V]
	GetFn    func(int) (*Node[K, int, V], error)
	PutFn    func(*Node[K, int, V]) (int, error)
	UpdateFn func(ptr int, n *Node[K, int, V]) error
	CloseFn  func() error
}

func (s *InMemoryStore_t[K, V]) Get(ptr int) (n *Node[K, int, V], err error) {
	if s.GetFn != nil {
		return s.GetFn(ptr)
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

func (s *InMemoryStore_t[K, V]) Update(ptr int, n *Node[K, int, V]) error {
	if s.UpdateFn != nil {
		return s.UpdateFn(ptr, n)
	}
	_, err := s.Get(ptr)
	if err != nil {
		return err
	}

	dup := cloneNode(n)
	dup.Next = n.Next
	s.store[ptr] = *dup
	return nil
}

func (s *InMemoryStore_t[K, V]) Close() error {
	if s.CloseFn != nil {
		return s.CloseFn()
	}
	return nil
}
