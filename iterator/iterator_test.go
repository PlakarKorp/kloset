package iterator_test

import (
	"testing"

	"github.com/PlakarKorp/kloset/iterator"
	"github.com/stretchr/testify/require"
)

// sliceIterator is a concrete implementation of Iterator[K,V] over a slice.
type sliceIterator[K, V any] struct {
	keys   []K
	values []V
	index  int
}

func newSliceIterator[K, V any](keys []K, values []V) *sliceIterator[K, V] {
	return &sliceIterator[K, V]{keys: keys, values: values, index: -1}
}

func (it *sliceIterator[K, V]) Next() bool {
	it.index++
	return it.index < len(it.keys)
}

func (it *sliceIterator[K, V]) Current() (K, V) {
	return it.keys[it.index], it.values[it.index]
}

func (it *sliceIterator[K, V]) Err() error {
	return nil
}

// verify the interface is satisfied at compile time
var _ iterator.Iterator[string, int] = (*sliceIterator[string, int])(nil)

func TestIteratorInterfaceCompiles(t *testing.T) {
	var it iterator.Iterator[string, int] = newSliceIterator(
		[]string{"a", "b", "c"},
		[]int{1, 2, 3},
	)

	var collected []string
	for it.Next() {
		k, _ := it.Current()
		collected = append(collected, k)
	}
	require.NoError(t, it.Err())
	require.Equal(t, []string{"a", "b", "c"}, collected)
}

func TestIteratorEmpty(t *testing.T) {
	var it iterator.Iterator[int, string] = newSliceIterator([]int{}, []string{})
	require.False(t, it.Next())
	require.NoError(t, it.Err())
}

func TestIteratorValues(t *testing.T) {
	var it iterator.Iterator[int, string] = newSliceIterator(
		[]int{10, 20},
		[]string{"ten", "twenty"},
	)

	it.Next()
	k, v := it.Current()
	require.Equal(t, 10, k)
	require.Equal(t, "ten", v)

	it.Next()
	k, v = it.Current()
	require.Equal(t, 20, k)
	require.Equal(t, "twenty", v)

	require.False(t, it.Next())
}
