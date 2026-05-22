package pebble

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInMemoryCache(t *testing.T) {
	c, err := New("", false)
	require.NoError(t, err)
	defer c.Close()

	key := []byte("mykey")
	val := []byte("myvalue")

	require.NoError(t, c.Put(key, val))

	got, err := c.Get(key)
	require.NoError(t, err)
	require.Equal(t, val, got)

	exists, err := c.Has(key)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestGetMissingKey(t *testing.T) {
	c, err := New("", false)
	require.NoError(t, err)
	defer c.Close()

	got, err := c.Get([]byte("missing"))
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestHasMissingKey(t *testing.T) {
	c, err := New("", false)
	require.NoError(t, err)
	defer c.Close()

	exists, err := c.Has([]byte("missing"))
	require.NoError(t, err)
	require.False(t, exists)
}

func TestScanForward(t *testing.T) {
	c, err := New("", false)
	require.NoError(t, err)
	defer c.Close()

	prefix := []byte("prefix:")
	keys := [][]byte{
		append(append([]byte{}, prefix...), 'a'),
		append(append([]byte{}, prefix...), 'b'),
		append(append([]byte{}, prefix...), 'c'),
	}
	for i, k := range keys {
		require.NoError(t, c.Put(k, []byte{byte(i)}))
	}

	var collected [][]byte
	for k, _ := range c.Scan(prefix, false) {
		collected = append(collected, append([]byte{}, k...))
	}
	require.Len(t, collected, 3)
	// Should be in ascending order
	require.Equal(t, keys[0], collected[0])
	require.Equal(t, keys[1], collected[1])
	require.Equal(t, keys[2], collected[2])
}

func TestScanReverse(t *testing.T) {
	c, err := New("", false)
	require.NoError(t, err)
	defer c.Close()

	prefix := []byte("rev:")
	keys := [][]byte{
		append(append([]byte{}, prefix...), 'a'),
		append(append([]byte{}, prefix...), 'b'),
		append(append([]byte{}, prefix...), 'c'),
	}
	for i, k := range keys {
		require.NoError(t, c.Put(k, []byte{byte(i)}))
	}

	var collected [][]byte
	for k, _ := range c.Scan(prefix, true) {
		collected = append(collected, append([]byte{}, k...))
	}
	require.Len(t, collected, 3)
	// Should be in descending order
	require.Equal(t, keys[2], collected[0])
	require.Equal(t, keys[0], collected[2])
}

func TestScanEmpty(t *testing.T) {
	c, err := New("", false)
	require.NoError(t, err)
	defer c.Close()

	var count int
	for range c.Scan([]byte("no-such-prefix:"), false) {
		count++
	}
	require.Equal(t, 0, count)
}

func TestBatchPutAndCommit(t *testing.T) {
	c, err := New("", false)
	require.NoError(t, err)
	defer c.Close()

	b := c.NewBatch()
	require.NotNil(t, b)

	require.NoError(t, b.Put([]byte("bk1"), []byte("bv1")))
	require.NoError(t, b.Put([]byte("bk2"), []byte("bv2")))
	require.Equal(t, uint32(2), b.Count())

	require.NoError(t, b.Commit())

	got, err := c.Get([]byte("bk1"))
	require.NoError(t, err)
	require.Equal(t, []byte("bv1"), got)

	got, err = c.Get([]byte("bk2"))
	require.NoError(t, err)
	require.Equal(t, []byte("bv2"), got)
}

func TestConstructor(t *testing.T) {
	dir := t.TempDir()
	ctor := Constructor(dir)
	require.NotNil(t, ctor)

	c, err := ctor("v1", "testcache", "repo-123", 0)
	require.NoError(t, err)
	require.NotNil(t, c)
	defer c.Close()
}

func TestInMemoryConstructor(t *testing.T) {
	ctor := InMemoryConstructor()
	require.NotNil(t, ctor)

	c, err := ctor("v1", "testcache", "repo-123", 0)
	require.NoError(t, err)
	require.NotNil(t, c)
	defer c.Close()
}

func TestDeleteOnClose(t *testing.T) {
	dir := t.TempDir()
	c, err := New(dir, true)
	require.NoError(t, err)
	require.NoError(t, c.Close())
}

func TestMakeKeyUpperBound(t *testing.T) {
	cases := []struct {
		input    []byte
		expected []byte
	}{
		{[]byte{0x01}, []byte{0x02}},
		{[]byte{0x01, 0x02}, []byte{0x01, 0x03}},
		{[]byte{0xFF}, nil}, // overflow → nil
	}

	for _, c := range cases {
		got := makeKeyUpperBound(c.input)
		require.Equal(t, c.expected, got)
	}
}

func TestMakeKeyUpperBoundAllFF(t *testing.T) {
	key := []byte{0xFF, 0xFF, 0xFF}
	got := makeKeyUpperBound(key)
	require.Nil(t, got)
}
