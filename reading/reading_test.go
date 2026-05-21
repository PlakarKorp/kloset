package reading_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/PlakarKorp/kloset/reading"
	"github.com/stretchr/testify/require"
)

type mockReaderAtCloser struct {
	data   []byte
	closed bool
}

func (m *mockReaderAtCloser) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(m.data)) {
		return 0, io.EOF
	}
	n := copy(p, m.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (m *mockReaderAtCloser) Close() error {
	m.closed = true
	return nil
}

func TestNewSectionReadCloser(t *testing.T) {
	data := []byte("hello, world!")
	mock := &mockReaderAtCloser{data: data}

	src := reading.NewSectionReadCloser(mock, 0, int64(len(data)))
	require.NotNil(t, src)
}

func TestSectionReadCloserRead(t *testing.T) {
	data := []byte("hello, world!")
	mock := &mockReaderAtCloser{data: data}

	src := reading.NewSectionReadCloser(mock, 0, int64(len(data)))

	got, err := io.ReadAll(src)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestSectionReadCloserOffset(t *testing.T) {
	data := []byte("hello, world!")
	mock := &mockReaderAtCloser{data: data}

	// Read from offset 7, length 5 → "world"
	src := reading.NewSectionReadCloser(mock, 7, 5)

	got, err := io.ReadAll(src)
	require.NoError(t, err)
	require.Equal(t, []byte("world"), got)
}

func TestSectionReadCloserClose(t *testing.T) {
	data := []byte("test data")
	mock := &mockReaderAtCloser{data: data}

	src := reading.NewSectionReadCloser(mock, 0, int64(len(data)))
	err := src.Close()
	require.NoError(t, err)
	require.True(t, mock.closed, "underlying closer should be called")
}

func TestSectionReadCloserReadAt(t *testing.T) {
	data := []byte("abcdefghij")
	mock := &mockReaderAtCloser{data: data}

	src := reading.NewSectionReadCloser(mock, 0, int64(len(data)))

	buf := make([]byte, 3)
	n, err := src.ReadAt(buf, 2)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, []byte("cde"), buf)
}

func TestSectionReadCloserEmptyData(t *testing.T) {
	mock := &mockReaderAtCloser{data: []byte{}}

	src := reading.NewSectionReadCloser(mock, 0, 0)
	got, err := io.ReadAll(src)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestSectionReadCloserLargerThanData(t *testing.T) {
	data := []byte("hi")
	mock := &mockReaderAtCloser{data: data}

	// Section larger than actual data — SectionReader limits to available bytes
	src := reading.NewSectionReadCloser(mock, 0, 100)
	got, err := io.ReadAll(src)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestSectionReadCloserCopyTo(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 1024)
	mock := &mockReaderAtCloser{data: data}

	src := reading.NewSectionReadCloser(mock, 0, int64(len(data)))

	var buf bytes.Buffer
	n, err := io.Copy(&buf, src)
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), n)
	require.Equal(t, data, buf.Bytes())
}
