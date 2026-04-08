package connectors_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	con "github.com/PlakarKorp/kloset/connectors"
	"github.com/stretchr/testify/require"
)

type readerReadCloser struct {
	reader     io.Reader
	closeErr   error
	closeCalls int
}

func (rrc *readerReadCloser) Read(p []byte) (int, error) { return rrc.reader.Read(p) }

func (rrc *readerReadCloser) Close() error {
	rrc.closeCalls++
	return rrc.closeErr
}

type failingReader struct {
	err error
}

func (fr failingReader) Read([]byte) (int, error) {
	return 0, fr.err
}

func TestNewLazyReader(t *testing.T) {
	t.Run("DoesNotOpenReaderImmediately", func(t *testing.T) {
		openCalls := 0

		reader := con.NewLazyReader(func() (io.ReadCloser, error) {
			openCalls++
			return &readerReadCloser{reader: strings.NewReader("hello")}, nil
		})
		require.NotNil(t, reader)
		require.Equal(t, 0, openCalls)
	})
}

func TestLazyReaderRead(t *testing.T) {
	t.Run("OpenReaderOnFirstRead", func(t *testing.T) {
		openCalls := 0

		reader := con.NewLazyReader(func() (io.ReadCloser, error) {
			openCalls++
			return &readerReadCloser{reader: strings.NewReader("hello")}, nil
		})

		buf := make([]byte, 5)
		n, err := reader.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 5, n)
		require.Equal(t, "hello", string(buf))
		require.Equal(t, 1, openCalls)
	})

	t.Run("ReuseOpenedReaderAcrossReads", func(t *testing.T) {
		openCalls := 0

		reader := con.NewLazyReader(func() (io.ReadCloser, error) {
			openCalls++
			return &readerReadCloser{reader: strings.NewReader("hello")}, nil
		})

		first := make([]byte, 2)
		n, err := reader.Read(first)
		require.NoError(t, err)
		require.Equal(t, 2, n)
		require.Equal(t, "he", string(first))

		second := make([]byte, 3)
		n, err = reader.Read(second)
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Equal(t, "llo", string(second))
		require.Equal(t, 1, openCalls)
	})

	t.Run("PropagateOpenError", func(t *testing.T) {
		expectedErr := errors.New("open failure")
		openCalls := 0

		reader := con.NewLazyReader(func() (io.ReadCloser, error) {
			openCalls++
			return nil, expectedErr
		})

		buf := make([]byte, 1)
		n, err := reader.Read(buf)
		require.Equal(t, 0, n)
		require.ErrorIs(t, err, expectedErr)

		n, err = reader.Read(buf)
		require.Equal(t, 0, n)
		require.ErrorIs(t, err, expectedErr)
		require.Equal(t, 1, openCalls)
	})

	t.Run("PropagatesReadError_NotReOpened", func(t *testing.T) {
		expectedErr := errors.New("read failure")
		openCalls := 0
		reader := con.NewLazyReader(func() (io.ReadCloser, error) {
			openCalls++
			return &readerReadCloser{reader: failingReader{err: expectedErr}}, nil
		})

		buf := make([]byte, 1)
		n, err := reader.Read(buf)
		require.Equal(t, 0, n)
		require.ErrorIs(t, err, expectedErr)

		n, err = reader.Read(buf)
		require.Equal(t, 0, n)
		require.ErrorIs(t, err, expectedErr)

		require.Equal(t, 1, openCalls)
	})
}
