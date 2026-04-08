package connectors_test

import (
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
