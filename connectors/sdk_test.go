package connectors_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	con "github.com/PlakarKorp/kloset/connectors"
	"github.com/stretchr/testify/require"
)

type sdkReadCloser struct {
	reader     io.Reader
	closeErr   error
	closeCalls int
}

func (src *sdkReadCloser) Read(p []byte) (int, error) {
	return src.reader.Read(p)
}

func (src *sdkReadCloser) Close() error {
	src.closeCalls++
	return src.closeErr
}

func TestRecordClose(t *testing.T) {
	t.Run("FailsIfReaderIsMissing", func(t *testing.T) {
		record := con.Record{}

		err := record.Close()
		require.ErrorIs(t, err, errors.ErrUnsupported)
	})

	t.Run("CloseReaderToo", func(t *testing.T) {
		reader := sdkReadCloser{reader: strings.NewReader("hello")}
		record := con.Record{Reader: &reader}

		err := record.Close()
		require.NoError(t, err)
		require.Equal(t, 1, reader.closeCalls)
	})

	t.Run("PropagatesCloseError", func(t *testing.T) {
		expectedErr := errors.New("close failure")
		reader := sdkReadCloser{
			reader:   strings.NewReader("hello"),
			closeErr: expectedErr,
		}
		record := con.Record{Reader: &reader}

		err := record.Close()
		require.ErrorIs(t, err, expectedErr)
		require.Equal(t, 1, reader.closeCalls)
	})
}

func TestRecordOk(t *testing.T) {
	t.Run("ReturnSuccessfulResultAndCloseReader", func(t *testing.T) {
		reader := sdkReadCloser{reader: strings.NewReader("hello")}
		record := con.Record{
			Reader:   &reader,
			Pathname: "file.txt",
			Target:   "target",
		}

		result := record.Ok()
		require.NotNil(t, result)
		require.NoError(t, result.Err)
		require.Equal(t, record, result.Record)
		require.Equal(t, 1, reader.closeCalls)
	})

	t.Run("IgnoreCloseError", func(t *testing.T) {
		reader := sdkReadCloser{
			reader:   strings.NewReader("hello"),
			closeErr: errors.New("close failure"),
		}
		record := con.Record{
			Reader:   &reader,
			Pathname: "file.txt",
		}

		result := record.Ok()
		require.NotNil(t, result)
		require.NoError(t, result.Err)
		require.Equal(t, record, result.Record)
		require.Equal(t, 1, reader.closeCalls)
	})
}

func TestRecordError(t *testing.T) {
	t.Run("ReturnFailedResultAndCloseReader", func(t *testing.T) {
		expectedErr := errors.New("record failure")
		reader := sdkReadCloser{reader: strings.NewReader("hello")}
		record := con.Record{
			Reader:   &reader,
			Pathname: "file.txt",
			Target:   "target",
		}

		result := record.Error(expectedErr)
		require.NotNil(t, result)
		require.ErrorIs(t, result.Err, expectedErr)
		require.Equal(t, record, result.Record)
		require.Equal(t, 1, reader.closeCalls)
	})

	t.Run("IgnoreCloseError", func(t *testing.T) {
		expectedErr := errors.New("record failure")
		reader := sdkReadCloser{
			reader:   strings.NewReader("hello"),
			closeErr: errors.New("close failure"),
		}
		record := con.Record{
			Reader:   &reader,
			Pathname: "file.txt",
		}

		result := record.Error(expectedErr)
		require.NotNil(t, result)
		require.ErrorIs(t, result.Err, expectedErr)
		require.Equal(t, record, result.Record)
		require.Equal(t, 1, reader.closeCalls)
	})
}
