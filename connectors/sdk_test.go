package connectors_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	con "github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/objects"
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

func TestNewRecord(t *testing.T) {
	t.Run("InitializeWithMetadata", func(t *testing.T) {
		var fileInfo objects.FileInfo
		xattrs := []string{"user.mime_type", "user.owner"}
		openCalls := 0

		record := con.NewRecord(
			"file.txt",
			"target",
			fileInfo,
			xattrs,
			func() (io.ReadCloser, error) {
				openCalls++
				return &sdkReadCloser{reader: strings.NewReader("payload")}, nil
			},
		)

		require.NotNil(t, record)
		require.Equal(t, "file.txt", record.Pathname)
		require.Equal(t, "target", record.Target)
		require.Equal(t, fileInfo, record.FileInfo)
		require.Equal(t, xattrs, record.ExtendedAttributes)
		require.NotNil(t, record.Reader)
		require.False(t, record.IsXattr)
		require.Equal(t, 0, openCalls)

		n, err := record.Reader.Read(make([]byte, 2))
		require.NoError(t, err)
		require.Equal(t, 2, n)
		require.Equal(t, 1, openCalls)
	})

	t.Run("DefaultUnsetFieldsToZeroValues", func(t *testing.T) {
		var fileInfo objects.FileInfo
		record := con.NewRecord(
			"file.txt",
			"target",
			fileInfo,
			[]string{"user.mime_type"},
			func() (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("payload")), nil
			},
		)

		require.NotNil(t, record)
		require.NoError(t, record.Err)
		require.Equal(t, fileInfo, record.FileInfo)
		require.False(t, record.IsXattr)
		require.Empty(t, record.XattrName)
		require.Zero(t, record.XattrType)
		require.Zero(t, record.FileAttributes)
	})

	t.Run("AcceptNilExtendedAttributes", func(t *testing.T) {
		var fileInfo objects.FileInfo
		record := con.NewRecord(
			"file.txt",
			"target",
			fileInfo,
			nil,
			func() (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("payload")), nil
			},
		)

		require.NotNil(t, record)
		require.Equal(t, fileInfo, record.FileInfo)
		require.Nil(t, record.ExtendedAttributes)
		require.NotNil(t, record.Reader)
	})

	t.Run("AcceptEmptyStringFields", func(t *testing.T) {
		var fileInfo objects.FileInfo
		record := con.NewRecord(
			"",
			"",
			fileInfo,
			nil,
			func() (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("payload")), nil
			},
		)

		require.NotNil(t, record)
		require.Empty(t, record.Pathname)
		require.Empty(t, record.Target)
		require.NoError(t, record.Err)
		require.False(t, record.IsXattr)
		require.Empty(t, record.XattrName)
	})

	t.Run("PanicWhenReadFuncIsNil", func(t *testing.T) {
		var readFn func() (io.ReadCloser, error)
		var fileInfo objects.FileInfo
		record := con.NewRecord(
			"file.txt",
			"target",
			fileInfo,
			nil,
			readFn,
		)

		require.NotNil(t, record)
		require.NotNil(t, record.Reader)

		require.Panics(t, func() {
			_, _ = record.Reader.Read(make([]byte, 1))
		})
	})
}

func TestNewXattr(t *testing.T) {
	t.Run("InitializeWithMetadata", func(t *testing.T) {
		var kind objects.Attribute
		openCalls := 0

		record := con.NewXattr(
			"file.txt",
			"user.mime_type",
			kind,
			func() (io.ReadCloser, error) {
				openCalls++
				return &sdkReadCloser{reader: strings.NewReader("payload")}, nil
			},
		)

		require.NotNil(t, record)
		require.Equal(t, "file.txt", record.Pathname)
		require.True(t, record.IsXattr)
		require.Equal(t, "user.mime_type", record.XattrName)
		require.Equal(t, kind, record.XattrType)
		require.NotNil(t, record.Reader)
		require.Equal(t, 0, openCalls)

		buf := make([]byte, 7)
		n, err := record.Reader.Read(buf)
		require.NoError(t, err)
		require.Equal(t, 7, n)
		require.Equal(t, "payload", string(buf))
		require.Equal(t, 1, openCalls)
	})

	t.Run("DefaultUnsetFieldsToZeroValues", func(t *testing.T) {
		var kind objects.Attribute
		record := con.NewXattr(
			"file.txt",
			"user.mime_type",
			kind,
			func() (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("payload")), nil
			},
		)

		require.NotNil(t, record)
		require.NoError(t, record.Err)
		require.Empty(t, record.Target)
		require.Nil(t, record.ExtendedAttributes)
		require.Zero(t, record.FileAttributes)
		require.Empty(t, record.FileInfo)
	})

	t.Run("AcceptEmptyStringFields", func(t *testing.T) {
		var kind objects.Attribute
		record := con.NewXattr(
			"",
			"",
			kind,
			func() (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader("payload")), nil
			},
		)

		require.NotNil(t, record)
		require.Empty(t, record.Pathname)
		require.True(t, record.IsXattr)
		require.Empty(t, record.XattrName)
		require.Equal(t, kind, record.XattrType)
		require.Empty(t, record.FileInfo)
	})

	t.Run("PanicWhenReadFuncIsNil", func(t *testing.T) {
		var readFn func() (io.ReadCloser, error)
		var kind objects.Attribute

		record := con.NewXattr(
			"file.txt",
			"user.mime_type",
			kind,
			readFn,
		)

		require.NotNil(t, record)
		require.NotNil(t, record.Reader)

		require.Panics(t, func() {
			_, _ = record.Reader.Read(make([]byte, 1))
		})
	})
}
