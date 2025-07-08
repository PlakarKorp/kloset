package reading

import (
	"fmt"
	"io"
)

type closingReader struct {
	reader   io.Reader
	closer   io.Closer
	isClosed bool
}

func (cr *closingReader) Read(p []byte) (int, error) {
	if cr.isClosed {
		return 0, io.EOF
	}

	n, err := cr.reader.Read(p)
	if err == io.EOF {
		// Close the file when EOF is reached
		closeErr := cr.closer.Close()
		cr.isClosed = true
		if closeErr != nil {
			return n, fmt.Errorf("error closing reader: %w", closeErr)
		}
	}
	return n, err
}

func ClosingReader(reader io.ReadCloser) io.Reader {
	return &closingReader{
		reader:   reader,
		closer:   reader,
		isClosed: false,
	}
}

type closingLimitedReader struct {
	reader   io.Reader
	closer   io.Closer
	isClosed bool
	read     int64
	limit    int64
}

func (cr *closingLimitedReader) Read(p []byte) (int, error) {
	if cr.isClosed {
		return 0, io.EOF
	}

	n, err := cr.reader.Read(p)
	cr.read += int64(n)
	if cr.read == cr.limit || err == io.EOF {
		closeErr := cr.closer.Close()
		cr.isClosed = true
		if closeErr != nil {
			return n, fmt.Errorf("error closing reader: %w", closeErr)
		}
	}

	return n, err
}

func ClosingLimitedReader(reader io.ReadCloser, length int64) io.Reader {
	return &closingLimitedReader{
		reader: &io.LimitedReader{
			R: reader,
			N: length,
		},
		closer:   reader,
		limit:    length,
		isClosed: false,
	}
}
