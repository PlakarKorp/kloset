package importer

import (
	"io"
)

type LazyReader struct {
	init func() (io.ReadCloser, error)

	rd  io.ReadCloser
	err error
}

func NewLazyReader(open func() (io.ReadCloser, error)) *LazyReader {
	return &LazyReader{init: open}
}

func (lr *LazyReader) Read(p []byte) (int, error) {
	if lr.rd == nil && lr.err == nil {
		lr.rd, lr.err = lr.init()
	}

	if lr.err != nil {
		return 0, lr.err
	}

	return lr.rd.Read(p)
}

func (lr *LazyReader) Close() error {
	if lr.err != nil {
		return lr.err
	}
	if lr.rd == nil {
		return nil
	}
	return lr.rd.Close()
}
