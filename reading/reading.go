package reading

import (
	"io"
)

type ReaderAtCloser interface {
	io.ReaderAt
	Close() error
}

type SectionReadCloser struct {
	rdc ReaderAtCloser
	*io.SectionReader
}

func NewSectionReadCloser(r ReaderAtCloser, off int64, n int64) *SectionReadCloser {
	return &SectionReadCloser{
		rdc:           r,
		SectionReader: io.NewSectionReader(r, off, n),
	}
}

func (sc *SectionReadCloser) Close() error {
	return sc.rdc.Close()
}
