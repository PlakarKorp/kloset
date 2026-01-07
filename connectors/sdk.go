package connectors

import (
	"errors"
	"io"

	"github.com/PlakarKorp/kloset/objects"
)

type Options struct {
	Hostname        string
	OperatingSystem string
	Architecture    string
	CWD             string
	MaxConcurrency  int
	Excludes        []string

	Stdin  io.Reader `msgpack:"-"`
	Stdout io.Writer `msgpack:"-"`
	Stderr io.Writer `msgpack:"-"`
}

type Row struct { // ScanResult
	Record *Record
	Error  *RecordError
}

type Record struct { // ScanRecord
	Reader io.ReadCloser

	Pathname           string
	Target             string
	FileInfo           objects.FileInfo
	ExtendedAttributes []string
	FileAttributes     uint32
	IsXattr            bool
	XattrName          string
	XattrType          objects.Attribute
}

func (s *Record) Close() error {
	if s.Reader == nil {
		return errors.ErrUnsupported
	}
	return s.Reader.Close()
}

type RecordError struct { // ScanError
	Pathname string //
	Err      error
}

type Result struct {
	Pathname string
	IsXattr  bool
	Err      error
}

func NewRecord(pathname, target string, fileinfo objects.FileInfo, xattr []string, read func() (io.ReadCloser, error)) *Row {
	return &Row{
		Record: &Record{
			Pathname:           pathname,
			Target:             target,
			FileInfo:           fileinfo,
			ExtendedAttributes: xattr,
			Reader:             NewLazyReader(read),
		},
	}
}

func NewXattr(pathname, xattr string, kind objects.Attribute, read func() (io.ReadCloser, error)) *Row {
	return &Row{
		Record: &Record{
			Pathname:  pathname,
			IsXattr:   true,
			XattrName: xattr,
			XattrType: kind,
			Reader:    NewLazyReader(read),
		},
	}
}

func NewError(pathname string, err error) *Row {
	return &Row{
		Error: &Error{
			Pathname: pathname,
			Err:      err,
		},
	}
}
