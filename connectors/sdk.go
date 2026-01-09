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

// requests
type Record struct {
	Reader io.ReadCloser

	Pathname string
	Err      error

	IsXattr   bool
	XattrName string
	XattrType objects.Attribute

	Target             string
	FileInfo           objects.FileInfo
	ExtendedAttributes []string
	FileAttributes     uint32
}

func (s *Record) Close() error {
	if s.Reader == nil {
		return errors.ErrUnsupported
	}
	return s.Reader.Close()
}

func (s *Record) Ok() *Result {
	return &Result{
		Record: *s,
		Err:    nil,
	}
}

func (s *Record) Error(err error) *Result {
	return &Result{
		Record: *s,
		Err:    err,
	}
}

// result
type Result struct {
	Record Record
	Err    error
}

func NewRecord(pathname, target string, fileinfo objects.FileInfo, xattr []string, read func() (io.ReadCloser, error)) *Record {
	return &Record{
		Pathname:           pathname,
		Target:             target,
		FileInfo:           fileinfo,
		ExtendedAttributes: xattr,
		Reader:             NewLazyReader(read),
	}
}

func NewXattr(pathname, xattr string, kind objects.Attribute, read func() (io.ReadCloser, error)) *Record {
	return &Record{
		Pathname:  pathname,
		IsXattr:   true,
		XattrName: xattr,
		XattrType: kind,
		Reader:    NewLazyReader(read),
	}
}

func NewError(pathname string, err error) *Record {
	return &Record{
		Pathname: pathname,
		Err:      err,
	}
}
