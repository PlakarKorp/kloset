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

func (s *Record) Ok() *Result {
	return &Result{
		Pathname: s.Pathname,
		FileInfo: s.FileInfo,
		IsError:  false,
		IsXattr:  s.IsXattr,
		Err:      nil,
	}
}

func (s *Record) Error(err error) *Result {
	return &Result{
		Pathname: s.Pathname,
		FileInfo: s.FileInfo,
		IsError:  false,
		IsXattr:  s.IsXattr,
		Err:      err,
	}
}

type RecordError struct { // ScanError
	Pathname string
	IsXattr  bool
	Err      error
}

func (re *RecordError) Ok() *Result {
	return &Result{
		Pathname: re.Pathname,
		IsError:  true,
		IsXattr:  re.IsXattr,
		Err:      nil,
	}
}

func (re *RecordError) Error(err error) *Result {
	return &Result{
		Pathname: re.Pathname,
		IsError:  true,
		IsXattr:  re.IsXattr,
		Err:      err,
	}
}

/// acknowledgment

type Result struct {
	Pathname string
	FileInfo objects.FileInfo
	IsError  bool
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
		Error: &RecordError{
			Pathname: pathname,
			Err:      err,
		},
	}
}
