/*
 * Copyright (c) 2023 Gilles Chehade <gilles@poolp.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package importer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"

	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
)

type ScanResult struct {
	Record *ScanRecord
	Error  *ScanError
}

type ExtendedAttributes struct {
	Name  string
	Value []byte
}

type ScanRecord struct {
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

func (s *ScanRecord) Close() error {
	if s.Reader == nil {
		return errors.ErrUnsupported
	}
	return s.Reader.Close()
}

type ScanError struct {
	Pathname string
	Err      error
}

type Importer interface {
	Origin() string
	Type() string
	Root() string
	Scan() (<-chan *ScanResult, error)
	Close() error
}

type Options struct {
	Hostname        string
	OperatingSystem string
	Architecture    string
	CWD             string
	MaxConcurrency  int

	Stdin  io.Reader `msgpack:"-"`
	Stdout io.Writer `msgpack:"-"`
	Stderr io.Writer `msgpack:"-"`
}

type ImporterFn func(context.Context, *Options, string, map[string]string) (Importer, error)

var backends = location.New[ImporterFn]("fs")

func Register(name string, flags location.Flags, backend ImporterFn) {
	if !backends.Register(name, backend, flags) {
		log.Fatalf("backend '%s' registered twice", name)
	}
}

func Backends() []string {
	return backends.Names()
}

func NewImporter(ctx *kcontext.KContext, opts *Options, config map[string]string) (Importer, error) {
	loc, ok := config["location"]
	if !ok {
		return nil, fmt.Errorf("missing location")
	}

	proto, loc, backend, flags, ok := backends.Lookup(loc)
	if !ok {
		return nil, fmt.Errorf("unsupported importer protocol")
	}

	if flags&location.FLAG_LOCALFS != 0 && !filepath.IsAbs(loc) {
		loc = filepath.Join(ctx.CWD, loc)
	}
	config["location"] = proto + "://" + loc

	return backend(ctx, opts, proto, config)
}

func NewScanRecord(pathname, target string, fileinfo objects.FileInfo, xattr []string, read func() (io.ReadCloser, error)) *ScanResult {
	return &ScanResult{
		Record: &ScanRecord{
			Pathname:           pathname,
			Target:             target,
			FileInfo:           fileinfo,
			ExtendedAttributes: xattr,
			Reader:             NewLazyReader(read),
		},
	}
}

func NewScanXattr(pathname, xattr string, kind objects.Attribute, read func() (io.ReadCloser, error)) *ScanResult {
	return &ScanResult{
		Record: &ScanRecord{
			Pathname:  pathname,
			IsXattr:   true,
			XattrName: xattr,
			XattrType: kind,
			Reader:    NewLazyReader(read),
		},
	}
}

func NewScanError(pathname string, err error) *ScanResult {
	return &ScanResult{
		Error: &ScanError{
			Pathname: pathname,
			Err:      err,
		},
	}
}
