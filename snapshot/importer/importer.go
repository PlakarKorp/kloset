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
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"syscall"

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
	Pathname           string
	Target             string
	FileInfo           objects.FileInfo
	ExtendedAttributes []string
	FileAttributes     uint32
	IsXattr            bool
	XattrName          string
	XattrType          objects.Attribute
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
	NewReader(string) (io.ReadCloser, error)
	NewExtendedAttributeReader(string, string) (io.ReadCloser, error)
	Close() error
}

type ImporterFn func(context.Context, string, map[string]string) (Importer, error)

var backends = location.New[ImporterFn]("fs")
var pluginsRegexp = regexp.MustCompile(`^[a-zA-Z0-9]+[a-zA-Z0-9-_]*-v[0-9]+\.[0-9]+\.[0-9]+\.ptar$`)

func forkChild(pluginPath string, name string) (int, int, error) {
	sp, err := syscall.Socketpair(syscall.AF_LOCAL, syscall.SOCK_STREAM, syscall.AF_UNSPEC)
	if err != nil {
		return -1, -1, fmt.Errorf("failed to create socketpair: %w", err)
	}

	procAttr := syscall.ProcAttr{}
	procAttr.Files = []uintptr{
		os.Stdin.Fd(),
		os.Stdout.Fd(),
		os.Stderr.Fd(),
		uintptr(sp[0]),
	}

	var pid int

	downloadPath := os.Getenv("HOME") + "/Downloads"
	pid, err = syscall.ForkExec(pluginPath, []string{pluginPath, downloadPath}, &procAttr)
	if err != nil {
		return -1, -1, fmt.Errorf("failed to ForkExec: %w", err)
	}

	if syscall.Close(sp[0]) != nil {
		return -1, -1, fmt.Errorf("failed to close socket: %w", err)
	}

	return pid, sp[1], nil
}

func Register(name string, backend ImporterFn) {
	if !backends.Register(name, backend) {
		log.Fatalf("backend '%s' registered twice", name)
	}
}

func Backends() []string {
	return backends.Names()
}

func NewImporter(ctx *kcontext.KContext, config map[string]string) (Importer, error) {
	location, ok := config["location"]
	if !ok {
		return nil, fmt.Errorf("missing location")
	}

	proto, location, backend, ok := backends.Lookup(location)
	if !ok {
		return nil, fmt.Errorf("unsupported importer protocol")
	}

	if proto == "fs" && !filepath.IsAbs(location) {
		location = filepath.Join(ctx.CWD, location)
		config["location"] = "fs://" + location
	} else {
		config["location"] = proto + "://" + location
	}

	return backend(ctx, proto, config)
}

func NewScanRecord(pathname, target string, fileinfo objects.FileInfo, xattr []string) *ScanResult {
	return &ScanResult{
		Record: &ScanRecord{
			Pathname:           pathname,
			Target:             target,
			FileInfo:           fileinfo,
			ExtendedAttributes: xattr,
		},
	}
}

func NewScanXattr(pathname, xattr string, kind objects.Attribute) *ScanResult {
	return &ScanResult{
		Record: &ScanRecord{
			Pathname:  pathname,
			IsXattr:   true,
			XattrName: xattr,
			XattrType: kind,
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
