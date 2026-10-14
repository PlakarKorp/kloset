/*
 * Copyright (c) 2026 Gilles Chehade <gilles@poolp.org>
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

// Package analyzer defines the connector through which files are classified
// as they are ingested during a backup.
//
// Everything crossing the interface is plain data so that an analyzer can
// later live out of process behind the same SDK as importers and exporters.
package analyzer

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"sync"

	"github.com/PlakarKorp/kloset/connectors"
)

// Scope tells which kind of results an analyzer produces.  Both may be set.
type Scope uint8

const (
	// ScopeFile results are recorded on the file entry and indexed.
	ScopeFile Scope = 1 << iota

	// ScopeSnapshot results are produced once by Finalize and recorded in
	// the snapshot header.  Per-file results of an analyzer without
	// ScopeFile are only fed to Observe and never persisted.
	ScopeSnapshot
)

// ContentMode tells how much of a file's content an analyzer needs.
type ContentMode uint8

const (
	// ContentNone means the analyzer works from metadata only.
	ContentNone ContentMode = iota
	// ContentHead means the analyzer receives the first Filter.HeadSize bytes.
	ContentHead
	// ContentFull means the analyzer receives the whole stream, unless it
	// stops early by returning false from Processor.Write.
	ContentFull
)

// Filter selects the files an analyzer is given.  Within a list, any entry
// matches; across lists, all non-empty lists must match.
type Filter struct {
	// Extensions such as ".jpg", compared case-insensitively.
	Extensions []string
	// MIMETypes such as "text/plain" or "image/*".
	MIMETypes []string
	// Xattrs lists extended attribute names, any of which must be present.
	Xattrs []string

	Content  ContentMode
	HeadSize int64
}

// File describes the file being analyzed.  ContentType is the type detected
// from the first bytes, or guessed from the extension for empty files.
type File struct {
	Path        string
	Size        int64
	ContentType string
	Xattrs      []string
}

// Class is a single classification result.  Names are lowercase and
// dot-separated ("pii.email", "secret.aws-key") so that prefixes can be
// queried.  Count is only meaningful for snapshot-level results.
type Class struct {
	Name        string
	Probability float64
	Count       uint64
}

// Result groups the classes produced by one analyzer.
type Result struct {
	Analyzer string
	Classes  []Class
}

// Analyzer is a classifier plugged at the ingestion level.
//
// NewProcessor is called concurrently from the backup workers.  Observe is
// never called concurrently for a given analyzer, and Finalize is called
// once after the last Observe.
type Analyzer interface {
	Name() string
	Version() string
	Scope() Scope
	Filter() Filter

	NewProcessor(ctx context.Context, file File) (Processor, error)

	// Observe receives the per-file results of a ScopeSnapshot analyzer.
	Observe(ctx context.Context, pathname string, classes []Class) error
	// Finalize returns the snapshot-level results of a ScopeSnapshot analyzer.
	Finalize(ctx context.Context) ([]Class, error)

	Close(ctx context.Context) error
}

// Processor analyzes a single file.  Write must not retain p.  Returning
// false from Write means no more content is needed.
type Processor interface {
	Write(p []byte) (bool, error)
	Close() ([]Class, error)
}

type AnalyzerFn func(context.Context, *connectors.Options, string, map[string]string) (Analyzer, error)

var (
	muBackends sync.Mutex
	backends   = make(map[string]AnalyzerFn)
)

// classNameRe bounds both analyzer and class names.  Its charset is what
// keeps analyzers from leaking matched content (an e-mail address, a key)
// into names that end up in the snapshot index.
var classNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

func validName(name string) bool {
	return classNameRe.MatchString(name)
}

func Register(name string, backend AnalyzerFn) error {
	if !validName(name) {
		return fmt.Errorf("invalid analyzer name %q", name)
	}

	muBackends.Lock()
	defer muBackends.Unlock()

	if _, ok := backends[name]; ok {
		return fmt.Errorf("analyzer backend '%s' already registered", name)
	}
	backends[name] = backend
	return nil
}

func Unregister(name string) error {
	muBackends.Lock()
	defer muBackends.Unlock()

	if _, ok := backends[name]; !ok {
		return fmt.Errorf("analyzer backend '%s' not registered", name)
	}
	delete(backends, name)
	return nil
}

func Backends() []string {
	muBackends.Lock()
	defer muBackends.Unlock()

	names := make([]string, 0, len(backends))
	for name := range backends {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func NewAnalyzer(ctx context.Context, opts *connectors.Options, name string, config map[string]string) (Analyzer, error) {
	muBackends.Lock()
	backend, ok := backends[name]
	muBackends.Unlock()

	if !ok {
		return nil, fmt.Errorf("unsupported analyzer %q", name)
	}

	a, err := backend(ctx, opts, name, config)
	if err != nil {
		return nil, fmt.Errorf("analyzer %q: %w", name, err)
	}
	return a, nil
}
