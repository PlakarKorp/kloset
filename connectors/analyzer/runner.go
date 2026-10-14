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

package analyzer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/PlakarKorp/kloset/logging"
)

// ClassError is recorded in place of an analyzer's results when it failed
// on a file, so that "not analyzed" can be told apart from "nothing found".
// It cannot collide with analyzer output: class names may not start with '_'.
const ClassError = "_error"

const (
	// DefaultFileBudget bounds the time an analyzer may spend on one file.
	DefaultFileBudget = 30 * time.Second

	maxClassesPerResult = 64
)

type registered struct {
	analyzer Analyzer
	name     string
	scope    Scope
	filter   Filter

	// serializes Observe so analyzers never deal with concurrency
	observeMu sync.Mutex
}

// Runner drives a set of analyzers over the files of a backup.  It isolates
// the backup from them: an analyzer that fails, panics or overruns its
// budget loses its result for that file, the backup goes on.
type Runner struct {
	analyzers  []*registered
	byName     map[string]*registered
	logger     *logging.Logger
	FileBudget time.Duration
}

func NewRunner(logger *logging.Logger, analyzers []Analyzer) (*Runner, error) {
	r := &Runner{
		byName:     make(map[string]*registered),
		logger:     logger,
		FileBudget: DefaultFileBudget,
	}

	for _, a := range analyzers {
		name := a.Name()
		if !validName(name) {
			return nil, fmt.Errorf("invalid analyzer name %q", name)
		}
		if _, ok := r.byName[name]; ok {
			return nil, fmt.Errorf("analyzer %q configured twice", name)
		}

		scope := a.Scope()
		if scope&(ScopeFile|ScopeSnapshot) == 0 {
			return nil, fmt.Errorf("analyzer %q has no scope", name)
		}

		filter, err := normalizeFilter(a.Filter())
		if err != nil {
			return nil, fmt.Errorf("analyzer %q: %w", name, err)
		}

		reg := &registered{
			analyzer: a,
			name:     name,
			scope:    scope,
			filter:   filter,
		}
		r.analyzers = append(r.analyzers, reg)
		r.byName[name] = reg
	}
	return r, nil
}

func normalizeFilter(f Filter) (Filter, error) {
	out := Filter{
		Content:  f.Content,
		HeadSize: f.HeadSize,
		Xattrs:   f.Xattrs,
	}

	switch f.Content {
	case ContentNone, ContentFull:
	case ContentHead:
		if f.HeadSize <= 0 {
			return Filter{}, fmt.Errorf("head content requested with a head size of %d", f.HeadSize)
		}
	default:
		return Filter{}, fmt.Errorf("unknown content mode %d", f.Content)
	}

	for _, ext := range f.Extensions {
		ext = strings.ToLower(ext)
		if !strings.HasPrefix(ext, ".") {
			ext = "." + ext
		}
		out.Extensions = append(out.Extensions, ext)
	}
	for _, mt := range f.MIMETypes {
		out.MIMETypes = append(out.MIMETypes, strings.ToLower(mt))
	}
	return out, nil
}

// Empty reports whether there is nothing to run.
func (r *Runner) Empty() bool {
	return r == nil || len(r.analyzers) == 0
}

// Persists reports whether the named analyzer's per-file results are kept,
// which is what decides whether cached results are carried forward.
func (r *Runner) Persists(name string) bool {
	if r == nil {
		return false
	}
	reg, ok := r.byName[name]
	return ok && reg.scope&ScopeFile != 0
}

func (r *Runner) call(what string, fn func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic in %s: %v", what, p)
		}
	}()
	if err := fn(); err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	return nil
}

func (r *Runner) warn(name, pathname string, err error) {
	if r.logger == nil {
		return
	}
	if pathname == "" {
		r.logger.Warn("analyzer %s: %v", name, err)
	} else {
		r.logger.Warn("analyzer %s: %s: %v", name, pathname, err)
	}
}

// sanitize drops the classes an analyzer must not produce and bounds what
// is kept.
func (r *Runner) sanitize(name, pathname string, classes []Class) []Class {
	out := make([]Class, 0, len(classes))
	seen := make(map[string]struct{}, len(classes))
	for _, c := range classes {
		if !validName(c.Name) {
			r.warn(name, pathname, fmt.Errorf("dropping invalid class name %q", c.Name))
			continue
		}
		if math.IsNaN(c.Probability) || c.Probability < 0 || c.Probability > 1 {
			r.warn(name, pathname, fmt.Errorf("dropping class %q with probability %v", c.Name, c.Probability))
			continue
		}
		if _, dup := seen[c.Name]; dup {
			continue
		}
		if len(out) == maxClassesPerResult {
			r.warn(name, pathname, fmt.Errorf("too many classes, keeping the first %d", maxClassesPerResult))
			break
		}
		seen[c.Name] = struct{}{}
		out = append(out, c)
	}
	return out
}

func extMatches(exts []string, pathname string) bool {
	if len(exts) == 0 {
		return true
	}
	ext := strings.ToLower(path.Ext(pathname))
	for _, e := range exts {
		if e == ext {
			return true
		}
	}
	return false
}

func xattrsMatch(want []string, have []string) bool {
	if len(want) == 0 {
		return true
	}
	for _, w := range want {
		for _, h := range have {
			if w == h {
				return true
			}
		}
	}
	return false
}

func mimeMatches(types []string, contentType string) bool {
	if len(types) == 0 {
		return true
	}
	mt, _, _ := strings.Cut(contentType, ";")
	mt = strings.ToLower(strings.TrimSpace(mt))
	major, _, _ := strings.Cut(mt, "/")
	for _, t := range types {
		if t == mt {
			return true
		}
		if prefix, ok := strings.CutSuffix(t, "/*"); ok && prefix == major {
			return true
		}
	}
	return false
}

type fileProcessor struct {
	reg       *registered
	proc      Processor
	remaining int64 // bytes still wanted, -1 for unbounded
	elapsed   time.Duration
	err       error
}

func (fp *fileProcessor) wantsContent() bool {
	return fp.err == nil && fp.remaining != 0
}

// FileRun is the analysis of one file.  It is owned by a single backup
// worker and must not be shared.
type FileRun struct {
	r          *Runner
	ctx        context.Context
	file       File
	candidates []*registered
	procs      []*fileProcessor
	begun      bool
}

// Start returns the analysis of a file, or nil when no analyzer can be
// interested in it.  This is the first filtering stage: extensions and
// extended attributes are known before reading any content.
func (r *Runner) Start(ctx context.Context, pathname string, size int64, xattrs []string) *FileRun {
	if r.Empty() {
		return nil
	}

	var candidates []*registered
	for _, reg := range r.analyzers {
		if !extMatches(reg.filter.Extensions, pathname) {
			continue
		}
		if !xattrsMatch(reg.filter.Xattrs, xattrs) {
			continue
		}
		candidates = append(candidates, reg)
	}
	if len(candidates) == 0 {
		return nil
	}

	return &FileRun{
		r:   r,
		ctx: ctx,
		file: File{
			Path:   pathname,
			Size:   size,
			Xattrs: xattrs,
		},
		candidates: candidates,
	}
}

// Begin is the second filtering stage, once the content type is known.
// It is called before the first Write, or from Finish for empty files.
func (fr *FileRun) Begin(contentType string) {
	if fr == nil || fr.begun {
		return
	}
	fr.begun = true
	fr.file.ContentType = contentType

	for _, reg := range fr.candidates {
		if !mimeMatches(reg.filter.MIMETypes, contentType) {
			continue
		}

		fp := &fileProcessor{reg: reg}
		switch reg.filter.Content {
		case ContentNone:
			fp.remaining = 0
		case ContentHead:
			fp.remaining = reg.filter.HeadSize
		case ContentFull:
			fp.remaining = -1
		}

		t0 := time.Now()
		fp.err = fr.r.call("NewProcessor", func() error {
			proc, err := reg.analyzer.NewProcessor(fr.ctx, fr.file)
			if err == nil && proc == nil {
				err = fmt.Errorf("no processor returned")
			}
			fp.proc = proc
			return err
		})
		fp.elapsed += time.Since(t0)
		fr.procs = append(fr.procs, fp)
	}
}

// Write hands a chunk of content to the processors that still want some.
func (fr *FileRun) Write(p []byte) {
	if fr == nil || len(p) == 0 {
		return
	}

	for _, fp := range fr.procs {
		if !fp.wantsContent() {
			continue
		}

		buf := p
		if fp.remaining > 0 && int64(len(buf)) > fp.remaining {
			buf = buf[:fp.remaining]
		}

		t0 := time.Now()
		var more bool
		fp.err = fr.r.call("Write", func() error {
			var err error
			more, err = fp.proc.Write(buf)
			return err
		})
		fp.elapsed += time.Since(t0)

		if fp.err == nil && fp.elapsed > fr.r.FileBudget {
			fp.err = fmt.Errorf("exceeded its budget of %s", fr.r.FileBudget)
		}

		switch {
		case fp.err != nil, !more:
			fp.remaining = 0
		case fp.remaining > 0:
			fp.remaining -= int64(len(buf))
		}
	}
}

// Finish closes the processors and returns the results to record on the
// file entry.  Results of snapshot-scoped analyzers are handed to Observe.
func (fr *FileRun) Finish(contentType string) []Result {
	if fr == nil {
		return nil
	}
	fr.Begin(contentType)

	var results []Result
	for _, fp := range fr.procs {
		var classes []Class
		if fp.err == nil {
			fp.err = fr.r.call("Close", func() error {
				var err error
				classes, err = fp.proc.Close()
				return err
			})
		}

		if fp.err != nil {
			fr.r.warn(fp.reg.name, fr.file.Path, fp.err)
			if fp.reg.scope&ScopeFile != 0 {
				results = append(results, Result{
					Analyzer: fp.reg.name,
					Classes:  []Class{{Name: ClassError}},
				})
			}
			continue
		}

		classes = fr.r.sanitize(fp.reg.name, fr.file.Path, classes)
		if len(classes) == 0 {
			continue
		}

		fr.r.observe(fr.ctx, fp.reg, fr.file.Path, classes)
		if fp.reg.scope&ScopeFile != 0 {
			results = append(results, Result{
				Analyzer: fp.reg.name,
				Classes:  classes,
			})
		}
	}
	return results
}

// ObserveCached feeds the results carried forward from a previous snapshot
// for a file that was not re-read, so snapshot-level results still account
// for it.
func (r *Runner) ObserveCached(ctx context.Context, pathname string, results []Result) {
	if r.Empty() {
		return
	}
	for _, res := range results {
		reg, ok := r.byName[res.Analyzer]
		if !ok {
			continue
		}
		var classes []Class
		for _, c := range res.Classes {
			if c.Name != ClassError {
				classes = append(classes, c)
			}
		}
		if len(classes) > 0 {
			r.observe(ctx, reg, pathname, classes)
		}
	}
}

func (r *Runner) observe(ctx context.Context, reg *registered, pathname string, classes []Class) {
	if reg.scope&ScopeSnapshot == 0 {
		return
	}

	reg.observeMu.Lock()
	defer reg.observeMu.Unlock()

	if err := r.call("Observe", func() error {
		return reg.analyzer.Observe(ctx, pathname, classes)
	}); err != nil {
		r.warn(reg.name, pathname, err)
	}
}

// Finalize returns the snapshot-level results.  It must be called once, after
// every FileRun has finished.
func (r *Runner) Finalize(ctx context.Context) []Result {
	if r.Empty() {
		return nil
	}

	var results []Result
	for _, reg := range r.analyzers {
		if reg.scope&ScopeSnapshot == 0 {
			continue
		}

		var classes []Class
		err := r.call("Finalize", func() error {
			var err error
			classes, err = reg.analyzer.Finalize(ctx)
			return err
		})
		if err != nil {
			r.warn(reg.name, "", err)
			results = append(results, Result{
				Analyzer: reg.name,
				Classes:  []Class{{Name: ClassError}},
			})
			continue
		}

		classes = r.sanitize(reg.name, "", classes)
		if len(classes) > 0 {
			results = append(results, Result{Analyzer: reg.name, Classes: classes})
		}
	}
	return results
}

func (r *Runner) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	var errs []error
	for _, reg := range r.analyzers {
		if err := r.call("Close", func() error {
			return reg.analyzer.Close(ctx)
		}); err != nil {
			errs = append(errs, fmt.Errorf("analyzer %s: %w", reg.name, err))
		}
	}
	return errors.Join(errs...)
}
