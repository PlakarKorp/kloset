package snapshot

import (
	"context"
	"iter"
	"path"
	"regexp"
	"strings"

	"github.com/PlakarKorp/kloset/snapshot/vfs"
)

type SearchOpts struct {
	// file filters
	Recursive  bool
	Prefix     string // prefix directory
	Mimes      []string
	NameFilter string

	// pagination
	Offset int
	Limit  int
}

func matchmime(matches []string, target string) bool {
	if len(matches) == 0 {
		return true
	}

	// Drop the parameters, as the content-type index does.
	target, _, _ = strings.Cut(target, ";")
	t := strings.SplitN(target, "/", 2)

	for _, match := range matches {
		m := strings.SplitN(match, "/", 2)

		if m[0] != t[0] {
			continue
		}
		if len(m) == 1 || (len(t) == 2 && m[1] == t[1]) {
			return true
		}
	}
	return false
}

// nameFilter matches base names against a search pattern: the exact name, a
// glob, or a regular expression, whichever matches first.
type nameFilter struct {
	pattern string
	re      *regexp.Regexp
}

func newNameFilter(pattern string) *nameFilter {
	if pattern == "" {
		return nil
	}
	// A pattern that is not a valid regular expression may still be a valid
	// glob, so a compile error only disables the regexp arm.
	re, _ := regexp.Compile(pattern)
	return &nameFilter{pattern: pattern, re: re}
}

func (f *nameFilter) match(name string) bool {
	if f == nil || name == f.pattern {
		return true
	}
	if matched, err := path.Match(f.pattern, name); err == nil && matched {
		return true
	}
	return f.re != nil && f.re.MatchString(name)
}

func visitmimes(ctx context.Context, snap *Snapshot, opts *SearchOpts, names *nameFilter) (iter.Seq2[*vfs.Entry, error], error) {
	idx, err := snap.ContentTypeIdx()
	if err != nil {
		return nil, err
	}
	if idx == nil {
		return nil, nil
	}

	fsp, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}

	return func(yield func(*vfs.Entry, error) bool) {
		for _, mime := range opts.Mimes {
			prefix := "/" + mime
			if !strings.HasSuffix(prefix, "/") {
				prefix += "/"
			}

			// XXX: to further optimize this routine we'd need a ScanGlob
			// method.  Basically what we're trying to do here is to
			// resolve globs like "application/*/prefix/*"
			it, err := idx.ScanFrom(prefix)
			if err != nil {
				yield(nil, err)
				return
			}

			for it.Next() {
				if err := ctx.Err(); err != nil {
					yield(nil, err)
					return
				}

				p, mac := it.Current()

				if !strings.HasPrefix(p, prefix) {
					break
				}

				entry, err := fsp.ResolveEntry(mac)
				if err != nil {
					yield(nil, err)
					return
				}
				entryPath := entry.Path()
				if !strings.HasPrefix(entryPath, opts.Prefix) {
					continue
				}

				if !names.match(path.Base(entryPath)) {
					continue
				}

				if !yield(entry, nil) {
					return
				}
			}
			if err := it.Err(); err != nil {
				yield(nil, err)
			}
		}
	}, nil
}

func visitfiles(ctx context.Context, snap *Snapshot, opts *SearchOpts, names *nameFilter) (iter.Seq2[*vfs.Entry, error], error) {
	if opts.Recursive && len(opts.Mimes) > 0 {
		it, err := visitmimes(ctx, snap, opts, names)
		if it != nil || err != nil {
			return it, err
		}
		// fallback
	}

	fsc, err := snap.Filesystem()
	if err != nil {
		return nil, err
	}

	var it iter.Seq2[*vfs.Entry, error]
	if opts.Recursive {
		it = fsc.Files(opts.Prefix)
	} else {
		it, err = fsc.Children(opts.Prefix)
		if err != nil {
			return nil, err
		}
	}

	return func(yield func(*vfs.Entry, error) bool) {
		for entry, err := range it {
			if err != nil {
				yield(nil, err)
				return
			}

			if err := ctx.Err(); err != nil {
				yield(nil, err)
				return
			}

			entryPath := entry.Path()
			if !strings.HasPrefix(entryPath, opts.Prefix) {
				continue
			}

			if !matchmime(opts.Mimes, entry.GetContentType()) {
				continue
			}

			if !names.match(path.Base(entryPath)) {
				continue
			}

			if !yield(entry, nil) {
				return
			}
		}
	}, nil
}

func (snap *Snapshot) Search(ctx context.Context, opts *SearchOpts) (iter.Seq2[*vfs.Entry, error], error) {
	if opts.Prefix != "" {
		opts.Prefix = path.Clean(opts.Prefix)
		if !strings.HasSuffix(opts.Prefix, "/") {
			opts.Prefix += "/"
		}
	}

	it, err := visitfiles(ctx, snap, opts, newNameFilter(opts.NameFilter))
	if err != nil {
		return nil, err
	}

	var n, m int
	return func(yield func(*vfs.Entry, error) bool) {
		for entry, err := range it {
			if err != nil {
				yield(nil, err)
				return
			}

			// eventually other filters on entry, e.g. size or pattern

			if opts.Recursive && entry.IsDir() {
				continue
			}

			if n++; opts.Offset != 0 && n <= opts.Offset {
				continue
			}
			if m++; opts.Limit != 0 && m > opts.Limit {
				return
			}
			if !yield(entry, nil) {
				return
			}
		}
	}, nil
}
