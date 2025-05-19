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

	t := strings.SplitN(target, "/", 2)

	for _, match := range matches {
		m := strings.SplitN(match, "/", 2)

		if len(m) == 2 && len(t) == 2 && m[0] == t[0] && m[0] == t[1] {
			return true
		} else if m[0] == t[0] {
			return true
		}
	}
	return false
}

func visitmimes(ctx context.Context, snap *Snapshot, opts *SearchOpts) (iter.Seq2[*vfs.Entry, error], error) {
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

				if opts.NameFilter != "" {
					matched := false
					if path.Base(entryPath) == opts.NameFilter {
						matched = true
					}
					if !matched {
						matched, err := path.Match(opts.NameFilter, path.Base(entryPath))
						if err != nil {
							continue
						}
						if !matched {
							continue
						}
					}
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

func visitfiles(ctx context.Context, snap *Snapshot, opts *SearchOpts) (iter.Seq2[*vfs.Entry, error], error) {
	if opts.Recursive && len(opts.Mimes) > 0 {
		it, err := visitmimes(ctx, snap, opts)
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

			if !matchmime(opts.Mimes, entry.ContentType()) {
				continue
			}

			if opts.NameFilter != "" {
				matched := false
				if path.Base(entryPath) == opts.NameFilter {
					matched = true
				}
				if !matched {
					matched, err = path.Match(opts.NameFilter, path.Base(entryPath))
					if err != nil {
						continue
					}
				}
				if !matched {
					matched, err = regexp.Match(opts.NameFilter, []byte(path.Base(entryPath)))
					if err != nil {
						continue
					}
					if !matched {
						continue
					}
				}
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

	it, err := visitfiles(ctx, snap, opts)
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
