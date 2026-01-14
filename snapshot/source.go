package snapshot

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/PlakarKorp/kloset/exclude"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/snapshot/header"
	"github.com/PlakarKorp/kloset/snapshot/importer"
)

type importerWrapper struct {
	root string
	imp  importer.Importer
}

type Source struct {
	ctx context.Context

	// Once Importer.Root() is ctx free get rid of this little shim.
	importers []*importerWrapper

	origin string
	typ    string
	root   string
	flags  location.Flags

	excludes *exclude.RuleSet

	failure error
}

func NewSource(ctx context.Context, flags location.Flags, importers ...importer.Importer) (*Source, error) {
	s := &Source{
		ctx:      ctx,
		flags:    flags,
		excludes: exclude.NewRuleSet(),
	}

	var is []*importerWrapper
	for i, imp := range importers {
		origin, err := imp.Origin(ctx)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			s.origin = origin
		} else if s.origin != origin {
			return nil, fmt.Errorf("mismatched origin when adding importer %q expected %q", origin, s.origin)
		}

		typ, err := imp.Type(ctx)
		if err != nil {
			return nil, err
		}

		if i == 0 {
			s.typ = typ
		} else if s.typ != typ {
			return nil, fmt.Errorf("mismatched type when adding importer %q expected %q", typ, s.typ)
		}

		root, err := imp.Root(ctx)
		if err != nil {
			return nil, err
		}

		root = path.Clean(root)

		is = append(is, &importerWrapper{root, imp})
	}

	slices.SortFunc(is, func(a, b *importerWrapper) int {
		return strings.Compare(a.root, b.root)
	})

	// Dedup common paths and shadowed paths eg:
	// /etc/foo, /etc would be one /etc import.
	paths := []string{}
	for _, p := range is {
		foundPrefix := false
		for _, m := range s.importers {
			if strings.HasPrefix(p.root, m.root) {
				foundPrefix = true
				break
			}
		}
		if !foundPrefix {
			s.importers = append(s.importers, p)
			paths = append(paths, p.root)
		}
	}

	// Now that everything is dedup'ed find the common root prefix eg:
	// /etc/bar, /etc/baz would have /etc as the common root.
	s.root = commonPathPrefixSlice(paths)

	return s, nil
}

func (s *Source) SetExcludes(excludes []string) error {
	return s.excludes.AddRulesFromArray(excludes)
}

func (s *Source) GetHeader() header.Source {
	hSource := header.NewSource()
	hSource.Importer.Origin = s.origin
	hSource.Importer.Type = s.typ
	hSource.Importer.Directory = s.root

	return hSource
}

func (s *Source) GetExcludes() *exclude.RuleSet {
	return s.excludes
}

func (s *Source) Origin() string {
	return s.origin
}

func (s *Source) Type() string {
	return s.typ
}

func (s *Source) Root() string {
	return s.root
}

func (s *Source) Flags() location.Flags {
	return s.flags
}

func (s *Source) GetScanner() (<-chan *importer.ScanResult, error) {
	if len(s.importers) == 1 {
		return s.importers[0].imp.Scan(s.ctx)
	}

	results := make(chan *importer.ScanResult, 1000)
	go func() {
		defer close(results)

		for _, w := range s.importers {
			importerChan, err := w.imp.Scan(s.ctx)
			if err != nil {
				s.failure = err
				return
			}

		innerloop:
			for {
				select {
				case <-s.ctx.Done():
					s.failure = s.ctx.Err()
					return
				case rec, ok := <-importerChan:
					if !ok {
						break innerloop
					}
					results <- rec
				}
			}
		}
	}()

	return results, nil
}

func commonPathPrefixSlice(paths []string) string {
	if len(paths) == 0 {
		return "/"
	}

	prefix := paths[0]
	for _, p := range paths[1:] {
		prefix = commonPathPrefix(prefix, p)
		if prefix == "" {
			return "/"
		}
	}
	return prefix
}

func commonPathPrefix(a, b string) string {
	as := strings.Split(strings.Trim(a, "/"), "/")
	bs := strings.Split(strings.Trim(b, "/"), "/")

	n := 0
	for n < len(as) && n < len(bs) && as[n] == bs[n] {
		n++
	}

	if n == 0 {
		return ""
	}
	return "/" + path.Join(as[:n]...)
}
