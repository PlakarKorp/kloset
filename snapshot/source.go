package snapshot

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
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

func NewSource(ctx context.Context, imp importer.Importer, flags location.Flags) (*Source, error) {
	origin, err := imp.Origin(ctx)
	if err != nil {
		return nil, err
	}

	typ, err := imp.Type(ctx)
	if err != nil {
		return nil, err
	}

	root, err := imp.Root(ctx)
	if err != nil {
		return nil, err
	}

	s := &Source{
		ctx:       ctx,
		importers: make([]*importerWrapper, 0, 1),
		origin:    origin,
		typ:       typ,
		root:      root,
		flags:     flags,
		excludes:  exclude.NewRuleSet(),
	}

	s.importers = append(s.importers, &importerWrapper{root, imp})
	return s, nil
}

func (s *Source) AddImporter(ctx context.Context, imp importer.Importer) error {
	origin, err := imp.Origin(ctx)
	if err != nil {
		return err
	}

	if s.origin != origin {
		return fmt.Errorf("mismatched origin when adding importer %q expected %q", origin, s.origin)
	}

	typ, err := imp.Type(ctx)
	if err != nil {
		return err
	}

	if s.typ != typ {
		return fmt.Errorf("mismatched typ when adding importer %q expected %q", typ, s.typ)
	}

	root, err := imp.Root(ctx)
	if err != nil {
		return err
	}

	fmt.Printf("Adding importer with %s\n", root)
	s.importers = append(s.importers, &importerWrapper{root, imp})
	return nil
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

func (s *Source) GetFlags() location.Flags {
	return s.flags
}

func (s *Source) GetScanner() (<-chan *importer.ScanResult, error) {
	if len(s.importers) == 1 {
		return s.importers[0].imp.Scan(s.ctx)
	}

	results := make(chan *importer.ScanResult, 1000)

	w := s.importers
	sort.Slice(w, func(i, j int) bool {
		return w[i].root < w[j].root
	})

	paths := []string{}
	wDedup := make([]*importerWrapper, 0)
	for _, p := range w {
		foundPrefix := false
		fmt.Printf("Going over %s\n", p.root)
		for _, m := range wDedup {
			if strings.HasPrefix(p.root, m.root) {
				foundPrefix = true
				break
			}
		}
		if !foundPrefix {
			wDedup = append(wDedup, p)
			paths = append(paths, p.root)
		}
	}

	fmt.Printf("dedup'ed %+v\n", wDedup)

	//s.root = commonPathPrefixSlice(paths)

	go func() {
		defer close(results)

		for _, w := range wDedup {
			iter, err := w.imp.Scan(s.ctx)
			if err != nil {
				s.failure = err
				return
			}

			i := 0
			for rec := range iter {
				if i%1000 == 0 {
					if s.ctx.Err() != nil {
						s.failure = err
						return
					}
				}
				results <- rec
			}
		}

	}()

	return results, nil
}

func commonPathPrefixSlice(paths []string) string {
	if len(paths) == 0 {
		return ""
	}

	prefix := paths[0]
	for _, p := range paths[1:] {
		prefix = commonPathPrefix(prefix, p)
		if prefix == "" {
			return ""
		}
	}
	return prefix
}

func commonPathPrefix(a, b string) string {
	a = filepath.Clean(a)
	b = filepath.Clean(b)

	// Windows ?
	// a = strings.ToLower(a); b = strings.ToLower(b)

	as := splitPath(a)
	bs := splitPath(b)

	n := 0
	for n < len(as) && n < len(bs) && as[n] == bs[n] {
		n++
	}

	if n == 0 {
		return ""
	}
	return filepath.Join(as[:n]...)
}

func splitPath(p string) []string {
	vol := filepath.VolumeName(p) // "C:" on Windows, "" elsewhere
	rest := strings.TrimPrefix(p, vol)
	rest = strings.Trim(rest, string(filepath.Separator))

	parts := []string{}
	if vol != "" {
		parts = append(parts, vol)
	}
	if rest == "" {
		return parts
	}
	parts = append(parts, strings.Split(rest, string(filepath.Separator))...)
	return parts
}
