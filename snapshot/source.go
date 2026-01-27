package snapshot

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/PlakarKorp/kloset/connectors/importer"
	"github.com/PlakarKorp/kloset/exclude"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/snapshot/header"
)

type Source struct {
	ctx context.Context

	importers []importer.Importer

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

	var is []importer.Importer
	for i, imp := range importers {
		var (
			origin = imp.Origin()
			typ    = imp.Type()
			flags  = imp.Flags()
		)

		if i == 0 {
			s.origin = origin
			s.typ = typ
			s.flags = flags
		} else if s.origin != origin {
			return nil, fmt.Errorf("mismatched origin when adding importer %q expected %q", origin, s.origin)
		} else if s.typ != typ {
			return nil, fmt.Errorf("mismatched type when adding importer %q expected %q", typ, s.typ)
		} else if s.flags != flags {
			return nil, fmt.Errorf("mismatched flags when adding importer %q expected %q", flags, s.flags)
		}

		is = append(is, imp)
	}

	slices.SortFunc(is, func(a, b importer.Importer) int {
		return strings.Compare(a.Root(), b.Root())
	})

	// Dedup common paths and shadowed paths eg:
	// /etc/foo, /etc would be one /etc import.
	paths := []string{}
	for _, p := range is {
		foundPrefix := false
		for _, m := range s.importers {
			if pathIsWithin(p.Root(), m.Root()) || p.Root() == m.Root() {
				foundPrefix = true
				break
			}
		}
		if !foundPrefix {
			s.importers = append(s.importers, p)
			paths = append(paths, p.Root())
		}
	}

	// Now that everything is dedup'ed find the common root prefix eg:
	// /etc/bar, /etc/baz would have /etc as the common root.
	s.root = commonPathPrefixSlice(paths)

	return s, nil
}

func pathIsWithin(pathname string, within string) bool {
	cleanPath := path.Clean(pathname)
	cleanWithin := path.Clean(within)

	if cleanWithin == "/" {
		return true
	}
	return strings.HasPrefix(cleanPath, cleanWithin+"/")
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

func (s *Source) Importers() []importer.Importer {
	return s.importers
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
