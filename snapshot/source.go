package snapshot

import (
	"context"

	"github.com/PlakarKorp/kloset/exclude"
	"github.com/PlakarKorp/kloset/snapshot/header"
	"github.com/PlakarKorp/kloset/snapshot/importer"
)

type Source struct {
	imp importer.Importer

	origin string
	typ    string
	root   string

	excludes *exclude.RuleSet
}

func NewSource(ctx context.Context, imp importer.Importer) (*Source, error) {
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

	return &Source{
		imp:      imp,
		origin:   origin,
		typ:      typ,
		root:     root,
		excludes: exclude.NewRuleSet(),
	}, nil
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
