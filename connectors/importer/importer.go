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
	"path/filepath"

	"github.com/PlakarKorp/kloset/connectors"
	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/location"
)

type Importer interface {
	Origin() string
	Type() string
	Root() string
	Import(context.Context, chan<- *connectors.Record, <-chan *connectors.Result) error
	Close(context.Context) error
}

type ImporterFn func(context.Context, *connectors.Options, string, map[string]string) (Importer, error)

var backends = location.New[ImporterFn]("fs")

func Register(name string, flags location.Flags, backend ImporterFn) error {
	if !backends.Register(name, backend, flags) {
		return fmt.Errorf("importer backend '%s' already registered", name)
	}
	return nil
}

func Unregister(name string) error {
	if !backends.Unregister(name) {
		return fmt.Errorf("importer backend '%s' not registered", name)
	}
	return nil
}

func Backends() []string {
	return backends.Names()
}

func NewImporter(ctx *kcontext.KContext, opts *connectors.Options, config map[string]string) (Importer, location.Flags, error) {
	loc, ok := config["location"]
	if !ok {
		return nil, 0, fmt.Errorf("missing location")
	}

	proto, loc, backend, flags, ok := backends.Lookup(loc)
	if !ok {
		return nil, 0, fmt.Errorf("unsupported importer protocol")
	}

	if flags&location.FLAG_LOCALFS != 0 && !filepath.IsAbs(loc) {
		loc = filepath.Join(ctx.CWD, loc)
	}
	config["location"] = proto + "://" + loc

	importer, err := backend(ctx, opts, proto, config)
	if err != nil {
		return nil, 0, err
	}
	return importer, flags, nil
}
