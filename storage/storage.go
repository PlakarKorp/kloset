/*
 * Copyright (c) 2021 Gilles Chehade <gilles@poolp.org>
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

package storage

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/PlakarKorp/kloset/chunking"
	"github.com/PlakarKorp/kloset/compression"
	"github.com/PlakarKorp/kloset/encryption"
	"github.com/PlakarKorp/kloset/hashing"
	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/packfile"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
)

const VERSION string = "1.0.0"

func init() {
	versioning.Register(resources.RT_CONFIG, versioning.FromString(VERSION))
}

var ErrNotWritable = fmt.Errorf("storage is not writable")
var ErrNotReadable = fmt.Errorf("storage is not readable")
var ErrInvalidLocation = fmt.Errorf("invalid location")
var ErrInvalidMagic = fmt.Errorf("invalid magic")
var ErrInvalidVersion = fmt.Errorf("invalid version")

type Configuration struct {
	Version      versioning.Version `msgpack:"-" json:"version"`
	Timestamp    time.Time          `json:"timestamp"`
	RepositoryID uuid.UUID          `json:"repository_id"`

	Packfile    packfile.Configuration     `json:"packfile"`
	Chunking    chunking.Configuration     `json:"chunking"`
	Hashing     hashing.Configuration      `json:"hashing"`
	Compression *compression.Configuration `json:"compression"`
	Encryption  *encryption.Configuration  `json:"encryption"`
}

func NewConfiguration() *Configuration {
	return &Configuration{
		Version:      versioning.FromString(VERSION),
		Timestamp:    time.Now(),
		RepositoryID: uuid.Must(uuid.NewRandom()),

		Packfile: *packfile.NewDefaultConfiguration(),
		Chunking: *chunking.NewDefaultConfiguration(),
		Hashing:  *hashing.NewDefaultConfiguration(),

		Compression: compression.NewDefaultConfiguration(),
		Encryption:  encryption.NewDefaultConfiguration(),
	}
}

func NewConfigurationFromBytes(version versioning.Version, data []byte) (*Configuration, error) {
	var configuration Configuration
	err := msgpack.Unmarshal(data, &configuration)
	if err != nil {
		return nil, err
	}
	configuration.Version = version
	return &configuration, nil
}

func NewConfigurationFromWrappedBytes(data []byte) (*Configuration, error) {
	var configuration Configuration

	version := versioning.Version(binary.LittleEndian.Uint32(data[12:16]))

	data = data[:len(data)-int(STORAGE_FOOTER_SIZE)]
	data = data[STORAGE_HEADER_SIZE:]

	err := msgpack.Unmarshal(data, &configuration)
	if err != nil {
		return nil, err
	}
	configuration.Version = version
	return &configuration, nil
}

func (c *Configuration) ToBytes() ([]byte, error) {
	return msgpack.Marshal(c)
}

type Mode uint32

const (
	ModeWrite Mode = 1 << 1
	ModeRead  Mode = 1 << 2
)

type Store interface {
	Create(ctx context.Context, config []byte) error
	Open(ctx context.Context) ([]byte, error)
	Location(ctx context.Context) (string, error)
	Mode(ctx context.Context) (Mode, error)
	Size(ctx context.Context) (int64, error) // this can be costly, call with caution

	GetStates(ctx context.Context) ([]objects.MAC, error)
	PutState(ctx context.Context, mac objects.MAC, rd io.Reader) (int64, error)
	GetState(ctx context.Context, mac objects.MAC) (io.ReadCloser, error)
	DeleteState(ctx context.Context, mac objects.MAC) error

	GetPackfiles(ctx context.Context) ([]objects.MAC, error)
	PutPackfile(ctx context.Context, mac objects.MAC, rd io.Reader) (int64, error)
	GetPackfile(ctx context.Context, mac objects.MAC) (io.ReadCloser, error)
	GetPackfileBlob(ctx context.Context, mac objects.MAC, offset uint64, length uint32) (io.ReadCloser, error)
	DeletePackfile(ctx context.Context, mac objects.MAC) error

	GetLocks(ctx context.Context) ([]objects.MAC, error)
	PutLock(ctx context.Context, lockID objects.MAC, rd io.Reader) (int64, error)
	GetLock(ctx context.Context, lockID objects.MAC) (io.ReadCloser, error)
	DeleteLock(ctx context.Context, lockID objects.MAC) error

	Close(ctx context.Context) error
}

type StoreFn func(context.Context, string, map[string]string) (Store, error)

var backends = location.New[StoreFn]("fs")

func Register(name string, flags location.Flags, backend StoreFn) error {
	if !backends.Register(name, backend, flags) {
		return fmt.Errorf("storage backend '%s' already registered", name)
	}
	return nil
}

func Unregister(name string) error {
	if !backends.Unregister(name) {
		return fmt.Errorf("storage backend '%s' not registered", name)
	}
	return nil
}

func Backends() []string {
	return backends.Names()
}

func New(ctx *kcontext.KContext, storeConfig map[string]string) (Store, error) {
	loc, ok := storeConfig["location"]
	if !ok {
		return nil, fmt.Errorf("missing location")
	}

	proto, loc, backend, flags, ok := backends.Lookup(loc)
	if !ok {
		return nil, fmt.Errorf("backend '%s' does not exist", proto)
	}

	if flags&location.FLAG_LOCALFS != 0 {
		if !filepath.IsAbs(loc) {
			loc = filepath.Join(ctx.CWD, loc)
		}

		if proto == "fs" && strings.HasSuffix(loc, ".ptar") {
			storeConfig["location"] = "ptar://" + loc
			return New(ctx, storeConfig)
		}
	}
	storeConfig["location"] = proto + "://" + loc

	return backend(ctx, proto, storeConfig)
}

func Open(ctx *kcontext.KContext, storeConfig map[string]string) (Store, []byte, error) {
	store, err := New(ctx, storeConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
		return nil, nil, err
	}

	serializedConfig, err := store.Open(ctx)
	if err != nil {
		return nil, nil, err
	}

	return store, serializedConfig, nil
}

func Create(ctx *kcontext.KContext, storeConfig map[string]string, configuration []byte) (Store, error) {
	store, err := New(ctx, storeConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %s\n", flag.CommandLine.Name(), err)
		return nil, err
	}

	if err = store.Create(ctx, configuration); err != nil {
		return nil, err
	} else {
		return store, nil
	}
}
