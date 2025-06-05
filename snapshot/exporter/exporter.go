package exporter

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/PlakarKorp/kloset/events"
	"github.com/PlakarKorp/kloset/kcontext"
	"github.com/PlakarKorp/kloset/location"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
)

type ExporterOptions struct {
	MaxConcurrency uint64
	Events         *events.Receiver
	SnapID         objects.MAC
	Strip          string
	Base           string
	Pathname       string
}

func (e *ExporterOptions) Event(evt events.Event) {
	if e.Events != nil {
		e.Events.Send(evt)
	}
}

type restoreContext struct {
	hardlinks      map[string]string
	hardlinksMutex sync.Mutex
	maxConcurrency chan bool
}

type Exporter interface {
	Root() string
	Export(context.Context, *ExporterOptions, *vfs.Filesystem) error
	Close() error
}

type FSExporter interface {
	Exporter
	CreateDirectory(pathname string) error
	StoreFile(pathname string, fp io.Reader, size int64) error
	SetPermissions(pathname string, fileinfo *objects.FileInfo) error
}

type ExporterFn func(context.Context, string, map[string]string) (Exporter, error)

var backends = location.New[ExporterFn]("fs")

func Register(name string, backend ExporterFn) {
	if !backends.Register(name, backend) {
		log.Fatalf("backend '%s' registered twice", name)
	}
}

func Backends() []string {
	return backends.Names()
}

func NewExporter(ctx *kcontext.KContext, config map[string]string) (Exporter, error) {
	location, ok := config["location"]
	if !ok {
		return nil, fmt.Errorf("missing location")
	}

	proto, location, backend, ok := backends.Lookup(location)
	if !ok {
		return nil, fmt.Errorf("unsupported exporter protocol")
	}

	if proto == "fs" && !filepath.IsAbs(location) {
		location = filepath.Join(ctx.CWD, location)
		config["location"] = "fs://" + location
	} else {
		config["location"] = proto + "://" + location
	}

	return backend(ctx, proto, config)
}

func fsexportPath(ctx context.Context, exp FSExporter, opts *ExporterOptions, restoreContext *restoreContext, fs *vfs.Filesystem, wg *sync.WaitGroup) func(entrypath string, e *vfs.Entry, err error) error {
	return func(entrypath string, e *vfs.Entry, err error) error {
		if err != nil {
			opts.Event(events.PathErrorEvent(opts.SnapID, entrypath, err.Error()))
			return err
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		opts.Event(events.PathEvent(opts.SnapID, entrypath))

		// Determine destination path by stripping the prefix.
		dest := path.Join(exp.Root(), strings.TrimPrefix(entrypath, opts.Strip))
		log.Printf("base=%s entrypath=%s strip=%s dest=%s",
			exp.Root(), entrypath, opts.Strip, dest)

		// Directory processing.
		if e.IsDir() {
			opts.Event(events.DirectoryEvent(opts.SnapID, entrypath))
			// Create directory if not root.
			if entrypath != "/" {
				if err := exp.CreateDirectory(dest); err != nil {
					opts.Event(events.DirectoryErrorEvent(opts.SnapID, entrypath, err.Error()))
					return err
				}
			}

			// WalkDir handles recursion so we don’t need to iterate children manually.
			if entrypath != "/" {
				if err := exp.SetPermissions(dest, e.Stat()); err != nil {
					opts.Event(events.DirectoryErrorEvent(opts.SnapID, entrypath, err.Error()))
					return err
				}
			}
			opts.Event(events.DirectoryOKEvent(opts.SnapID, entrypath))
			return nil
		}

		// For non-directory entries, only process regular files.
		if !e.Stat().Mode().IsRegular() {
			opts.Event(events.FileErrorEvent(opts.SnapID, entrypath, "unexpected vfs entry type"))
			return nil
		}

		opts.Event(events.FileEvent(opts.SnapID, entrypath))
		restoreContext.maxConcurrency <- true
		wg.Add(1)
		go func(e *vfs.Entry, entrypath string) {
			defer wg.Done()
			defer func() { <-restoreContext.maxConcurrency }()

			// Handle hard links.
			if e.Stat().Nlink() > 1 {
				key := fmt.Sprintf("%d:%d", e.Stat().Dev(), e.Stat().Ino())
				restoreContext.hardlinksMutex.Lock()
				v, ok := restoreContext.hardlinks[key]
				restoreContext.hardlinksMutex.Unlock()
				if ok {
					// Create a new link and return.
					if err := os.Link(v, dest); err != nil {
						opts.Event(events.FileErrorEvent(opts.SnapID, entrypath, err.Error()))
					}
					return
				} else {
					restoreContext.hardlinksMutex.Lock()
					restoreContext.hardlinks[key] = dest
					restoreContext.hardlinksMutex.Unlock()
				}
			}

			rd := e.Open(fs)
			defer rd.Close()

			// Ensure the parent directory exists.
			if err := exp.CreateDirectory(path.Dir(dest)); err != nil {
				opts.Event(events.FileErrorEvent(opts.SnapID, entrypath, err.Error()))
			}

			// Restore the file content.
			if err := exp.StoreFile(dest, rd, e.Size()); err != nil {
				opts.Event(events.FileErrorEvent(opts.SnapID, entrypath, err.Error()))
			} else if err := exp.SetPermissions(dest, e.Stat()); err != nil {
				opts.Event(events.FileErrorEvent(opts.SnapID, entrypath, err.Error()))
			} else {
				opts.Event(events.FileOKEvent(opts.SnapID, entrypath, e.Size()))
			}
		}(e, entrypath)
		return nil
	}
}

func Export(ctx context.Context, exp FSExporter, opts *ExporterOptions, fs *vfs.Filesystem) error {
	opts.Event(events.StartEvent())
	defer opts.Event(events.DoneEvent())

	restoreContext := &restoreContext{
		hardlinks:      make(map[string]string),
		hardlinksMutex: sync.Mutex{},
		maxConcurrency: make(chan bool, opts.MaxConcurrency),
	}
	defer close(restoreContext.maxConcurrency)

	wg := sync.WaitGroup{}
	defer wg.Wait()

	log.Printf("pathname=%s target=%s", opts.Pathname, opts.Base)

	return fs.WalkDir(opts.Pathname, fsexportPath(ctx, exp, opts, restoreContext, fs, &wg))
}
