package vfs

import (
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

var _ fs.FS = (*MultiFS)(nil)

type MultiFS struct {
	mu     sync.RWMutex
	mounts map[string]*Filesystem // absolute mount path -> FS
}

func NewMultiFS() *MultiFS {
	return &MultiFS{
		mounts: make(map[string]*Filesystem),
	}
}

func (mfs *MultiFS) Mount(pathname string, fsys *Filesystem) error {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	p := cleanAbs(pathname)
	if p == "/" {
		return fmt.Errorf("cannot mount on root path")
	}
	if _, exists := mfs.mounts[p]; exists {
		return fmt.Errorf("mount point %s already in use", p)
	}
	mfs.mounts[p] = fsys
	return nil
}

func (mfs *MultiFS) Unmount(pathname string) error {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()
	p := cleanAbs(pathname)
	if _, exists := mfs.mounts[p]; !exists {
		return fmt.Errorf("mount point %s not found", p)
	}
	delete(mfs.mounts, p)
	return nil
}

// Open implements fs.FS. It accepts either absolute ("/a/b") or FS-style ("a/b") names.
func (mfs *MultiFS) Open(name string) (fs.File, error) {
	mfs.mu.RLock()
	defer mfs.mu.RUnlock()

	p := cleanAbs(name)

	// exact mount: delegate to child FS
	if fsys, ok := mfs.mounts[p]; ok {
		return fsys.Open(p)
	}

	// parent of one or more mounts? return synthetic dir
	children := mfs.nextComponentsLocked(p)
	if len(children) > 0 {
		return newMultiDir(p, children), nil
	}

	// maybe the path is inside a mounted FS; route by longest prefix
	if fsys, sub, ok := mfs.routeLocked(p); ok {
		return fsys.Open(sub)
	}

	return nil, fs.ErrNotExist
}

func (mfs *MultiFS) ReadDir(name string) ([]fs.DirEntry, error) {
	mfs.mu.RLock()
	defer mfs.mu.RUnlock()

	p := cleanAbs(name)

	// exact mount: delegate
	if fsys, ok := mfs.mounts[p]; ok {
		return fsys.ReadDir(p)
	}

	// synthetic parent
	children := mfs.nextComponentsLocked(p)
	if len(children) > 0 {
		ents := make([]fs.DirEntry, 0, len(children))
		for _, n := range children {
			ents = append(ents, dirEntry{name: n, mode: fs.ModeDir})
		}
		return ents, nil
	}

	// inside a mounted FS
	if fsys, sub, ok := mfs.routeLocked(p); ok {
		return fsys.ReadDir(sub)
	}

	return nil, fs.ErrNotExist
}

// routeLocked finds the mounted FS by longest-prefix match and returns (fs, subpath, ok).
func (mfs *MultiFS) routeLocked(abs string) (*Filesystem, string, bool) {
	var bestMount string
	for mp := range mfs.mounts {
		if abs == mp || strings.HasPrefix(abs, mp+"/") {
			if len(mp) > len(bestMount) {
				bestMount = mp
			}
		}
	}
	if bestMount == "" {
		return nil, "", false
	}
	sub := abs
	// child Filesystem expects absolute paths in your implementation,
	// so keep sub absolute by not trimming the mount (delegate as-is).
	// If you want to pass a path *inside* the child, change to:
	//   sub = strings.TrimPrefix(abs, bestMount)
	//   if sub == "" { sub = "/" }
	return mfs.mounts[bestMount], sub, true
}

// nextComponentsLocked returns the unique next-level names under prefix p
// across all mount points. Example:
//
//	mounts: /snap/a, /snap/b, /data/x
//	nextComponentsLocked("/") -> ["data","snap"]
//	nextComponentsLocked("/snap") -> ["a","b"]
func (mfs *MultiFS) nextComponentsLocked(p string) []string {
	if p != "/" {
		p = strings.TrimRight(p, "/")
	}
	prefix := p
	if prefix != "/" {
		prefix += "/"
	}

	set := make(map[string]struct{})
	for mp := range mfs.mounts {
		if mp == p {
			// exact mount exists here; do NOT synthesize children in this case
			// (caller will delegate via exact-match path)
			return nil
		}
		if strings.HasPrefix(mp, prefix) {
			rest := strings.TrimPrefix(mp, prefix)
			if rest == "" {
				continue
			}
			comp := rest
			if i := strings.IndexByte(rest, '/'); i >= 0 {
				comp = rest[:i]
			}
			if comp != "" {
				set[comp] = struct{}{}
			}
		}
	}
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ---- synthetic directory nodes ----

type dirEntry struct {
	name string
	mode fs.FileMode
}

func (d dirEntry) Name() string               { return d.name }
func (d dirEntry) IsDir() bool                { return d.mode.IsDir() }
func (d dirEntry) Type() fs.FileMode          { return d.mode }
func (d dirEntry) Info() (fs.FileInfo, error) { return fileInfo{name: d.name, mode: d.mode}, nil }

type fileInfo struct {
	name string
	mode fs.FileMode
}

func (fi fileInfo) Name() string       { return fi.name }
func (fi fileInfo) Size() int64        { return 0 }
func (fi fileInfo) Mode() fs.FileMode  { return fi.mode }
func (fi fileInfo) ModTime() time.Time { return time.Time{} }
func (fi fileInfo) IsDir() bool        { return fi.mode.IsDir() }
func (fi fileInfo) Sys() any           { return nil }

// multiDir is a synthetic fs.ReadDirFile for parent directories we create.
type multiDir struct {
	full  string
	i     int
	names []string
}

func newMultiDir(full string, names []string) *multiDir {
	return &multiDir{full: full, names: names}
}

func (d *multiDir) Read([]byte) (int, error) { return 0, io.EOF }
func (d *multiDir) Close() error             { return nil }
func (d *multiDir) Stat() (fs.FileInfo, error) {
	return fileInfo{name: path.Base(d.full), mode: fs.ModeDir}, nil
}

func (d *multiDir) ReadDir(n int) ([]fs.DirEntry, error) {
	if d.i >= len(d.names) {
		return nil, io.EOF
	}
	remain := len(d.names) - d.i
	if n <= 0 || n > remain {
		n = remain
	}
	out := make([]fs.DirEntry, 0, n)
	for j := 0; j < n; j++ {
		out = append(out, dirEntry{name: d.names[d.i], mode: fs.ModeDir})
		d.i++
	}
	return out, nil
}

// ---- helpers ----

func cleanAbs(name string) string {
	// fs.FS normally gets relative paths; accept absolute too.
	if name == "" {
		return "/"
	}
	if !strings.HasPrefix(name, "/") {
		name = "/" + name
	}
	return path.Clean(name)
}
