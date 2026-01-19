package vfs

import (
	"errors"
	"io"
	"io/fs"
	"iter"
	"os"
	"path"
	"strings"

	"golang.org/x/sync/singleflight"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/PlakarKorp/kloset/caching/lru"
	"github.com/PlakarKorp/kloset/iterator"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	versioning.Register(resources.RT_VFS_BTREE, versioning.FromString(btree.BTREE_VERSION))
	versioning.Register(resources.RT_VFS_NODE, versioning.FromString(btree.NODE_VERSION))
}

type Score struct {
	Key   string  `msgpack:"key" json:"key"`
	Value float64 `msgpack:"value" json:"value"`
}

type Classification struct {
	Analyzer string   `msgpack:"analyzer" json:"analyzer"`
	Classes  []string `msgpack:"classes" json:"classes"`
	Scores   []Score  `msgpack:"scores" json:"scores"`
}

type ExtendedAttribute struct {
	Name  string `msgpack:"name" json:"name"`
	Value []byte `msgpack:"value" json:"value"`
}

type CustomMetadata struct {
	Key   string `msgpack:"key" json:"key"`
	Value []byte `msgpack:"value" json:"value"`
}

type Filesystem struct {
	tree         *btree.BTree[string, objects.MAC, objects.MAC]
	xattrs       *btree.BTree[string, objects.MAC, objects.MAC]
	errors       *btree.BTree[string, objects.MAC, objects.MAC]
	dirpack      *btree.BTree[string, objects.MAC, objects.MAC]
	repo         *repository.Repository
	dirpackCache *lru.Cache[string, map[string]*Entry]
	dirpackSF    singleflight.Group
}

func PathCmp(a, b string) int {
	da := strings.Count(a, "/")
	db := strings.Count(b, "/")

	if da > db {
		return 1
	}
	if da < db {
		return -1
	}
	return strings.Compare(a, b)
}

func isEntryBelow(parent, entry string) bool {
	if !strings.HasPrefix(entry, parent) {
		return false
	}
	if strings.Contains(entry[len(parent):], "/") {
		return false
	}
	return true
}

func NodeFromBytes(data []byte) (*btree.Node[string, objects.MAC, objects.MAC], error) {
	var node btree.Node[string, objects.MAC, objects.MAC]
	err := msgpack.Unmarshal(data, &node)
	return &node, err
}

func NewFilesystem(repo *repository.Repository, root, xattrs, errors objects.MAC, dirpackidx *btree.BTree[string, objects.MAC, objects.MAC]) (*Filesystem, error) {
	rd, err := repo.GetBlob(resources.RT_VFS_BTREE, root)
	if err != nil {
		return nil, err
	}

	fsstore := repository.NewRepositoryStore[string, objects.MAC](repo, resources.RT_VFS_NODE)
	tree, err := btree.Deserialize(rd, fsstore, PathCmp)
	if err != nil {
		return nil, err
	}

	rd, err = repo.GetBlob(resources.RT_XATTR_BTREE, xattrs)
	if err != nil {
		return nil, err
	}

	xstore := repository.NewRepositoryStore[string, objects.MAC](repo, resources.RT_XATTR_NODE)
	xtree, err := btree.Deserialize(rd, xstore, strings.Compare)
	if err != nil {
		return nil, err
	}

	rd, err = repo.GetBlob(resources.RT_ERROR_BTREE, errors)
	if err != nil {
		return nil, err
	}

	errstore := repository.NewRepositoryStore[string, objects.MAC](repo, resources.RT_ERROR_NODE)
	errtree, err := btree.Deserialize(rd, errstore, strings.Compare)
	if err != nil {
		return nil, err
	}

	fs := &Filesystem{
		tree:    tree,
		xattrs:  xtree,
		errors:  errtree,
		dirpack: dirpackidx,
		repo:    repo,
	}

	return fs, nil
}

// XXX - until we do refacto to remove object resolve from ResolveEntry, ONLY CALL IN SUBCOMMAND BACKUP
func NewFilesystemWithCache(repo *repository.Repository, root, xattrs, errors objects.MAC, dirpackidx *btree.BTree[string, objects.MAC, objects.MAC]) (*Filesystem, error) {
	fs, err := NewFilesystem(repo, root, xattrs, errors, dirpackidx)
	if err != nil {
		return nil, err
	}
	fs.dirpackCache = lru.New[string, map[string]*Entry](256, nil)

	return fs, nil
}

func (fsc *Filesystem) lookup(entrypath string) (*Entry, error) {
	entrypath = path.Clean(entrypath)
	if !path.IsAbs(entrypath) {
		return nil, fs.ErrInvalid
	}

	csum, found, err := fsc.tree.Find(entrypath)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fs.ErrNotExist
	}

	return fsc.ResolveEntry(csum)
}

func (fsc *Filesystem) ResolveEntry(csum objects.MAC) (*Entry, error) {
	rd, err := fsc.repo.GetBlob(resources.RT_VFS_ENTRY, csum)
	if err != nil {
		return nil, err
	}

	bytes, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	entry, err := EntryFromBytes(bytes)
	if err != nil {
		return nil, err
	}

	entry.MAC = csum

	if entry.HasObject() {
		rd, err := fsc.repo.GetBlob(resources.RT_OBJECT, entry.Object)
		if err != nil {
			return nil, err
		}

		bytes, err := io.ReadAll(rd)
		if err != nil {
			return nil, err
		}

		obj, err := objects.NewObjectFromBytes(bytes)
		if err != nil {
			return nil, err
		}

		entry.ResolvedObject = obj
	}

	return entry, nil
}

func (fsc *Filesystem) ResolveXattr(mac objects.MAC) (*Xattr, error) {
	rd, err := fsc.repo.GetBlob(resources.RT_XATTR_ENTRY, mac)
	if err != nil {
		return nil, err
	}

	bytes, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	xattr, err := XattrFromBytes(bytes)
	if err != nil {
		return nil, err
	}

	rd, err = fsc.repo.GetBlob(resources.RT_OBJECT, xattr.Object)
	if err != nil {
		return nil, err
	}

	bytes, err = io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	xattr.ResolvedObject, err = objects.NewObjectFromBytes(bytes)
	return xattr, err
}

func (fsc *Filesystem) Open(path string) (fs.File, error) {
	entry, err := fsc.GetEntry(path)
	if err != nil {
		return nil, err
	}

	return entry.Open(fsc)
}

func (fsc *Filesystem) Stat(name string) (fs.FileInfo, error) {
	entry, err := fsc.GetEntry(name)
	if err != nil {
		return nil, err
	}
	return entry.FileInfo, nil
}

func (fsc *Filesystem) ReadDir(path string) ([]fs.DirEntry, error) {
	it, err := fsc.Children(path)
	if err != nil {
		return nil, err
	}

	var entries []fs.DirEntry
	for entry, err := range it {
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

func (fsc *Filesystem) filesbelow(prefix string) iter.Seq2[*Entry, error] {
	return func(yield func(*Entry, error) bool) {
		err := fsc.WalkDir(prefix, func(path string, entry *Entry, err error) error {
			if err != nil {
				return err
			}
			if !yield(entry, nil) {
				return fs.SkipAll
			}
			return nil
		})
		if err != nil {
			yield(nil, err)
		}
	}
}

func (fsc *Filesystem) Files(prefix string) iter.Seq2[*Entry, error] {
	prefix = path.Clean(prefix)
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	if prefix != "/" {
		return fsc.filesbelow(prefix)
	}

	return func(yield func(*Entry, error) bool) {
		iter, err := fsc.tree.ScanAll()
		if err != nil {
			yield(nil, err)
			return
		}

		for iter.Next() {
			_, csum := iter.Current()
			entry, err := fsc.ResolveEntry(csum)
			if err != nil {
				if !yield(nil, err) {
					return
				}
				continue
			}
			if !yield(entry, nil) {
				return
			}
		}
		if err := iter.Err(); err != nil {
			yield(nil, err)
			return
		}
	}
}

func (fsc *Filesystem) Pathnames() iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		iter, err := fsc.tree.ScanAll()
		if err != nil {
			yield("", err)
			return
		}

		for iter.Next() {
			path, _ := iter.Current()
			if !yield(path, nil) {
				return
			}
		}

		if err := iter.Err(); err != nil {
			yield("", err)
			return
		}
	}
}

func (fsc *Filesystem) GetEntry(entrypath string) (*Entry, error) {
	return fsc.getEntry(entrypath, true)
}

func (fsc *Filesystem) GetEntryNoFollow(entrypath string) (*Entry, error) {
	return fsc.getEntry(entrypath, false)
}

func (fsc *Filesystem) getEntry(entrypath string, follow bool) (*Entry, error) {
	if !strings.HasPrefix(entrypath, "/") {
		entrypath = "/" + entrypath
	}

	entrypath = path.Clean(entrypath)
	if !path.IsAbs(entrypath) {
		return nil, fs.ErrInvalid
	}

	if follow {
		return fsc.getEntryFollow(entrypath)
	}
	return fsc.getEntryNoFollow(entrypath)
}

// XXX - until we do refacto to remove object resolve from ResolveEntry
func (fsc *Filesystem) GetEntryForBackup(entrypath string) (*Entry, error) {
	return fsc.getEntryForBackup(entrypath)
}

func (fsc *Filesystem) getEntryForBackup(entrypath string) (*Entry, error) {
	if !strings.HasPrefix(entrypath, "/") {
		entrypath = "/" + entrypath
	}

	entrypath = path.Clean(entrypath)
	if !path.IsAbs(entrypath) {
		return nil, fs.ErrInvalid
	}

	if fsc.dirpack == nil || fsc.dirpackCache == nil {
		return fsc.getEntryNoFollow(entrypath)
	}

	parentPath := path.Dir(entrypath)
	base := path.Base(entrypath)

	/*
		if m, exists := fsc.dirpackCache.Get(parentPath); exists {
			if entry, ok := m[base]; ok {
				return entry, nil
			}
			return nil, fs.ErrNotExist
		}
	*/

	v, err, _ := fsc.dirpackSF.Do(parentPath, func() (any, error) {
		if m, exists := fsc.dirpackCache.Get(parentPath); exists {
			return m, nil
		}
		return fsc.loadDirpackMap(parentPath)
	})
	if err != nil {
		return nil, err
	}
	m := v.(map[string]*Entry)

	if entry, ok := m[base]; ok {
		return entry, nil
	}
	return nil, fs.ErrNotExist
}

func (fsc *Filesystem) getEntryFollow(entrypath string) (*Entry, error) {
	csum, found, err := fsc.tree.Find(entrypath)
	if err != nil {
		return nil, err
	}

	var entry *Entry
	if !found {
		// try again, but this time resolving each component
		// since there might be symlinks.
		components := strings.Split(entrypath, "/")
		wip := "/"
		for _, c := range components {
			new := path.Join(wip, c)
			csum, found, err = fsc.tree.Find(new)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, os.ErrNotExist
			}

			entry, err = fsc.ResolveEntry(csum)
			if err != nil {
				return nil, err
			}

			if entry.FileInfo.Lmode&os.ModeSymlink == 0 {
				wip = new
				continue
			}

			// handle symlink
			if path.IsAbs(entry.SymlinkTarget) {
				wip = entry.SymlinkTarget
			} else {
				wip = path.Join(wip, entry.SymlinkTarget)
			}
		}
	}

	if entry == nil {
		entry, err = fsc.ResolveEntry(csum)
		if err != nil {
			return nil, err
		}
	}

	if entry.FileInfo.Lmode&os.ModeSymlink == 0 {
		return entry, nil
	}

	if path.IsAbs(entry.SymlinkTarget) {
		entry, err = fsc.lookup(entry.SymlinkTarget)
		if err != nil {
			return nil, err
		}
		return entry, nil
	}

	entry, err = fsc.lookup(path.Join(path.Dir(entry.Path()), entry.SymlinkTarget))
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (fsc *Filesystem) getEntryNoFollow(entrypath string) (*Entry, error) {
	csum, found, err := fsc.tree.Find(entrypath)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fs.ErrNotExist
	}

	entry, err := fsc.ResolveEntry(csum)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

func (fsc *Filesystem) loadDirpackMap(parentPath string) (map[string]*Entry, error) {
	objectMac, found, err := fsc.dirpack.Find(parentPath)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fs.ErrNotExist
	}

	buffer, err := fsc.repo.GetBlobBytes(resources.RT_OBJECT, objectMac)
	if err != nil {
		return nil, err
	}

	obj, err := objects.NewObjectFromBytes(buffer)
	if err != nil {
		return nil, err
	}

	var size int64
	for _, c := range obj.Chunks {
		size += int64(c.Length)
	}

	//rd := NewObjectReader(fsc.repo, obj, size, -1)
	rd := NewObjectReader(fsc.repo, obj, size, 8<<20)

	cache := make(map[string]*Entry)
	for {
		_, siz, err := readDirPackHdr(rd)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		var entry Entry
		lrd := io.LimitReader(rd, int64(siz-uint32(len(entry.MAC))))
		if err := msgpack.NewDecoder(lrd).Decode(&entry); err != nil {
			return nil, err
		}
		if _, err := io.ReadFull(rd, entry.MAC[:]); err != nil {
			return nil, err
		}

		// resolve object to extract content-type, nchunks and entropy
		if entry.GetContentType() == "" && entry.HasObject() {
			rd, err := fsc.repo.GetBlob(resources.RT_OBJECT, entry.Object)
			if err != nil {
				return nil, err
			}

			bytes, err := io.ReadAll(rd)
			if err != nil {
				return nil, err
			}

			obj, err := objects.NewObjectFromBytes(bytes)
			if err != nil {
				return nil, err
			}

			entry.ContentType = obj.ContentType
			entry.Entropy = obj.Entropy
			entry.Chunks = uint64(len(obj.Chunks))
		}

		cache[entry.Name()] = &entry
	}

	_ = fsc.dirpackCache.Put(parentPath, cache)

	return cache, nil
}

func (fsc *Filesystem) Children(path string) (iter.Seq2[*Entry, error], error) {
	fp, err := fsc.GetEntry(path)
	if err != nil {
		return nil, err
	}

	if !fp.IsDir() {
		return nil, fs.ErrInvalid
	}

	return func(yield func(*Entry, error) bool) {
		it, err := fp.Getdents(fsc)
		if err != nil {
			yield(nil, err)
			return
		}
		for entry, err := range it {
			if !yield(entry, err) {
				return
			}
		}
	}, nil
}

func (fsc *Filesystem) IterNodes() iterator.Iterator[objects.MAC, *btree.Node[string, objects.MAC, objects.MAC]] {
	return fsc.tree.IterDFS()
}

func (fsc *Filesystem) XattrNodes() iterator.Iterator[objects.MAC, *btree.Node[string, objects.MAC, objects.MAC]] {
	return fsc.xattrs.IterDFS()
}

func (fsc *Filesystem) FileMacs() (iter.Seq2[objects.MAC, error], error) {
	iter, err := fsc.tree.ScanAll()
	if err != nil {
		return nil, err
	}

	return func(yield func(objects.MAC, error) bool) {
		for iter.Next() {
			_, csum := iter.Current()
			if err != nil {
				yield(objects.MAC{}, err)
				return
			}
			if !yield(csum, nil) {
				return
			}
		}
		if err := iter.Err(); err != nil {
			yield(objects.MAC{}, err)
			return
		}
	}, nil
}

// XXX
func (fsc *Filesystem) BTrees() (*btree.BTree[string, objects.MAC, objects.MAC], *btree.BTree[string, objects.MAC, objects.MAC], *btree.BTree[string, objects.MAC, objects.MAC]) {
	return fsc.tree, fsc.errors, fsc.xattrs
}
