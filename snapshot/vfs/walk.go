package vfs

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"iter"
	"path"
	"strings"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
)

type WalkDirFunc func(path string, entry *Entry, err error) error

func (fsc *Filesystem) walkdir(entry *Entry, fn WalkDirFunc) error {
	path := entry.Path()
	if err := fn(path, entry, nil); err != nil {
		return err
	}

	if !entry.FileInfo.Mode().IsDir() {
		return nil
	}

	iter, err := entry.Getdents(fsc)
	if err != nil {
		return fn(path, nil, err)
	}

	for entry, err := range iter {
		if err != nil {
			return fn(path, nil, err)
		}

		if err := fsc.walkdir(entry, fn); err != nil {
			if err == fs.SkipDir {
				continue
			}
			return err
		}
	}

	return nil
}

func (fsc *Filesystem) WalkDir(root string, fn WalkDirFunc) error {
	entry, err := fsc.GetEntry(root)
	if err != nil {
		return fn(root, nil, err)
	}

	if err = fsc.walkdir(entry, fn); err != nil {
		if err == fs.SkipDir || err == fs.SkipAll {
			err = nil
		}
	}
	return err
}

type WalkDirpackOpts struct {
	Window         int
	ResolveObjects bool
}

const defaultDirpackWindow = 1024

// WalkDirpack visits a tree hierarchy like Walkdir but based on the dirpack
// index. The guarantee your get is that parents are always visited before
// children.
//
// Directories are fetched a window at a time so that their reads coalesce
// instead of paying a round-trip each.
func (fsc *Filesystem) WalkDirpack(ctx context.Context, root string, opts *WalkDirpackOpts, fn WalkDirFunc) error {
	if fsc.dirpack == nil {
		return fsc.WalkDir(root, fn)
	}

	var o WalkDirpackOpts
	if opts != nil {
		o = *opts
	}
	if o.Window <= 0 {
		o.Window = defaultDirpackWindow
	}

	entry, err := fsc.GetEntry(root)
	if err != nil {
		return fn(root, nil, err)
	}

	if err = fsc.walkDirpack(ctx, entry.Path(), entry, &o, fn); err != nil {
		if err == fs.SkipDir || err == fs.SkipAll {
			err = nil
		}
	}
	return err
}

func (fsc *Filesystem) walkDirpack(ctx context.Context, root string, rootEntry *Entry, opts *WalkDirpackOpts, fn WalkDirFunc) error {
	if err := fn(root, rootEntry, nil); err != nil {
		return err
	}

	if !rootEntry.FileInfo.Mode().IsDir() {
		return nil
	}

	prefix := root
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	iter, err := fsc.dirpack.ScanFrom(root)
	if err != nil {
		return fn(root, nil, err)
	}

	// directories fn asked to skip: their descendants are separate
	// index keys, so each key is checked against its ancestors.
	skipped := make(map[string]struct{})

	window := make([]*dirpackDir, 0, opts.Window)
	for iter.Next() {
		dirpath, objectMac := iter.Current()

		if dirpath != root && !strings.HasPrefix(dirpath, prefix) {
			// We are past the end, the index is lexical.
			if dirpath > prefix {
				break
			}

			// Special case due to the lexical nature of the index, one of the
			// sibling of root sorts between root and root + "/" eg :
			// /usr.bak sorts between /root and /root/ but is outside of the
			// scope of this walkdir.
			continue
		}

		window = append(window, &dirpackDir{path: dirpath, mac: objectMac})
		if len(window) < opts.Window {
			continue
		}

		if err := fsc.walkDirpackWindow(ctx, root, window, opts, skipped, fn); err != nil {
			return err
		}
		window = window[:0]
	}

	if err := fsc.walkDirpackWindow(ctx, root, window, opts, skipped, fn); err != nil {
		return err
	}

	if err := iter.Err(); err != nil {
		return fn(root, nil, err)
	}
	return nil
}

func walkDirpackPayload(dirpath string, dents iter.Seq2[*Entry, error], skipped map[string]struct{}, fn WalkDirFunc) error {
	for entry, err := range dents {
		if err != nil {
			// the payload iterator terminates itself after an error
			if err := fn(dirpath, nil, err); err != nil {
				return err
			}
			continue
		}

		entrypath := entry.Path()
		if err := fn(entrypath, entry, nil); err != nil {
			if err == fs.SkipDir {
				if entry.FileInfo.Mode().IsDir() {
					skipped[entrypath] = struct{}{}
				}
				continue
			}
			return err
		}
	}
	return nil
}

func skippedAncestor(skipped map[string]struct{}, root, dirpath string) bool {
	if len(skipped) == 0 {
		return false
	}
	for p := dirpath; p != root && p != "/"; p = path.Dir(p) {
		if _, ok := skipped[p]; ok {
			return true
		}
	}
	return false
}

// RT_OBJECT whose content is above this threshold gets read through VfsReader
// rather than inlined in the current walk collapse.
var dirpackBatchSize int64 = 32 << 20

type dirpackDir struct {
	path string
	mac  objects.MAC

	obj     *objects.Object
	size    int64
	entries []*Entry
	err     error
}

// buffered reports whether the payload of d is fetched with its batch rather
// than streamed.
func (d *dirpackDir) buffered() bool {
	return d.obj != nil && d.size <= dirpackBatchSize
}

func (fsc *Filesystem) walkDirpackWindow(ctx context.Context, root string, window []*dirpackDir, opts *WalkDirpackOpts, skipped map[string]struct{}, fn WalkDirFunc) error {
	if len(window) == 0 {
		return nil
	}

	macs := make([]objects.MAC, 0, len(window))
	for _, d := range window {
		macs = append(macs, d.mac)
	}

	blobs, err := fsc.getBlobs(ctx, resources.RT_OBJECT, macs)
	if err != nil {
		return err
	}

	for _, d := range window {
		b := blobs[d.mac]
		if b.err != nil {
			d.err = b.err
			continue
		}

		if d.obj, d.err = objects.NewObjectFromBytes(b.data); d.err == nil {
			d.size = d.obj.Size()
		}
	}

	start, size := 0, int64(0)
	for i, d := range window {
		if !d.buffered() {
			continue
		}

		if size+d.size > dirpackBatchSize {
			if err := fsc.walkDirpackBatch(ctx, root, window[start:i], opts, skipped, fn); err != nil {
				return err
			}
			start, size = i, 0
		}
		size += d.size
	}

	return fsc.walkDirpackBatch(ctx, root, window[start:], opts, skipped, fn)
}

func (fsc *Filesystem) walkDirpackBatch(ctx context.Context, root string, batch []*dirpackDir, opts *WalkDirpackOpts, skipped map[string]struct{}, fn WalkDirFunc) error {
	var macs []objects.MAC
	for _, d := range batch {
		if d.buffered() {
			for _, c := range d.obj.Chunks {
				macs = append(macs, c.ContentMAC)
			}
		}
	}

	chunks, err := fsc.getBlobs(ctx, resources.RT_CHUNK, macs)
	if err != nil {
		return err
	}

	for _, d := range batch {
		if d.buffered() {
			d.decode(chunks)
		}
	}

	if opts.ResolveObjects {
		if err := fsc.resolveObjects(ctx, batch); err != nil {
			return err
		}
	}

	for _, d := range batch {
		if skippedAncestor(skipped, root, d.path) {
			continue
		}

		if err := walkDirpackPayload(d.path, fsc.dirpackDirEntries(d), skipped, fn); err != nil {
			return err
		}
	}
	return nil
}

func (d *dirpackDir) decode(chunks map[objects.MAC]blobResult) {
	var chunkErr error
	payload := make([]byte, 0, d.size)
	for _, c := range d.obj.Chunks {
		b := chunks[c.ContentMAC]
		if b.err != nil {
			chunkErr = b.err
			break
		}
		payload = append(payload, b.data...)
	}

	for e, err := range dirpackEntriesIter(d.path, bytes.NewReader(payload)) {
		if err != nil {
			d.err = err
			break
		}
		d.entries = append(d.entries, e)
	}

	if chunkErr != nil {
		d.err = fmt.Errorf("failed to read: %w", chunkErr)
	}
}

func (fsc *Filesystem) dirpackDirEntries(d *dirpackDir) iter.Seq2[*Entry, error] {
	if d.obj != nil && !d.buffered() {
		return dirpackEntriesIter(d.path, NewObjectReader(fsc.repo, d.obj, d.size, -1))
	}

	return func(yield func(*Entry, error) bool) {
		for _, e := range d.entries {
			if !yield(e, nil) {
				return
			}
		}
		if d.err != nil {
			yield(nil, d.err)
		}
	}
}

// resolveObjects fetches the objects of the files of the batch. It is best
// effort: a file left unresolved is fetched by Entry.Open, which then reports
// the error.
func (fsc *Filesystem) resolveObjects(ctx context.Context, batch []*dirpackDir) error {
	var macs []objects.MAC
	for _, d := range batch {
		for _, e := range d.entries {
			if e.HasObject() {
				macs = append(macs, e.Object)
			}
		}
	}

	blobs, err := fsc.getBlobs(ctx, resources.RT_OBJECT, macs)
	if err != nil {
		return err
	}

	objs := make(map[objects.MAC]*objects.Object, len(blobs))
	for mac, b := range blobs {
		if b.err != nil {
			continue
		}
		if obj, err := objects.NewObjectFromBytes(b.data); err == nil {
			objs[mac] = obj
		}
	}

	for _, d := range batch {
		for _, e := range d.entries {
			if obj, ok := objs[e.Object]; ok {
				e.ResolvedObject = obj
			}
		}
	}
	return nil
}

type blobResult struct {
	data []byte
	err  error
}

func (fsc *Filesystem) getBlobs(ctx context.Context, rtype resources.Type, macs []objects.MAC) (map[objects.MAC]blobResult, error) {
	reqs := make([]repository.BlobReq, 0, len(macs))
	for _, mac := range macs {
		reqs = append(reqs, repository.BlobReq{Type: rtype, MAC: mac})
	}

	opts := &repository.GetBlobsOpts{
		Concurrency: uint32(min(fsc.repo.AppContext().MaxConcurrency, 16)),
	}

	res := make(map[objects.MAC]blobResult, len(macs))
	for b, err := range fsc.repo.GetBlobs(ctx, reqs, opts) {
		res[b.MAC] = blobResult{data: b.Data, err: err}
	}

	// GetBlobs answers every request unless it got cancelled.
	return res, ctx.Err()
}
