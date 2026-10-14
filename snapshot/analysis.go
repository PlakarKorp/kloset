package snapshot

import (
	"bytes"
	"fmt"
	"iter"
	"slices"
	"strings"

	"github.com/PlakarKorp/kloset/btree"
	"github.com/PlakarKorp/kloset/connectors/analyzer"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/snapshot/header"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
)

// cachedPath is a path reused from the previous snapshot along with the
// classifications its entry carried, which are not re-computed.
type cachedPath struct {
	*objects.CachedPath
	classifications []vfs.Classification
}

// classificationKey is "/<analyzer>/<class><path>".  Neither analyzer nor
// class names may contain a '/', so the key splits unambiguously and a prefix
// scan selects an analyzer, a class or a class subtree.
func classificationKey(analyzerName, class, pathname string) string {
	return "/" + analyzerName + "/" + class + pathname
}

func toVFSClassifications(results []analyzer.Result) []vfs.Classification {
	if len(results) == 0 {
		return nil
	}
	out := make([]vfs.Classification, 0, len(results))
	for _, res := range results {
		c := vfs.Classification{Analyzer: res.Analyzer}
		for _, class := range res.Classes {
			c.Classes = append(c.Classes, class.Name)
			c.Scores = append(c.Scores, vfs.Score{Key: class.Name, Value: class.Probability})
		}
		out = append(out, c)
	}
	return out
}

func fromVFSClassifications(classifications []vfs.Classification) []analyzer.Result {
	out := make([]analyzer.Result, 0, len(classifications))
	for _, c := range classifications {
		scores := make(map[string]float64, len(c.Scores))
		for _, s := range c.Scores {
			scores[s.Key] = s.Value
		}
		res := analyzer.Result{Analyzer: c.Analyzer}
		for _, name := range c.Classes {
			res.Classes = append(res.Classes, analyzer.Class{Name: name, Probability: scores[name]})
		}
		out = append(out, res)
	}
	return out
}

func toHeaderClassifications(results []analyzer.Result) []header.Classification {
	out := make([]header.Classification, 0, len(results))
	for _, res := range results {
		c := header.Classification{Analyzer: res.Analyzer}
		for _, class := range res.Classes {
			c.Classes = append(c.Classes, header.Class{
				Name:        class.Name,
				Probability: class.Probability,
				Count:       class.Count,
			})
		}
		out = append(out, c)
	}
	return out
}

func classificationEqual(a, b vfs.Classification) bool {
	return a.Analyzer == b.Analyzer && slices.Equal(a.Classes, b.Classes) && slices.Equal(a.Scores, b.Scores)
}

// entryClassifications decides what a file entry carries.  A synchronized
// snapshot carries exactly what the source entry did.  Otherwise unchanged
// files are not re-analyzed: they keep their previous results, but only for
// the analyzers enabled for this backup.
func (snap *Builder) entryClassifications(cachedPath *cachedPath, meta *contentMeta, pathname string) ([]vfs.Classification, error) {
	if snap.syncSource != nil {
		entry, err := snap.syncSource.GetEntryNoFollow(pathname)
		if err != nil {
			return nil, fmt.Errorf("failed to get the classifications of %s: %w", pathname, err)
		}
		return entry.Classifications, nil
	}

	if cachedPath == nil {
		return meta.Classifications, nil
	}

	var kept []vfs.Classification
	for _, c := range cachedPath.classifications {
		if snap.analysis.Persists(c.Analyzer) {
			kept = append(kept, c)
		}
	}
	snap.analysis.ObserveCached(snap.AppContext(), pathname, fromVFSClassifications(kept))
	return kept, nil
}

// recordClassifications indexes the classifications carried by a file entry.
func (sourceCtx *sourceContext) recordClassifications(pathname string, mac objects.MAC, classifications []vfs.Classification) error {
	for _, c := range classifications {
		for _, class := range c.Classes {
			if err := sourceCtx.scanLog.PutClass(classificationKey(c.Analyzer, class, pathname), mac); err != nil {
				return err
			}
		}
	}
	return nil
}

func (snap *Snapshot) ClassificationIdxRoot() (objects.MAC, bool) {
	return snap.getidx("classification", "btree")
}

func (snap *Snapshot) ClassificationIdx() (*btree.BTree[string, objects.MAC, objects.MAC], error) {
	mac, found := snap.ClassificationIdxRoot()
	if !found {
		return nil, nil
	}

	d, err := snap.repository.GetBlobBytes(resources.RT_BTREE_ROOT, mac)
	if err != nil {
		return nil, err
	}

	store := SnapshotStore[string, objects.MAC]{
		blobtype:   resources.RT_BTREE_NODE,
		snapReader: snap,
	}
	return btree.Deserialize(bytes.NewReader(d), &store, strings.Compare)
}

type Classified struct {
	Analyzer string
	Class    string
	Path     string
	MAC      objects.MAC
}

// ListClassified yields the files classified by analyzerName, restricted to
// classes equal to class or below it ("pii" matches "pii.email").  An empty
// class selects every class of the analyzer.
func (snap *Snapshot) ListClassified(analyzerName, class string) iter.Seq2[Classified, error] {
	return func(yield func(Classified, error) bool) {
		idx, err := snap.ClassificationIdx()
		if err != nil {
			yield(Classified{}, err)
			return
		}
		if idx == nil {
			return
		}

		prefix := "/" + analyzerName + "/" + class
		it, err := idx.ScanFrom(prefix)
		if err != nil {
			yield(Classified{}, err)
			return
		}
		for it.Next() {
			key, mac := it.Current()
			if !strings.HasPrefix(key, prefix) {
				break
			}

			rest := strings.TrimPrefix(key, "/"+analyzerName+"/")
			name, pathname, ok := strings.Cut(rest, "/")
			if !ok {
				continue
			}
			// "pii" must not match "piity.x": the class must end here or
			// continue with a dot
			if class != "" && name != class && !strings.HasPrefix(name, class+".") {
				continue
			}
			if !yield(Classified{Analyzer: analyzerName, Class: name, Path: "/" + pathname, MAC: mac}, nil) {
				return
			}
		}
		if err := it.Err(); err != nil {
			yield(Classified{}, err)
		}
	}
}
