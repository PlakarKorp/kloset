// package locate

package locate

import (
	"encoding/hex"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/snapshot"
)

// LocateSnapshotIDs preserves the legacy behavior using policy.FilterAndSort.
// It does NOT apply retention caps; it only filters/sorts + optional "latest".

func Match(repo *repository.Repository, opts *LocateOptions) ([]objects.MAC, map[objects.MAC]Reason, error) {
	if opts == nil {
		opts = NewDefaultLocateOptions()
	}

	now := time.Now().UTC()

	items := buildPolicyItems(repo)

	// Run policy: this applies retention (windows + caps) and produces reasons.
	matches, reasons := opts.Match(items, now)

	rmatches := make([]objects.MAC, 0, len(matches))
	for itemID := range matches {
		rmatches = append(rmatches, itemID)
	}

	return rmatches, reasons, nil
}

func LocateSnapshotIDs(repo *repository.Repository, opts *LocateOptions) ([]objects.MAC, error) {
	if opts == nil {
		opts = NewDefaultLocateOptions()
	}

	// Build items from repo (concurrently)
	items := buildPolicyItems(repo)

	now := time.Now().UTC()

	// Run policy: this applies retention (windows + caps) and produces reasons.
	keptSet, _ := opts.Match(items, now)

	// Order: use the same ordering policy.FilterAndSort uses (sort-order + latest)
	ordered := opts.FilterAndSort(items)

	out := make([]objects.MAC, 0, len(keptSet))
	for _, it := range ordered {
		if _, kept := keptSet[it.ItemID]; !kept {
			continue
		}
		out = append(out, it.ItemID)
	}

	return out, nil
}

func buildPolicyItems(repo *repository.Repository) []Item {
	var (
		mu       sync.Mutex
		items    = make([]Item, 0, 256)
		hexToMAC = make(map[string]objects.MAC, 256)
		wg       sync.WaitGroup
		sem      = make(chan struct{}, max(1, repo.AppContext().MaxConcurrency))
	)

	for snapshotID := range repo.ListSnapshots() {
		id := snapshotID // capture
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()

			snap, err := snapshot.Load(repo, id)
			if err != nil {
				return
			}
			defer snap.Close()

			h := snap.Header
			hexID := hex.EncodeToString(id[:])

			roots := []string{}
			for _, src := range h.Sources {
				roots = append(roots, src.Importer.Directory)
			}

			it := Item{
				ItemID:    id,
				Timestamp: h.Timestamp,
				Filters: ItemFilters{
					Name:        h.Name,
					Category:    h.Category,
					Environment: h.Environment,
					Perimeter:   h.Perimeter,
					Job:         h.Job,
					// adjust to however tags are exposed on your header:
					// Tags: h.Tags, or h.GetTags()
					Tags:  h.Tags,
					Roots: roots,
				},
			}

			mu.Lock()
			items = append(items, it)
			hexToMAC[hexID] = id
			mu.Unlock()
		}()
	}
	wg.Wait()
	return items
}

func ParseSnapshotPath(snapshotPath string) (string, string) {
	if strings.HasPrefix(snapshotPath, "/") {
		return "", snapshotPath
	}
	tmp := strings.SplitN(snapshotPath, ":", 2)
	prefix := snapshotPath
	pattern := ""
	if len(tmp) == 2 {
		prefix, pattern = tmp[0], tmp[1]
	}
	return prefix, pattern
}

func LookupSnapshotByPrefix(repo *repository.Repository, prefix string) []objects.MAC {
	ret := make([]objects.MAC, 0)
	for snapshotID := range repo.ListSnapshots() {
		if strings.HasPrefix(hex.EncodeToString(snapshotID[:]), prefix) {
			ret = append(ret, snapshotID)
		}
	}
	return ret
}

func LocateSnapshotByPrefix(repo *repository.Repository, prefix string) (objects.MAC, error) {
	snapshots := LookupSnapshotByPrefix(repo, prefix)
	if len(snapshots) == 0 {
		return objects.MAC{}, fmt.Errorf("no snapshot has prefix: %s", prefix)
	}
	if len(snapshots) > 1 {
		return objects.MAC{}, fmt.Errorf("snapshot ID is ambiguous: %s (matches %d snapshots)", prefix, len(snapshots))
	}
	return snapshots[0], nil
}

func OpenSnapshotByPath(repo *repository.Repository, snapshotPath string) (*snapshot.Snapshot, string, error) {
	prefix, pathname := ParseSnapshotPath(snapshotPath)

	snapshotID, err := LocateSnapshotByPrefix(repo, prefix)
	if err != nil {
		return nil, "", err
	}

	snap, err := snapshot.Load(repo, snapshotID)
	if err != nil {
		return nil, "", err
	}

	var snapRoot string
	if strings.HasPrefix(pathname, "/") {
		snapRoot = pathname
	} else {
		snapRoot = path.Clean(path.Join(snap.Header.GetSource(0).Importer.Directory, pathname))
	}
	return snap, path.Clean(snapRoot), err
}

func OpenSnapshotByPathRelative(repo *repository.Repository, snapshotPath string) (*snapshot.Snapshot, string, string, error) {
	prefix, pathname := ParseSnapshotPath(snapshotPath)

	snapshotID, err := LocateSnapshotByPrefix(repo, prefix)
	if err != nil {
		return nil, "", "", err
	}

	snap, err := snapshot.Load(repo, snapshotID)
	if err != nil {
		return nil, "", "", err
	}

	var snapRoot string
	var relative string
	if strings.HasPrefix(pathname, "/") {
		snapRoot = pathname
		relative = ""
	} else {
		snapRoot = path.Clean(path.Join(snap.Header.GetSource(0).Importer.Directory, pathname))
		relative = pathname
		if relative == "" {
			relative = "."
		}
	}

	return snap, path.Clean(snapRoot), relative, err
}
