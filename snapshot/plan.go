package snapshot

import (
	"sync"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/snapshot/vfs"
)

func (snap *Snapshot) PlanPackfileFetches(pathname string, concurrency int) (repository.FetchPlan, error) {
	if concurrency <= 0 {
		concurrency = 1
	}

	pvfs, err := snap.FilesystemWithCache()
	if err != nil {
		return nil, err
	}

	var objectMACs []objects.MAC
	err = pvfs.WalkDir(pathname, func(_ string, entry *vfs.Entry, err error) error {
		if err != nil {
			return err
		}
		if entry == nil || !entry.HasObject() {
			return nil
		}
		objectMACs = append(objectMACs, entry.Object)
		return nil
	})
	if err != nil {
		return nil, err
	}

	objectPlan := make(repository.FetchPlan)
	for _, mac := range objectMACs {
		loc, exists, err := snap.repository.GetLocationForBlob(resources.RT_OBJECT, mac)
		if err != nil || !exists {
			continue
		}
		objectPlan[loc.Packfile] = append(objectPlan[loc.Packfile], repository.PackfileFetchRange{
			Offset: loc.Offset,
			Length: loc.Length,
		})
	}
	_ = snap.repository.PrefetchPlan(objectPlan, repository.PrefetchPlanOptions{Concurrency: concurrency})

	plan := make(repository.FetchPlan)
	var planMu sync.Mutex
	var firstErr error
	var errMu sync.Mutex

	jobs := make(chan objects.MAC)
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for mac := range jobs {
				obj, err := snap.LookupObject(mac)
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					continue
				}

				for _, chunk := range obj.Chunks {
					loc, exists, err := snap.repository.GetLocationForBlob(resources.RT_CHUNK, chunk.ContentMAC)
					if err != nil || !exists {
						continue
					}

					planMu.Lock()
					plan[loc.Packfile] = append(plan[loc.Packfile], repository.PackfileFetchRange{
						Offset: loc.Offset,
						Length: loc.Length,
					})
					planMu.Unlock()
				}
			}
		}()
	}

	for _, mac := range objectMACs {
		jobs <- mac
	}
	close(jobs)
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return plan, nil
}
