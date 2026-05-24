package caching

import (
	"fmt"
	"iter"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/google/uuid"
)

type RepositoryCache struct {
	kvcache
}

type repoBatch struct {
	Batch
}

func newRepositoryCache(cons Constructor, repositoryID uuid.UUID) (*RepositoryCache, error) {
	cache, err := cons(CACHE_VERSION, "repository", repositoryID.String(), None)
	if err != nil {
		return nil, err
	}

	return &RepositoryCache{kvcache{cache}}, nil
}

func (c *RepositoryCache) NewBatch() StateBatch {
	return &repoBatch{c.cache.NewBatch()}
}

func (c *RepositoryCache) PutState(stateID objects.MAC, data []byte) error {
	return c.put("__state__", fmt.Sprintf("%x", stateID), data)
}

func (c *RepositoryCache) HasState(stateID objects.MAC) (bool, error) {
	return c.has("__state__", fmt.Sprintf("%x", stateID))
}

func (c *RepositoryCache) GetState(stateID objects.MAC) ([]byte, error) {
	return c.get("__state__", fmt.Sprintf("%x", stateID))
}

func (c *RepositoryCache) GetLatestState() (objects.MAC, error) {
	return objects.NilMac, nil
}

func (c *RepositoryCache) DelState(stateID objects.MAC) error {
	return c.delete("__state__", fmt.Sprintf("%x", stateID))
}

func (c *RepositoryCache) GetStates() (map[objects.MAC][]byte, error) {
	ret := make(map[objects.MAC][]byte, 0)

	for key, val := range c.getObjectsWithMAC("__state__:") {
		value := make([]byte, len(val))
		copy(value, val)

		ret[key] = value
	}

	return ret, nil
}

func (c *RepositoryCache) GetDelta(blobType resources.Type, blobCsum objects.MAC) iter.Seq2[objects.MAC, []byte] {
	return c.getObjectsWithMAC(fmt.Sprintf("__delta__:%d:%x:", blobType, blobCsum))
}

func (c *repoBatch) PutDelta(blobType resources.Type, blobCsum, packfile objects.MAC, data []byte) error {
	return c.Put(fmt.Appendf(nil, "__delta__:%d:%x:%x", blobType, blobCsum, packfile), data)
}

func (c *RepositoryCache) PutDelta(blobType resources.Type, blobCsum, packfile objects.MAC, data []byte) error {
	return c.put("__delta__", fmt.Sprintf("%d:%x:%x", blobType, blobCsum, packfile), data)
}

func (c *RepositoryCache) GetDeltasByType(blobType resources.Type) iter.Seq2[objects.MAC, []byte] {
	return c.getObjectsWithMAC(fmt.Sprintf("__delta__:%d:", blobType))
}

func (c *RepositoryCache) GetDeltas() iter.Seq2[objects.MAC, []byte] {
	return c.getObjectsWithMAC("__delta__:")
}

func (c *RepositoryCache) DelDelta(blobType resources.Type, blobCsum, packfileMAC objects.MAC) error {
	return c.delete("__delta__", fmt.Sprintf("%d:%x:%x", blobType, blobCsum, packfileMAC))
}

func (c *RepositoryCache) PutColoured(blobType resources.Type, blobCsum objects.MAC, data []byte) error {
	return c.put("__deleted__", fmt.Sprintf("%d:%x", blobType, blobCsum), data)
}

func (c *RepositoryCache) HasColoured(blobType resources.Type, blobCsum objects.MAC) (bool, error) {
	return c.has("__deleted__", fmt.Sprintf("%d:%x", blobType, blobCsum))
}

func (c *RepositoryCache) GetColouredEntries() iter.Seq2[objects.MAC, []byte] {
	return c.getObjectsWithMAC("__deleted__:")
}

func (c *RepositoryCache) GetColouredEntriesByType(blobType resources.Type) iter.Seq2[objects.MAC, []byte] {
	return c.getObjectsWithMAC(fmt.Sprintf("__deleted__:%d:", blobType))
}

func (c *RepositoryCache) DelColoured(blobType resources.Type, blobCsum objects.MAC) error {
	return c.delete("__deleted__", fmt.Sprintf("%d:%x", blobType, blobCsum))
}

func (c *RepositoryCache) PutPackfile(packfile objects.MAC, data []byte) error {
	return c.put("__packfile__", fmt.Sprintf("%x", packfile), data)
}

func (c *RepositoryCache) HasPackfile(packfile objects.MAC) (bool, error) {
	return c.has("__packfile__", fmt.Sprintf("%x", packfile))
}

func (c *RepositoryCache) DelPackfile(packfile objects.MAC) error {
	return c.delete("__packfile__", fmt.Sprintf("%x", packfile))
}

func (c *RepositoryCache) GetPackfiles() iter.Seq2[objects.MAC, []byte] {
	return c.getObjectsWithMAC("__packfile__:")
}

func (c *RepositoryCache) PutConfiguration(key string, data []byte) error {
	return c.put("__configuration__", key, data)
}

func (c *RepositoryCache) GetConfiguration(key string) ([]byte, error) {
	return c.get("__configuration__", key)
}

func (c *RepositoryCache) GetConfigurations() iter.Seq[[]byte] {
	return c.getObjects("__configuration__:")
}

// Statcache: per-source, per-path cache of file metadata + ObjectMAC + VFS
// entry MAC, used as a local fast-path to skip chunking and BlobExists
// roundtrips for unchanged files across backups. The source key is
// "<type>:<origin>:<root>", encoded by the caller.
func (c *RepositoryCache) PutStatEntry(source, path string, data []byte) error {
	return c.put("__statcache__", source+":"+path, data)
}

func (c *RepositoryCache) GetStatEntry(source, path string) ([]byte, error) {
	return c.get("__statcache__", source+":"+path)
}

func (c *RepositoryCache) DelStatEntry(source, path string) error {
	return c.delete("__statcache__", source+":"+path)
}

// Dircache: per-source, per-directory cache of the previous backup's
// dirpack MAC and the sorted list of immediate child names. Used to skip
// rebuilding a dirpack for unchanged subtrees on subsequent backups.
func (c *RepositoryCache) PutDirCacheEntry(source, path string, data []byte) error {
	return c.put("__dircache__", source+":"+path, data)
}

func (c *RepositoryCache) GetDirCacheEntry(source, path string) ([]byte, error) {
	return c.get("__dircache__", source+":"+path)
}

func (c *RepositoryCache) DelDirCacheEntry(source, path string) error {
	return c.delete("__dircache__", source+":"+path)
}

func (c *RepositoryCache) PutSnapshot(stateID objects.MAC, data []byte) error {
	return c.put("__snapshot__", fmt.Sprintf("%x", stateID), data)
}

func (c *RepositoryCache) HasSnapshot(stateID objects.MAC) (bool, error) {
	return c.has("__snapshot__", fmt.Sprintf("%x", stateID))
}

func (c *RepositoryCache) GetSnapshot(stateID objects.MAC) ([]byte, error) {
	return c.get("__snapshot__", fmt.Sprintf("%x", stateID))
}

func (c *RepositoryCache) DelSnapshot(stateID objects.MAC) error {
	return c.delete("__snapshot__", fmt.Sprintf("%x", stateID))
}

// Those two are only to construct deltas, when working with the local state we
// do the actual deletion.
func (c *RepositoryCache) PutDeleted(typ uint8, blobCsum objects.MAC, data []byte) error {
	panic("PutDeleted is not to be used with local state")
}

func (c *RepositoryCache) GetDeletedEntries() iter.Seq[[]byte] {
	panic("PutDeleted is not to be used with local state")
}
