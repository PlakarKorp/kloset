package caching

import (
	"bytes"
	"encoding/hex"
	"iter"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/google/uuid"
)

type MaintenanceCache struct {
	cache Cache
}

func newMaintenanceCache(cons Constructor, repositoryID uuid.UUID) (*MaintenanceCache, error) {
	cache, err := cons(CACHE_VERSION, "maintenance", repositoryID.String(), None)
	if err != nil {
		return nil, err
	}

	return &MaintenanceCache{cache}, nil
}

func (c *MaintenanceCache) Close() error { return c.cache.Close() }

func (c *MaintenanceCache) PutSnapshot(snapshotID objects.MAC, data []byte) error {
	return c.cache.Put(key("__snapshot__", snapshotID), data)
}

func (c *MaintenanceCache) HasSnapshot(snapshotID objects.MAC) (bool, error) {
	return c.cache.Has(key("__snapshot__", snapshotID))
}

func (c *MaintenanceCache) DeleteSnapshot(snapshotID objects.MAC) error {
	return c.cache.Delete(key("__snapshot__", snapshotID))
}

func (c *MaintenanceCache) PutPackfile(snapshotID, packfileMAC objects.MAC) error {
	return c.cache.Put(key2("__packfile__", packfileMAC, snapshotID), packfileMAC[:])
}

func (c *MaintenanceCache) HasPackfile(packfileMAC objects.MAC) bool {
	for range c.cache.Scan(key("__packfile__", packfileMAC), false) {
		return true
	}

	return false
}

func (c *MaintenanceCache) GetPackfiles(snapshotID objects.MAC) iter.Seq[objects.MAC] {
	return func(yield func(objects.MAC) bool) {
		for _, p := range c.cache.Scan(key("__packfile__", snapshotID), false) {
			if !yield(objects.MAC(p)) {
				return
			}
		}
	}
}

func (c *MaintenanceCache) DeleletePackfiles(snapshotID objects.MAC) error {
	for key, _ := range c.cache.Scan(nil, false) {
		hex_mac := string(key[bytes.LastIndexByte(key, byte(':'))+1:])
		mac, err := hex.DecodeString(hex_mac)
		if err != nil {
			return err
		}

		if objects.MAC(mac) == snapshotID {
			err := c.cache.Delete(key)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
