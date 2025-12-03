package caching

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"iter"

	"github.com/PlakarKorp/kloset/objects"
)

type kvcache struct {
	cache Cache
}

func (c *kvcache) put(prefix, key string, data []byte) error {
	return c.cache.Put(fmt.Appendf(nil, "%s:%s", prefix, key), data)
}

func (c *kvcache) get(prefix, key string) ([]byte, error) {
	return c.cache.Get(fmt.Appendf(nil, "%s:%s", prefix, key))
}

func (c *kvcache) has(prefix, key string) (bool, error) {
	return c.cache.Has(fmt.Appendf(nil, "%s:%s", prefix, key))
}

func (c *kvcache) delete(prefix, key string) error {
	return c.cache.Delete(fmt.Appendf(nil, "%s:%s", prefix, key))
}

// Iterate over keys sharing `keyPrefix` prefix. Extracts the MAC out of the
// key rather than having to unmarshal the value. Beware you can't hold a
// reference to the value between call to Next().
func (c *kvcache) getObjectsWithMAC(keyPrefix string) iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		for key, val := range c.cache.Scan([]byte(keyPrefix), false) {
			/* Extract the csum part of the key, this avoids decoding the full
			 * entry later on if that's the only thing we need */
			hex_csum := string(key[bytes.LastIndexByte(key, byte(':'))+1:])
			csum, _ := hex.DecodeString(hex_csum)

			if !yield(objects.MAC(csum), val) {
				return
			}
		}
	}
}

// Iterate over keys sharing `keyPrefix` prefix. Beware you can't hold a
// reference to the value between call to Next().
func (c *kvcache) getObjects(keyPrefix string) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		for _, val := range c.cache.Scan([]byte(keyPrefix), false) {
			if !yield(val) {
				return
			}
		}
	}
}

func (c *kvcache) Close() error {
	return c.cache.Close()
}
