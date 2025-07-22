package caching

import (
	"bytes"
	"encoding/hex"
	"iter"
	"strconv"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
)

func key(prefix string, mac objects.MAC) []byte {
	key := make([]byte, 0, len(prefix)+1+hex.EncodedLen(len(mac[:])))
	key = append(key, []byte(prefix)...)
	key = append(key, ':')
	key = append(key, hex.EncodeToString(mac[:])...)
	return key
}

func key2(prefix string, mac1, mac2 objects.MAC) []byte {
	len1 := hex.EncodedLen(len(mac1[:]))
	len2 := hex.EncodedLen(len(mac2[:]))
	siz := len(prefix) + 1 + len1 + 1 + len2

	key := make([]byte, 0, siz)
	key = append(key, []byte(prefix)...)
	key = append(key, ':')
	key = append(key, hex.EncodeToString(mac1[:])...)
	key = append(key, ':')
	key = append(key, hex.EncodeToString(mac2[:])...)
	return key
}

func keyT(prefix string, t resources.Type, mac objects.MAC) []byte {
	ts := strconv.Itoa(int(t))
	key := make([]byte, 0, len(prefix)+1+len(ts)+1+hex.EncodedLen(len(mac[:])))
	key = append(key, []byte(prefix)...)
	key = append(key, ':')
	key = append(key, []byte(ts)...)
	key = append(key, ':')
	key = append(key, hex.EncodeToString(mac[:])...)
	return key
}

func keyT2(prefix string, t resources.Type, mac1, mac2 objects.MAC) []byte {
	ts := strconv.Itoa(int(t))

	len1 := hex.EncodedLen(len(mac1[:]))
	len2 := hex.EncodedLen(len(mac2[:]))
	siz := len(prefix) + 1 + len(ts) + 1 + len1 + 1 + len2

	key := make([]byte, 0, siz)
	key = append(key, []byte(prefix)...)
	key = append(key, ':')
	key = append(key, []byte(ts)...)
	key = append(key, ':')
	key = append(key, hex.EncodeToString(mac1[:])...)
	key = append(key, ':')
	key = append(key, hex.EncodeToString(mac2[:])...)
	return key
}

func scanMAC2(c Cache, key []byte) iter.Seq2[objects.MAC, []byte] {
	return func(yield func(objects.MAC, []byte) bool) {
		for k, v := range c.Scan(key, false) {
			hex_mac := string(k[bytes.LastIndexByte(k, byte(':'))+1:])
			mac, _ := hex.DecodeString(hex_mac)

			if !yield(objects.MAC(mac), v) {
				return
			}
		}
	}
}
