package objects

import (
	"github.com/vmihailenco/msgpack/v5"
)

type CachedPath struct {
	MAC         MAC      `msgpack:"MAC"`
	ObjectMAC   MAC      `msgpack:"objectMAC"`
	FileInfo    FileInfo `msgpack:"fileinfo"`
	Objects     uint32   `msgpack:"objects"`
	Chunks      uint64   `msgpack:"chunks"`
	Entropy     float64  `msgpack:"entropy"`
	ContentType string   `msgpack:"content_type"`
}

func NewCachedPathFromBytes(serialized []byte) (*CachedPath, error) {
	var o CachedPath
	if err := msgpack.Unmarshal(serialized, &o); err != nil {
		return nil, err
	}
	return &o, nil
}

func (o *CachedPath) Serialize() ([]byte, error) {
	return msgpack.Marshal(o)
}

func (o *CachedPath) Stat() *FileInfo {
	return &o.FileInfo
}
