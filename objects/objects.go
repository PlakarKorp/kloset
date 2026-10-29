package objects

import (
	"encoding/json"

	"github.com/PlakarKorp/integration-sdk"
	"github.com/PlakarKorp/kloset/resources"
	"github.com/PlakarKorp/kloset/versioning"
	"github.com/vmihailenco/msgpack/v5"
)

const OBJECT_VERSION = "1.0.0"
const CHUNK_VERSION = "1.0.0"

func init() {
	versioning.Register(resources.RT_OBJECT, versioning.FromString(OBJECT_VERSION))
	versioning.Register(resources.RT_CHUNK, versioning.FromString(CHUNK_VERSION))
}

type (
	MAC = integration.MAC
)

var (
	NilMac = integration.NilMac
)

func RandomMAC() MAC {
	return integration.RandomMAC()
}

type Object struct {
	Version     versioning.Version `msgpack:"version" json:"version"`
	ContentMAC  MAC                `msgpack:"contentMAC" json:"contentMAC"`
	Chunks      []Chunk            `msgpack:"chunks" json:"-"`
	ContentType string             `msgpack:"content_type,omitempty" json:"content_type"`
	Entropy     float64            `msgpack:"entropy,omitempty" json:"entropy"`
	Flags       uint32             `msgpack:"flags" json:"flags"`
}

func (o *Object) Size() int64 {
	var size int64
	for _, chunk := range o.Chunks {
		size += int64(chunk.Length)
	}
	return size
}

func NewObject() *Object {
	return &Object{
		Version: versioning.FromString(OBJECT_VERSION),
	}
}

func NewObjectFromBytes(serialized []byte) (*Object, error) {
	var o Object
	if err := msgpack.Unmarshal(serialized, &o); err != nil {
		return nil, err
	}
	return &o, nil
}

func (o *Object) Serialize() ([]byte, error) {
	return msgpack.Marshal(o)
}

type Chunk struct {
	Version    versioning.Version `msgpack:"version" json:"version"`
	ContentMAC MAC                `msgpack:"contentMAC" json:"contentMAC"`
	Length     uint32             `msgpack:"length" json:"length"`
	Entropy    float64            `msgpack:"entropy" json:"entropy"`
	Flags      uint32             `msgpack:"flags" json:"flags"`
}

func NewChunk() *Chunk {
	return &Chunk{
		Version: versioning.FromString(CHUNK_VERSION),
	}
}

func NewChunkFromBytes(serialized []byte) (*Chunk, error) {
	var c Chunk
	if err := msgpack.Unmarshal(serialized, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Chunk) Serialize() ([]byte, error) {
	return msgpack.Marshal(c)
}

func (c *Chunk) MarshalJSON() ([]byte, error) {
	// Create an alias to avoid recursive MarshalJSON calls
	type Alias Chunk
	return json.Marshal((*Alias)(c))
}
