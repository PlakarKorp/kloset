// Package ecc provides optional error-correcting codes (ECC) for kloset
// storage objects, using Reed-Solomon coding over erasures.
//
// # Model
//
// A stored object (a packfile or a state) is treated as an opaque blob. When
// ECC is enabled the blob is split into DataShards equal-sized data shards
// (zero-padded) and ParityShards parity shards are computed over them. Only the
// parity shards are persisted, in a *separate* storage object keyed by the same
// MAC as the primary object. The primary object stays byte-identical to a
// non-ECC repository, so:
//
//   - old clients (and ranged reads) read the primary object unchanged;
//   - whether ECC exists for an object is decided purely by whether its parity
//     object exists, never by sniffing the data stream.
//
// # Erasure vs error
//
// Reed-Solomon reconstructs *erasures* (shards known to be missing), not
// *errors* (shards that are present but silently wrong): a present-but-corrupt
// shard would poison the reconstruction. To turn corruption into erasure the
// parity object stores a CRC-32 checksum for every shard — both the data shards
// (which live in the primary) and the parity shards. On read we recompute each
// shard's checksum; any shard that fails is treated as erased and rebuilt from
// the survivors.
//
// This yields a precise guarantee: the original bytes are recoverable as long
// as the corruption or loss touches at most ParityShards of the
// DataShards+ParityShards shards.
package ecc

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/klauspost/reedsolomon"
)

// MAGIC tags a serialized parity object so a corrupted or truncated blob is
// rejected rather than silently mis-parsed.
var MAGIC = [8]byte{'_', 'K', 'E', 'C', 'C', '_', '0', '1'}

// VERSION of the parity-object on-disk format.
const VERSION uint32 = 1

// Fixed parity-object layout:
//
//	magic[8] | version u32 | dataShards u32 | parityShards u32 | dataLen u64 |
//	crc[(dataShards+parityShards)] u32 each | parity-shard bytes
const (
	fieldsSize       = 8 + 4 + 4 + 4 + 8 // magic + version + dataShards + parityShards + dataLen
	crcSize          = 4
	dataLenOffset    = 20
	checksumsOffset  = fieldsSize
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Configuration describes the Reed-Solomon parameters for a repository. It is
// persisted as part of the storage Configuration; a nil *Configuration means
// ECC is disabled.
type Configuration struct {
	DataShards   int `json:"data_shards" msgpack:"data_shards"`
	ParityShards int `json:"parity_shards" msgpack:"parity_shards"`
}

// NewDefaultConfiguration returns the default ECC parameters: 10 data shards
// and 3 parity shards. This tolerates corruption or loss affecting any 3 of the
// 13 shards, for roughly 30% storage overhead on protected objects.
func NewDefaultConfiguration() *Configuration {
	return &Configuration{
		DataShards:   10,
		ParityShards: 3,
	}
}

// Validate checks that the configuration is usable by the Reed-Solomon coder.
func (c *Configuration) Validate() error {
	if c == nil {
		return fmt.Errorf("ecc: nil configuration")
	}
	if c.DataShards <= 0 {
		return fmt.Errorf("ecc: data shards must be > 0, got %d", c.DataShards)
	}
	if c.ParityShards <= 0 {
		return fmt.Errorf("ecc: parity shards must be > 0, got %d", c.ParityShards)
	}
	// reedsolomon caps the total number of shards at 256 for GF(2^8).
	if c.DataShards+c.ParityShards > 256 {
		return fmt.Errorf("ecc: data+parity shards must be <= 256, got %d", c.DataShards+c.ParityShards)
	}
	return nil
}

// Codec wraps a Reed-Solomon encoder configured for a given shard layout.
type Codec struct {
	cfg *Configuration
	enc reedsolomon.Encoder
}

// NewCodec builds a Codec from the given configuration.
func NewCodec(cfg *Configuration) (*Codec, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	enc, err := reedsolomon.New(cfg.DataShards, cfg.ParityShards)
	if err != nil {
		return nil, fmt.Errorf("ecc: %w", err)
	}
	return &Codec{cfg: cfg, enc: enc}, nil
}

// shardSize returns the per-shard size needed to hold dataLen bytes split
// across DataShards shards (ceil division). It is always >= 1 so that even an
// empty object produces well-formed (zero) shards.
func (c *Codec) shardSize(dataLen int) int {
	ds := c.cfg.DataShards
	n := (dataLen + ds - 1) / ds
	if n == 0 {
		n = 1
	}
	return n
}

// split divides data into DataShards+ParityShards equal-sized shards. The data
// is zero-padded to fill the data shards; parity shards are zeroed (Encode
// fills them).
func (c *Codec) split(data []byte) [][]byte {
	shardSize := c.shardSize(len(data))
	total := c.cfg.DataShards + c.cfg.ParityShards

	shards := make([][]byte, total)
	for i := range shards {
		shards[i] = make([]byte, shardSize)
	}

	remaining := data
	for i := 0; i < c.cfg.DataShards; i++ {
		n := copy(shards[i], remaining)
		remaining = remaining[n:]
	}
	return shards
}

// Encode computes the parity shards for data and returns a serialized parity
// object: a self-describing header (including a CRC for every shard) followed
// by the concatenated parity-shard bytes. The returned bytes are what callers
// persist under the ECC storage resource.
func (c *Codec) Encode(data []byte) ([]byte, error) {
	shards := c.split(data)
	if err := c.enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("ecc: encode: %w", err)
	}

	total := c.cfg.DataShards + c.cfg.ParityShards
	shardSize := len(shards[0])

	out := make([]byte, checksumsOffset+total*crcSize+shardSize*c.cfg.ParityShards)
	copy(out[0:8], MAGIC[:])
	binary.LittleEndian.PutUint32(out[8:12], VERSION)
	binary.LittleEndian.PutUint32(out[12:16], uint32(c.cfg.DataShards))
	binary.LittleEndian.PutUint32(out[16:20], uint32(c.cfg.ParityShards))
	binary.LittleEndian.PutUint64(out[dataLenOffset:dataLenOffset+8], uint64(len(data)))

	// Checksums for every shard (data shards first, then parity shards).
	for i := 0; i < total; i++ {
		crc := crc32.Checksum(shards[i], crcTable)
		binary.LittleEndian.PutUint32(out[checksumsOffset+i*crcSize:], crc)
	}

	// Parity-shard payload.
	off := checksumsOffset + total*crcSize
	for i := 0; i < c.cfg.ParityShards; i++ {
		off += copy(out[off:], shards[c.cfg.DataShards+i])
	}
	return out, nil
}

// ParityHeader describes a parsed parity object.
type ParityHeader struct {
	Version      uint32
	DataShards   int
	ParityShards int
	DataLen      int

	checksums   []uint32 // one per shard, data shards first
	parityBytes []byte   // concatenated parity-shard payload
}

// shardSize returns the per-shard byte length implied by the header.
func (h ParityHeader) shardSize() int {
	ds := h.DataShards
	n := (h.DataLen + ds - 1) / ds
	if n == 0 {
		n = 1
	}
	return n
}

// ParseParityObject validates the magic and decodes a serialized parity object.
func ParseParityObject(parity []byte) (*ParityHeader, error) {
	if len(parity) < checksumsOffset {
		return nil, fmt.Errorf("ecc: parity object too small (%d bytes)", len(parity))
	}
	for i := range MAGIC {
		if parity[i] != MAGIC[i] {
			return nil, fmt.Errorf("ecc: bad parity magic")
		}
	}
	h := &ParityHeader{
		Version:      binary.LittleEndian.Uint32(parity[8:12]),
		DataShards:   int(binary.LittleEndian.Uint32(parity[12:16])),
		ParityShards: int(binary.LittleEndian.Uint32(parity[16:20])),
		DataLen:      int(binary.LittleEndian.Uint64(parity[dataLenOffset : dataLenOffset+8])),
	}
	if h.Version != VERSION {
		return nil, fmt.Errorf("ecc: unsupported parity version %d", h.Version)
	}
	if h.DataShards <= 0 || h.ParityShards <= 0 {
		return nil, fmt.Errorf("ecc: invalid shard counts %d/%d", h.DataShards, h.ParityShards)
	}

	total := h.DataShards + h.ParityShards
	shardSize := h.shardSize()
	want := checksumsOffset + total*crcSize + shardSize*h.ParityShards
	if len(parity) != want {
		return nil, fmt.Errorf("ecc: parity object length %d does not match expected %d", len(parity), want)
	}

	h.checksums = make([]uint32, total)
	for i := 0; i < total; i++ {
		h.checksums[i] = binary.LittleEndian.Uint32(parity[checksumsOffset+i*crcSize:])
	}
	h.parityBytes = parity[checksumsOffset+total*crcSize:]
	return h, nil
}

// dataShards lays an unpadded primary object (exactly DataLen bytes) out into
// DataShards equal-sized shards, zero-padding the final shard. The returned
// slices reference fresh memory so callers may mutate them freely. ok is false
// if primary is not the expected DataLen.
func (h *ParityHeader) dataShards(primary []byte) (shards [][]byte, ok bool) {
	if len(primary) != h.DataLen {
		return nil, false
	}
	shardSize := h.shardSize()
	shards = make([][]byte, h.DataShards)
	remaining := primary
	for i := 0; i < h.DataShards; i++ {
		shard := make([]byte, shardSize)
		n := copy(shard, remaining)
		remaining = remaining[n:]
		shards[i] = shard
	}
	return shards, true
}

// Verify reports whether primary (the unpadded, original-length object bytes)
// matches the parity object's recorded data-shard checksums. It is a cheap
// integrity check that does not require Reed-Solomon work.
func Verify(primary []byte, parity []byte) (bool, error) {
	h, err := ParseParityObject(parity)
	if err != nil {
		return false, err
	}
	shards, ok := h.dataShards(primary)
	if !ok {
		return false, nil
	}
	for i := 0; i < h.DataShards; i++ {
		if crc32.Checksum(shards[i], crcTable) != h.checksums[i] {
			return false, nil
		}
	}
	return true, nil
}

// Reconstruct rebuilds the original object bytes from the (possibly corrupt or
// nil) primary data and its serialized parity object.
//
// primary is the raw bytes of the primary storage object as read back, or nil
// if the primary is missing entirely. Each shard's recorded CRC is checked;
// shards that fail (or that fall outside a short/long primary) are treated as
// erasures and rebuilt from the survivors. Reconstruction succeeds as long as
// at least DataShards of the DataShards+ParityShards shards survive intact.
//
// On success it returns exactly DataLen bytes of reconstructed original data.
func Reconstruct(primary []byte, parity []byte) ([]byte, error) {
	h, err := ParseParityObject(parity)
	if err != nil {
		return nil, err
	}

	codec, err := NewCodec(&Configuration{DataShards: h.DataShards, ParityShards: h.ParityShards})
	if err != nil {
		return nil, err
	}

	shardSize := h.shardSize()
	total := h.DataShards + h.ParityShards
	shards := make([][]byte, total)

	// Data shards come from the primary (unpadded, DataLen bytes); keep only
	// those whose CRC matches, erase the rest.
	if dataShards, ok := h.dataShards(primary); ok {
		for i := 0; i < h.DataShards; i++ {
			if crc32.Checksum(dataShards[i], crcTable) == h.checksums[i] {
				shards[i] = dataShards[i]
			}
		}
	}

	// Parity shards come from the parity object; keep only valid ones.
	for i := 0; i < h.ParityShards; i++ {
		shard := h.parityBytes[i*shardSize : (i+1)*shardSize]
		if crc32.Checksum(shard, crcTable) == h.checksums[h.DataShards+i] {
			shards[h.DataShards+i] = shard
		}
	}

	survivors := 0
	for _, s := range shards {
		if s != nil {
			survivors++
		}
	}
	if survivors < h.DataShards {
		return nil, fmt.Errorf("ecc: too much damage: %d/%d shards survive, need %d",
			survivors, total, h.DataShards)
	}

	if err := codec.enc.Reconstruct(shards); err != nil {
		return nil, fmt.Errorf("ecc: reconstruct: %w", err)
	}
	if ok, err := codec.enc.Verify(shards); err != nil || !ok {
		return nil, fmt.Errorf("ecc: reconstruction failed verification")
	}

	out := make([]byte, 0, shardSize*h.DataShards)
	for i := 0; i < h.DataShards; i++ {
		out = append(out, shards[i]...)
	}
	if h.DataLen > len(out) {
		return nil, fmt.Errorf("ecc: data length %d exceeds reconstructed size %d", h.DataLen, len(out))
	}
	return out[:h.DataLen], nil
}
