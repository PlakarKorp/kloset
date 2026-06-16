package ecc

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"testing"
)

// makeData returns n deterministic but non-trivial bytes.
func makeData(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((i*7 + 13) % 251)
	}
	return b
}

func TestDefaultConfiguration(t *testing.T) {
	c := NewDefaultConfiguration()
	if c.DataShards != 10 || c.ParityShards != 3 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		cfg *Configuration
		ok  bool
	}{
		{nil, false},
		{&Configuration{0, 3}, false},
		{&Configuration{10, 0}, false},
		{&Configuration{10, 3}, true},
		{&Configuration{200, 100}, false}, // > 256 total
		{&Configuration{128, 128}, true},  // == 256 total
	}
	for i, tc := range cases {
		err := tc.cfg.Validate()
		if (err == nil) != tc.ok {
			t.Errorf("case %d: cfg=%+v got err=%v, want ok=%v", i, tc.cfg, err, tc.ok)
		}
	}
}

func TestEncodeReconstructRoundTrip(t *testing.T) {
	sizes := []int{0, 1, 7, 100, 1000, 4096, 65537}
	for _, n := range sizes {
		t.Run(fmt.Sprintf("size=%d", n), func(t *testing.T) {
			data := makeData(n)
			codec, err := NewCodec(NewDefaultConfiguration())
			if err != nil {
				t.Fatal(err)
			}
			parity, err := codec.Encode(data)
			if err != nil {
				t.Fatal(err)
			}

			// Intact primary (the unpadded original bytes): Verify must pass
			// and Reconstruct must return the same bytes.
			ok, err := Verify(data, parity)
			if err != nil || !ok {
				t.Fatalf("Verify(intact) = %v, %v; want true, nil", ok, err)
			}
			got, err := Reconstruct(data, parity)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, data) {
				t.Fatalf("reconstructed data mismatch for size %d", n)
			}
		})
	}
}

// shardSizeFor mirrors the codec's per-shard sizing for tests that corrupt
// specific data shards within an unpadded primary.
func shardSizeFor(dataLen, dataShards int) int {
	n := (dataLen + dataShards - 1) / dataShards
	if n == 0 {
		n = 1
	}
	return n
}

func TestReconstructToleratesParityShardLosses(t *testing.T) {
	cfg := NewDefaultConfiguration() // tolerates 3
	data := makeData(50000)
	codec, _ := NewCodec(cfg)
	parity, _ := codec.Encode(data)

	shardSize := shardSizeFor(len(data), cfg.DataShards)

	// Corrupt exactly ParityShards data shards (the tolerance limit).
	corrupt := append([]byte(nil), data...)
	for i := 0; i < cfg.ParityShards; i++ {
		// flip a byte inside data shard i
		corrupt[i*shardSize] ^= 0xFF
	}

	// Verify must detect the corruption.
	if ok, _ := Verify(corrupt, parity); ok {
		t.Fatal("Verify should fail on corrupt primary")
	}

	got, err := Reconstruct(corrupt, parity)
	if err != nil {
		t.Fatalf("reconstruct at tolerance limit failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("reconstructed data mismatch at tolerance limit")
	}
}

func TestReconstructFromMissingPrimary(t *testing.T) {
	// With DataShards=3, ParityShards=3 we can lose all 3 data shards and
	// rebuild purely from parity.
	cfg := &Configuration{DataShards: 3, ParityShards: 3}
	data := makeData(9000)
	codec, _ := NewCodec(cfg)
	parity, _ := codec.Encode(data)

	got, err := Reconstruct(nil, parity)
	if err != nil {
		t.Fatalf("reconstruct from nil primary failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("reconstructed data mismatch from parity-only")
	}
}

func TestReconstructFailsBeyondTolerance(t *testing.T) {
	cfg := NewDefaultConfiguration() // tolerates 3
	data := makeData(50000)
	codec, _ := NewCodec(cfg)
	parity, _ := codec.Encode(data)

	shardSize := shardSizeFor(len(data), cfg.DataShards)

	// Corrupt ParityShards+1 data shards: beyond tolerance.
	corrupt := append([]byte(nil), data...)
	for i := 0; i < cfg.ParityShards+1; i++ {
		corrupt[i*shardSize] ^= 0xFF
	}

	if _, err := Reconstruct(corrupt, parity); err == nil {
		t.Fatal("reconstruct should fail beyond tolerance")
	}
}

func TestReconstructToleratesCorruptParityShard(t *testing.T) {
	cfg := NewDefaultConfiguration()
	data := makeData(40000)
	codec, _ := NewCodec(cfg)
	parity, _ := codec.Encode(data)

	// Corrupt one parity shard and one data shard (2 total, within tolerance).
	corruptParity := append([]byte(nil), parity...)
	// flip a byte in the parity payload region
	corruptParity[len(corruptParity)-1] ^= 0xFF

	corruptPrimary := append([]byte(nil), data...)
	corruptPrimary[0] ^= 0xFF

	got, err := Reconstruct(corruptPrimary, corruptParity)
	if err != nil {
		t.Fatalf("reconstruct with corrupt parity+data failed: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data mismatch with corrupt parity+data shard")
	}
}

func TestParseParityObjectRejectsGarbage(t *testing.T) {
	if _, err := ParseParityObject([]byte("too small")); err == nil {
		t.Fatal("expected error on tiny input")
	}
	// Valid-size buffer with bad magic.
	buf := make([]byte, checksumsOffset+100)
	if _, err := ParseParityObject(buf); err == nil {
		t.Fatal("expected error on bad magic")
	}
}

func TestVerifyDetectsWrongLength(t *testing.T) {
	cfg := NewDefaultConfiguration()
	data := makeData(1000)
	codec, _ := NewCodec(cfg)
	parity, _ := codec.Encode(data)

	if ok, _ := Verify([]byte("short"), parity); ok {
		t.Fatal("Verify should fail on wrong-length primary")
	}
}

func TestChecksumTableStable(t *testing.T) {
	// Guard: the on-disk format depends on the Castagnoli polynomial.
	got := crc32.Checksum([]byte("kloset"), crcTable)
	want := crc32.Checksum([]byte("kloset"), crc32.MakeTable(crc32.Castagnoli))
	if got != want {
		t.Fatal("crc table mismatch")
	}
}
