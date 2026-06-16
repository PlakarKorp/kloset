package repository

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/PlakarKorp/kloset/connectors/storage"
	"github.com/PlakarKorp/kloset/ecc"
	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/resources"
)

// eccEnabled reports whether the repository has ECC configured.
func (r *Repository) eccEnabled() bool {
	return r.configuration.ECC != nil
}

// eccResourceFor maps a primary storage resource to its companion ECC resource.
// It returns (resource, true) for protected resources and (_, false) otherwise.
func eccResourceFor(res storage.StorageResource) (storage.StorageResource, bool) {
	switch res {
	case storage.StorageResourcePackfile:
		return storage.StorageResourceECCPackfile, true
	case storage.StorageResourceState:
		return storage.StorageResourceECCState, true
	}
	return storage.StorageResourceUndefined, false
}

// eccWriter wraps an io.Reader so the bytes streamed to store.Put are also
// captured into a buffer, from which parity can be computed afterwards. The
// primary object stays byte-identical to the non-ECC case; only the captured
// copy is used to derive parity.
type eccCapture struct {
	src io.Reader
	buf bytes.Buffer
}

func (e *eccCapture) Read(p []byte) (int, error) {
	n, err := e.src.Read(p)
	if n > 0 {
		e.buf.Write(p[:n])
	}
	return n, err
}

// putWithECC streams rd into the primary resource and, when ECC is enabled and
// the resource is protected, computes and stores a parity object under the
// companion ECC resource keyed by the same MAC.
//
// It returns the number of bytes written to the primary object (so existing
// I/O accounting is unchanged).
func (r *Repository) putWithECC(ctx context.Context, res storage.StorageResource, mac objects.MAC, rd io.Reader) (int64, error) {
	eccRes, protected := eccResourceFor(res)
	if !r.eccEnabled() || !protected {
		return r.store.Put(ctx, res, mac, rd)
	}

	capture := &eccCapture{src: rd}
	nbytes, err := r.store.Put(ctx, res, mac, capture)
	if err != nil {
		return nbytes, err
	}

	codec, err := ecc.NewCodec(r.configuration.ECC)
	if err != nil {
		return nbytes, fmt.Errorf("ecc: %w", err)
	}
	parity, err := codec.Encode(capture.buf.Bytes())
	if err != nil {
		return nbytes, fmt.Errorf("ecc: encode %s %x: %w", res, mac, err)
	}

	if _, err := r.store.Put(ctx, eccRes, mac, bytes.NewReader(parity)); err != nil {
		return nbytes, fmt.Errorf("ecc: store parity %s %x: %w", eccRes, mac, err)
	}
	return nbytes, nil
}

// getWithECC reads the full primary object for a protected resource and, if it
// is unreadable or corrupt, transparently reconstructs it from the companion
// parity object. It returns the raw (still serialized/enveloped) primary bytes,
// exactly as a plain store.Get + io.ReadAll would have, so callers downstream
// are unchanged.
//
// verify is an optional callback that decides whether the raw primary bytes are
// intact. It lets the caller reuse the existing HMAC/MAC checks instead of
// re-deriving corruption detection here. When verify is nil, only the parity
// object's per-shard checksums are used to detect corruption.
func (r *Repository) getWithECC(ctx context.Context, res storage.StorageResource, mac objects.MAC, verify func([]byte) bool) ([]byte, error) {
	eccRes, protected := eccResourceFor(res)

	primary, primaryErr := r.readFull(ctx, res, mac)

	// If ECC is not applicable, just surface whatever we read.
	if !r.eccEnabled() || !protected {
		return primary, primaryErr
	}

	// Fetch the parity object; if there is none, this object is not protected
	// (e.g. written before ECC was enabled) so behave like a plain read.
	parity, parityErr := r.readFull(ctx, eccRes, mac)
	if parityErr != nil {
		if errors.Is(parityErr, os.ErrNotExist) {
			return primary, primaryErr
		}
		// Parity unreadable for another reason: fall back to the primary.
		if primaryErr == nil {
			return primary, nil
		}
		return nil, fmt.Errorf("ecc: primary unreadable (%v) and parity unreadable (%w)", primaryErr, parityErr)
	}

	// Decide whether the primary is good. A read error means it is missing or
	// truncated; otherwise consult verify (if given) and the parity checksums.
	primaryGood := primaryErr == nil
	if primaryGood && verify != nil {
		primaryGood = verify(primary)
	}
	if primaryGood {
		if ok, err := ecc.Verify(primary, parity); err == nil && ok {
			return primary, nil
		}
		// Checksums disagree: treat as corrupt and reconstruct.
		primaryGood = false
	}

	if primaryErr != nil {
		primary = nil // missing primary -> let Reconstruct erase all data shards
	}
	repaired, err := ecc.Reconstruct(primary, parity)
	if err != nil {
		return nil, fmt.Errorf("ecc: repair %s %x: %w", res, mac, err)
	}
	r.Logger().Warn("ecc", "repaired %s %x using parity", res, mac)
	return repaired, nil
}

// verifyEnvelope returns a callback that reports whether raw enveloped bytes
// pass the storage HMAC check for the given resource type. This mirrors what
// storage.Deserialize validates while streaming, but lets getWithECC decide
// up-front whether to trust the primary or fall back to parity.
//
// Envelope layout (see connectors/storage/serialization.go):
//
//	header[16] | data[...] | hmac[32]  where hmac = MAC(header || data)
func (r *Repository) verifyEnvelope(resourceType resources.Type) func([]byte) bool {
	return func(raw []byte) bool {
		const headerSize = 16
		const footerSize = 32
		if len(raw) < headerSize+footerSize {
			return false
		}
		// The header carries the resource type at bytes [8:12]; a mismatch
		// means the bytes are not what we expect.
		if resources.Type(binary.LittleEndian.Uint32(raw[8:12])) != resourceType {
			return false
		}
		hasher := r.GetMACHasher()
		hasher.Reset()
		hasher.Write(raw[:len(raw)-footerSize])
		return bytes.Equal(hasher.Sum(nil), raw[len(raw)-footerSize:])
	}
}

// readFull reads an entire storage object into memory.
func (r *Repository) readFull(ctx context.Context, res storage.StorageResource, mac objects.MAC) ([]byte, error) {
	rd, err := r.store.Get(ctx, res, mac, nil)
	if err != nil {
		return nil, err
	}
	defer rd.Close()
	return io.ReadAll(rd)
}

// RepairObject verifies a protected object and, if it is corrupt, rewrites the
// primary from its parity object. It is the explicit (opt-in) self-healing
// entry point; ordinary reads reconstruct in memory but never write back.
//
// It returns true if a repair was performed, false if the object was already
// intact, and an error if the object cannot be repaired.
func (r *RepositoryWriter) RepairObject(res storage.StorageResource, mac objects.MAC) (bool, error) {
	_, protected := eccResourceFor(res)
	if !r.eccEnabled() || !protected {
		return false, fmt.Errorf("ecc: not enabled for resource %s", res)
	}

	ctx := r.appContext

	parity, err := r.readFull(ctx, eccResourceForMust(res), mac)
	if err != nil {
		return false, fmt.Errorf("ecc: read parity: %w", err)
	}

	primary, primaryErr := r.readFull(ctx, res, mac)
	if primaryErr == nil {
		if ok, verr := ecc.Verify(primary, parity); verr == nil && ok {
			return false, nil // already intact
		}
	} else {
		primary = nil
	}

	repaired, err := ecc.Reconstruct(primary, parity)
	if err != nil {
		return false, fmt.Errorf("ecc: reconstruct: %w", err)
	}
	if _, err := r.store.Put(ctx, res, mac, bytes.NewReader(repaired)); err != nil {
		return false, fmt.Errorf("ecc: rewrite primary: %w", err)
	}
	return true, nil
}

// eccResourceForMust is eccResourceFor for callers that have already validated
// the resource is protected.
func eccResourceForMust(res storage.StorageResource) storage.StorageResource {
	eccRes, _ := eccResourceFor(res)
	return eccRes
}
