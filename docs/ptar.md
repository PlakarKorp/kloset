# PTAR archive format — specification

Status: descriptive specification of the format as implemented, not a design proposal.

Reference implementation:

| Component | Module | Version |
|---|---|---|
| Container (`ptar://`, `ptar+http(s)://` store) | `github.com/PlakarKorp/integrations/ptar/storage` | v1.1.1 |
| Repository layers (wrapper, packfile, state, snapshot) | `github.com/PlakarKorp/kloset` | v1.1.8 |
| Writer front-end (`plakar ptar`) | `github.com/PlakarKorp/plakar/subcommands/ptar` | v1.1.7 (embeds the two above) |

Where the code and this document disagree, the code wins. Write to the maintainers so the document gets fixed. Behaviour that looks unintended is listed in §12 and is not normative.

---

## 1. Overview

A PTAR file is a complete, read-only Kloset repository serialized into a single file. It holds:

- one **configuration**, which sets the repository ID, hashing, chunking, compression and encryption;
- exactly one **packfile**, which holds every blob of every snapshot in the archive;
- exactly one **state**, the index that maps each blob MAC to its location in the packfile.

PTAR reuses the regular Kloset object model unchanged. Snapshots, VFS trees, objects and chunks are the same blobs a normal repository stores. Only the storage layout differs:

- a regular repository spreads many packfiles and states across a backend;
- PTAR concatenates one of each behind a fixed header and a fixed-size trailer.

Because of this, a PTAR can be:

- opened in place, as a local file or over HTTP range requests;
- synced into a normal repository;
- produced from a normal repository (`plakar ptar -k`).

### 1.1 Layering

```
+-------------------------------------------------------------+
| PTAR container        magic, version, 3 regions, trailer    |  §3
|  +-------------------------------------------------------+  |
|  | Storage wrapper   "_KLOSET_" header + payload + MAC   |  |  §4
|  |  +-------------------------------------------------+  |  |
|  |  | Config (msgpack) | Packfile | State stream      |  |  |  §6, §7, §9
|  |  |                    blobs, index and footer go   |  |  |
|  |  |                    through the encoding pipeline|  |  |  §5
|  |  +-------------------------------------------------+  |  |
|  +-------------------------------------------------------+  |
+-------------------------------------------------------------+
Blobs inside the packfile form the snapshot graph               §10
```

### 1.2 Conventions

- All integers are **little-endian**.
- `u8/u16/u32/u64` are unsigned; `i64` is signed two's complement.
- `MAC` is 32 raw bytes.
- `Version` is a `u32` encoding `major<<16 | minor<<8 | patch`, each component on 8 bits.
  - `1.0.0` = `0x00010000`, serialized as bytes `00 00 01 00`.
  - `1.1.0` = `0x00010100`.
- **msgpack** means `github.com/vmihailenco/msgpack/v5`. Structs are encoded as maps.
  - The key is the `msgpack:` tag. Without a tag, the key is the **Go field name**. `json:` tags are ignored.
  - `time.Time` uses the msgpack timestamp extension (-1).
  - `uuid.UUID` is `bin` of 16 bytes.
  - `[32]byte` is `bin` of 32 bytes.
- "MUST", "SHOULD" and "MAY" are used as in RFC 2119.

---

## 2. Naming and access

| Scheme | Access | Notes |
|---|---|---|
| `ptar:///abs/path.ptar` | local file, read and write (create only) | |
| `/path/x.ptar` or `fs://…x.ptar` | local file | rewritten to `ptar://` because the path ends in `.ptar` |
| `ptar+http://host/path`, `ptar+https://…` | read-only | Uses `HEAD` to get `Content-Length`, then `GET` with `Range: bytes=a-b` |

HTTP servers MUST:

- return `200` with a `Content-Length` to `HEAD`;
- honour `Range` with `206 Partial Content`.

A `200` reply to a ranged `GET` is treated as an error.

The suggested file extension is `.ptar`. There is no registered media type; use `application/octet-stream`.

---

## 3. Container layout

```
offset            size  field
0                 8     magic           = "_PLATAR_" (5F 50 4C 41 54 41 52 5F)
8                 4     container_version u32 = 0x00010000 (1.0.0)
12                C     CONFIG region     (wrapped config, §4, §6)
12+C              P     PACKFILE region   (wrapped packfile, §4, §7)
12+C+P            S     STATE region      (wrapped state, §4, §9)
12+C+P+S          48    TRAILER
```

TRAILER (48 bytes, at `filesize - 48`):

| Off | Type | Field |
|---|---|---|
| 0 | i64 | `config_offset` (always 12 as written) |
| 8 | i64 | `config_length` = C |
| 16 | i64 | `packfile_offset` = 12 + C |
| 24 | i64 | `packfile_length` = P |
| 32 | i64 | `state_offset` = 12 + C + P |
| 40 | i64 | `state_length` = S |

Normative rules:

- The three regions are contiguous and in this order. Nothing sits between them, and the trailer comes directly after STATE.
- The trailer has no magic and no checksum. A reader locates it only by its position, 48 bytes before EOF.
- A file with no trailer is incomplete and unreadable. The writer appends the trailer at close, so an interrupted write leaves a truncated, invalid file.
- `config_length` MUST satisfy `0 < config_length ≤ 32768`. The reader rejects anything else.
- Readers MUST check that the magic is `_PLATAR_`.
- The current reader reads `container_version` but **does not validate it**. Future readers SHOULD reject any major version other than 1.
- Readers SHOULD bounds-check all six trailer values against the file size; see §11.

### 3.1 Virtual store view

The container exposes a fixed view to the repository layer:

| Resource | `List()` returns | `Get(mac)` |
|---|---|---|
| State | `[0f 0e 0d … 01 00]` (one fixed MAC) | Returns the STATE region. MAC must equal that constant. |
| Packfile | `[00 01 02 … 0e 0f]` (one fixed MAC) | Returns the PACKFILE region, or a sub-range of it. **The MAC argument is ignored.** |
| Lock | `[]` | Empty |

The state refers to the packfile by a **random** MAC chosen at write time (§7.4), not by the fixed MAC above. This works only because the packfile `Get` ignores its MAC argument.

A range request `(offset, length)` maps to the absolute file range `[packfile_offset + offset, packfile_offset + offset + length)`.

---

## 4. Storage wrapper

Each region is one *wrapped resource*:

```
off   size  field
0     8     magic          = "_KLOSET_"   (readers also accept legacy "_PLAKAR_")
8     4     resource_type  u32   (1 = config, 4 = packfile, 3 = state)
12    4     resource_version u32 (Version)
16    N     payload
16+N  32    mac            = MACH( bytes[0 .. 16+N) )
```

The MAC covers the 16-byte header and the payload.

Which `MACH` is used depends on the region:

| Region | Hash algorithm | Key |
|---|---|---|
| CONFIG | **BLAKE3 always**, regardless of `Hashing.Algorithm` | Encrypted: the derived key K (§8). Plaintext: **unkeyed** BLAKE3. |
| PACKFILE, STATE | `config.Hashing.Algorithm` | Encrypted: K. Plaintext: `H(RepositoryID)`, where H is the configured hash, unkeyed, over the 16 raw UUID bytes. |

The keyed constructions are:

- `BLAKE3` keyed: `blake3.NewKeyed(key)`, which requires a 32-byte key, with a 32-byte output.
- `SHA256` keyed: `HMAC-SHA256(key, ·)`.

Current versions:

| Resource | Version |
|---|---|
| config | 1.0.0 |
| packfile | 1.0.0 |
| state | 1.1.0 (1.0.0 still readable, §9.5) |

Readers MUST:

- reject a resource whose `resource_type` does not match the region;
- reject a resource whose version is newer than they support;
- verify the trailing MAC whenever the whole payload is streamed.

Ranged blob reads inside the packfile cannot verify the wrapper MAC; see §11.

**Offsets inside the packfile** (§7) are relative to the **start of the payload**, i.e. `packfile_offset + 16`.

---

## 5. Encoding pipeline

Every *encoded* byte string goes through the same pipeline:

```
encode(x)  = encrypt( compress(x) )      // each step skipped if its config is nil
decode(y)  = decompress( decrypt(y) )
```

The following pieces are encoded **individually**, each as its own stream:

- every blob in the packfile;
- the packfile index, encoded as one stream;
- the packfile footer, encoded as one stream;
- the state stream, encoded as one stream.

The configuration is **not** encoded. It is plaintext msgpack inside its wrapper.

### 5.1 Compression

`config.Compression` is either nil (no compression) or one of the following:

| Algorithm | Stream format |
|---|---|
| `LZ4` (default) | LZ4 **frame** format, `pierrec/lz4/v4` writer defaults |
| `ZSTD` | Zstandard frame, `klauspost/compress/zstd` defaults |
| `GZIP` | RFC 1952 gzip, Go `compress/gzip` defaults |

The `Level`, `WindowSize`, `ChunkSize`, `BlockSize` and `EnableCRC` fields are stored in the config but **not applied**. The writers always use library defaults. Readers MUST NOT depend on these fields.

### 5.2 Encryption

`config.Encryption` is either nil (plaintext) or holds:

- `SubKeyAlgorithm`: `AES256-KW` (default) or `AES256-GCM`;
- `DataAlgorithm`: MUST be `AES256-GCM-SIV`;
- `ChunkSize`: default 65536.

An encrypted stream is laid out as follows:

```
[ wrapped subkey ]   AES256-KW : 40 bytes = RFC 3394 AES key wrap of a fresh random 32-byte subkey under K
                     AES256-GCM: 12-byte nonce || AES-256-GCM(K, nonce, subkey) (32+16 bytes) = 60 bytes
[ frame 0 ] [ frame 1 ] ... [ frame n-1 ]
```

Each frame is:

```
frame = nonce(12) || AES-256-GCM-SIV_subkey(nonce, plaintext_i, aad = ∅) || tag(16)
```

- A fresh subkey is generated for **every** encoded stream: every blob, every index, every footer and the state.
- The plaintext is cut into consecutive `ChunkSize`-byte pieces. Every frame except the last carries exactly `ChunkSize` plaintext bytes, i.e. `ChunkSize + 28` bytes on disk. The last frame carries 1..`ChunkSize` bytes.
- An empty plaintext produces the wrapped subkey and no frames.
- Nonces are random, 96-bit. No frame counter, final-frame flag or AAD binds the frames together; see §11.
- Readers MUST split the ciphertext on the fixed frame size, `ChunkSize + 28`. They MUST NOT rely on read boundaries.

Encoded size of an `n`-byte plaintext `p` (after compression), using AES-KW:

```
40 + n + 28 * ceil(n / ChunkSize)
```

---

## 6. Configuration region

The payload is the msgpack map of `storage.Configuration`. It has **no msgpack tags**, so its keys are Go field names:

```
{
  "Timestamp":    time.Time,
  "RepositoryID": bin(16),                       // UUIDv4
  "Packfile":     { "MinSize": u64, "AvgSize": u64, "MaxSize": u64 },
  "Chunking":     { "Algorithm": str, "MinSize": u32, "NormalSize": u32, "MaxSize": u32 },
  "Hashing":      { "Algorithm": "BLAKE3"|"SHA256", "Bits": 256 },
  "Compression":  nil | { "Algorithm": str, "Level": int, "WindowSize": int,
                          "ChunkSize": int, "BlockSize": int, "EnableCRC": bool },
  "Encryption":   nil | {
      "SubKeyAlgorithm": "AES256-KW", "DataAlgorithm": "AES256-GCM-SIV", "ChunkSize": 65536,
      "KDFParams": { "KDF": "ARGON2ID"|"SCRYPT"|"PBKDF2", "Salt": bin(16),
                     "argon2id"?: { "SaltSize","Time","Memory","Threads","KeyLen" },
                     "scrypt"?:   { "SaltSize","N","R","P","KeyLen" },
                     "pbkdf2"?:   { "SaltSize","Iterations","KeyLen","Hashing" } },
      "Canary": bin
  }
}
```

The config `Version` is not stored in the map. It is taken from the wrapper header.

Values written by `plakar ptar`:

| Field | Value |
|---|---|
| `Hashing` | `BLAKE3/256` by default; `-hashing SHA256` selects SHA-256 |
| `Chunking` | `fastcdc-v1.0.0`, min 512 KiB, normal 1 MiB, max 8 MiB |
| `Compression` | `LZ4`, or nil with `-no-compression` |
| `Encryption` | Argon2id, t=4, m=262144 KiB, p=1, keylen 32, 16-byte salt. nil with `-plaintext`. |
| `Packfile.MaxSize` | `2^64-1`, which forces a single packfile |

---

## 7. Packfile region

The payload of the PACKFILE wrapper is laid out as follows:

```
payload offset
0                 encode(blob_0)
…                 encode(blob_1) … encode(blob_{n-1})          (back to back, no padding)
index_offset      encode(INDEX)                                (one stream)
…                 encode(FOOTER)                               (one stream, F bytes)
end-4             u32   F  = encoded footer length
```

### 7.1 Index (plaintext)

The index is `count` records of 56 bytes each, with no header:

| Off | Type | Field |
|---|---|---|
| 0 | u32 | `type` (resource type, §10.1) |
| 4 | u32 | `version` (blob format Version) |
| 8 | MAC | `mac` (blob MAC) |
| 40 | u64 | `offset` (payload-relative offset of the **encoded** blob) |
| 48 | u32 | `length` (length of the encoded blob) |
| 52 | u32 | `flags` (always 0) |

Record order is whatever order the writer's packing cache yields. Readers MUST NOT assume any sort order.

### 7.2 Footer (plaintext, 56 bytes)

| Off | Type | Field |
|---|---|---|
| 0 | i64 | `timestamp` (**0 in PTAR**: the PTAR writer never sets it) |
| 8 | u32 | `count` (number of index records) |
| 12 | u64 | `index_offset` (payload offset where the encoded index begins) |
| 20 | MAC | `index_mac` = `MACH_data(plaintext index bytes)` |
| 52 | u32 | `flags` (0) |

`MACH_data` is the PACKFILE/STATE hasher from §4.

### 7.3 Reading

There are two ways to read the packfile:

- **Whole packfile:**
  1. Strip the wrapper and verify its MAC.
  2. Read `F` from the last 4 bytes.
  3. Decode the `F` bytes before it to get the footer.
  4. Decode `[index_offset, end-4-F)` to get the index.
  5. Check `index_mac`.
- **Single blob:** read the encoded bytes at `offset`/`length`, taken from the state (§9) or the index, then decode them. Each blob's subkey is self-contained, so no other context is needed.

### 7.4 Packfile identity

A regular repository names a packfile by the MAC of its bytes. PTAR streams the packfile while writing it, so it names the packfile with a **random** 32-byte MAC instead. This MAC appears only inside state records (§9).

---

## 8. Key derivation and passphrase check

```
K = KDF(passphrase, KDFParams)        // Argon2id / scrypt / PBKDF2 per config, 32 bytes
```

`Canary` = `encrypt_stream(K, 32 random bytes)`, as in §5.2.

A passphrase is correct if and only if the canary decrypts without error.

The config can only be authenticated once K is known. Readers therefore proceed in this order:

1. Parse the config payload **without** verifying the MAC (strip 16 + 32 bytes).
2. If `Encryption` is not nil:
   1. Derive K.
   2. Verify the canary.
   3. Verify the CONFIG wrapper MAC with keyed BLAKE3(K).
3. If `Encryption` is nil, verify the CONFIG wrapper MAC with unkeyed BLAKE3.

---

## 9. State region

The STATE wrapper payload is `encode(state_stream)`. Version 1.1.0 is laid out as follows:

```
[32]  parent MAC (all-zero for a fresh PTAR)
repeat:
  [1]  entry_type
  [4]  u32 L
  [L]  payload
terminator:
  [1]  0x01  (ET_METADATA — no length field)
  [4]  u32  metadata version (0x00010100)
  [8]  u64  timestamp, UnixNano
  [16] serial (UUID, raw)
```

Writers emit records grouped by type in this order: DELETE (6), LOCATIONS (2), COLOURED (3), PACKFILE (4), CONFIGURATION (5).

Readers MUST:

- skip unknown types using `L`;
- reject a fixed-size type whose `L` differs from the constant in the tables below.

### 9.1 LOCATIONS (2), L = 85: one per blob

| Off | Type | Field |
|---|---|---|
| 0 | u8 | blob type (resource type truncated to 8 bits) |
| 1 | u32 | blob version |
| 5 | MAC | blob MAC |
| 37 | MAC | packfile MAC (the random MAC of §7.4) |
| 69 | u64 | offset (payload-relative, as in §7.1) |
| 77 | u32 | length |
| 81 | u32 | flags (0) |

### 9.2 PACKFILE (4), L = 72

| Off | Type | Field |
|---|---|---|
| 0 | MAC | packfile MAC |
| 32 | MAC | state ID |
| 64 | u64 | timestamp, UnixNano |

### 9.3 COLOURED (3), L = 41, and DELETE (6), L = 66

These are tombstone records. A freshly built PTAR has none of them, but readers MUST accept them.

- COLOURED (3), 41 bytes: `type u8 | MAC[32] | when u64 UnixNano`.
- DELETE (6), 66 bytes: `deleted_entry_type u8 | blob_type u8 | blob MAC[32] | packfile MAC[32]`.

### 9.4 CONFIGURATION (5), variable length

`keyLen u8 | key | valueLen u16 | value | createdAt u64 UnixNano`, so `L = 11 + keyLen + valueLen`.

These records are not produced by current writers.

### 9.5 State 1.0.0

Version 1.0.0 has the same layout without the leading 32-byte parent MAC and without DELETE records. The reader picks the parser from the wrapper's `resource_version`.

---

## 10. Logical content (blobs)

### 10.1 Resource types

| Value | Name | Blob encoding | Blob MAC |
|---|---|---|---|
| 5 | SNAPSHOT | msgpack `Header` | `Header.Identifier`, a **random** value |
| 6 | SIGNATURE | raw ed25519 signature over `MAC(msgpack(Header))` | `Header.Identifier` |
| 7 | OBJECT | msgpack `Object` | `MAC(serialized)` |
| 9 | CHUNK | raw file bytes | `MAC(bytes)` |
| 10 / 11 | VFS_BTREE / VFS_NODE | msgpack btree root / node | `MAC(serialized)` |
| 12 | VFS_ENTRY | msgpack `Entry` | `MAC(serialized)` |
| 13 / 14 / 15 | ERROR_BTREE / NODE / ENTRY | msgpack | `MAC(serialized)` |
| 16 / 17 / 18 | XATTR_BTREE / NODE / ENTRY | msgpack | `MAC(serialized)` |
| 19 / 20 | BTREE_ROOT / BTREE_NODE | msgpack (generic indexes) | `MAC(serialized)` |
| 21 | VFS_SUMMARY | msgpack `Summary` | `MAC(serialized)` |

Value 8 is reserved. Types MUST be ≤ 255 because state records store them in one byte.

All blob versions are 1.0.0, with the exception noted in §12. `MAC(x)` is `MACH_data(x)` over the plaintext before encoding.

A PTAR MAY hold several snapshots. One snapshot is created per backup target, plus one per snapshot synced from `-k` sources. To enumerate them, take every LOCATIONS record whose type is 5.

### 10.2 Reference graph

```
SNAPSHOT Header
 ├─ sources[i].root.root    → VFS_BTREE   → VFS_NODE*    key=path        value=MAC → VFS_ENTRY
 ├─ sources[i].root.errors  → ERROR_BTREE → ERROR_NODE*  key=path        value=MAC → ERROR_ENTRY
 ├─ sources[i].root.xattrs  → XATTR_BTREE → XATTR_NODE*  key=path+name+sep value=MAC → XATTR_ENTRY → OBJECT
 ├─ sources[i].indexes[]    → BTREE_ROOT  → BTREE_NODE*
 │     "content-type"  key="/<type/subtype><path>"  value = VFS_ENTRY MAC
 │     "dirpack"       key=dir path         value = OBJECT MAC (dirpack stream)
 │     "summary"       key=dir path         value = VFS_SUMMARY MAC
 └─ sources[i].summary      (inline vfs.Summary)
VFS_ENTRY.object → OBJECT → chunks[].contentMAC → CHUNK
```

Notes on the graph:

- **VFS keys are absolute paths on the source host.** Every ancestor of the backup root, up to and including `/`, is present as a synthetic directory entry (mode `ModeDir|0750`, mtime Unix epoch). A backup of `/a/b/c` therefore contains entries for `/`, `/a` and `/a/b`.
- **The content-type key** is `"/"`, then the entry's `content_type` truncated at the first `;` (parameters dropped: `text/plain`, not `text/plain; charset=utf-8`), then the absolute path. Every non-directory entry is indexed, symlinks included.

### 10.3 Snapshot header (msgpack keys)

- **Header:** `version`, `identifier` MAC, `timestamp`, `duration` (i64 ns), `identity {identifier uuid, public_key bin}`, `name`, `category`, `environment`, `perimeter`, `job`, `replicas` u32, `dataset`, `data_classes []str`, `classifications [{analyzer, classes:[{name, probability}]}]`, `tags []str`, `context [{key,value}]`, `sources []Source`.
- **Source:** `importer {type, origin, directory}`, `context [{key,value}]`, `root {root, xattrs, errors}` (all MACs), `indexes [{name, type, value MAC}]`, `summary`.

### 10.4 Btrees

- **Node:** `{version, keys [], pointers [MAC], values [], prev?, next?}`.
  - A leaf has no pointers, and `len(values) = len(keys)`.
  - An internal node has `len(pointers) = len(keys) + 1` and no values.
  - Leaves are chained through `next`.
  - Lookup: `pointers[i]` covers keys `< keys[i]`, and keys equal to `keys[i]` go right.
- **Root:** a map with **Go-name keys** `{Version, Order, Count, Root}`. Only `Root` (a node MAC) and `Order` (50) are meaningful.
- **Key ordering:**
  - The VFS tree compares by number of `/` first, then bytewise.
  - All other trees compare bytewise.

### 10.5 Entries, objects, chunks

- **VFS_ENTRY:** `version`, `parent_path`, `file_info {name, size, mode, mod_time, dev, ino, uid, gid, nlink, username, groupname, flags}`, `summary`, `symlink_target?`, `object?` MAC, `content_type?`, `entropy?`, `chunks?`, `alternate_data_streams?`, `security_descriptor?`, `file_attributes?`, `extended_attributes?`, `classifications?`, `custom_metadata?`, `tags?`.
- **OBJECT:** `{version, contentMAC, chunks [{version, contentMAC, length u32, entropy, flags}], content_type?, entropy?, flags}`.
  - `contentMAC` is the MAC of the whole file.
  - File content = the CHUNK blobs concatenated in order.
  - Size = Σ `length`.
- **ERROR_ENTRY:** `{version, name, error}`.
- **XATTR_ENTRY:** `{version, path, name, size, type (0 xattr, 1 ADS, 2 custom), object?}`.
  - The xattr tree key is `path + name + sep`, where `sep` is `:`, `@`, `%` or `#`.
- **Dirpack stream** (contents of the dirpack OBJECT): repeated records `type u8 (1 dir, 2 file) | u32 len | entry bytes | MAC(entry bytes)[32]`, where `len = len(entry) + 32`.

---

## 11. Security considerations

- **The trailer is unauthenticated.**
  - Offsets come from 48 unprotected bytes.
  - Readers MUST bounds-check them against the file size and against each other before seeking or allocating.
  - The reference reader checks only `config_length`.
- **Plaintext PTARs have integrity, not authenticity.**
  - The config is protected by unkeyed BLAKE3.
  - Data MACs are keyed with `H(RepositoryID)`, and `RepositoryID` is stored in clear.
  - Anyone can forge a consistent plaintext archive. Signatures (type 6) are the only authenticity mechanism.
- **Frames inside an encoded stream are not bound together** (§5.2). Within one blob, an attacker who can edit ciphertext can drop, reorder or truncate whole 64 KiB frames without breaking any frame's AEAD tag.
  - On a full read, the wrapper MAC catches this.
  - Ranged blob reads skip the wrapper MAC, so they rely on the consumer re-checking the decoded blob against its content MAC.
  - Implementations SHOULD perform that check. Whether every kloset read path does was not verified for this document.
- **Compression happens before encryption,** so encoded lengths leak compressibility. Readers add random padding to range reads (padmé), but on-disk lengths are exact.
- **Argon2id at 256 MiB** applies to every open of an encrypted PTAR. Constrained readers need that much memory.

---

## 12. Implementation quirks (non-normative, observed in v1.1.8)

1. `container_version` is written but never checked on open.
2. In PTAR the packfile footer `timestamp` is 0. Regular in-memory packfiles set it.
3. Persisted btree nodes and roots carry `Version = 0`, not 1.0.0, and roots have `Count = 0`.
4. Btree roots use untagged Go field names as msgpack keys, unlike every other blob.
5. The xattr tree is built with the VFS path comparator but searched with a bytewise comparator. Lookups can miss on paths with differing depth.
6. Compression level and the other tuning fields in the config are ignored (§5.1).
7. The config wrapper is always BLAKE3, even when the repository uses SHA-256.

---

## 13. Validation

This spec was checked against real archives with an independent decoder. The decoder was written from this document alone. It imports no PlakarKorp package, only generic primitives: BLAKE3, Tink's RFC 8452 AES-GCM-SIV, LZ4, msgpack, Argon2. It implements RFC 3394 unwrap by hand.

### 13.1 Test archives

Four archives were produced by `plakar` v1.1.7, which embeds kloset v1.1.8 and ptar v1.1.1:

| Archive | Encryption | Hash | Compression |
|---|---|---|---|
| A | Argon2id | BLAKE3 | LZ4 |
| B | plaintext | BLAKE3 | LZ4 |
| C | plaintext | SHA-256 | none |
| D | Argon2id | SHA-256 | LZ4 |

Test tree:

- an empty file and an 11-byte file carrying a user xattr;
- a 10 MiB random file (9 chunks);
- a 3 MB compressible file;
- a symlink;
- nested directories.

### 13.2 Results

**615 / 615 checks pass on all four archives.** The verified claims cover:

- **Container (§3):**
  - magic and version bytes;
  - trailer arithmetic;
  - contiguous regions.
- **Wrapper (§4):**
  - magic, type and version per region;
  - every wrapper MAC. The CONFIG MAC is BLAKE3 even with SHA-256; the PACKFILE/STATE MACs use the configured hash keyed with K or `H(RepositoryID)`.
- **Encoding (§5):**
  - KW-wrapped subkey;
  - frame layout `nonce ‖ ct ‖ tag`;
  - encoded-size formula, over every stream;
  - LZ4 frame magic on every compressed stream.
  - Cross-checked by decrypting with Python `cryptography` (`aes_key_unwrap` and `AESGCMSIV`), which shares no code with Tink.
- **Config (§6):**
  - every key name and default listed;
  - Argon2id parameters;
  - canary accepts the right passphrase and rejects a wrong one.
- **Packfile (§7):**
  - footer fields, including `timestamp = 0`;
  - index size and `index_mac`;
  - blobs back to back up to `index_offset`.
- **State (§9):**
  - parent MAC, record framing and fixed lengths;
  - group order;
  - terminator ends the stream exactly;
  - LOCATIONS equal the packfile index one-to-one;
  - the packfile MAC is random.
- **Logical layer (§10):**
  - every non-snapshot blob MAC equals `MAC(plaintext)`;
  - all blob versions are 1.0.0;
  - every header, source, entry, file_info, object, chunk and xattr key set;
  - btree root keys, versions 0 and Order 50;
  - node shape, leaf chaining and key order (path comparator for VFS and xattr, bytewise for the rest);
  - Go `FileMode` dir and symlink bits;
  - byte-exact reconstruction of every file;
  - `object.contentMAC`;
  - xattr keys and values;
  - dirpack framing;
  - summary and content-type indexes.

Validation found **one error in the first draft**: the content-type index key drops media-type parameters (§10.2). It is now fixed.

### 13.3 Not exercised

These parts are described from code reading only:

- ZSTD and GZIP compression. `plakar ptar` cannot select them.
- The `AES256-GCM` subkey wrapping.
- scrypt and PBKDF2.
- Signatures (type 6), error entries, and COLOURED, DELETE and CONFIGURATION state records.
- State 1.0.0.
- Archives with several snapshots (`-k`).
- `ptar+http(s)` access.
- The xattr comparator mismatch on the read side (§12 item 5).

---

## 14. Reader algorithm (summary)

1. Read 8 bytes and check `_PLATAR_`. Read the `u32` version.
2. Seek to `EOF - 48` and read the trailer. Bounds-check it.
3. Read CONFIG. Parse the msgpack payload, skipping 16 bytes at the start and 32 at the end.
4. If encrypted: derive K, verify the canary, then verify the CONFIG MAC. Otherwise verify it with unkeyed BLAKE3.
5. Build `MACH_data` from `Hashing.Algorithm`, keyed with K or `H(RepositoryID)`.
6. Read STATE: unwrap and verify the MAC, `decode`, then parse §9. Build a map from `(type, MAC)` to `(offset, length)`.
7. For each SNAPSHOT location:
   1. Fetch `packfile_offset + 16 + offset`, `length` bytes, then `decode` and parse the msgpack header.
   2. Walk the VFS btree.
   3. Resolve entries to objects and objects to chunks.
