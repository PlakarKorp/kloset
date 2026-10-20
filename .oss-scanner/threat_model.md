# Threat model: kloset

## What this project does and where untrusted input enters

kloset is a Go library, the storage engine under plakar (https://github.com/PlakarKorp/plakar, enrolled on
its own). It takes a stream of records from an importer, splits content with content-defined chunking,
deduplicates the chunks, compresses and encrypts them, packs them into immutable packfiles and writes those,
with the states that index them, to a storage backend through the `connectors/storage.Store` interface.
Every stored object is addressed by a keyed MAC. A snapshot is a point-in-time view of a source, with its
header, its virtual filesystem (`snapshot/vfs`, indexed by on-disk B-trees) and its data. The library
loads snapshots back to browse, check, synchronize between repositories, and export through an exporter.

The repository is designed to live on storage the user does not trust: a public cloud, a shared host, a
server run by someone else. Confidentiality and integrity rest on the passphrase and the keys derived from
it, not on the storage. The design is documented in `encryption/README.md` and was reviewed externally
(`docs/audit/2025-02-cryptography.md`): keyed BLAKE3 for MACs, AES-256-GCM-SIV for data, AES-KW for
subkeys, Argon2id as the KDF. Treat all of the following as attacker-controlled:

- **Every byte a `Store` returns**: the serialized repository configuration returned by `Open` (which
  carries the hashing, compression, encryption and KDF parameters and the encrypted subkeys, and is read
  before anything is authenticated), the list of MACs from `List`, and the content of each packfile, state,
  snapshot header and lock from `Get`, including partial reads through `Range`. The parsers are
  `connectors/storage` (configuration), `packfile/` (blob index and footer), `repository/state`,
  `snapshot/header`, `snapshot/vfs` (entries, directory packs, B-tree nodes in `btree/`), `objects/`, and
  the msgpack decoding they share.
- **Records from an importer**: paths, symlink targets, extended attributes, file sizes, timestamps and
  modes, and the order and shape of the stream (a parent arriving after its children, an unclean root,
  loops, millions of entries in one directory). `snapshot/backup.go`, `snapshot/builder.go` and
  `snapshot/vfs` turn them into a snapshot; what they record is what a later export will write.
- **The contents of a snapshot at export time**: `snapshot/export.go` hands entry paths, link targets,
  modes and xattrs taken from the snapshot to an exporter. Nothing in a snapshot may make an export write
  outside the exporter's root, follow a symlink the export just created, or touch something it did not
  create. The exporter is the caller's (plakar ships one for the filesystem), so the contract is shared:
  say which side should have refused the path.
- **The passphrase and KDF parameters**: `encryption.DeriveKey` runs the KDF with the parameters found in
  the configuration read from the store, so the cost of opening a repository is partly chosen by whoever
  wrote the configuration.
- **`.ptar` archives** (`docs/ptar.md`, written by `repository/packer/platar.go`, read through the ptar
  storage integration): a repository in one file, meant to be moved around, so one from an unknown source
  is untrusted input.

Configuration passed by the embedding application, the secret, and the `kcontext` are trusted.

## Components that matter most / least

Most important, in this order:

1. `encryption/`, `hashing/`: key derivation, the canary, subkey wrapping, the AEAD stream and its nonce
   handling, MAC computation. A mistake here breaks every repository.
2. `packfile/`, `repository/` (with `repository/state` and `repository/packer`), `resources/`,
   `versioning/`: what is read from the store and how it is checked before use, the content-addressing and
   deduplication logic, what a state says about which packfiles and blobs exist.
3. `snapshot/` with `snapshot/header`, `snapshot/vfs` and `btree/`: loading and walking a snapshot, path
   handling in entries and directory packs, export.
4. `snapshot/backup.go`, `snapshot/builder.go`, `chunking/`, `classifier/`, `exclude/`: what the engine
   records from a source.
5. `caching/` (pebble, sqlite, lru): local caches of repository state; `locate/`, `iterator/`,
   `throttle/`, `events/`, `logging/`, `iostat/`.

Out of scope: `docs/`, `testing/` (mocks and fixtures), test files, `.github/`. Dependencies have their own
processes (`cockroachdb/pebble`, `modernc.org/sqlite`, `vmihailenco/msgpack`, `zeebo/blake3`,
`x/crypto`); report a flaw in one when kloset's use of it makes it reachable from a hostile store or
source, and show the path. The storage, importer and exporter implementations live in
https://github.com/PlakarKorp/integrations and are not enrolled; a contract between kloset and a connector
that lets a hostile path through is in scope here, and should say which side you think owns the fix.

## How to exercise it

The image has only loopback networking. Every module is in the Go module cache (`go env GOMODCACHE`),
`GOPROXY=off` and `GOTOOLCHAIN=local` are set, and every package's tests are built, with and without
`-race`. There is no binary: everything is driven from Go tests.

- **Tests**: `go test ./...`, and `go test -race ./...`. `make test` adds `go vet`.
- **A repository in memory**: `testing.GenerateRepository` creates an encrypted repository over
  `testing.MockBackend`, and `testing.GenerateSnapshot` fills it from `testing.MockImporter` with files
  you describe (`NewMockFile`, `NewMockDir`, `WithGenerator` for a stream of arbitrary records).
  `GenerateRepositoryWithConfig` lets you change the configuration before it is written. Tests in
  `repository/`, `snapshot/` and `snapshot/vfs` show the pattern.
- **Tampering**: `MockBackend` keeps everything in memory, so a test can corrupt a packfile, a state or
  the configuration between a write and the read that follows, then call `snapshot.Load`, `Check`,
  `Export` or `repository.GetBlob` and see what is detected. The formats are in `packfile/README.md`,
  `encryption/README.md` and the comments in `repository/state/state.go` and `snapshot/header/header.go`.
- **A hostile source**: `WithGenerator` streams whatever records you want through `snapshot.Create` and
  `Builder.Backup`; the
  `unclean_root_test.go` and `export_symlink_test.go` tests are a starting point.
- **ptar**: `docs/ptar.md` is the specification; `repository/packer/platar.go` writes archives. Reading
  one needs the ptar storage integration, which plakar's enrolment ships.
- **The real thing**: plakar, in its own enrolment, builds this library into a command with on-disk
  repositories; a finding that is easier to show there can be reported here with the plakar command that
  reaches it.

## How you rate severity

A finding counts when a hostile store, a hostile source, or a hostile snapshot makes the engine do
something its caller did not ask for, through the documented API. Say which of the three the attacker
controls; a finding against the default configuration weighs more than one that needs a plaintext or
otherwise non-default repository.

- **Critical**: recovering content, metadata or key material from a repository without the passphrase,
  including through MAC or nonce misuse, key reuse across subkeys, or a side channel in how the canary
  and keys are checked. A tampered object (packfile, blob, state, header, vfs node) that is accepted as
  authentic and changes what a load, check or export returns. An export that writes outside its root or
  through a symlink it created. Code execution from any of the three sources.
- **High**: a hostile store or source that makes `Backup`, `Check`, `Load`, `Export` or `Synchronize`
  panic, hang, or use memory out of proportion with the input: a backup tool that dies on malformed
  input has lost the user's run, and a restore that cannot finish is the failure we exist to prevent.
  Deduplication or content-addressing confusion that makes one object stand in for another (a blob
  served under the wrong MAC, type or version, a state replayed to resurrect or hide a snapshot). Silent
  loss: a record the importer delivered that the snapshot does not contain and no error reports.
- **Medium**: the same classes when they need a plaintext repository or a non-default algorithm, or when
  the failure is confined to one call and reported cleanly but the state left behind is wrong. KDF
  parameters from a hostile configuration that cost far more than the defaults with no bound.
- **Low**: crashes in diagnostic or rarely used paths (`locate`, `iostat`, `events`) that the main paths
  handle correctly. Resource use merely proportional to a large input.

Bugs that are not vulnerabilities: performance, API ergonomics, a design choice you would have made
differently (see the audit for the ones already discussed), and format deviations with no security
consequence.

## How to report

- A reproducer as a Go test in this image, using `testing/` mocks where possible: what was written,
  what bytes were changed, what was called, what was observed and what should have happened. For
  resource exhaustion, the measured memory or time against the input size.
- A minimal patch against `main` with a regression test next to the existing ones. Go 1.26, `gofmt`,
  errors wrapped with `%w`, no `panic` outside `main` and `init`, every goroutine with an owner, no
  unbounded reads of input the engine did not produce. Keep refactoring out of it.
- One root cause once, listing each API entry point it is reachable from. If the fix belongs in a
  connector, say so.

## Anything to leave alone

- The repository configuration (algorithms, KDF parameters, salt, wrapped subkeys) is stored in clear by
  design; it must be readable before the passphrase is known. The number, size and timestamps of packfiles
  and states are visible to the storage by design.
- Algorithms are configurable in the code but frozen to the audited defaults; do not report that a
  weaker algorithm could be selected by an embedding application.
- Deduplication reveals that two chunks are equal within one repository, by design: this is what the
  keyed MAC and the per-repository key bound the exposure of. An attacker who can write to the repository
  and observe whether a blob was stored already learns equality; findings must go beyond that.
- A weak passphrase is the user's choice; strength checking is the caller's job.
- Deleting snapshots and packfiles through maintenance is the caller's decision; removing more than what
  was selected is a finding, the API is not.
- The audit document records trade-offs that were accepted after discussion; a finding that restates one
  of them needs a new argument.
