# Kloset - Immutable Data Store Engine

[![Go Report Card](https://goreportcard.com/badge/github.com/PlakarKorp/kloset)](https://goreportcard.com/report/github.com/PlakarKorp/kloset)
[![codecov](https://codecov.io/gh/PlakarKorp/kloset/branch/main/graph/badge.svg)](https://codecov.io/gh/PlakarKorp/kloset)

## What is Kloset?

Kloset is an open-source, immutable data store engine designed to package data along with its context, structure, metadata, and integrity. It is the storage engine powering [Plakar](https://github.com/PlakarKorp/plakar).

Rather than treating backups as opaque archives, Kloset represents all data internally as a virtual filesystem. This abstraction fits any data source — filesystems, databases, object stores, APIs — and supports efficient storage, indexing, and recovery without relying on external state or centralized coordination.

What Kloset provides:

- **Immutable storage**: data is split into deduplicated, compressed, and encrypted chunks stored by content hash. Once written, it is never modified;
- **Self-describing snapshots**: every snapshot includes all the metadata needed to understand the structure and context of the backup, without external dependencies;
- **Granular restore**: snapshots can be browsed and partially restored without unpacking the full dataset;
- **Pluggable connectors**: sources, targets, and storage backends are modular — Kloset does not care what it is backing up, only that the data can be listed and read;
- **Cryptographic auditability**: each snapshot carries a digest tree for data and metadata, making it independently verifiable and tamper-evident;
- **Built-in encryption**: all data is encrypted at the source before leaving the client, covering both data and metadata.

For a deeper look at the architecture, read the [Kloset blog post](https://www.plakar.io/posts/2025-04-29/kloset-the-immutable-data-store/).

## Architecture

Kloset is organized into three main layers:

**Storage layer.** Handles writing and retrieving streams of raw, opaque bytes. All data reaching this layer is already compressed and encrypted. The storage layer is fully parallelized, backpressure-aware, and built on a pluggable connector model supporting filesystems, SFTP, S3-compatible object stores, and more.

**Repository layer.** Sits above storage and acts as a local encoding and decoding proxy. During backup, it compresses and encrypts data before passing it to storage. During restore, it decrypts and decompresses before exposing data to higher layers. It also maintains a local content index to avoid redundant writes.

**Snapshot layer.** Organizes raw data into coherent, structured groups representing a backup at a specific point in time. Each snapshot captures a complete view of a dataset including type, tree hierarchy, metadata, content hashes, and logical relationships. Snapshots enable incremental backups, change tracking, and historical inspection.

## Contributing and reporting issues

Please read the [contributing guidelines](./CONTRIBUTING.md) and [code of conduct](./CODE_OF_CONDUCT.md) before opening a pull request.

If you find a bug or want to request a change, please open an issue:

https://github.com/PlakarKorp/kloset/issues

## Community

- [Discord](https://discord.gg/A2yvjS6r2C)
- [Reddit](https://www.reddit.com/r/plakar/)
- [YouTube](https://www.youtube.com/@PlakarKorp)

## Changelog

See [CHANGELOG.md](./CHANGELOG.md) for release notes and version history.
