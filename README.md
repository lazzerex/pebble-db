# PebbleDB

A small educational persistent key-value database written in Rust.

PebbleDB exists to demonstrate how a storage engine works internally. It implements an LSM-tree style architecture with a WAL, memtable, and SSTables.

## Architecture

```text
                  SET / DELETE
                       |
                       v
                  +---------+
                  |   WAL   |
                  +----+----+
                       |
                       v
                  +---------+
                  | Memtable |
                  +----+----+
                       | flush
                       v
              +-----------------+
              |    SSTable      |
              +-----------------+
              | sorted records  |
              +-----------------+
```

- **CLI** parses commands and calls the database layer.
- **DB** manages the in-memory memtable, coordinates writes, and orchestrates flushes.
- **WAL** provides append-only logging with CRC32 checksums for crash recovery.
- **SSTables** are immutable on-disk sorted files that persist flushed memtable data.

## Write Path

1. Write the operation to the WAL and fsync.
2. Apply the operation to the in-memory memtable.
3. If the memtable reaches the flush threshold, automatically flush it to an SSTable.

## Read Path

```text
GET key
  |
  v
Memtable  -->  found?  -->  return value (or None if tombstone)
  |
  v (not found)
Newest SSTable  -->  found?  -->  return value (or None if tombstone)
  |
  v (not found)
Older SSTable  -->  found?  -->  return value (or None if tombstone)
  |
  v (not found)
None
```

The most recent write always wins. If the newest matching record is a tombstone, the key is considered deleted.

## Flush Process

When the memtable reaches a configurable threshold (default 1024 entries):

1. Snapshot the memtable contents, sorted by key.
2. Write the SSTable to a temporary file.
3. Atomic rename to the final SSTable filename.
4. Truncate the WAL (data is now durable on disk).
5. Clear the memtable.

Crash safety: if a crash occurs during flush, orphaned temp files are cleaned up on startup. The WAL replay reconstructs any data that was not yet persisted to an SSTable.

## SSTable Format

```text
[Header: 12 bytes]
  magic:      u32  (0x50454242 = "PEBB")
  version:    u32  (1)
  record_count: u32

[Records: sorted by key]
  For each record:
    op:        u8   (1=SET, 2=DELETE)
    key_len:   u32
    value_len: u32
    key:       [u8; key_len]
    value:     [u8; value_len]

[Footer: 4 bytes]
  checksum:   u32  (CRC32 of header + records)
```

SSTable files are named `sstable_NNNNNN.sst` with monotonically increasing IDs.

## Compaction

Manual compaction merges all SSTables into a single new SSTable:

1. Read all SSTable entries.
2. For duplicate keys, the newest record (highest SSTable ID) wins.
3. Tombstones are dropped (all SSTables are merged, so nothing remains to shadow).
4. Write the merged SSTable.
5. Delete old SSTable files.

Tombstones are safe to remove during full compaction because there are no older SSTables left to expose.

## Crash Recovery

On startup:

1. Clean up orphaned temp files (`.sst.tmp`).
2. Discover and load all SSTables by filename, sorted by ID.
3. Replay the WAL into the memtable.

The database is consistent after replay because:
- SSTables contain previously flushed data.
- The WAL contains any operations that were not yet flushed.
- Duplicate entries (in both SSTable and WAL) are harmless; the memtable takes precedence on reads.

## Durability

Each `set` and `delete` call writes to the WAL and calls `fsync` before returning success. SSTables are written to a temp file and atomically renamed with fsync.

## Commands

```text
pebbledb set <key> <value>
pebbledb get <key>
pebbledb delete <key>
pebbledb exists <key>
pebbledb keys
pebbledb scan
pebbledb compact
pebbledb stats
```

Examples:

```text
$ pebbledb set name lazzerex
OK
$ pebbledb get name
lazzerex
$ pebbledb scan
name: lazzerex
$ pebbledb stats
Memtable
  entries:       1
SSTables
  count:         0
  total size:    0 bytes
WAL
  size:          27 bytes
$ pebbledb compact
OK
```

## Building and Testing

```text
cargo build
cargo test
```

33 unit tests covering:
- WAL round-trip, incomplete records, checksum corruption
- SSTable write/read, checksum validation, empty files
- Memtable flushing and SSTable creation
- Read path: memtable override, SSTable lookup, tombstone behavior
- Compaction: merge, deduplication, tombstone cleanup
- Crash recovery: WAL replay after flush, temp file cleanup
- Scan ordering, stats

## Benchmark

```text
cargo run --release --example bench
```

Compares memtable-only (no flush) against SSTable-backed operations.

## Limitations

- Single-process access only. No locking or concurrency.
- No bloom filters, block indexes, or block-based SSTables.
- No transactions, replication, or networking.
- Fixed database directory (`.pebbledb/` in the current working directory).
- Sequential scan only; no range queries.
- Compaction is a simple full merge; no leveled or tiered strategy.
