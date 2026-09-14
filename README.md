# PebbleDB

A small educational persistent key-value database written in Rust.

PebbleDB exists to demonstrate how a storage engine works internally. It implements an LSM-tree style architecture with a WAL, memtable, SSTables, bloom filters, and concurrent access via RwLock.

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
                  | Memtable |  <-- RwLock for concurrent access
                  +----+----+
                       | flush
                       v
              +-----------------+
              |    SSTable v2   |
              +-----------------+
              | Block Index     |  <-- sparse index: first_key -> block offset
              | Bloom Filter    |  <-- probabilistic rejection of absent keys
              | sorted blocks   |  <-- fixed-size blocks for efficient reads
              +-----------------+
```

- **CLI** parses commands and calls the database layer.
- **DB** manages the in-memory memtable behind an `RwLock<DbInner>` for thread-safe concurrent reads/writes. Tracks performance counters (memtable hits, bloom rejects, block reads, compactions).
- **WAL** provides append-only logging with CRC32 checksums for crash recovery.
- **SSTables** are immutable block-based on-disk files with a sparse index and bloom filter for fast lookups.
- **Bloom Filter** per SSTable rejects lookups for keys that definitely don't exist, avoiding unnecessary disk reads.

## Write Path

1. Acquire write lock on the memtable.
2. Write the operation to the WAL and fsync.
3. Apply the operation to the in-memory memtable.
4. If the memtable reaches the flush threshold, automatically flush it to an SSTable.

## Read Path

```text
GET key
  |
  v
Memtable  -->  found?  -->  return value (or None if tombstone)
  |
  v (not found)
For each SSTable (newest first):
  Bloom filter  -->  might_contain?  -->  skip if definitely absent
  Block index   -->  find target block
  Binary search block  -->  found?  -->  return value (or None if tombstone)
  |
  v (not found)
None
```

The most recent write always wins. If the newest matching record is a tombstone, the key is considered deleted. Bloom filters skip SSTables that definitely don't contain the key.

## Flush Process

When the memtable reaches a configurable threshold (default 1024 entries):

1. Snapshot the memtable contents, sorted by key.
2. Build bloom filter from all keys.
3. Partition records into fixed-size blocks (default 4096 bytes).
4. Build sparse index (first key per block).
5. Write the SSTable to a temporary file.
6. Atomic rename to the final SSTable filename.
7. Truncate the WAL (data is now durable on disk).
8. Clear the memtable.

Crash safety: if a crash occurs during flush, orphaned temp files are cleaned up on startup. The WAL replay reconstructs any data that was not yet persisted to an SSTable.

## SSTable Format (v2)

```text
[Header: 20 bytes]
  magic:        u32  (0x50454242 = "PEBB")
  version:      u32  (2)
  record_count: u32
  block_count:  u32
  block_size:   u32  (max bytes per block)

[Blocks: variable size]
  For each block:
    For each record:
      op:        u8   (1=SET, 2=DELETE)
      key_len:   u32
      value_len: u32
      key:       [u8; key_len]
      value:     [u8; value_len]

[Sparse Index: variable]
  For each block:
    key_len:     u32
    first_key:   [u8; key_len]
    block_offset: u32  (byte offset from file start)
    block_size:   u32

[Bloom Filter: variable]
  filter_len:  u32
  filter_data: [u8; filter_len]

[Footer: 20 bytes]
  index_offset: u32
  index_size:   u32
  bloom_offset: u32
  bloom_size:   u32
  checksum:     u32  (CRC32 of everything before this field)
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

## Concurrency

PebbleDB uses `Arc<RwLock<DbInner>>` for thread-safe access:

- **Reads** (`get`, `keys`, `scan`, `exists`, `stats`) acquire a read lock, allowing concurrent readers.
- **Writes** (`set`, `delete`, `flush`, `compact`) acquire a write lock, ensuring exclusive access.
- **Stats counters** use `AtomicU64` for lock-free tracking during reads.

Multiple threads can safely read from the same PebbleDB instance simultaneously.

## Crash Recovery

On startup:

1. Clean up orphaned temp files (`.sst.tmp`).
2. Discover and load all SSTables by filename, sorted by ID.
3. Parse block indexes and bloom filters.
4. Replay the WAL into the memtable.

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
  bytes:         10
SSTables
  count:         0
  total size:    0 bytes
WAL
  size:          27 bytes
Performance Counters
  memtable hits: 1
  bloom rejects: 0
  block reads:   0
  compactions:   0
$ pebbledb compact
OK
```

## Building and Testing

```text
cargo build
cargo test
```

45 unit tests covering:
- WAL round-trip, incomplete records, checksum corruption
- Bloom filter insert, lookup, false positives, encode/decode
- SSTable v2 block-based format, sparse index, bloom filter, checksum validation
- Memtable flushing and SSTable creation
- Read path: memtable override, SSTable lookup, bloom rejection, tombstone behavior
- Compaction: merge, deduplication, tombstone cleanup
- Crash recovery: WAL replay after flush, temp file cleanup
- Concurrent set/get from multiple threads
- Scan ordering, stats with counters

## Benchmark

```text
cargo run --release --example bench
```

Benchmarks three scenarios:
- **Memtable only** (no flush): raw in-memory throughput.
- **SSTable-backed** (threshold=500): includes flush overhead and SSTable reads.
- **Concurrent** (4 threads): thread-safe parallel reads/writes.

## Limitations

- No transactions, replication, or networking.
- Fixed database directory (`.pebbledb/` in the current working directory).
- Sequential scan only; no range queries.
- Compaction is a simple full merge; no leveled or tiered strategy.
- Bloom filter false positive rate ~1% (10 bits/key, 7 hash functions).
