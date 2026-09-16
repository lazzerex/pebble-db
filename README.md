# PebbleDB

A small educational persistent key-value database written in Rust.

PebbleDB exists to demonstrate how a storage engine works internally. It implements an LSM-tree style architecture with a WAL, memtable, leveled SSTables, bloom filters, and concurrent access via RwLock.

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
                  +----------+
                  | Memtable |  <-- RwLock for concurrent access
                  +----+-----+
                       | flush
                       v
                  L0  [SSTable] [SSTable]   <-- newest level, files may overlap
                       | compaction
                       v
                  L1  [SSTable] [SSTable]   <-- files within a level never overlap
                       | compaction
                       v
                  L2  [SSTable]
                       |
                      ...
```

- **CLI** parses commands and calls the database layer.
- **DB** manages the in-memory memtable behind an `RwLock<DbInner>` for thread-safe concurrent reads/writes. Tracks performance counters (memtable hits, bloom rejects, block reads, compactions).
- **WAL** provides append-only logging with CRC32 checksums for crash recovery.
- **SSTables** are immutable block-based on-disk files with a sparse index and bloom filter for fast lookups. Each file belongs to a level (L0, L1, L2, ...) encoded in its filename.
- **Leveled Compaction** keeps files within each level sorted and non-overlapping. When L0 exceeds the compaction threshold its files are merged with overlapping L1 files into new L1 files. If the resulting L1 exceeds its size budget the oldest oversized file cascades into L2, and so on. Tombstones are dropped when they reach the deepest level that still holds the key.
- **Bloom Filter** per SSTable rejects lookups for keys that definitely don't exist, avoiding unnecessary disk reads.
- **Iterators** provide ordered, lazy access to a single source (memtable or SSTable) and a merge iterator combines them into one ascending view for range queries.

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
Level 0  [SSTable] ...  -->  newest first within level
Level 1  [SSTable] ...
Level 2  [SSTable] ...
  |
  v
None
```

The read path walks the memtable first, then SSTables level by level (L0 newest-first, then L1, L2, ...) and returns the first matching record. Within L0 files may overlap so every file is checked. From L1 onward files within each level are sorted and non-overlapping, so a single file per level is sufficient once the key range is known. If the newest matching record is a tombstone the key is considered deleted. Bloom filters still serve point `get` only: an ordered seek must find the next existing key, so the absence of a key does

## Range Reads

Range queries reuse the same ordering rule as point reads:

```text
RANGE start..end
      |
      v
  MergeIterator
      +-- MemtableIterator   <-- newest writes
      +-- SSTableIterator    <-- newest SSTable first
      +-- SSTableIterator
      ...
      |
      v
  RangeIterator  -->  ascending (key, value) pairs
```

Each source exposes an ordered cursor. On a seek the sparse index locates the block whose first key is the largest one `<= target`, and only that block is read, so a seek never scans the whole SSTable. The merge iterator keeps one cursor per source, always emits the smallest remaining key, prefers the newest source when the same key exists in several sources, and drops any key whose newest record is a tombstone.

Bloom filters still serve point `get` only: an ordered seek must find the next existing key, so the absence of a key does not end the scan.

## Iteration and Range Queries

`PebbleDB::range` accepts any `RangeBounds<String>` and returns a `RangeIterator` yielding `(key, value)` pairs in ascending key order:

```rust
use pebbledb::db::PebbleDB;

let db = PebbleDB::open(".pebbledb").unwrap();
db.set("a".into(), "1".into()).unwrap();
db.set("b".into(), "2".into()).unwrap();
db.set("c".into(), "3".into()).unwrap();

for (key, value) in db.range("a".to_string().."c".to_string()) {
    println!("{} = {}", key, value);
}
// a = 1
// b = 2

let one = db.range("b".to_string()..="b".to_string()).collect::<Vec<_>>();
// [("b".to_string(), "2".to_string())]
```

Supported bounds: `..` (everything), `start..`, `..end`, `start..end` and `start..=end`. A key whose newest record is a tombstone is never returned. `scan()` and `keys()` are implemented on top of the same iterator, so all three APIs share one merge implementation.

Iterators can also be positioned explicitly with the `StorageIterator` methods:

```rust
let mut iter = db.range("a".to_string().."z".to_string());
iter.seek("b"); // first live key >= "b"
if iter.valid() {
    println!("{:?} {:?}", iter.key(), iter.value());
}
iter.advance(); // next live key
```

A range iterator snapshots the memtable and clones an `Arc` handle per SSTable when it is created, then releases the read lock. Iteration therefore does not block writers, and the iterator sees the database as it was when `range` was called.

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

- **Reads** (`get`, `keys`, `scan`, `range`, `exists`, `stats`) acquire a read lock, allowing concurrent readers.
- **Writes** (`set`, `delete`, `flush`, `compact`) acquire a write lock, ensuring exclusive access.
- **Stats counters** use `AtomicU64` for lock-free tracking during reads.

Multiple threads can safely read from the same PebbleDB instance simultaneously. `range` returns an iterator that only holds the read lock while it is being created, so iteration itself never blocks writers.

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

## Compaction Strategy

PebbleDB uses leveled compaction to bound read amplification and reclaim space:

```text
  flush_threshold     -- memtable entries before flushing to a new L0 file
  l0_compaction_threshold -- L0 file count that triggers L0 -> L1 compaction
  target_level_size   -- size budget of L1 in bytes
  level_size_multiplier -- growth factor for deeper levels (clamped to >= 2)
  target_file_size    -- approximate bytes per compaction output file
```

**L0 -> L1 compaction** is triggered when the number of L0 files reaches `l0_compaction_threshold`. All L0 files are merged with any L1 files whose key range overlaps. The output is written to L1 in non-overlapping file segments of at most `target_file_size` bytes each.

**Cascade** happens after every flush: if a level's total size exceeds its budget (`target_level_size * multiplier^(level - 1)`), the oldest oversized file is merged with its overlapping neighbors in the next level. Cascades repeat until every level is within budget or the deepest level absorbs everything.

**Tombstone handling**: tombstones are kept while a deeper level holds data that they might shadow. When all overlapping files live in the deepest level containing that key, the tombstone is dropped and the dead data is reclaimed.

**Crash safety** follows the same atomic-rename protocol used by flushes: compaction output is written to a `.sst.tmp` file, fsynced, then renamed into place. Input files are deleted only after all outputs are durable. On startup orphaned `.sst.tmp` files are removed, and any leftover input/output duplicates are resolved by the next compaction cycle.

`compact()` runs the leveled compaction loop until no level needs work. `compact_full()` is a legacy baseline that merges every SSTable into a single file at level 1.

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
  levels:
    L0           0 files
    L1           0 files
    L2           0 files
WAL
  size:          27 bytes
Performance Counters
  memtable hits: 1
  bloom rejects: 0
  block reads:   0
  compactions:   0
  full compactions: 0
  write amplification: --
$ pebbledb compact
OK
```

## Building and Testing

```text
cargo build
cargo test
```

114 unit tests covering:
- WAL round-trip, incomplete records, checksum corruption
- Bloom filter insert, lookup, false positives, encode/decode
- SSTable v2 block-based format, sparse index, bloom filter, checksum validation
- Memtable flushing and SSTable creation
- Read path: memtable override, SSTable lookup, bloom rejection, tombstone behavior
- Compaction policy: level size budgets, L0 threshold, cascade cascading, tombstone handling, file splitting, non-overlap invariants, overlap tolerance, deterministic picker
- Leveled compaction DB tests: flush to L0, threshold trigger, L0/L1 overlap, L1 non-overlap, multi-level cascade, tombstone retention/removal, recovery, legacy filenames, partial-compaction crash duplicates, temp-file cleanup, metrics, full-compaction baseline, concurrent reads
- Crash recovery: WAL replay after flush, temp file cleanup
- Concurrent set/get from multiple threads
- Scan ordering, stats with counters
- Memtable iterator: key ordering, seeking, tombstones, exhaustion
- SSTable iterator: multi-block traversal, sparse index seeks, tombstones, empty tables
- Merge iterator: cross-source ordering, newest-version wins, tombstone suppression
- Range queries: unbounded and half-open bounds, inclusive and exclusive ends, exact single-key ranges, empty ranges, ranges spanning the memtable and several SSTables

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
- Iteration is ascending only; there is no reverse iterator.
- A range iterator copies the memtable into memory when it is created (bounded by the flush threshold). `SSTable::load` already keeps each SSTable file in memory; the iterator decodes one block at a time instead of materialising every entry.
- Bloom filters are not used for ordered seeks, because finding the successor of a missing key still requires reading a block.
- Compaction runs synchronously on the flush path (no background thread).
- Bloom filter false positive rate ~1% (10 bits/key, 7 hash functions).
