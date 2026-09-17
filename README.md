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
- **Fault Injection** (`src/fault.rs`) can deterministically fail or abort a database at any persistence boundary for crash-recovery tests. It is disabled by default and costs one `Option` check per boundary.
- **PebbleCache** (`src/cache.rs`) is a small TTL cache built on top of PebbleDB, demonstrating use of the engine as persistent cache storage.

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

1. Remove orphaned temp files (`.tmp`) left behind by an interrupted flush or compaction.
2. Replay an interrupted compaction from `merge.pending`.
3. Discover and load all SSTables by filename, sorted by ID.
4. Parse block indexes and bloom filters.
5. Replay the WAL into the memtable, discarding a torn tail.

The database is consistent after replay because:

- SSTables contain previously flushed data.
- The WAL contains any operations that were not yet flushed.
- Duplicate entries (in both SSTable and WAL) are harmless; the memtable takes precedence on reads.

## Durability Contract

PebbleDB makes exactly one durability promise, and the crash tests verify it:

```text
If set() or delete() returns Ok:
    the operation is in the WAL, fsynced, and survives a process crash
```

More precisely:

- `set` and `delete` append a length-prefixed, CRC32-protected record to `wal.log` and `fsync` it before returning `Ok`.
- Acknowledged operations that were not yet flushed are replayed from the WAL on the next open.
- An operation that returns an error (I/O failure or an injected fault) may or may not be present after recovery. Only `Ok` is a promise.
- Flushes and compactions rename a complete `.sst.tmp` file into place before the WAL is truncated, so an SSTable that exists on disk is never partially written.
- SSTable files are not `fsync`ed and the database directory is not `fsync`ed after a rename. The contract therefore covers process termination (panic, `abort`, `SIGKILL`), not sudden power loss.

## WAL Recovery Policy

Records are parsed one at a time and each case has an explicit policy:

```text
truncated length prefix                -> torn tail, discarded
record whose declared size passes EOF  -> torn tail, discarded
payload shorter than declared size     -> torn tail, discarded
declared size shorter than a header    -> garbage tail, discarded
CRC mismatch on a complete record      -> corruption, open fails
complete final record with bad CRC     -> corruption, open fails
```

Discarding a torn tail is safe because a record only becomes durable after `fsync`, so a partially written record was never acknowledged. Damage that cannot be explained by an interrupted append is reported as `PebbleError::WalCorruption` instead of being skipped silently.

After a torn tail is discarded the WAL is truncated to the end of the last complete record, so subsequent appends stay parseable. Recovery is repeatable: opening the same directory again neither changes the logical contents nor re-triggers the discard.

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

**Crash safety**: compaction output is written to a `.sst.tmp` file and renamed into place before any input is touched. Before the first input is deleted, the ids of the outputs and inputs are recorded in a small `merge.pending` file (written through a temp file and renamed as well). Inputs are then deleted one at a time, and `merge.pending` is removed only after the in-memory metadata has been updated.

If a crash happens anywhere in that sequence, `open` replays the pending merge: when every output listed in `merge.pending` exists on disk, the listed inputs are removed again (a no-op for the ones already gone) and the marker is deleted. When an output is missing, the inputs are kept, because duplicates are harmless — reads prefer the newest file within a level — and the next compaction merges them again.

Replaying the marker is a correctness fix, not just cleanup. A compaction that drops tombstones rewrites the surviving data into the output, so a crash after the tombstone's input file was deleted but before an older input file was deleted would let a deleted value reappear. Finishing the pending merge makes the whole input set disappear together. `db::tests::test_compaction_crash_before_input_cleanup_does_not_resurrect_deleted_keys` pins this window down.

`compact()` runs the leveled compaction loop until no level needs work. `compact_full()` is a legacy baseline that merges every SSTable into a single file at level 1.

## Fault Injection

Persistence boundaries can be failed or aborted deterministically through a small test-oriented abstraction (`src/fault.rs`):

```text
before_wal_append              after_wal_append              after_wal_fsync
after_memtable_update          before_sstable_creation       after_sstable_write
before_atomic_rename           after_atomic_rename           before_metadata_update
after_metadata_update          before_obsolete_file_delete   after_obsolete_file_delete
before_wal_truncation          after_wal_truncation          after_compaction_output_creation
```

A fault is a triple:

- `point` — the boundary to trip.
- `hit` — the Nth time that boundary is reached. Hits are counted per database instance, so a fixed seed and workload always trip at the same place.
- `mode` — `Error` returns `PebbleError::InjectedFault` (fast, in-process), `Abort` calls `std::process::abort()` (a real process termination).

Every database instance owns its own `FaultInjector`, so faulted tests cannot disturb each other and can run in parallel. `DbOptions::fault` defaults to `None`, and each injection point then costs a single `Option` check, so ordinary builds pay nothing.

The library itself never reads test configuration from the environment. `src/bin/crash_child.rs` is a separate harness binary that reads its settings from environment variables and passes them into `DbOptions`.

## Persistent Cache (PebbleCache)

`src/cache.rs` builds a small cache on top of PebbleDB. It shows what an embedded key-value store is good at: the cache is just an application on top of the storage engine, and PebbleDB remains the only thing that touches the disk.

```text
Application
    |
    v
PebbleCache          <- TTL, lazy expiration, statistics, prefix
    |
    v
PebbleDB
    |
    +-- WAL
    +-- Memtable
    +-- SSTables
```

Why a persistent cache at all: an ordinary in-memory cache disappears on restart, so every process restart pays the expensive lookups again. Keeping the cache in PebbleDB means entries survive restarts, compaction reclaims space from expired entries, and no second persistence mechanism (no dump file, no separate format) has to be maintained.

API:

```rust
use std::time::Duration;
use pebbledb::cache::PebbleCache;

let cache = PebbleCache::open(".pebblecache").unwrap();

match cache.get("what-is-stow").unwrap() {
    Some(value) => println!("hit: {}", value),
    None => {
        let value = expensive_lookup();
        cache.set("what-is-stow", &value, Duration::from_secs(60 * 60 * 24)).unwrap();
    }
}

cache.delete("what-is-stow").unwrap();
cache.clear().unwrap();
let stats = cache.stats();
```

How it works:

- **Entry encoding** — a cache entry is stored as `"<expires_at_ms>:<value>"`. `0` means "never expires", anything else is a Unix timestamp in milliseconds. Values may contain colons; only the first colon is a separator.
- **TTL** — `set(key, value, ttl)` computes `now + ttl`; `Duration::ZERO` stores `0` and never expires.
- **Expiration** — lazily, on `get` (and therefore on `exists`). An expired entry is treated as a miss, deleted from PebbleDB, and counted in `CacheStats::expired`. There is no background scanner and no timer thread: reading the entry is the only moment a miss can matter, and deletion-on-read keeps the design explicit.
- **Hits and misses** — a `get` that finds a live entry increments `hits`; an absent or expired entry increments `misses`. `sets`, `deletes`, `expired` and `clears` are counted too.
- **Namespace** — every cache entry is stored under the key prefix `cache:`, so cache data can never collide with ordinary PebbleDB keys that the application writes itself. `clear()` walks the prefix and stops at the first key that does not carry it, which is why `cachez:...` is left alone.
- **Concurrency** — the underlying `PebbleDB` is already thread-safe (`Arc<RwLock<DbInner>>`), so the cache only protects its own counters with a `Mutex<CacheStats>`. `PebbleCache` is `Send + Sync` and can be shared with `Arc`. This is intentionally the simplest mechanism that works, not an optimized concurrent cache.
- **Limitations, on purpose** — no size or entry limit and no eviction policy (an LRU would need access-order state that PebbleDB does not keep, plus either a background thread or write amplification on reads), no background expiration, no TTL refresh on read, no metrics export, no Redis compatibility, no network protocol.

Run the demonstration:

```text
cargo run --example cache_demo
```

It performs a request that misses, computes and stores the value, repeats the request as a hit, expires an entry with a short TTL, reopens the database to show persistence, and prints the statistics.

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

The suite is split into unit tests (`#[cfg(test)]` modules next to the code) and integration tests:

```text
tests/crash_recovery.rs    fault-injection sweep, reference model, subprocess crashes
tests/wal_corruption.rs    torn, truncated and corrupted WAL tails
tests/crash_stress.rs      crash stress mode (ignored by default)
src/bin/crash_child.rs     harness binary that dies mid-operation for the tests above
```

What is covered:

- **WAL** — round-trip, incomplete records, checksum corruption, torn tail discarded, garbage tail discarded, corrupted durable record rejected, recovery truncates the torn tail
- **Bloom filter** — insert, lookup, false positives, encode/decode
- **SSTable v2** — block-based format, sparse index, bloom filter, checksum validation
- **Flush and reads** — memtable flushing, SSTable creation, memtable override, tombstone behavior, bloom rejection, path counters
- **Compaction** — level budgets, L0 threshold, cascade, tombstone retention/removal, file splitting, non-overlap invariants, overlap tolerance, deterministic picker, full-compaction baseline
- **Compaction crash safety** — `merge.pending` replay, cleanup when outputs are missing, no resurrection of deleted keys after a crash during input deletion, legacy filenames, duplicate tolerance
- **Fault injection** — every fault point fires; all acknowledged operations survive a crash at every point and hit index; in-process errors and real subprocess aborts; identical reproduction of a crash run
- **Properties** — recovery, delete (no resurrection), range/scan equality with a `BTreeMap` reference model, idempotent repeated recovery, deterministic randomized workloads across flush/compaction/reopen
- **Iterators and ranges** — memtable, SSTable and merge iterators, seeks, tombstones, unbounded/inclusive/exclusive bounds
- **Cache** — set/get/miss, overwrite, values with colons, delete, exists, TTL expiration, non-expiring entries, lazy deletion of expired entries, statistics, clear (without touching other keys), prefix namespace, persistence across reopen, survival across flush, concurrent access

## Crash-Recovery Testing

```text
cargo test --test crash_recovery                 # fast in-process sweep + real process crashes
cargo test --test wal_corruption                 # WAL tail damage
cargo test --test crash_stress -- --ignored --nocapture
```

How the tests are built:

- **Reference model** — the test keeps its own `BTreeMap` of the operations it issued. Nothing from PebbleDB is used to compute expected results.
- **In-process sweep** — for every fault point and the first three hits, a deterministic randomized workload runs against a fresh database until the injected fault stops it. The handle is dropped, the database is reopened, and the result is compared with the reference model: acknowledged operations intact, deleted keys absent, `scan()` equal to the model.
- **Subprocess crashes** — `src/bin/crash_child.rs` runs a fixed workload, prints `TRY` before each operation, `ACK` after it, then dies inside the storage engine. The parent process turns that output into the reference model, reopens the database and verifies it. The same seed, fault point and hit index reproduce the same crash exactly (`crash_child_reproduces_the_same_run_twice`).
- **Unacknowledged operations** — the operation that was in flight when the process died may be present or absent afterwards; the contract only requires acknowledged operations to survive, so both outcomes are accepted for that one key and everything else must match.
- **Stress mode** (`--ignored`) — throws many crashes across all fault points and prints a summary:

```text
seed:               12345
operations:         120
crashes:            30
recovery failures:  0
data mismatches:    0
```

Longer runs are configured with `PEBBLEDB_STRESS_SEED`, `PEBBLEDB_STRESS_OPS`, `PEBBLEDB_STRESS_CRASHES` and `PEBBLEDB_STRESS_MODE` (`abort` or `error`). Failures print the seed, the fault point, the hit index and how many operations had been acknowledged, so they can be replayed.

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

### Known durability risks

- Durability is proven against process termination, not against power loss: neither SSTable files nor the database directory are `fsync`ed, so a rename can be lost by a kernel or disk failure.
- WAL corruption that is not a torn tail (a flipped byte in a durable record) makes the database fail to open; there is no repair or salvage tool.
- The WAL is a single file that is only truncated on flush, so a database configured with a very large flush threshold grows the WAL without bound.
- All writes share one `RwLock`, so flush and compaction block readers and writers while they run.
- `PebbleCache` has no eviction: a cache written faster than it is read grows until the caller deletes entries or calls `clear()`.
- Fault injection is a test facility: an injected fault leaves the live handle partially updated. Recovery is guaranteed by reopening the database, which is exactly what the tests do.
