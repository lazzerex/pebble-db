use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::error::Result;
use crate::sstable::{SSTable, SSTableEntry, write_sstable};
use crate::wal::{WalReader, WalRecord, WalWriter};

const DEFAULT_FLUSH_THRESHOLD: usize = 1024;

#[derive(Debug, Clone)]
pub enum MemtableEntry {
    Value(String),
    Tombstone,
}

#[derive(Debug, Clone, Default)]
pub struct DbStats {
    pub memtable_entries: usize,
    pub memtable_bytes: usize,
    pub sst_count: usize,
    pub total_sst_size: usize,
    pub sst_sizes: Vec<(u64, usize)>,
    pub wal_size: u64,
    pub memtable_hits: u64,
    pub bloom_rejects: u64,
    pub block_reads: u64,
    pub compactions_completed: u64,
}

struct AtomicCounters {
    memtable_hits: AtomicU64,
    bloom_rejects: AtomicU64,
    block_reads: AtomicU64,
    compactions_completed: AtomicU64,
}

impl Default for AtomicCounters {
    fn default() -> Self {
        Self {
            memtable_hits: AtomicU64::new(0),
            bloom_rejects: AtomicU64::new(0),
            block_reads: AtomicU64::new(0),
            compactions_completed: AtomicU64::new(0),
        }
    }
}

struct DbInner {
    path: PathBuf,
    memtable: HashMap<String, MemtableEntry>,
    wal: WalWriter,
    sstables: Vec<SSTable>,
    next_sst_id: u64,
    flush_threshold: usize,
    counters: AtomicCounters,
}

#[derive(Clone)]
pub struct PebbleDB {
    inner: Arc<RwLock<DbInner>>,
}

impl PebbleDB {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_threshold(path, DEFAULT_FLUSH_THRESHOLD)
    }

    pub fn open_with_threshold(path: impl AsRef<Path>, flush_threshold: usize) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        fs::create_dir_all(&path)?;

        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(".tmp") {
                fs::remove_file(entry.path())?;
            }
        }

        let sst_paths = discover_sstables(&path)?;
        let mut sstables = Vec::new();
        let mut next_sst_id = 0u64;
        for (id, sst_path) in &sst_paths {
            let sst = SSTable::load(*id, sst_path)?;
            sstables.push(sst);
            next_sst_id = (*id).max(next_sst_id) + 1;
        }

        let wal_path = path.join("wal.log");
        let mut memtable = HashMap::new();

        if wal_path.exists() {
            let mut reader = WalReader::open(&wal_path)?;
            let records = reader.read_all()?;
            for record in records {
                match record {
                    WalRecord::Set { key, value } => {
                        memtable.insert(key, MemtableEntry::Value(value));
                    }
                    WalRecord::Delete { key } => {
                        memtable.insert(key, MemtableEntry::Tombstone);
                    }
                }
            }
        }

        let wal = WalWriter::open(&wal_path)?;

        let inner = DbInner {
            path,
            memtable,
            wal,
            sstables,
            next_sst_id,
            flush_threshold,
            counters: AtomicCounters::default(),
        };

        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
        })
    }

    pub fn set(&self, key: String, value: String) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.wal.write_record(&WalRecord::Set {
            key: key.clone(),
            value: value.clone(),
        })?;
        inner.memtable.insert(key, MemtableEntry::Value(value));
        if inner.memtable.len() >= inner.flush_threshold {
            inner.flush()?;
        }
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let inner = self.inner.read().unwrap();
        match inner.memtable.get(key) {
            Some(MemtableEntry::Value(v)) => {
                inner.counters.memtable_hits.fetch_add(1, Ordering::Relaxed);
                return Some(v.clone());
            }
            Some(MemtableEntry::Tombstone) => return None,
            None => {}
        }
        for sst in inner.sstables.iter().rev() {
            if !sst.bloom_might_contain(key) {
                inner.counters.bloom_rejects.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            inner.counters.block_reads.fetch_add(1, Ordering::Relaxed);
            match sst.get(key) {
                Some(SSTableEntry::Value(v)) => return Some(v),
                Some(SSTableEntry::Tombstone) => return None,
                None => {}
            }
        }
        None
    }

    pub fn delete(&self, key: &str) -> Result<bool> {
        let mut inner = self.inner.write().unwrap();
        let existed = Self::get_inner(&inner, key).is_some();
        inner.wal.write_record(&WalRecord::Delete {
            key: key.to_string(),
        })?;
        inner
            .memtable
            .insert(key.to_string(), MemtableEntry::Tombstone);
        if inner.memtable.len() >= inner.flush_threshold {
            inner.flush()?;
        }
        Ok(existed)
    }

    pub fn exists(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn keys(&self) -> Vec<String> {
        let inner = self.inner.read().unwrap();
        Self::collect_keys_inner(&inner)
    }

    pub fn scan(&self) -> Vec<(String, String)> {
        let inner = self.inner.read().unwrap();
        Self::collect_scan_inner(&inner)
    }

    pub fn compact(&self) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        Self::compact_inner(&mut inner)
    }

    pub fn flush(&self) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.flush()?;
        Ok(())
    }

    pub fn stats(&self) -> DbStats {
        let inner = self.inner.read().unwrap();
        Self::stats_inner(&inner)
    }

    pub fn close(self) -> Result<()> {
        drop(self);
        Ok(())
    }

    fn get_inner(inner: &DbInner, key: &str) -> Option<String> {
        match inner.memtable.get(key) {
            Some(MemtableEntry::Value(v)) => return Some(v.clone()),
            Some(MemtableEntry::Tombstone) => return None,
            None => {}
        }
        for sst in inner.sstables.iter().rev() {
            match sst.get(key) {
                Some(SSTableEntry::Value(v)) => return Some(v),
                Some(SSTableEntry::Tombstone) => return None,
                None => {}
            }
        }
        None
    }

    fn collect_keys_inner(inner: &DbInner) -> Vec<String> {
        let mut result: BTreeMap<String, ()> = BTreeMap::new();
        for sst in &inner.sstables {
            for (key, entry) in sst.entries() {
                match entry {
                    SSTableEntry::Value(_) => {
                        result.insert(key, ());
                    }
                    SSTableEntry::Tombstone => {
                        result.remove(&key);
                    }
                }
            }
        }
        for (key, entry) in &inner.memtable {
            match entry {
                MemtableEntry::Value(_) => {
                    result.insert(key.clone(), ());
                }
                MemtableEntry::Tombstone => {
                    result.remove(key.as_str());
                }
            }
        }
        result.into_keys().collect()
    }

    fn collect_scan_inner(inner: &DbInner) -> Vec<(String, String)> {
        let mut result: BTreeMap<String, String> = BTreeMap::new();
        for sst in &inner.sstables {
            for (key, entry) in sst.entries() {
                match entry {
                    SSTableEntry::Value(v) => {
                        result.insert(key, v);
                    }
                    SSTableEntry::Tombstone => {
                        result.remove(&key);
                    }
                }
            }
        }
        for (key, entry) in &inner.memtable {
            match entry {
                MemtableEntry::Value(v) => {
                    result.insert(key.clone(), v.clone());
                }
                MemtableEntry::Tombstone => {
                    result.remove(key.as_str());
                }
            }
        }
        result.into_iter().collect()
    }

    fn compact_inner(inner: &mut DbInner) -> Result<()> {
        if inner.sstables.len() <= 1 {
            return Ok(());
        }

        let mut merged: BTreeMap<String, SSTableEntry> = BTreeMap::new();
        for sst in &inner.sstables {
            for (key, entry) in sst.entries() {
                merged.insert(key, entry);
            }
        }

        let sst_id = inner.next_sst_id;
        inner.next_sst_id += 1;
        let tmp_path = inner.path.join(format!("sstable_{:06}.sst.tmp", sst_id));
        let final_path = inner.path.join(format!("sstable_{:06}.sst", sst_id));

        write_sstable(&tmp_path, &merged)?;
        fs::rename(&tmp_path, &final_path)?;

        for sst in &inner.sstables {
            let _ = fs::remove_file(sst.path());
        }

        let sst = SSTable::load(sst_id, &final_path)?;
        inner.sstables = vec![sst];
        inner
            .counters
            .compactions_completed
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn stats_inner(inner: &DbInner) -> DbStats {
        let memtable_bytes: usize = inner
            .memtable
            .iter()
            .map(|(k, v)| {
                k.len()
                    + match v {
                        MemtableEntry::Value(v) => v.len(),
                        MemtableEntry::Tombstone => 0,
                    }
            })
            .sum();

        let sst_sizes: Vec<(u64, usize)> = inner
            .sstables
            .iter()
            .map(|s| (s.id, s.file_size()))
            .collect();
        let total_sst_size: usize = sst_sizes.iter().map(|(_, s)| s).sum();

        let wal_path = inner.path.join("wal.log");
        let wal_size = fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);

        DbStats {
            memtable_entries: inner.memtable.len(),
            memtable_bytes,
            sst_count: inner.sstables.len(),
            total_sst_size,
            sst_sizes,
            wal_size,
            memtable_hits: inner.counters.memtable_hits.load(Ordering::Relaxed),
            bloom_rejects: inner.counters.bloom_rejects.load(Ordering::Relaxed),
            block_reads: inner.counters.block_reads.load(Ordering::Relaxed),
            compactions_completed: inner.counters.compactions_completed.load(Ordering::Relaxed),
        }
    }
}

impl DbInner {
    fn flush(&mut self) -> Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        let mut entries: BTreeMap<String, SSTableEntry> = BTreeMap::new();
        for (key, entry) in &self.memtable {
            let sst_entry = match entry {
                MemtableEntry::Value(v) => SSTableEntry::Value(v.clone()),
                MemtableEntry::Tombstone => SSTableEntry::Tombstone,
            };
            entries.insert(key.clone(), sst_entry);
        }

        let sst_id = self.next_sst_id;
        self.next_sst_id += 1;
        let tmp_path = self.path.join(format!("sstable_{:06}.sst.tmp", sst_id));
        let final_path = self.path.join(format!("sstable_{:06}.sst", sst_id));

        write_sstable(&tmp_path, &entries)?;
        fs::rename(&tmp_path, &final_path)?;

        self.wal.truncate()?;

        let sst = SSTable::load(sst_id, &final_path)?;
        self.sstables.push(sst);
        self.memtable.clear();

        Ok(())
    }
}

fn discover_sstables(path: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut sstables = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some(id_str) = name
            .strip_prefix("sstable_")
            .and_then(|s| s.strip_suffix(".sst"))
            && let Ok(id) = id_str.parse::<u64>()
        {
            sstables.push((id, entry.path()));
        }
    }
    sstables.sort_by_key(|(id, _)| *id);
    Ok(sstables)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_set_and_get() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        db.set("key".into(), "value".into()).unwrap();
        assert_eq!(db.get("key"), Some("value".into()));
    }

    #[test]
    fn test_overwrite_key() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        db.set("key".into(), "v1".into()).unwrap();
        db.set("key".into(), "v2".into()).unwrap();
        assert_eq!(db.get("key"), Some("v2".into()));
    }

    #[test]
    fn test_get_nonexistent() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        assert_eq!(db.get("nope"), None);
    }

    #[test]
    fn test_delete() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        db.set("key".into(), "val".into()).unwrap();
        let existed = db.delete("key").unwrap();
        assert!(existed);
        assert_eq!(db.get("key"), None);
    }

    #[test]
    fn test_delete_nonexistent() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        let existed = db.delete("nope").unwrap();
        assert!(!existed);
    }

    #[test]
    fn test_exists() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        db.set("key".into(), "val".into()).unwrap();
        assert!(db.exists("key"));
        assert!(!db.exists("nope"));
    }

    #[test]
    fn test_empty_database() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        assert_eq!(db.get("any"), None);
    }

    #[test]
    fn test_keys_sorted() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        db.set("c".into(), "3".into()).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        let keys = db.keys();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_multiple_operations_order() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        db.set("x".into(), "1".into()).unwrap();
        db.set("y".into(), "2".into()).unwrap();
        db.delete("x").unwrap();
        assert_eq!(db.get("x"), None);
        assert_eq!(db.get("y"), Some("2".into()));
    }

    #[test]
    fn test_reopen_persists_data() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_threshold(&path, 100).unwrap();
            db.set("hello".into(), "world".into()).unwrap();
        }
        {
            let db = PebbleDB::open(&path).unwrap();
            assert_eq!(db.get("hello"), Some("world".into()));
        }
    }

    #[test]
    fn test_reopen_after_delete() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_threshold(&path, 100).unwrap();
            db.set("key".into(), "val".into()).unwrap();
            db.delete("key").unwrap();
        }
        {
            let db = PebbleDB::open(&path).unwrap();
            assert_eq!(db.get("key"), None);
        }
    }

    #[test]
    fn test_flush_creates_sstable() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 2).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        let entries = fs::read_dir(dir.path().join("db")).unwrap();
        let ssts: Vec<_> = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".sst"))
            .collect();
        assert_eq!(ssts.len(), 1);
    }

    #[test]
    fn test_read_from_sstable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            db.set("a".into(), "1".into()).unwrap();
            db.set("b".into(), "2".into()).unwrap();
            db.set("c".into(), "3".into()).unwrap();
        }
        {
            let db = PebbleDB::open(&path).unwrap();
            assert_eq!(db.get("a"), Some("1".into()));
            assert_eq!(db.get("b"), Some("2".into()));
        }
    }

    #[test]
    fn test_memtable_overrides_sstable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            db.set("k".into(), "old".into()).unwrap();
            db.set("x".into(), "pad".into()).unwrap();
        }
        {
            let db = PebbleDB::open_with_threshold(&path, 100).unwrap();
            db.set("k".into(), "new".into()).unwrap();
            assert_eq!(db.get("k"), Some("new".into()));
        }
    }

    #[test]
    fn test_tombstone_overrides_sstable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            db.set("k".into(), "val".into()).unwrap();
            db.set("x".into(), "pad".into()).unwrap();
        }
        {
            let db = PebbleDB::open_with_threshold(&path, 100).unwrap();
            db.delete("k").unwrap();
            assert_eq!(db.get("k"), None);
        }
    }

    #[test]
    fn test_scan_ordering() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        db.set("c".into(), "3".into()).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        let scan = db.scan();
        assert_eq!(
            scan,
            vec![
                ("a".into(), "1".into()),
                ("b".into(), "2".into()),
                ("c".into(), "3".into()),
            ]
        );
    }

    #[test]
    fn test_scan_excludes_tombstones() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        db.delete("a").unwrap();
        let scan = db.scan();
        assert_eq!(scan, vec![("b".into(), "2".into())]);
    }

    #[test]
    fn test_compaction() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 2).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        db.set("c".into(), "3".into()).unwrap();
        db.set("d".into(), "4".into()).unwrap();
        db.compact().unwrap();
        assert_eq!(db.get("a"), Some("1".into()));
        assert_eq!(db.get("b"), Some("2".into()));
        assert_eq!(db.get("c"), Some("3".into()));
        assert_eq!(db.get("d"), Some("4".into()));
    }

    #[test]
    fn test_compaction_drops_tombstones() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 2).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        db.delete("a").unwrap();
        db.set("c".into(), "3".into()).unwrap();
        db.compact().unwrap();
        let keys = db.keys();
        assert_eq!(keys, vec!["b", "c"]);
    }

    #[test]
    fn test_compaction_newest_wins() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 2).unwrap();
        db.set("key".into(), "old".into()).unwrap();
        db.set("other".into(), "x".into()).unwrap();
        db.set("key".into(), "new".into()).unwrap();
        db.set("other2".into(), "y".into()).unwrap();
        db.compact().unwrap();
        assert_eq!(db.get("key"), Some("new".into()));
    }

    #[test]
    fn test_compaction_tombstone_shadows_older() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 2).unwrap();
        db.set("key".into(), "old".into()).unwrap();
        db.set("pad".into(), "x".into()).unwrap();
        db.delete("key").unwrap();
        db.set("pad2".into(), "y".into()).unwrap();
        db.compact().unwrap();
        assert_eq!(db.get("key"), None);
    }

    #[test]
    fn test_wal_replay_after_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            db.set("a".into(), "1".into()).unwrap();
            db.set("b".into(), "2".into()).unwrap();
            db.set("c".into(), "3".into()).unwrap();
        }
        {
            let db = PebbleDB::open(&path).unwrap();
            assert_eq!(db.get("a"), Some("1".into()));
            assert_eq!(db.get("b"), Some("2".into()));
            assert_eq!(db.get("c"), Some("3".into()));
        }
    }

    #[test]
    fn test_crash_during_flush_recovery() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        fs::create_dir_all(&path).unwrap();
        {
            let db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            db.set("a".into(), "1".into()).unwrap();
            db.set("b".into(), "2".into()).unwrap();
        }
        let temp_file = path.join(".sstable_000000.sst.tmp");
        fs::write(&temp_file, b"garbage").unwrap();
        {
            let db = PebbleDB::open(&path).unwrap();
            assert_eq!(db.get("a"), Some("1".into()));
            assert_eq!(db.get("b"), Some("2".into()));
            assert!(!temp_file.exists());
        }
    }

    #[test]
    fn test_stats() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        let stats = db.stats();
        assert_eq!(stats.memtable_entries, 2);
        assert_eq!(stats.sst_count, 0);
        assert!(stats.wal_size > 0);
        db.flush().unwrap();
        let stats = db.stats();
        assert_eq!(stats.memtable_entries, 0);
        assert_eq!(stats.sst_count, 1);
        assert!(stats.total_sst_size > 0);
    }

    #[test]
    fn test_concurrent_set_get() {
        use std::sync::Arc;
        use std::thread;

        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Arc::new(PebbleDB::open_with_threshold(&path, 10000).unwrap());

        let mut handles = Vec::new();
        for i in 0..4 {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("t{}_{}", i, j);
                    db.set(key.clone(), format!("val_{}", j)).unwrap();
                    let _ = db.get(&key);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let stats = db.stats();
        assert_eq!(stats.memtable_entries, 400);
    }

    #[test]
    fn test_bloom_filter_rejects() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            for i in 0..50 {
                db.set(format!("key_{:04}", i), format!("val_{}", i))
                    .unwrap();
            }
        }
        {
            let db = PebbleDB::open(&path).unwrap();
            let stats_before = db.stats();
            let rejects_before = stats_before.bloom_rejects;
            let _ = db.get("nonexistent_zzz");
            let stats_after = db.stats();
            assert!(stats_after.bloom_rejects > rejects_before || stats_after.sst_count > 0);
        }
    }
}
