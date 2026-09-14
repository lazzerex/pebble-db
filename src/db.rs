use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::sstable::{SSTable, SSTableEntry, write_sstable};
use crate::wal::{WalReader, WalRecord, WalWriter};

const DEFAULT_FLUSH_THRESHOLD: usize = 1024;

#[derive(Debug, Clone)]
pub enum MemtableEntry {
    Value(String),
    Tombstone,
}

pub struct PebbleDB {
    path: PathBuf,
    memtable: HashMap<String, MemtableEntry>,
    wal: WalWriter,
    sstables: Vec<SSTable>,
    next_sst_id: u64,
    flush_threshold: usize,
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
        Ok(Self {
            path,
            memtable,
            wal,
            sstables,
            next_sst_id,
            flush_threshold,
        })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.wal.write_record(&WalRecord::Set {
            key: key.clone(),
            value: value.clone(),
        })?;
        self.memtable.insert(key, MemtableEntry::Value(value));
        if self.memtable.len() >= self.flush_threshold {
            self.flush()?;
        }
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        match self.memtable.get(key) {
            Some(MemtableEntry::Value(v)) => return Some(v),
            Some(MemtableEntry::Tombstone) => return None,
            None => {}
        }
        for sst in self.sstables.iter().rev() {
            match sst.get(key) {
                Some(SSTableEntry::Value(v)) => return Some(v),
                Some(SSTableEntry::Tombstone) => return None,
                None => {}
            }
        }
        None
    }

    pub fn delete(&mut self, key: &str) -> Result<bool> {
        let existed = self.get(key).is_some();
        self.wal.write_record(&WalRecord::Delete {
            key: key.to_string(),
        })?;
        self.memtable
            .insert(key.to_string(), MemtableEntry::Tombstone);
        Ok(existed)
    }

    pub fn exists(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn keys(&self) -> Vec<String> {
        let mut result: BTreeMap<String, ()> = BTreeMap::new();
        for sst in &self.sstables {
            for (key, entry) in sst.entries() {
                match entry {
                    SSTableEntry::Value(_) => {
                        result.insert(key.clone(), ());
                    }
                    SSTableEntry::Tombstone => {
                        result.remove(key);
                    }
                }
            }
        }
        for (key, entry) in &self.memtable {
            match entry {
                MemtableEntry::Value(_) => {
                    result.insert(key.clone(), ());
                }
                MemtableEntry::Tombstone => {
                    result.remove(key);
                }
            }
        }
        result.keys().cloned().collect()
    }

    pub fn scan(&self) -> Vec<(String, String)> {
        let mut result: BTreeMap<String, Option<String>> = BTreeMap::new();
        for sst in &self.sstables {
            for (key, entry) in sst.entries() {
                match entry {
                    SSTableEntry::Value(v) => {
                        result.insert(key.clone(), Some(v.clone()));
                    }
                    SSTableEntry::Tombstone => {
                        result.insert(key.clone(), None);
                    }
                }
            }
        }
        for (key, entry) in &self.memtable {
            match entry {
                MemtableEntry::Value(v) => {
                    result.insert(key.clone(), Some(v.clone()));
                }
                MemtableEntry::Tombstone => {
                    result.insert(key.clone(), None);
                }
            }
        }
        result
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect()
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        let entries: BTreeMap<String, SSTableEntry> = self
            .memtable
            .iter()
            .map(|(k, v)| {
                let entry = match v {
                    MemtableEntry::Value(val) => SSTableEntry::Value(val.clone()),
                    MemtableEntry::Tombstone => SSTableEntry::Tombstone,
                };
                (k.clone(), entry)
            })
            .collect();

        let sst_id = self.next_sst_id;
        self.next_sst_id += 1;
        let sst_path = self.path.join(format!("sstable_{:06}.sst", sst_id));
        let temp_path = self.path.join(format!(".sstable_{:06}.sst.tmp", sst_id));

        write_sstable(&temp_path, &entries)?;
        fs::rename(&temp_path, &sst_path)?;

        let sst = SSTable::load(sst_id, &sst_path)?;
        self.sstables.push(sst);

        self.wal.truncate()?;
        self.memtable.clear();

        Ok(())
    }

    pub fn compact(&mut self) -> Result<()> {
        if self.sstables.len() <= 1 {
            return Ok(());
        }

        let mut merged: BTreeMap<String, SSTableEntry> = BTreeMap::new();
        for sst in &self.sstables {
            for (key, entry) in sst.entries() {
                merged.insert(key.clone(), entry.clone());
            }
        }

        merged.retain(|_, entry| !matches!(entry, SSTableEntry::Tombstone));

        for sst in &self.sstables {
            let _ = fs::remove_file(sst.path());
        }
        self.sstables.clear();

        if !merged.is_empty() {
            let sst_id = self.next_sst_id;
            self.next_sst_id += 1;
            let sst_path = self.path.join(format!("sstable_{:06}.sst", sst_id));
            let temp_path = self.path.join(format!(".sstable_{:06}.sst.tmp", sst_id));
            write_sstable(&temp_path, &merged)?;
            fs::rename(&temp_path, &sst_path)?;
            let sst = SSTable::load(sst_id, &sst_path)?;
            self.sstables.push(sst);
        }

        Ok(())
    }

    pub fn stats(&self) -> DbStats {
        let wal_path = self.path.join("wal.log");
        let wal_size = fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);

        let sst_sizes: Vec<(u64, u64)> = self
            .sstables
            .iter()
            .map(|sst| {
                let size = fs::metadata(sst.path()).map(|m| m.len()).unwrap_or(0);
                (sst.id, size)
            })
            .collect();

        let total_sst_size: u64 = sst_sizes.iter().map(|(_, s)| s).sum();

        DbStats {
            memtable_entries: self.memtable.len(),
            sst_count: self.sstables.len(),
            sst_sizes,
            total_sst_size,
            wal_size,
        }
    }

    pub fn close(self) -> Result<()> {
        self.wal.close()
    }
}

pub struct DbStats {
    pub memtable_entries: usize,
    pub sst_count: usize,
    pub sst_sizes: Vec<(u64, u64)>,
    pub total_sst_size: u64,
    pub wal_size: u64,
}

fn discover_sstables(path: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut sstables = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if let Some(id_str) = name_str
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
        let mut db = PebbleDB::open(dir.path().join("db")).unwrap();
        db.set("key1".into(), "value1".into()).unwrap();
        assert_eq!(db.get("key1"), Some("value1"));
    }

    #[test]
    fn test_overwrite_key() {
        let dir = tempdir().unwrap();
        let mut db = PebbleDB::open(dir.path().join("db")).unwrap();
        db.set("key1".into(), "value1".into()).unwrap();
        db.set("key1".into(), "value2".into()).unwrap();
        assert_eq!(db.get("key1"), Some("value2"));
    }

    #[test]
    fn test_delete() {
        let dir = tempdir().unwrap();
        let mut db = PebbleDB::open(dir.path().join("db")).unwrap();
        db.set("key1".into(), "value1".into()).unwrap();
        let deleted = db.delete("key1").unwrap();
        assert!(deleted);
        assert_eq!(db.get("key1"), None);
    }

    #[test]
    fn test_delete_nonexistent() {
        let dir = tempdir().unwrap();
        let mut db = PebbleDB::open(dir.path().join("db")).unwrap();
        let deleted = db.delete("missing").unwrap();
        assert!(!deleted);
    }

    #[test]
    fn test_exists() {
        let dir = tempdir().unwrap();
        let mut db = PebbleDB::open(dir.path().join("db")).unwrap();
        assert!(!db.exists("key1"));
        db.set("key1".into(), "value1".into()).unwrap();
        assert!(db.exists("key1"));
    }

    #[test]
    fn test_reopen_persists_data() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let mut db = PebbleDB::open(&path).unwrap();
            db.set("name".into(), "lazzerex".into()).unwrap();
            db.close().unwrap();
        }
        {
            let db = PebbleDB::open(&path).unwrap();
            assert_eq!(db.get("name"), Some("lazzerex"));
        }
    }

    #[test]
    fn test_reopen_after_delete() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let mut db = PebbleDB::open(&path).unwrap();
            db.set("key1".into(), "value1".into()).unwrap();
            db.delete("key1").unwrap();
            db.close().unwrap();
        }
        {
            let db = PebbleDB::open(&path).unwrap();
            assert_eq!(db.get("key1"), None);
        }
    }

    #[test]
    fn test_multiple_operations_order() {
        let dir = tempdir().unwrap();
        let mut db = PebbleDB::open(dir.path().join("db")).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        db.set("c".into(), "3".into()).unwrap();
        db.delete("b").unwrap();
        db.set("a".into(), "10".into()).unwrap();
        assert_eq!(db.get("a"), Some("10"));
        assert_eq!(db.get("b"), None);
        assert_eq!(db.get("c"), Some("3"));
    }

    #[test]
    fn test_empty_database() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open(dir.path().join("db")).unwrap();
        assert_eq!(db.get("anything"), None);
        assert!(!db.exists("anything"));
        assert!(db.keys().is_empty());
    }

    #[test]
    fn test_keys_sorted() {
        let dir = tempdir().unwrap();
        let mut db = PebbleDB::open(dir.path().join("db")).unwrap();
        db.set("cherry".into(), "red".into()).unwrap();
        db.set("apple".into(), "green".into()).unwrap();
        db.set("banana".into(), "yellow".into()).unwrap();
        assert_eq!(db.keys(), vec!["apple", "banana", "cherry"]);
    }

    #[test]
    fn test_flush_creates_sstable() {
        let dir = tempdir().unwrap();
        let mut db = PebbleDB::open_with_threshold(dir.path().join("db"), 3).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        assert_eq!(db.sstables.len(), 0);
        db.set("c".into(), "3".into()).unwrap();
        assert_eq!(db.sstables.len(), 1);
        assert_eq!(db.memtable.len(), 0);
    }

    #[test]
    fn test_read_from_sstable() {
        let dir = tempdir().unwrap();
        let mut db = PebbleDB::open_with_threshold(dir.path().join("db"), 2).unwrap();
        db.set("key1".into(), "value1".into()).unwrap();
        db.set("key2".into(), "value2".into()).unwrap();
        assert_eq!(db.sstables.len(), 1);
        assert_eq!(db.get("key1"), Some("value1"));
        assert_eq!(db.get("key2"), Some("value2"));
    }

    #[test]
    fn test_memtable_overrides_sstable() {
        let dir = tempdir().unwrap();
        let mut db = PebbleDB::open_with_threshold(dir.path().join("db"), 2).unwrap();
        db.set("key1".into(), "v1".into()).unwrap();
        db.set("key2".into(), "v2".into()).unwrap();
        db.set("key1".into(), "updated".into()).unwrap();
        assert_eq!(db.get("key1"), Some("updated"));
    }

    #[test]
    fn test_tombstone_overrides_sstable() {
        let dir = tempdir().unwrap();
        let mut db = PebbleDB::open_with_threshold(dir.path().join("db"), 2).unwrap();
        db.set("key1".into(), "value1".into()).unwrap();
        db.set("key2".into(), "value2".into()).unwrap();
        db.delete("key1").unwrap();
        assert_eq!(db.get("key1"), None);
    }

    #[test]
    fn test_reopen_after_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let mut db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            db.set("key1".into(), "value1".into()).unwrap();
            db.set("key2".into(), "value2".into()).unwrap();
            db.close().unwrap();
        }
        {
            let db = PebbleDB::open(&path).unwrap();
            assert_eq!(db.get("key1"), Some("value1"));
            assert_eq!(db.get("key2"), Some("value2"));
        }
    }

    #[test]
    fn test_scan_ordering() {
        let dir = tempdir().unwrap();
        let mut db = PebbleDB::open_with_threshold(dir.path().join("db"), 2).unwrap();
        db.set("cherry".into(), "red".into()).unwrap();
        db.set("apple".into(), "green".into()).unwrap();
        db.set("banana".into(), "yellow".into()).unwrap();
        let result = db.scan();
        let keys: Vec<String> = result.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec!["apple", "banana", "cherry"]);
    }

    #[test]
    fn test_scan_excludes_tombstones() {
        let dir = tempdir().unwrap();
        let mut db = PebbleDB::open_with_threshold(dir.path().join("db"), 2).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        db.delete("a").unwrap();
        let result = db.scan();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "b");
    }

    #[test]
    fn test_compaction() {
        let dir = tempdir().unwrap();
        let mut db = PebbleDB::open_with_threshold(dir.path().join("db"), 2).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        assert_eq!(db.sstables.len(), 1);
        db.set("c".into(), "3".into()).unwrap();
        db.set("d".into(), "4".into()).unwrap();
        assert_eq!(db.sstables.len(), 2);
        db.compact().unwrap();
        assert_eq!(db.sstables.len(), 1);
        assert_eq!(db.get("a"), Some("1"));
        assert_eq!(db.get("b"), Some("2"));
        assert_eq!(db.get("c"), Some("3"));
        assert_eq!(db.get("d"), Some("4"));
    }

    #[test]
    fn test_compaction_drops_tombstones() {
        let dir = tempdir().unwrap();
        let mut db = PebbleDB::open_with_threshold(dir.path().join("db"), 2).unwrap();
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
        let mut db = PebbleDB::open_with_threshold(dir.path().join("db"), 2).unwrap();
        db.set("key".into(), "old".into()).unwrap();
        db.set("other".into(), "x".into()).unwrap();
        db.set("key".into(), "new".into()).unwrap();
        db.set("other2".into(), "y".into()).unwrap();
        db.compact().unwrap();
        assert_eq!(db.get("key"), Some("new"));
    }

    #[test]
    fn test_compaction_tombstone_shadows_older() {
        let dir = tempdir().unwrap();
        let mut db = PebbleDB::open_with_threshold(dir.path().join("db"), 2).unwrap();
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
            let mut db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            db.set("a".into(), "1".into()).unwrap();
            db.set("b".into(), "2".into()).unwrap();
            db.set("c".into(), "3".into()).unwrap();
            db.close().unwrap();
        }
        {
            let db = PebbleDB::open(&path).unwrap();
            assert_eq!(db.get("a"), Some("1"));
            assert_eq!(db.get("b"), Some("2"));
            assert_eq!(db.get("c"), Some("3"));
        }
    }

    #[test]
    fn test_crash_during_flush_recovery() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        fs::create_dir_all(&path).unwrap();
        {
            let mut db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            db.set("a".into(), "1".into()).unwrap();
            db.set("b".into(), "2".into()).unwrap();
            db.close().unwrap();
        }
        let temp_file = path.join(".sstable_000000.sst.tmp");
        fs::write(&temp_file, b"garbage").unwrap();
        {
            let db = PebbleDB::open(&path).unwrap();
            assert_eq!(db.get("a"), Some("1"));
            assert_eq!(db.get("b"), Some("2"));
            assert!(!temp_file.exists());
        }
    }

    #[test]
    fn test_stats() {
        let dir = tempdir().unwrap();
        let mut db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
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
}
