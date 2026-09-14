use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use crate::error::{PebbleError, Result};
use crate::wal::crc32;

const MAGIC: u32 = 0x50454242;
const VERSION: u32 = 1;
const OP_SET: u8 = 1;
const OP_DELETE: u8 = 2;
const HEADER_SIZE: usize = 12;
const FOOTER_SIZE: usize = 4;

#[derive(Debug, Clone, PartialEq)]
pub enum SSTableEntry {
    Value(String),
    Tombstone,
}

pub struct SSTable {
    pub id: u64,
    path: std::path::PathBuf,
    entries: BTreeMap<String, SSTableEntry>,
}

impl SSTable {
    pub fn load(id: u64, path: &Path) -> Result<Self> {
        let entries = read_sstable(path)?;
        Ok(Self {
            id,
            path: path.to_path_buf(),
            entries,
        })
    }

    pub fn get(&self, key: &str) -> Option<&SSTableEntry> {
        self.entries.get(key)
    }

    pub fn entries(&self) -> &BTreeMap<String, SSTableEntry> {
        &self.entries
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn record_count(&self) -> usize {
        self.entries.len()
    }
}

pub fn write_sstable(path: &Path, entries: &BTreeMap<String, SSTableEntry>) -> Result<()> {
    let mut buf = Vec::new();

    buf.extend_from_slice(&MAGIC.to_le_bytes());
    buf.extend_from_slice(&VERSION.to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());

    for (key, entry) in entries {
        let key_bytes = key.as_bytes();
        let (op, value_bytes) = match entry {
            SSTableEntry::Value(v) => (OP_SET, v.as_bytes()),
            SSTableEntry::Tombstone => (OP_DELETE, &b""[..]),
        };

        buf.push(op);
        buf.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(key_bytes);
        buf.extend_from_slice(value_bytes);
    }

    let checksum = crc32(&buf);
    buf.extend_from_slice(&checksum.to_le_bytes());

    fs::write(path, &buf)?;
    Ok(())
}

fn read_sstable(path: &Path) -> Result<BTreeMap<String, SSTableEntry>> {
    let data = fs::read(path)?;

    if data.len() < HEADER_SIZE + FOOTER_SIZE {
        return Err(PebbleError::SSTableCorruption("file too short".into()));
    }

    let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
    if magic != MAGIC {
        return Err(PebbleError::SSTableCorruption(
            "invalid magic number".into(),
        ));
    }

    let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
    if version != VERSION {
        return Err(PebbleError::SSTableCorruption(format!(
            "unsupported version: {}",
            version
        )));
    }

    let record_count = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;

    let checksum_offset = data.len() - FOOTER_SIZE;
    let stored = u32::from_le_bytes(data[checksum_offset..].try_into().unwrap());
    let computed = crc32(&data[..checksum_offset]);
    if stored != computed {
        return Err(PebbleError::SSTableCorruption("checksum mismatch".into()));
    }

    let mut entries = BTreeMap::new();
    let mut offset = HEADER_SIZE;

    for _ in 0..record_count {
        if offset >= checksum_offset {
            return Err(PebbleError::SSTableCorruption(
                "unexpected end of records".into(),
            ));
        }

        let op = data[offset];
        offset += 1;

        let key_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let value_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if offset + key_len + value_len > checksum_offset {
            return Err(PebbleError::SSTableCorruption(
                "record data exceeds file".into(),
            ));
        }

        let key = String::from_utf8(data[offset..offset + key_len].to_vec())
            .map_err(|_| PebbleError::SSTableCorruption("invalid UTF-8 in key".into()))?;
        offset += key_len;

        let entry = match op {
            OP_SET => {
                let value = String::from_utf8(data[offset..offset + value_len].to_vec())
                    .map_err(|_| PebbleError::SSTableCorruption("invalid UTF-8 in value".into()))?;
                offset += value_len;
                SSTableEntry::Value(value)
            }
            OP_DELETE => {
                offset += value_len;
                SSTableEntry::Tombstone
            }
            _ => {
                return Err(PebbleError::SSTableCorruption(format!(
                    "unknown op: {}",
                    op
                )));
            }
        };

        entries.insert(key, entry);
    }

    if entries.len() != record_count {
        return Err(PebbleError::SSTableCorruption(
            "record count mismatch".into(),
        ));
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_sstable_write_read_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let mut entries = BTreeMap::new();
        entries.insert("apple".into(), SSTableEntry::Value("red".into()));
        entries.insert("banana".into(), SSTableEntry::Value("yellow".into()));
        entries.insert("cherry".into(), SSTableEntry::Tombstone);
        write_sstable(&path, &entries).unwrap();
        let loaded = read_sstable(&path).unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(
            loaded.get("apple"),
            Some(&SSTableEntry::Value("red".into()))
        );
        assert_eq!(
            loaded.get("banana"),
            Some(&SSTableEntry::Value("yellow".into()))
        );
        assert_eq!(loaded.get("cherry"), Some(&SSTableEntry::Tombstone));
    }

    #[test]
    fn test_sstable_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.sst");
        let entries = BTreeMap::new();
        write_sstable(&path, &entries).unwrap();
        let loaded = read_sstable(&path).unwrap();
        assert_eq!(loaded.len(), 0);
    }

    #[test]
    fn test_sstable_checksum_corruption() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let mut entries = BTreeMap::new();
        entries.insert("key1".into(), SSTableEntry::Value("val1".into()));
        write_sstable(&path, &entries).unwrap();
        let mut data = fs::read(&path).unwrap();
        data[HEADER_SIZE] ^= 0xFF;
        fs::write(&path, &data).unwrap();
        let result = read_sstable(&path);
        assert!(matches!(result, Err(PebbleError::SSTableCorruption(_))));
    }

    #[test]
    fn test_sstable_load_struct() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let mut entries = BTreeMap::new();
        entries.insert("key1".into(), SSTableEntry::Value("val1".into()));
        write_sstable(&path, &entries).unwrap();
        let sst = SSTable::load(42, &path).unwrap();
        assert_eq!(sst.id, 42);
        assert_eq!(sst.record_count(), 1);
        assert_eq!(sst.get("key1"), Some(&SSTableEntry::Value("val1".into())));
    }
}
