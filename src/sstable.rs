use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use crate::bloom::BloomFilter;
use crate::error::{PebbleError, Result};
use crate::wal::crc32;

const MAGIC: u32 = 0x50454242;
const VERSION: u32 = 2;
const OP_SET: u8 = 1;
const OP_DELETE: u8 = 2;
const HEADER_SIZE: usize = 20;
const FOOTER_SIZE: usize = 20;
const DEFAULT_BLOCK_SIZE: usize = 4096;

#[derive(Debug, Clone, PartialEq)]
pub enum SSTableEntry {
    Value(String),
    Tombstone,
}

pub struct IndexEntry {
    pub first_key: String,
    pub block_offset: u32,
    pub block_size: u32,
}

pub struct SSTable {
    pub id: u64,
    path: std::path::PathBuf,
    data: Vec<u8>,
    index: Vec<IndexEntry>,
    bloom: BloomFilter,
    record_count: usize,
    block_count: usize,
}

impl SSTable {
    pub fn load(id: u64, path: &Path) -> Result<Self> {
        let data = fs::read(path)?;
        Self::parse(id, path, data)
    }

    fn parse(id: u64, path: &Path, data: Vec<u8>) -> Result<Self> {
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
        let block_count = u32::from_le_bytes(data[12..16].try_into().unwrap()) as usize;

        let footer_start = data.len() - FOOTER_SIZE;
        let index_offset =
            u32::from_le_bytes(data[footer_start..footer_start + 4].try_into().unwrap()) as usize;
        let index_size =
            u32::from_le_bytes(data[footer_start + 4..footer_start + 8].try_into().unwrap())
                as usize;
        let bloom_offset = u32::from_le_bytes(
            data[footer_start + 8..footer_start + 12]
                .try_into()
                .unwrap(),
        ) as usize;
        let bloom_size = u32::from_le_bytes(
            data[footer_start + 12..footer_start + 16]
                .try_into()
                .unwrap(),
        ) as usize;

        let checksum_stored = u32::from_le_bytes(
            data[footer_start + 16..footer_start + 20]
                .try_into()
                .unwrap(),
        );
        let checksum_computed = crc32(&data[..data.len() - 4]);
        if checksum_stored != checksum_computed {
            return Err(PebbleError::SSTableCorruption("checksum mismatch".into()));
        }

        let index = Self::parse_index(&data[index_offset..index_offset + index_size])?;
        let bloom = BloomFilter::decode(&data[bloom_offset..bloom_offset + bloom_size])
            .ok_or_else(|| PebbleError::SSTableCorruption("invalid bloom filter".into()))?;

        Ok(Self {
            id,
            path: path.to_path_buf(),
            data,
            index,
            bloom,
            record_count,
            block_count,
        })
    }

    fn parse_index(data: &[u8]) -> Result<Vec<IndexEntry>> {
        let mut index = Vec::new();
        let mut offset = 0;
        while offset + 8 <= data.len() {
            let key_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + key_len + 8 > data.len() {
                return Err(PebbleError::SSTableCorruption("truncated index key".into()));
            }
            let first_key = String::from_utf8(data[offset..offset + key_len].to_vec())
                .map_err(|_| PebbleError::SSTableCorruption("invalid UTF-8 in index key".into()))?;
            offset += key_len;
            let block_offset = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            offset += 4;
            let block_size = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            offset += 4;
            index.push(IndexEntry {
                first_key,
                block_offset,
                block_size,
            });
        }
        Ok(index)
    }

    pub fn get(&self, key: &str) -> Option<SSTableEntry> {
        if !self.bloom.might_contain(key) {
            return None;
        }
        let block_idx = self.find_block(key)?;
        let block = self.read_block(block_idx)?;
        for (k, entry) in &block {
            match k.as_str().cmp(key) {
                std::cmp::Ordering::Equal => return Some(entry.clone()),
                std::cmp::Ordering::Greater => return None,
                _ => {}
            }
        }
        None
    }

    fn find_block(&self, key: &str) -> Option<usize> {
        let mut result = 0;
        for (i, entry) in self.index.iter().enumerate() {
            if entry.first_key.as_str() <= key {
                result = i;
            } else {
                break;
            }
        }
        Some(result)
    }

    fn read_block(&self, idx: usize) -> Option<Vec<(String, SSTableEntry)>> {
        let entry = self.index.get(idx)?;
        let start = entry.block_offset as usize;
        let end = start + entry.block_size as usize;
        if end > self.data.len() {
            return None;
        }
        Self::decode_block(&self.data[start..end]).ok()
    }

    fn decode_block(data: &[u8]) -> Result<Vec<(String, SSTableEntry)>> {
        let mut records = Vec::new();
        let mut offset = 0;
        while offset + 9 <= data.len() {
            let op = data[offset];
            offset += 1;
            let key_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let value_len =
                u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + key_len + value_len > data.len() {
                return Err(PebbleError::SSTableCorruption(
                    "truncated block record".into(),
                ));
            }
            let key = String::from_utf8(data[offset..offset + key_len].to_vec())
                .map_err(|_| PebbleError::SSTableCorruption("invalid UTF-8 in key".into()))?;
            offset += key_len;
            let entry = match op {
                OP_SET => {
                    let value = String::from_utf8(data[offset..offset + value_len].to_vec())
                        .map_err(|_| {
                            PebbleError::SSTableCorruption("invalid UTF-8 in value".into())
                        })?;
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
            records.push((key, entry));
        }
        Ok(records)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn record_count(&self) -> usize {
        self.record_count
    }
    pub fn file_size(&self) -> usize {
        self.data.len()
    }
    pub fn block_count(&self) -> usize {
        self.block_count
    }

    pub fn entries(&self) -> BTreeMap<String, SSTableEntry> {
        let mut entries = BTreeMap::new();
        for idx in 0..self.index.len() {
            if let Some(block) = self.read_block(idx) {
                for (key, entry) in block {
                    entries.insert(key, entry);
                }
            }
        }
        entries
    }

    pub fn first_key(&self) -> Option<&str> {
        self.index.first().map(|e| e.first_key.as_str())
    }

    pub fn bloom_might_contain(&self, key: &str) -> bool {
        self.bloom.might_contain(key)
    }
}

pub fn write_sstable(path: &Path, entries: &BTreeMap<String, SSTableEntry>) -> Result<()> {
    SSTable::write_sstable_with_block_size(path, entries, DEFAULT_BLOCK_SIZE)
}

impl SSTable {
    pub fn write_sstable_with_block_size(
        path: &Path,
        entries: &BTreeMap<String, SSTableEntry>,
        block_size: usize,
    ) -> Result<()> {
        let bloom = BloomFilter::from_entries(entries.keys().map(|k| k.as_str()));

        let mut blocks: Vec<Vec<u8>> = Vec::new();
        let mut index_entries: Vec<IndexEntry> = Vec::new();
        let mut current_block = Vec::new();
        let mut current_block_first_key: Option<String> = None;

        let header_placeholder = [0u8; HEADER_SIZE];
        let mut file_buf = Vec::new();
        file_buf.extend_from_slice(&header_placeholder);

        for (key, entry) in entries {
            let key_bytes = key.as_bytes();
            let (op, value_bytes) = match entry {
                SSTableEntry::Value(v) => (OP_SET, v.as_bytes()),
                SSTableEntry::Tombstone => (OP_DELETE, &b""[..]),
            };
            let mut record = Vec::new();
            record.push(op);
            record.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            record.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
            record.extend_from_slice(key_bytes);
            record.extend_from_slice(value_bytes);

            if !current_block.is_empty() && current_block.len() + record.len() > block_size {
                let block_offset = file_buf.len() as u32;
                let block_len = current_block.len() as u32;
                let first_key = current_block_first_key.take().unwrap();
                blocks.push(std::mem::take(&mut current_block));
                current_block_first_key = Some(key.clone());
                index_entries.push(IndexEntry {
                    first_key,
                    block_offset,
                    block_size: block_len,
                });
                file_buf.extend_from_slice(blocks.last().unwrap());
            }

            if current_block_first_key.is_none() {
                current_block_first_key = Some(key.clone());
            }
            current_block.extend_from_slice(&record);
        }

        if !current_block.is_empty() {
            let block_offset = file_buf.len() as u32;
            let block_len = current_block.len() as u32;
            let first_key = current_block_first_key.take().unwrap();
            blocks.push(current_block);
            file_buf.extend_from_slice(blocks.last().unwrap());
            index_entries.push(IndexEntry {
                first_key,
                block_offset,
                block_size: block_len,
            });
        }

        let mut index_data = Vec::new();
        for ie in &index_entries {
            let fk = ie.first_key.as_bytes();
            index_data.extend_from_slice(&(fk.len() as u32).to_le_bytes());
            index_data.extend_from_slice(fk);
            index_data.extend_from_slice(&ie.block_offset.to_le_bytes());
            index_data.extend_from_slice(&ie.block_size.to_le_bytes());
        }

        let index_offset = file_buf.len() as u32;
        file_buf.extend_from_slice(&index_data);
        let index_size = index_data.len() as u32;

        let bloom_encoded = bloom.encode();
        let bloom_offset = file_buf.len() as u32;
        file_buf.extend_from_slice(&bloom_encoded);
        let bloom_size = bloom_encoded.len() as u32;

        file_buf.extend_from_slice(&index_offset.to_le_bytes());
        file_buf.extend_from_slice(&index_size.to_le_bytes());
        file_buf.extend_from_slice(&bloom_offset.to_le_bytes());
        file_buf.extend_from_slice(&bloom_size.to_le_bytes());

        file_buf[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        file_buf[4..8].copy_from_slice(&VERSION.to_le_bytes());
        file_buf[8..12].copy_from_slice(&(entries.len() as u32).to_le_bytes());
        file_buf[12..16].copy_from_slice(&(blocks.len() as u32).to_le_bytes());
        file_buf[16..20].copy_from_slice(&(block_size as u32).to_le_bytes());

        let checksum = crc32(&file_buf);
        file_buf.extend_from_slice(&checksum.to_le_bytes());

        fs::write(path, &file_buf)?;
        Ok(())
    }
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
        let sst = SSTable::load(0, &path).unwrap();
        assert_eq!(sst.record_count(), 3);
        assert_eq!(sst.get("apple"), Some(SSTableEntry::Value("red".into())));
        assert_eq!(
            sst.get("banana"),
            Some(SSTableEntry::Value("yellow".into()))
        );
        assert_eq!(sst.get("cherry"), Some(SSTableEntry::Tombstone));
    }

    #[test]
    fn test_sstable_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.sst");
        let entries = BTreeMap::new();
        write_sstable(&path, &entries).unwrap();
        let sst = SSTable::load(0, &path).unwrap();
        assert_eq!(sst.record_count(), 0);
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
        let result = SSTable::load(0, &path);
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
        assert_eq!(sst.get("key1"), Some(SSTableEntry::Value("val1".into())));
    }

    #[test]
    fn test_sstable_block_based_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let mut entries = BTreeMap::new();
        for i in 0..100 {
            entries.insert(
                format!("key_{:04}", i),
                SSTableEntry::Value(format!("val_{}", i)),
            );
        }
        write_sstable(&path, &entries).unwrap();
        let sst = SSTable::load(0, &path).unwrap();
        assert_eq!(sst.record_count(), 100);
        assert_eq!(
            sst.get("key_0050"),
            Some(SSTableEntry::Value("val_50".into()))
        );
        assert_eq!(
            sst.get("key_0099"),
            Some(SSTableEntry::Value("val_99".into()))
        );
        assert_eq!(
            sst.get("key_0000"),
            Some(SSTableEntry::Value("val_0".into()))
        );
    }

    #[test]
    fn test_sstable_bloom_rejects_nonexistent() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let mut entries = BTreeMap::new();
        for i in 0..100 {
            entries.insert(
                format!("key_{}", i),
                SSTableEntry::Value(format!("val_{}", i)),
            );
        }
        write_sstable(&path, &entries).unwrap();
        let sst = SSTable::load(0, &path).unwrap();
        assert_eq!(sst.get("nonexistent_key_xyz"), None);
    }

    #[test]
    fn test_sstable_small_blocks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let mut entries = BTreeMap::new();
        for i in 0..50 {
            entries.insert(format!("k{:04}", i), SSTableEntry::Value(format!("v{}", i)));
        }
        SSTable::write_sstable_with_block_size(&path, &entries, 64).unwrap();
        let sst = SSTable::load(0, &path).unwrap();
        assert_eq!(sst.record_count(), 50);
        assert_eq!(sst.get("k0025"), Some(SSTableEntry::Value("v25".into())));
    }

    #[test]
    fn test_sstable_entries_method() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let mut entries = BTreeMap::new();
        entries.insert("a".into(), SSTableEntry::Value("1".into()));
        entries.insert("b".into(), SSTableEntry::Tombstone);
        write_sstable(&path, &entries).unwrap();
        let sst = SSTable::load(0, &path).unwrap();
        let collected = sst.entries();
        assert_eq!(collected.len(), 2);
        assert_eq!(collected.get("a"), Some(&SSTableEntry::Value("1".into())));
        assert_eq!(collected.get("b"), Some(&SSTableEntry::Tombstone));
    }

    #[test]
    fn test_sstable_first_key() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");
        let mut entries = BTreeMap::new();
        entries.insert("cherry".into(), SSTableEntry::Value("red".into()));
        entries.insert("apple".into(), SSTableEntry::Value("green".into()));
        write_sstable(&path, &entries).unwrap();
        let sst = SSTable::load(0, &path).unwrap();
        assert_eq!(sst.first_key(), Some("apple"));
    }
}
