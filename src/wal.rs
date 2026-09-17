use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use crate::error::{PebbleError, Result};

const OP_SET: u8 = 1;
const OP_DELETE: u8 = 2;

const CRC32_POLY: u32 = 0xEDB88320;

const CRC32_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ CRC32_POLY;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

pub(crate) fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFFFFFFu32;
    for &byte in data {
        crc = (crc >> 8) ^ CRC32_TABLE[((crc ^ byte as u32) & 0xFF) as usize];
    }
    crc ^ 0xFFFFFFFF
}

#[derive(Debug, Clone, PartialEq)]
pub enum WalRecord {
    Set { key: String, value: String },
    Delete { key: String },
}

impl WalRecord {
    fn encode(&self) -> Vec<u8> {
        match self {
            WalRecord::Set { key, value } => {
                let key_bytes = key.as_bytes();
                let value_bytes = value.as_bytes();
                let key_len = key_bytes.len() as u32;
                let value_len = value_bytes.len() as u32;
                let total_len = 1 + 4 + 4 + key_len + value_len;

                let mut buf = Vec::with_capacity(4 + total_len as usize + 4);
                buf.extend_from_slice(&total_len.to_le_bytes());
                buf.push(OP_SET);
                buf.extend_from_slice(&key_len.to_le_bytes());
                buf.extend_from_slice(&value_len.to_le_bytes());
                buf.extend_from_slice(key_bytes);
                buf.extend_from_slice(value_bytes);

                let checksum = crc32(&buf);
                buf.extend_from_slice(&checksum.to_le_bytes());
                buf
            }
            WalRecord::Delete { key } => {
                let key_bytes = key.as_bytes();
                let key_len = key_bytes.len() as u32;
                let value_len = 0u32;
                let total_len = 1 + 4 + 4 + key_len;

                let mut buf = Vec::with_capacity(4 + total_len as usize + 4);
                buf.extend_from_slice(&total_len.to_le_bytes());
                buf.push(OP_DELETE);
                buf.extend_from_slice(&key_len.to_le_bytes());
                buf.extend_from_slice(&value_len.to_le_bytes());
                buf.extend_from_slice(key_bytes);

                let checksum = crc32(&buf);
                buf.extend_from_slice(&checksum.to_le_bytes());
                buf
            }
        }
    }

    fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 9 {
            return Err(PebbleError::WalCorruption("record too short".into()));
        }

        let op = data[0];
        let key_len = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
        let value_len = u32::from_le_bytes([data[5], data[6], data[7], data[8]]) as usize;

        if data.len() < 9 + key_len + value_len {
            return Err(PebbleError::WalCorruption("record truncated".into()));
        }

        let key = String::from_utf8(data[9..9 + key_len].to_vec())
            .map_err(|_| PebbleError::WalCorruption("invalid UTF-8 in key".into()))?;

        match op {
            OP_SET => {
                let value = String::from_utf8(data[9 + key_len..9 + key_len + value_len].to_vec())
                    .map_err(|_| PebbleError::WalCorruption("invalid UTF-8 in value".into()))?;
                Ok(WalRecord::Set { key, value })
            }
            OP_DELETE => {
                if value_len != 0 {
                    return Err(PebbleError::WalCorruption(
                        "DELETE record has non-zero value length".into(),
                    ));
                }
                Ok(WalRecord::Delete { key })
            }
            _ => Err(PebbleError::WalCorruption(format!(
                "unknown operation: {}",
                op
            ))),
        }
    }
}

pub struct WalWriter {
    writer: BufWriter<File>,
}

impl WalWriter {
    pub fn open(path: &Path) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    pub fn append_record(&mut self, record: &WalRecord) -> Result<()> {
        let encoded = record.encode();
        self.writer.write_all(&encoded)?;
        self.writer.flush()?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    pub fn write_record(&mut self, record: &WalRecord) -> Result<()> {
        self.append_record(record)?;
        self.sync()
    }

    pub fn truncate(&mut self) -> Result<()> {
        self.writer.flush()?;
        let file = self.writer.get_ref();
        file.set_len(0)?;
        file.sync_all()?;
        Ok(())
    }

    pub fn close(mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }
}

pub struct WalRecovery {
    pub records: Vec<WalRecord>,
    pub valid_len: u64,
}

pub struct WalReader {
    reader: BufReader<File>,
    file_len: u64,
}

impl WalReader {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let file_len = file.metadata()?.len();
        Ok(Self {
            reader: BufReader::new(file),
            file_len,
        })
    }

    pub fn read_all(&mut self) -> Result<Vec<WalRecord>> {
        Ok(self.read_records(false)?.records)
    }

    pub fn recover(&mut self) -> Result<WalRecovery> {
        self.read_records(true)
    }

    fn read_records(&mut self, tolerate_torn_tail: bool) -> Result<WalRecovery> {
        let mut records = Vec::new();
        let mut valid_len = 0u64;

        loop {
            let mut len_buf = [0u8; 4];
            let filled = self.read_len_prefix(&mut len_buf)?;
            if filled == 0 {
                break;
            }
            if filled < len_buf.len() {
                return self.stop_at_tail(records, valid_len, tolerate_torn_tail);
            }

            let total_len = u32::from_le_bytes(len_buf) as usize;
            if total_len < 9 {
                if tolerate_torn_tail {
                    return Ok(WalRecovery { records, valid_len });
                }
                break;
            }
            if valid_len + 8 + total_len as u64 > self.file_len {
                return self.stop_at_tail(records, valid_len, tolerate_torn_tail);
            }

            let mut payload = vec![0u8; 4 + total_len + 4];
            payload[..4].copy_from_slice(&len_buf);
            match self.reader.read_exact(&mut payload[4..]) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    return self.stop_at_tail(records, valid_len, tolerate_torn_tail);
                }
                Err(e) => return Err(e.into()),
            }

            let record_bytes = &payload[4..4 + total_len];
            let stored = u32::from_le_bytes(
                payload[4 + total_len..4 + total_len + 4]
                    .try_into()
                    .unwrap(),
            );
            let computed = crc32(&payload[..4 + total_len]);
            if computed != stored {
                return Err(PebbleError::WalCorruption("checksum mismatch".into()));
            }

            let record = WalRecord::decode(record_bytes)?;
            records.push(record);
            valid_len += 8 + total_len as u64;
        }

        Ok(WalRecovery { records, valid_len })
    }

    fn read_len_prefix(&mut self, buf: &mut [u8; 4]) -> Result<usize> {
        let mut filled = 0;
        while filled < buf.len() {
            let read = self.reader.read(&mut buf[filled..])?;
            if read == 0 {
                break;
            }
            filled += read;
        }
        Ok(filled)
    }

    fn stop_at_tail(
        &self,
        records: Vec<WalRecord>,
        valid_len: u64,
        tolerate_torn_tail: bool,
    ) -> Result<WalRecovery> {
        if tolerate_torn_tail {
            Ok(WalRecovery { records, valid_len })
        } else {
            Err(PebbleError::WalIncomplete)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_wal_write_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let mut writer = WalWriter::open(&path).unwrap();
        writer
            .write_record(&WalRecord::Set {
                key: "key1".into(),
                value: "value1".into(),
            })
            .unwrap();
        writer
            .write_record(&WalRecord::Set {
                key: "key2".into(),
                value: "value2".into(),
            })
            .unwrap();
        writer
            .write_record(&WalRecord::Delete { key: "key1".into() })
            .unwrap();
        writer.close().unwrap();

        let mut reader = WalReader::open(&path).unwrap();
        let records = reader.read_all().unwrap();

        assert_eq!(records.len(), 3);
        assert_eq!(
            records[0],
            WalRecord::Set {
                key: "key1".into(),
                value: "value1".into()
            }
        );
        assert_eq!(
            records[1],
            WalRecord::Set {
                key: "key2".into(),
                value: "value2".into()
            }
        );
        assert_eq!(records[2], WalRecord::Delete { key: "key1".into() });
    }

    #[test]
    fn test_wal_incomplete_trailing_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let mut writer = WalWriter::open(&path).unwrap();
        writer
            .write_record(&WalRecord::Set {
                key: "key1".into(),
                value: "value1".into(),
            })
            .unwrap();
        writer.close().unwrap();

        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0u8; 10]).unwrap();
        drop(file);

        let mut reader = WalReader::open(&path).unwrap();
        let records = reader.read_all().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0],
            WalRecord::Set {
                key: "key1".into(),
                value: "value1".into()
            }
        );
    }

    #[test]
    fn test_wal_truncated_payload() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let mut writer = WalWriter::open(&path).unwrap();
        writer
            .write_record(&WalRecord::Set {
                key: "key1".into(),
                value: "value1".into(),
            })
            .unwrap();
        writer.close().unwrap();

        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[100, 0, 0, 0, 1]).unwrap();
        drop(file);

        let mut reader = WalReader::open(&path).unwrap();
        let result = reader.read_all();
        assert!(matches!(result, Err(PebbleError::WalIncomplete)));
    }

    #[test]
    fn test_wal_checksum_corruption() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let mut writer = WalWriter::open(&path).unwrap();
        writer
            .write_record(&WalRecord::Set {
                key: "key1".into(),
                value: "value1".into(),
            })
            .unwrap();
        writer.close().unwrap();

        let mut data = fs::read(&path).unwrap();
        data[5] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let mut reader = WalReader::open(&path).unwrap();
        let result = reader.read_all();
        assert!(matches!(result, Err(PebbleError::WalCorruption(_))));
    }

    #[test]
    fn test_empty_wal() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        fs::write(&path, b"").unwrap();

        let mut reader = WalReader::open(&path).unwrap();
        let records = reader.read_all().unwrap();
        assert_eq!(records.len(), 0);
    }

    #[test]
    fn test_recover_discards_torn_final_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let mut writer = WalWriter::open(&path).unwrap();
        writer
            .write_record(&WalRecord::Set {
                key: "key1".into(),
                value: "value1".into(),
            })
            .unwrap();
        writer.close().unwrap();

        let complete_len = fs::metadata(&path).unwrap().len();
        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[40, 0, 0, 0, 1, 2, 3]).unwrap();
        drop(file);

        let mut reader = WalReader::open(&path).unwrap();
        let recovery = reader.recover().unwrap();
        assert_eq!(recovery.records.len(), 1);
        assert_eq!(recovery.valid_len, complete_len);

        assert!(matches!(
            WalReader::open(&path).unwrap().read_all(),
            Err(PebbleError::WalIncomplete)
        ));
    }

    #[test]
    fn test_recover_discards_partial_record_payload() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let mut writer = WalWriter::open(&path).unwrap();
        writer
            .write_record(&WalRecord::Delete { key: "gone".into() })
            .unwrap();
        writer.close().unwrap();

        let complete_len = fs::metadata(&path).unwrap().len();
        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[100, 0, 0, 0, 1]).unwrap();
        drop(file);

        let recovery = WalReader::open(&path).unwrap().recover().unwrap();
        assert_eq!(
            recovery.records,
            vec![WalRecord::Delete { key: "gone".into() }]
        );
        assert_eq!(recovery.valid_len, complete_len);
    }

    #[test]
    fn test_recover_rejects_checksum_corruption() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let mut writer = WalWriter::open(&path).unwrap();
        writer
            .write_record(&WalRecord::Set {
                key: "key1".into(),
                value: "value1".into(),
            })
            .unwrap();
        writer
            .write_record(&WalRecord::Set {
                key: "key2".into(),
                value: "value2".into(),
            })
            .unwrap();
        writer.close().unwrap();

        let mut data = fs::read(&path).unwrap();
        data[5] ^= 0xFF;
        fs::write(&path, &data).unwrap();

        let result = WalReader::open(&path).unwrap().recover();
        assert!(matches!(result, Err(PebbleError::WalCorruption(_))));
    }

    #[test]
    fn test_recover_rejects_corrupt_tail_with_valid_checksum_length() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let mut writer = WalWriter::open(&path).unwrap();
        writer
            .write_record(&WalRecord::Set {
                key: "key1".into(),
                value: "value1".into(),
            })
            .unwrap();
        writer.close().unwrap();

        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[
            18, 0, 0, 0, 1, 4, 0, 0, 0, 5, 0, 0, 0, b'g', b'a', b'r', b'b', b'v', b'a', b'l', b'u',
            b'e', 0, 0, 0, 0,
        ])
        .unwrap();
        drop(file);

        let result = WalReader::open(&path).unwrap().recover();
        assert!(matches!(result, Err(PebbleError::WalCorruption(_))));
    }

    #[test]
    fn test_recover_stops_at_garbage_tail_shorter_than_a_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        let mut writer = WalWriter::open(&path).unwrap();
        writer
            .write_record(&WalRecord::Set {
                key: "key1".into(),
                value: "value1".into(),
            })
            .unwrap();
        writer.close().unwrap();

        let complete_len = fs::metadata(&path).unwrap().len();
        let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0u8; 10]).unwrap();
        drop(file);

        let recovery = WalReader::open(&path).unwrap().recover().unwrap();
        assert_eq!(recovery.records.len(), 1);
        assert_eq!(recovery.valid_len, complete_len);
    }
}
