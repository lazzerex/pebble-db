use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

use pebbledb::db::PebbleDB;
use pebbledb::error::PebbleError;
use tempfile::tempdir;

fn seed_database(path: &Path) -> Vec<(String, String)> {
    let db = PebbleDB::open(path).unwrap();
    let entries = vec![
        ("alpha".to_string(), "1".to_string()),
        ("beta".to_string(), "2".to_string()),
        ("gamma".to_string(), "3".to_string()),
    ];
    for (key, value) in &entries {
        db.set(key.clone(), value.clone()).unwrap();
    }
    entries
}

fn append_to_wal(path: &Path, bytes: &[u8]) {
    let mut file = OpenOptions::new()
        .append(true)
        .open(path.join("wal.log"))
        .unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

fn assert_entries_present(path: &Path, entries: &[(String, String)]) {
    let db = PebbleDB::open(path).unwrap();
    for (key, value) in entries {
        assert_eq!(db.get(key), Some(value.clone()));
    }
}

#[test]
fn torn_final_record_is_discarded() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let entries = seed_database(&path);

    let wal = path.join("wal.log");
    let complete_len = fs::metadata(&wal).unwrap().len();
    append_to_wal(&path, &[40, 0, 0, 0, 1, 2, 3]);

    assert_entries_present(&path, &entries);
    assert_eq!(fs::metadata(&wal).unwrap().len(), complete_len);
}

#[test]
fn partially_written_record_payload_is_discarded() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let entries = seed_database(&path);

    let wal = path.join("wal.log");
    let complete_len = fs::metadata(&wal).unwrap().len();
    append_to_wal(&path, &[100, 0, 0, 0, 1, b'x']);

    assert_entries_present(&path, &entries);
    assert_eq!(fs::metadata(&wal).unwrap().len(), complete_len);
}

#[test]
fn garbage_tail_shorter_than_a_record_is_discarded() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let entries = seed_database(&path);

    let wal = path.join("wal.log");
    let complete_len = fs::metadata(&wal).unwrap().len();
    append_to_wal(&path, &[0u8; 10]);

    assert_entries_present(&path, &entries);
    assert_eq!(fs::metadata(&wal).unwrap().len(), complete_len);
}

#[test]
fn torn_tail_recovery_is_repeatable_and_accepts_new_writes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let entries = seed_database(&path);
    append_to_wal(&path, &[64, 0, 0, 0, 1, 2]);

    assert_entries_present(&path, &entries);
    assert_entries_present(&path, &entries);

    let db = PebbleDB::open(&path).unwrap();
    db.set("delta".into(), "4".into()).unwrap();
    drop(db);

    assert_entries_present(&path, &entries);
    let db = PebbleDB::open(&path).unwrap();
    assert_eq!(db.get("delta"), Some("4".into()));
}

#[test]
fn corrupted_earlier_record_prevents_recovery() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    seed_database(&path);

    let wal = path.join("wal.log");
    let mut data = fs::read(&wal).unwrap();
    data[6] ^= 0xFF;
    fs::write(&wal, &data).unwrap();

    let result = PebbleDB::open(&path);
    assert!(matches!(result, Err(PebbleError::WalCorruption(_))));
}

#[test]
fn garbage_tail_with_intact_length_but_bad_checksum_prevents_recovery() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    seed_database(&path);

    append_to_wal(
        &path,
        &[
            18, 0, 0, 0, 1, 4, 0, 0, 0, 5, 0, 0, 0, b'g', b'a', b'r', b'b', b'v', b'a', b'l', b'u',
            b'e', 0, 0, 0, 0,
        ],
    );

    let result = PebbleDB::open(&path);
    assert!(matches!(result, Err(PebbleError::WalCorruption(_))));
}

#[test]
fn empty_wal_is_recovered_as_an_empty_database() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    fs::create_dir_all(&path).unwrap();
    fs::write(path.join("wal.log"), b"").unwrap();

    let db = PebbleDB::open(&path).unwrap();
    assert_eq!(db.scan(), Vec::new());
}

#[test]
fn torn_wal_tail_does_not_block_later_writes() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");

    let db = PebbleDB::open(&path).unwrap();
    db.set("survivor".into(), "value".into()).unwrap();
    drop(db);

    append_to_wal(&path, &[48, 0, 0, 0, 2, 5, 0, 0, 0]);

    let db = PebbleDB::open(&path).unwrap();
    assert_eq!(db.get("survivor"), Some("value".into()));
    db.set("next".into(), "value".into()).unwrap();
    drop(db);

    let db = PebbleDB::open(&path).unwrap();
    assert_eq!(db.get("survivor"), Some("value".into()));
    assert_eq!(db.get("next"), Some("value".into()));
}
