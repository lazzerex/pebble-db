use std::time::Instant;

use pebbledb::db::PebbleDB;

fn main() {
    let iterations = 1_000;

    println!("PebbleDB Benchmark");
    println!("{} operations per test\n", iterations);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bench_memtable");
    let mut db = PebbleDB::open_with_threshold(&path, usize::MAX).unwrap();

    let start = Instant::now();
    for i in 0..iterations {
        db.set(format!("key_{:06}", i), format!("value_{}", i))
            .unwrap();
    }
    let set_time = start.elapsed();

    let start = Instant::now();
    for i in 0..iterations {
        db.get(&format!("key_{:06}", i)).unwrap();
    }
    let get_time = start.elapsed();

    let start = Instant::now();
    for i in 0..iterations {
        if i % 2 == 0 {
            db.set(format!("key_{:06}", i), format!("updated_{}", i))
                .unwrap();
        } else {
            db.get(&format!("key_{:06}", i)).unwrap();
        }
    }
    let mixed_time = start.elapsed();

    println!("Memtable only (no flush):");
    println!(
        "  SET:     {:?} ({} ops/sec)",
        set_time,
        (iterations as f64 / set_time.as_secs_f64()) as u64
    );
    println!(
        "  GET:     {:?} ({} ops/sec)",
        get_time,
        (iterations as f64 / get_time.as_secs_f64()) as u64
    );
    println!(
        "  MIXED:   {:?} ({} ops/sec)",
        mixed_time,
        (iterations as f64 / mixed_time.as_secs_f64()) as u64
    );

    db.close().unwrap();

    println!();

    let dir2 = tempfile::tempdir().unwrap();
    let path2 = dir2.path().join("bench_sstable");
    let mut db2 = PebbleDB::open_with_threshold(&path2, 500).unwrap();

    let start = Instant::now();
    for i in 0..iterations {
        db2.set(format!("key_{:06}", i), format!("value_{}", i))
            .unwrap();
    }
    let set_time = start.elapsed();

    let start = Instant::now();
    for i in 0..iterations {
        db2.get(&format!("key_{:06}", i)).unwrap();
    }
    let get_time = start.elapsed();

    let start = Instant::now();
    for i in 0..iterations {
        if i % 2 == 0 {
            db2.set(format!("key_{:06}", i), format!("updated_{}", i))
                .unwrap();
        } else {
            db2.get(&format!("key_{:06}", i)).unwrap();
        }
    }
    let mixed_time = start.elapsed();

    let stats = db2.stats();

    println!("SSTable-backed (threshold=500):");
    println!(
        "  SET:     {:?} ({} ops/sec)",
        set_time,
        (iterations as f64 / set_time.as_secs_f64()) as u64
    );
    println!(
        "  GET:     {:?} ({} ops/sec)",
        get_time,
        (iterations as f64 / get_time.as_secs_f64()) as u64
    );
    println!(
        "  MIXED:   {:?} ({} ops/sec)",
        mixed_time,
        (iterations as f64 / mixed_time.as_secs_f64()) as u64
    );
    println!(
        "  SSTables: {}, Total size: {} bytes",
        stats.sst_count, stats.total_sst_size
    );
}
