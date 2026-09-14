use std::time::Instant;

use pebbledb::db::PebbleDB;

fn bench(name: &str, iterations: usize, f: impl Fn(usize)) {
    let start = Instant::now();
    for i in 0..iterations {
        f(i);
    }
    let elapsed = start.elapsed();
    let ops_sec = (iterations as f64 / elapsed.as_secs_f64()) as u64;
    println!("  {:<12} {:>8.2?}  ({} ops/sec)", name, elapsed, ops_sec);
}

fn main() {
    let iterations = 1_000;

    println!("=== PebbleDB Benchmark ===\n");

    // --- Memtable only (no flush) ---
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bench_memtable");
    let db = PebbleDB::open_with_threshold(&path, usize::MAX).unwrap();

    println!("Memtable only (no flush, {} ops):", iterations);
    bench("SET", iterations, |i| {
        db.set(format!("key_{:06}", i), format!("value_{}", i))
            .unwrap();
    });
    bench("GET", iterations, |i| {
        db.get(&format!("key_{:06}", i)).unwrap();
    });
    bench("MIXED", iterations, |i| {
        if i % 2 == 0 {
            db.set(format!("key_{:06}", i), format!("updated_{}", i))
                .unwrap();
        } else {
            db.get(&format!("key_{:06}", i)).unwrap();
        }
    });
    let stats = db.stats();
    println!(
        "  Bloom rejects: {}, Block reads: {}\n",
        stats.bloom_rejects, stats.block_reads
    );
    db.close().unwrap();

    // --- SSTable-backed ---
    let dir2 = tempfile::tempdir().unwrap();
    let path2 = dir2.path().join("bench_sstable");
    let db2 = PebbleDB::open_with_threshold(&path2, 500).unwrap();

    println!("SSTable-backed (threshold=500, {} ops):", iterations);
    bench("SET", iterations, |i| {
        db2.set(format!("key_{:06}", i), format!("value_{}", i))
            .unwrap();
    });
    bench("GET", iterations, |i| {
        db2.get(&format!("key_{:06}", i)).unwrap();
    });
    bench("MIXED", iterations, |i| {
        if i % 2 == 0 {
            db2.set(format!("key_{:06}", i), format!("updated_{}", i))
                .unwrap();
        } else {
            db2.get(&format!("key_{:06}", i)).unwrap();
        }
    });
    let stats = db2.stats();
    println!(
        "  SSTables: {}, Total size: {} bytes",
        stats.sst_count, stats.total_sst_size
    );
    println!(
        "  Bloom rejects: {}, Block reads: {}\n",
        stats.bloom_rejects, stats.block_reads
    );
    db2.close().unwrap();

    // --- Concurrent access ---
    let dir3 = tempfile::tempdir().unwrap();
    let path3 = dir3.path().join("bench_concurrent");
    let db3 = PebbleDB::open_with_threshold(&path3, usize::MAX).unwrap();

    let threads = 4;
    let ops_per_thread = iterations / threads;
    println!(
        "Concurrent ({} threads, {} ops each):",
        threads, ops_per_thread
    );

    let start = Instant::now();
    let mut handles = Vec::new();
    for t in 0..threads {
        let db = db3.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = format!("t{}_{:06}", t, i);
                db.set(key.clone(), format!("value_{}", i)).unwrap();
                db.get(&key).unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let elapsed = start.elapsed();
    let total_ops = threads * ops_per_thread;
    let ops_sec = (total_ops as f64 / elapsed.as_secs_f64()) as u64;
    println!("  Total:       {:>8.2?}  ({} ops/sec)", elapsed, ops_sec);

    db3.close().unwrap();
}
