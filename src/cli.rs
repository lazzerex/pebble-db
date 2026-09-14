use std::env;
use std::process;

use pebbledb::db::PebbleDB;

const DEFAULT_DB_PATH: &str = ".pebbledb";

pub fn run() {
    let args: Vec<String> = env::args().skip(1).collect();

    if args.is_empty() {
        eprintln!("Usage: pebbledb <command> [args]");
        eprintln!("Commands: set, get, delete, exists, keys, scan, compact, stats");
        process::exit(1);
    }

    let db = PebbleDB::open(DEFAULT_DB_PATH);
    let db = match db {
        Ok(db) => db,
        Err(e) => {
            eprintln!("Error opening database: {}", e);
            process::exit(1);
        }
    };

    let command = args[0].as_str();
    match command {
        "set" => {
            if args.len() < 3 {
                eprintln!("Usage: pebbledb set <key> <value>");
                process::exit(1);
            }
            match db.set(args[1].clone(), args[2].clone()) {
                Ok(()) => println!("OK"),
                Err(e) => {
                    eprintln!("Error: {}", e);
                    process::exit(1);
                }
            }
        }
        "get" => {
            if args.len() < 2 {
                eprintln!("Usage: pebbledb get <key>");
                process::exit(1);
            }
            match db.get(&args[1]) {
                Some(value) => println!("{}", value),
                None => println!("(null)"),
            }
        }
        "delete" => {
            if args.len() < 2 {
                eprintln!("Usage: pebbledb delete <key>");
                process::exit(1);
            }
            match db.delete(&args[1]) {
                Ok(true) => println!("OK"),
                Ok(false) => println!("(null)"),
                Err(e) => {
                    eprintln!("Error: {}", e);
                    process::exit(1);
                }
            }
        }
        "exists" => {
            if args.len() < 2 {
                eprintln!("Usage: pebbledb exists <key>");
                process::exit(1);
            }
            if db.exists(&args[1]) {
                println!("true");
            } else {
                println!("false");
            }
        }
        "keys" => {
            for key in db.keys() {
                println!("{}", key);
            }
        }
        "scan" => {
            for (key, value) in db.scan() {
                println!("{}: {}", key, value);
            }
        }
        "compact" => match db.compact() {
            Ok(()) => println!("OK"),
            Err(e) => {
                eprintln!("Error: {}", e);
                process::exit(1);
            }
        },
        "stats" => {
            let stats = db.stats();
            println!("Memtable");
            println!("  entries:       {}", stats.memtable_entries);
            println!("  bytes:         {}", stats.memtable_bytes);
            println!("SSTables");
            println!("  count:         {}", stats.sst_count);
            println!("  total size:    {} bytes", stats.total_sst_size);
            for (id, size) in &stats.sst_sizes {
                println!("  sst_{}:        {} bytes", id, size);
            }
            println!("WAL");
            println!("  size:          {} bytes", stats.wal_size);
            println!("Performance Counters");
            println!("  memtable hits: {}", stats.memtable_hits);
            println!("  bloom rejects: {}", stats.bloom_rejects);
            println!("  block reads:   {}", stats.block_reads);
            println!("  compactions:   {}", stats.compactions_completed);
        }
        _ => {
            eprintln!("Unknown command: {}", command);
            eprintln!("Commands: set, get, delete, exists, keys, scan, compact, stats");
            process::exit(1);
        }
    }
}
