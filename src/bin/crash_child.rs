use std::env;
use std::io::Write;
use std::path::PathBuf;
use std::process;

use pebbledb::db::{DbOptions, PebbleDB};
use pebbledb::fault::{Fault, FaultMode, FaultPoint};

const KEY_SPACE: u64 = 24;

fn main() {
    let path = PathBuf::from(env::var("PEBBLEDB_CRASH_PATH").expect("PEBBLEDB_CRASH_PATH"));
    let seed = env_u64("PEBBLEDB_CRASH_SEED", 12_345);
    let operations = env_u64("PEBBLEDB_CRASH_OPS", 120);
    let flush_threshold = env_u64("PEBBLEDB_CRASH_FLUSH", 4) as usize;
    let l0_threshold = env_u64("PEBBLEDB_CRASH_L0", 2) as usize;
    let at = env_u64("PEBBLEDB_CRASH_AT", 1);

    let fault = env::var("PEBBLEDB_CRASH_POINT").ok().map(|name| Fault {
        point: FaultPoint::parse(&name).unwrap_or_else(|| panic!("unknown fault point: {}", name)),
        at,
        mode: match env::var("PEBBLEDB_CRASH_MODE").as_deref() {
            Ok("error") => FaultMode::Error,
            _ => FaultMode::Abort,
        },
    });

    let options = DbOptions {
        flush_threshold,
        l0_compaction_threshold: l0_threshold,
        level_size_multiplier: 2,
        target_level_size: 512,
        target_file_size: 256,
        fault,
    };

    let db = match PebbleDB::open_with_options(&path, options) {
        Ok(db) => db,
        Err(error) => {
            println!("OPEN_FAILED {}", error);
            process::exit(3);
        }
    };

    let mut state = seed;
    for index in 0..operations {
        state = next_state(state);
        let key = format!("key{:03}", state % KEY_SPACE);

        if state.is_multiple_of(5) {
            announce(&format!("TRY del {} {}", index, key));
            if let Err(error) = db.delete(&key) {
                announce(&format!("FAULT delete {} {}", index, error));
                process::exit(2);
            }
            announce(&format!("ACK del {} {}", index, key));
        } else {
            state = next_state(state);
            let value = format!("value{}_{}", index, state % 1000);
            announce(&format!("TRY put {} {} {}", index, key, value));
            if let Err(error) = db.set(key.clone(), value.clone()) {
                announce(&format!("FAULT set {} {}", index, error));
                process::exit(2);
            }
            announce(&format!("ACK put {} {} {}", index, key, value));
        }

        if index % 16 == 15
            && let Err(error) = db.flush()
        {
            announce(&format!("FAULT flush {} {}", index, error));
            process::exit(2);
        }
        if index % 32 == 31
            && let Err(error) = db.compact()
        {
            announce(&format!("FAULT compact {} {}", index, error));
            process::exit(2);
        }
    }

    announce("DONE");
}

fn announce(line: &str) {
    println!("{}", line);
    std::io::stdout().flush().expect("flush stdout");
}

fn next_state(state: u64) -> u64 {
    state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
