mod common;

use std::env;

use common::*;
use pebbledb::db::PebbleDB;
use pebbledb::fault::{FaultMode, FaultPoint};
use tempfile::tempdir;

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[test]
#[ignore]
fn crash_stress() {
    let seed = env_u64("PEBBLEDB_STRESS_SEED", 12_345);
    let operations = env_u64("PEBBLEDB_STRESS_OPS", 120);
    let crashes = env_u64("PEBBLEDB_STRESS_CRASHES", 30);
    let mode = match env::var("PEBBLEDB_STRESS_MODE").as_deref() {
        Ok("error") => FaultMode::Error,
        _ => FaultMode::Abort,
    };

    let mut recovery_failures = 0;
    let mut data_mismatches = 0;

    for index in 0..crashes {
        let point = FaultPoint::ALL[index as usize % FaultPoint::ALL.len()];
        let hit = 1 + index / FaultPoint::ALL.len() as u64;
        let run_seed = seed.wrapping_add(index);
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");

        let child = run_crash_child(&path, run_seed, operations, point, hit, mode);
        let context = format!(
            "seed {} operations {} point {} hit {} acknowledged {}",
            run_seed,
            operations,
            point.name(),
            hit,
            child.acknowledged.len()
        );

        match PebbleDB::open(&path) {
            Err(error) => {
                recovery_failures += 1;
                println!("recovery failure: {} ({})", context, error);
            }
            Ok(db) => {
                if let Err(error) = check_state(&db, &child.acknowledged, child.in_flight.as_ref())
                {
                    data_mismatches += 1;
                    println!("data mismatch: {} ({})", context, error);
                } else {
                    println!("{}: ok", context);
                }
                drop(db);
            }
        }
    }

    println!();
    println!("seed:               {}", seed);
    println!("operations:         {}", operations);
    println!("crashes:            {}", crashes);
    println!("recovery failures:  {}", recovery_failures);
    println!("data mismatches:    {}", data_mismatches);

    assert_eq!(recovery_failures, 0, "recovery failures detected");
    assert_eq!(data_mismatches, 0, "data mismatches detected");
}
