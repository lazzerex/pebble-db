#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

use pebbledb::db::{DbOptions, PebbleDB};
use pebbledb::fault::{Fault, FaultMode, FaultPoint};

pub const KEY_SPACE: u64 = 24;
pub const SEED: u64 = 7;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Put { key: String, value: String },
    Delete { key: String },
}

impl Op {
    pub fn key(&self) -> &str {
        match self {
            Op::Put { key, .. } => key,
            Op::Delete { key } => key,
        }
    }

    pub fn result(&self) -> Option<String> {
        match self {
            Op::Put { value, .. } => Some(value.clone()),
            Op::Delete { .. } => None,
        }
    }
}

pub fn next_state(state: u64) -> u64 {
    state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407)
}

pub fn workload_entry(state: u64, index: u64) -> (u64, Op) {
    let state = next_state(state);
    let key = format!("key{:03}", state % KEY_SPACE);
    if state.is_multiple_of(5) {
        (state, Op::Delete { key })
    } else {
        let state = next_state(state);
        let value = format!("value{}_{}", index, state % 1000);
        (state, Op::Put { key, value })
    }
}

pub fn scenario_options(
    flush_threshold: usize,
    l0_threshold: usize,
    fault: Option<Fault>,
) -> DbOptions {
    DbOptions {
        flush_threshold,
        l0_compaction_threshold: l0_threshold,
        level_size_multiplier: 2,
        target_level_size: 512,
        target_file_size: 256,
        fault,
    }
}

pub fn model(operations: &[Op]) -> BTreeMap<String, Option<String>> {
    let mut model = BTreeMap::new();
    for operation in operations {
        model.insert(operation.key().to_string(), operation.result());
    }
    model
}

pub fn check_state(
    db: &PebbleDB,
    acknowledged: &[Op],
    in_flight: Option<&Op>,
) -> Result<(), String> {
    let model = model(acknowledged);
    let mut keys: Vec<String> = model.keys().cloned().collect();
    if let Some(operation) = in_flight
        && !keys.iter().any(|key| key == operation.key())
    {
        keys.push(operation.key().to_string());
    }
    keys.sort();

    for key in &keys {
        let before = model.get(key).cloned().unwrap_or(None);
        let mut allowed = vec![before];
        if let Some(operation) = in_flight.filter(|operation| operation.key() == key) {
            let after = operation.result();
            if !allowed.contains(&after) {
                allowed.push(after);
            }
        }
        let actual = db.get(key);
        if !allowed.contains(&actual) {
            return Err(format!(
                "key {} expected one of {:?} but database returned {:?}",
                key, allowed, actual
            ));
        }
    }

    let mut expected_pairs: Vec<(String, String)> = model
        .iter()
        .filter_map(|(key, value)| value.clone().map(|value| (key.clone(), value)))
        .collect();
    let mut actual_pairs = db.scan();
    if let Some(operation) = in_flight {
        expected_pairs.retain(|(key, _)| key != operation.key());
        actual_pairs.retain(|(key, _)| key != operation.key());
    }
    if actual_pairs != expected_pairs {
        return Err(format!(
            "range mismatch: expected {:?} but database returned {:?}",
            expected_pairs, actual_pairs
        ));
    }

    Ok(())
}

pub fn verify_recovery(
    path: &Path,
    acknowledged: &[Op],
    in_flight: Option<&Op>,
) -> Result<(), String> {
    for attempt in 0..3 {
        let db = PebbleDB::open(path)
            .map_err(|error| format!("reopen {} failed: {}", attempt, error))?;
        check_state(&db, acknowledged, in_flight)
            .map_err(|error| format!("reopen {} mismatch: {}", attempt, error))?;
    }
    Ok(())
}

pub struct InProcessRun {
    pub acknowledged: Vec<Op>,
    pub in_flight: Option<Op>,
    pub faulted: bool,
    pub completed: bool,
}

pub fn run_workload(
    path: &Path,
    seed: u64,
    operations: u64,
    fault: Option<Fault>,
    flush_threshold: usize,
    l0_threshold: usize,
) -> InProcessRun {
    let db =
        PebbleDB::open_with_options(path, scenario_options(flush_threshold, l0_threshold, fault))
            .expect("open database");

    let mut acknowledged = Vec::new();
    let mut in_flight = None;
    let mut faulted = false;
    let mut completed = false;
    let mut state = seed;

    for index in 0..operations {
        let (next, operation) = workload_entry(state, index);
        state = next;

        let result = match &operation {
            Op::Put { key, value } => db.set(key.clone(), value.clone()),
            Op::Delete { key } => db.delete(key).map(|_| ()),
        };
        match result {
            Ok(()) => acknowledged.push(operation),
            Err(_) => {
                in_flight = Some(operation);
                faulted = true;
                break;
            }
        }

        if index % 16 == 15 && db.flush().is_err() {
            faulted = true;
            break;
        }
        if index % 32 == 31 && db.compact().is_err() {
            faulted = true;
            break;
        }
        completed = index + 1 == operations;
    }

    drop(db);
    InProcessRun {
        acknowledged,
        in_flight,
        faulted,
        completed,
    }
}

pub struct ChildOutcome {
    pub acknowledged: Vec<Op>,
    pub in_flight: Option<Op>,
    pub fault: Option<String>,
    pub completed: bool,
    pub aborted: bool,
    pub stdout: String,
}

pub fn run_crash_child(
    path: &Path,
    seed: u64,
    operations: u64,
    point: FaultPoint,
    at: u64,
    mode: FaultMode,
) -> ChildOutcome {
    let output = Command::new(env!("CARGO_BIN_EXE_crash_child"))
        .env("PEBBLEDB_CRASH_PATH", path)
        .env("PEBBLEDB_CRASH_SEED", seed.to_string())
        .env("PEBBLEDB_CRASH_OPS", operations.to_string())
        .env("PEBBLEDB_CRASH_FLUSH", "4")
        .env("PEBBLEDB_CRASH_L0", "2")
        .env("PEBBLEDB_CRASH_POINT", point.name())
        .env("PEBBLEDB_CRASH_AT", at.to_string())
        .env(
            "PEBBLEDB_CRASH_MODE",
            match mode {
                FaultMode::Abort => "abort",
                FaultMode::Error => "error",
            },
        )
        .output()
        .expect("run crash_child");

    parse_child_output(
        std::str::from_utf8(&output.stdout).expect("child stdout is UTF-8"),
        output.status.code(),
    )
}

pub fn parse_child_output(stdout: &str, exit_code: Option<i32>) -> ChildOutcome {
    let mut acknowledged = Vec::new();
    let mut in_flight = None;
    let mut fault = None;
    let mut completed = false;

    for line in stdout.lines() {
        let mut fields = line.split(' ');
        match fields.next() {
            Some("TRY") => in_flight = parse_op(&mut fields),
            Some("ACK") => {
                if let Some(operation) = parse_op(&mut fields) {
                    acknowledged.push(operation);
                }
                in_flight = None;
            }
            Some("DONE") => completed = true,
            Some("FAULT") | Some("OPEN_FAILED") => fault = Some(line.to_string()),
            _ => {}
        }
    }

    ChildOutcome {
        acknowledged,
        in_flight,
        fault,
        completed,
        aborted: exit_code != Some(0) && exit_code != Some(2),
        stdout: stdout.to_string(),
    }
}

fn parse_op<'a>(fields: &mut impl Iterator<Item = &'a str>) -> Option<Op> {
    match fields.next()? {
        "put" => {
            fields.next()?;
            let key = fields.next()?.to_string();
            let value = fields.next()?.to_string();
            Some(Op::Put { key, value })
        }
        "del" => {
            fields.next()?;
            let key = fields.next()?.to_string();
            Some(Op::Delete { key })
        }
        _ => None,
    }
}
