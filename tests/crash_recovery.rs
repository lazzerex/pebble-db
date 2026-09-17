mod common;

use common::*;
use pebbledb::db::PebbleDB;
use pebbledb::fault::{Fault, FaultMode, FaultPoint};
use tempfile::tempdir;

const OPERATIONS: u64 = 64;

#[test]
fn every_fault_point_fires_during_the_workload() {
    for point in FaultPoint::ALL {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let run = run_workload(
            &path,
            SEED,
            OPERATIONS * 4,
            Some(Fault::new(point, 1)),
            4,
            2,
        );
        assert!(run.faulted, "fault point {} never fired", point.name());
    }
}

#[test]
fn acknowledged_operations_survive_crashes_at_every_write_path_boundary() {
    let mut scenarios = 0;
    for point in FaultPoint::ALL {
        for hit in 1..=3 {
            let dir = tempdir().unwrap();
            let path = dir.path().join("db");
            let run = run_workload(&path, SEED, OPERATIONS, Some(Fault::new(point, hit)), 4, 2);
            verify_recovery(&path, &run.acknowledged, run.in_flight.as_ref())
                .unwrap_or_else(|error| panic!("{} hit {}: {}", point.name(), hit, error));
            scenarios += 1;
        }
    }
    assert_eq!(scenarios, FaultPoint::ALL.len() * 3);
}

#[test]
fn crashes_without_a_fault_recover_the_full_workload() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let run = run_workload(&path, SEED, OPERATIONS, None, 4, 2);
    assert!(run.completed);
    assert!(!run.faulted);
    verify_recovery(&path, &run.acknowledged, None).unwrap();
}

#[test]
fn randomized_workload_matches_the_reference_model_across_reopens() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let mut acknowledged: Vec<Op> = Vec::new();
    let mut state = SEED;
    let mut db = PebbleDB::open_with_options(&path, scenario_options(4, 2, None)).unwrap();

    for index in 0..160u64 {
        let (next, operation) = workload_entry(state, index);
        state = next;
        match &operation {
            Op::Put { key, value } => db.set(key.clone(), value.clone()).unwrap(),
            Op::Delete { key } => {
                db.delete(key).unwrap();
            }
        }
        acknowledged.push(operation);

        if index % 24 == 23 {
            db.flush().unwrap();
            db.compact().unwrap();
            drop(db);
            verify_recovery(&path, &acknowledged, None).unwrap();
            db = PebbleDB::open_with_options(&path, scenario_options(4, 2, None)).unwrap();
        }
    }

    drop(db);
    verify_recovery(&path, &acknowledged, None).unwrap();
}

#[test]
fn deleted_keys_do_not_reappear_after_flush_and_compaction() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let db = PebbleDB::open_with_options(&path, scenario_options(4, 2, None)).unwrap();

    let mut acknowledged: Vec<Op> = Vec::new();
    let mut state = SEED;
    for index in 0..120u64 {
        let (next, operation) = workload_entry(state, index);
        state = next;
        match &operation {
            Op::Put { key, value } => db.set(key.clone(), value.clone()).unwrap(),
            Op::Delete { key } => {
                db.delete(key).unwrap();
            }
        }
        acknowledged.push(operation);
    }

    let final_state = model(&acknowledged);
    let deleted: Vec<String> = final_state
        .iter()
        .filter(|(_, value)| value.is_none())
        .map(|(key, _)| key.clone())
        .collect();
    assert!(!deleted.is_empty());

    db.flush().unwrap();
    db.compact().unwrap();
    for key in &deleted {
        assert_eq!(db.get(key), None, "deleted key {} is still visible", key);
    }
    drop(db);

    let db = PebbleDB::open(&path).unwrap();
    for key in &deleted {
        assert_eq!(db.get(key), None, "deleted key {} reappeared", key);
    }
    check_state(&db, &acknowledged, None).unwrap();
}

#[test]
fn repeated_recovery_of_the_same_database_is_idempotent() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("db");
    let run = run_workload(
        &path,
        SEED,
        OPERATIONS,
        Some(Fault::new(FaultPoint::AfterCompactionOutputCreation, 1)),
        4,
        2,
    );
    assert!(run.faulted);

    let baseline = PebbleDB::open(&path).unwrap();
    let expected = baseline.scan();
    assert!(!expected.is_empty());
    drop(baseline);

    for _ in 0..5 {
        let db = PebbleDB::open(&path).unwrap();
        check_state(&db, &run.acknowledged, run.in_flight.as_ref()).unwrap();
        assert_eq!(db.scan(), expected);
        drop(db);
    }
}

#[test]
fn aborted_process_keeps_acknowledged_writes() {
    for (point, hit) in [
        (FaultPoint::AfterWalAppend, 3),
        (FaultPoint::AfterWalFsync, 5),
        (FaultPoint::AfterMemtableUpdate, 9),
    ] {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let child = run_crash_child(&path, SEED, 60, point, hit, FaultMode::Abort);
        assert!(child.aborted, "child did not abort for {}", point.name());
        assert!(!child.completed);
        assert!(child.fault.is_none());
        assert!(!child.acknowledged.is_empty());
        assert!(child.in_flight.is_some());
        verify_recovery(&path, &child.acknowledged, child.in_flight.as_ref())
            .unwrap_or_else(|error| panic!("{} hit {}: {}", point.name(), hit, error));
    }
}

#[test]
fn aborted_process_during_flush_or_compaction_recovers() {
    for (point, hit) in [
        (FaultPoint::BeforeSstableCreation, 2),
        (FaultPoint::AfterSstableWrite, 2),
        (FaultPoint::BeforeAtomicRename, 2),
        (FaultPoint::AfterAtomicRename, 2),
        (FaultPoint::AfterCompactionOutputCreation, 1),
        (FaultPoint::BeforeObsoleteFileDelete, 3),
        (FaultPoint::AfterObsoleteFileDelete, 1),
        (FaultPoint::AfterMetadataUpdate, 4),
    ] {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let child = run_crash_child(&path, SEED, 60, point, hit, FaultMode::Abort);
        assert!(child.aborted, "child did not abort for {}", point.name());
        verify_recovery(&path, &child.acknowledged, child.in_flight.as_ref())
            .unwrap_or_else(|error| panic!("{} hit {}: {}", point.name(), hit, error));
    }
}

#[test]
fn injected_error_terminates_the_child_cleanly_and_recovery_holds() {
    for point in [
        FaultPoint::AfterWalFsync,
        FaultPoint::AfterSstableWrite,
        FaultPoint::BeforeObsoleteFileDelete,
    ] {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let child = run_crash_child(&path, SEED, 60, point, 2, FaultMode::Error);
        assert!(!child.completed);
        assert!(child.fault.is_some(), "child reported no fault");
        verify_recovery(&path, &child.acknowledged, child.in_flight.as_ref())
            .unwrap_or_else(|error| panic!("{} hit 2: {}", point.name(), error));
    }
}

#[test]
fn crash_child_reproduces_the_same_run_twice() {
    let dir = tempdir().unwrap();
    let first_path = dir.path().join("first");
    let second_path = dir.path().join("second");

    let first = run_crash_child(
        &first_path,
        SEED,
        60,
        FaultPoint::AfterWalFsync,
        4,
        FaultMode::Abort,
    );
    let second = run_crash_child(
        &second_path,
        SEED,
        60,
        FaultPoint::AfterWalFsync,
        4,
        FaultMode::Abort,
    );

    assert_eq!(first.acknowledged, second.acknowledged);
    assert_eq!(first.in_flight, second.in_flight);
    assert_eq!(first.aborted, second.aborted);
    assert_eq!(first.stdout, second.stdout);
}
