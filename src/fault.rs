use crate::error::{PebbleError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    BeforeWalAppend,
    AfterWalAppend,
    AfterWalFsync,
    AfterMemtableUpdate,
    BeforeSstableCreation,
    AfterSstableWrite,
    BeforeAtomicRename,
    AfterAtomicRename,
    BeforeMetadataUpdate,
    AfterMetadataUpdate,
    BeforeObsoleteFileDelete,
    AfterObsoleteFileDelete,
    BeforeWalTruncation,
    AfterWalTruncation,
    AfterCompactionOutputCreation,
}

impl FaultPoint {
    pub const ALL: [FaultPoint; 15] = [
        FaultPoint::BeforeWalAppend,
        FaultPoint::AfterWalAppend,
        FaultPoint::AfterWalFsync,
        FaultPoint::AfterMemtableUpdate,
        FaultPoint::BeforeSstableCreation,
        FaultPoint::AfterSstableWrite,
        FaultPoint::BeforeAtomicRename,
        FaultPoint::AfterAtomicRename,
        FaultPoint::BeforeMetadataUpdate,
        FaultPoint::AfterMetadataUpdate,
        FaultPoint::BeforeObsoleteFileDelete,
        FaultPoint::AfterObsoleteFileDelete,
        FaultPoint::BeforeWalTruncation,
        FaultPoint::AfterWalTruncation,
        FaultPoint::AfterCompactionOutputCreation,
    ];

    pub fn name(self) -> &'static str {
        match self {
            FaultPoint::BeforeWalAppend => "before_wal_append",
            FaultPoint::AfterWalAppend => "after_wal_append",
            FaultPoint::AfterWalFsync => "after_wal_fsync",
            FaultPoint::AfterMemtableUpdate => "after_memtable_update",
            FaultPoint::BeforeSstableCreation => "before_sstable_creation",
            FaultPoint::AfterSstableWrite => "after_sstable_write",
            FaultPoint::BeforeAtomicRename => "before_atomic_rename",
            FaultPoint::AfterAtomicRename => "after_atomic_rename",
            FaultPoint::BeforeMetadataUpdate => "before_metadata_update",
            FaultPoint::AfterMetadataUpdate => "after_metadata_update",
            FaultPoint::BeforeObsoleteFileDelete => "before_obsolete_file_delete",
            FaultPoint::AfterObsoleteFileDelete => "after_obsolete_file_delete",
            FaultPoint::BeforeWalTruncation => "before_wal_truncation",
            FaultPoint::AfterWalTruncation => "after_wal_truncation",
            FaultPoint::AfterCompactionOutputCreation => "after_compaction_output_creation",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        FaultPoint::ALL
            .into_iter()
            .find(|point| point.name() == name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultMode {
    Error,
    Abort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fault {
    pub point: FaultPoint,
    pub at: u64,
    pub mode: FaultMode,
}

impl Fault {
    pub fn new(point: FaultPoint, at: u64) -> Self {
        Self {
            point,
            at: at.max(1),
            mode: FaultMode::Error,
        }
    }

    pub fn abort(point: FaultPoint, at: u64) -> Self {
        Self {
            point,
            at: at.max(1),
            mode: FaultMode::Abort,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FaultInjector {
    fault: Option<Fault>,
    hits: u64,
}

impl FaultInjector {
    pub(crate) fn new(fault: Option<Fault>) -> Self {
        Self { fault, hits: 0 }
    }

    pub(crate) fn hit(&mut self, point: FaultPoint) -> Result<()> {
        let Some(fault) = self.fault else {
            return Ok(());
        };
        if fault.point != point {
            return Ok(());
        }
        self.hits += 1;
        if self.hits < fault.at {
            return Ok(());
        }
        match fault.mode {
            FaultMode::Abort => std::process::abort(),
            FaultMode::Error => Err(PebbleError::InjectedFault(format!(
                "{} at hit {}",
                point.name(),
                self.hits
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_injector_never_fires() {
        let mut injector = FaultInjector::default();
        for _ in 0..10 {
            assert!(injector.hit(FaultPoint::AfterWalFsync).is_ok());
        }
    }

    #[test]
    fn injector_fires_only_at_the_configured_hit() {
        let mut injector = FaultInjector::new(Some(Fault::new(FaultPoint::AfterWalFsync, 3)));
        assert!(injector.hit(FaultPoint::AfterWalFsync).is_ok());
        assert!(injector.hit(FaultPoint::AfterWalFsync).is_ok());
        assert!(matches!(
            injector.hit(FaultPoint::AfterWalFsync),
            Err(PebbleError::InjectedFault(_))
        ));
    }

    #[test]
    fn injector_ignores_other_points() {
        let mut injector = FaultInjector::new(Some(Fault::new(FaultPoint::AfterWalFsync, 1)));
        assert!(injector.hit(FaultPoint::BeforeWalAppend).is_ok());
        assert!(injector.hit(FaultPoint::BeforeWalAppend).is_ok());
        assert!(injector.hit(FaultPoint::AfterWalFsync).is_err());
    }

    #[test]
    fn point_names_round_trip() {
        for point in FaultPoint::ALL {
            assert_eq!(FaultPoint::parse(point.name()), Some(point));
        }
        assert_eq!(FaultPoint::parse("nope"), None);
    }

    #[test]
    fn hit_index_is_clamped_to_one() {
        assert_eq!(Fault::new(FaultPoint::AfterWalAppend, 0).at, 1);
    }
}
