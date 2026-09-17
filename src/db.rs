use std::collections::BTreeMap;
use std::fs;
use std::fs::OpenOptions;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::compaction::{CompactionPlan, FileMeta, LevelConfig, pick_compaction};
use crate::error::Result;
use crate::fault::{Fault, FaultInjector, FaultPoint};
use crate::iter::{
    MemtableIterator, MergeIterator, RangeIterator, SSTableIterator, StorageIterator,
};
use crate::sstable::{SSTable, SSTableEntry, write_sstable};
use crate::wal::{WalReader, WalRecord, WalWriter};

const DEFAULT_FLUSH_THRESHOLD: usize = 1024;
const DEFAULT_L0_COMPACTION_THRESHOLD: usize = 4;
const DEFAULT_LEVEL_SIZE_MULTIPLIER: usize = 10;
const DEFAULT_TARGET_LEVEL_SIZE: usize = 8 * 1024 * 1024;
const DEFAULT_TARGET_FILE_SIZE: usize = 2 * 1024 * 1024;
const MERGE_PENDING_FILE: &str = "merge.pending";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbOptions {
    pub flush_threshold: usize,
    pub l0_compaction_threshold: usize,
    pub level_size_multiplier: usize,
    pub target_level_size: usize,
    pub target_file_size: usize,
    pub fault: Option<Fault>,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            flush_threshold: DEFAULT_FLUSH_THRESHOLD,
            l0_compaction_threshold: DEFAULT_L0_COMPACTION_THRESHOLD,
            level_size_multiplier: DEFAULT_LEVEL_SIZE_MULTIPLIER,
            target_level_size: DEFAULT_TARGET_LEVEL_SIZE,
            target_file_size: DEFAULT_TARGET_FILE_SIZE,
            fault: None,
        }
    }
}

impl DbOptions {
    fn level_config(&self) -> LevelConfig {
        LevelConfig::new(
            self.l0_compaction_threshold,
            self.level_size_multiplier,
            self.target_level_size,
        )
    }
}

#[derive(Debug, Clone)]
pub enum MemtableEntry {
    Value(String),
    Tombstone,
}

#[derive(Debug, Clone, Default)]
pub struct LevelStats {
    pub level: u32,
    pub files: usize,
    pub bytes: usize,
}

#[derive(Debug, Clone, Default)]
pub struct DbStats {
    pub memtable_entries: usize,
    pub memtable_bytes: usize,
    pub sst_count: usize,
    pub total_sst_size: usize,
    pub sst_sizes: Vec<(u64, usize)>,
    pub wal_size: u64,
    pub memtable_hits: u64,
    pub bloom_rejects: u64,
    pub block_reads: u64,
    pub compactions_completed: u64,
    pub full_compactions: u64,
    pub compactions_by_level: Vec<(u32, u64)>,
    pub compaction_bytes_read: u64,
    pub compaction_bytes_written: u64,
    pub compaction_input_files: u64,
    pub compaction_output_files: u64,
    pub bytes_flushed: u64,
    pub write_amplification: Option<f64>,
    pub levels: Vec<LevelStats>,
}

struct AtomicCounters {
    memtable_hits: AtomicU64,
    bloom_rejects: AtomicU64,
    block_reads: AtomicU64,
}

impl Default for AtomicCounters {
    fn default() -> Self {
        Self {
            memtable_hits: AtomicU64::new(0),
            bloom_rejects: AtomicU64::new(0),
            block_reads: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Metrics {
    compactions: u64,
    full_compactions: u64,
    compactions_by_level: Vec<u64>,
    bytes_read: u64,
    bytes_written: u64,
    input_files: u64,
    output_files: u64,
    bytes_flushed: u64,
}

impl Metrics {
    fn record(&mut self, level: Option<u32>, outcome: &JobOutcome) {
        self.compactions += 1;
        self.bytes_read += outcome.bytes_read;
        self.bytes_written += outcome.bytes_written;
        self.input_files += outcome.input_files;
        self.output_files += outcome.output_files;
        match level {
            Some(level) => {
                let index = level as usize;
                if self.compactions_by_level.len() <= index {
                    self.compactions_by_level.resize(index + 1, 0);
                }
                self.compactions_by_level[index] += 1;
            }
            None => self.full_compactions += 1,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct JobOutcome {
    bytes_read: u64,
    bytes_written: u64,
    input_files: u64,
    output_files: u64,
}

struct CompactionJob {
    inputs: Vec<(u32, Arc<SSTable>)>,
    output_level: u32,
    drop_tombstones: bool,
    target_file_size: usize,
}

struct DbInner {
    path: PathBuf,
    memtable: BTreeMap<String, MemtableEntry>,
    wal: WalWriter,
    levels: Vec<Vec<Arc<SSTable>>>,
    next_sst_id: u64,
    options: DbOptions,
    counters: AtomicCounters,
    metrics: Metrics,
    fault: FaultInjector,
}

#[derive(Clone)]
pub struct PebbleDB {
    inner: Arc<RwLock<DbInner>>,
}

impl PebbleDB {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, DbOptions::default())
    }

    pub fn open_with_threshold(path: impl AsRef<Path>, flush_threshold: usize) -> Result<Self> {
        Self::open_with_options(
            path,
            DbOptions {
                flush_threshold,
                ..DbOptions::default()
            },
        )
    }

    pub fn open_with_options(path: impl AsRef<Path>, options: DbOptions) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let fault = options.fault;
        fs::create_dir_all(&path)?;

        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(".tmp") {
                fs::remove_file(entry.path())?;
            }
        }

        finish_pending_merge(&path)?;

        let mut levels: Vec<Vec<Arc<SSTable>>> = vec![Vec::new()];
        let mut next_sst_id = 0u64;
        for (id, level, sst_path) in discover_sstables(&path)? {
            let sst = SSTable::load(id, &sst_path)?;
            let level = level as usize;
            while levels.len() <= level {
                levels.push(Vec::new());
            }
            levels[level].push(Arc::new(sst));
            next_sst_id = id.max(next_sst_id) + 1;
        }

        let wal_path = path.join("wal.log");
        let mut memtable = BTreeMap::new();

        if wal_path.exists() {
            let mut reader = WalReader::open(&wal_path)?;
            let recovery = reader.recover()?;
            for record in recovery.records {
                match record {
                    WalRecord::Set { key, value } => {
                        memtable.insert(key, MemtableEntry::Value(value));
                    }
                    WalRecord::Delete { key } => {
                        memtable.insert(key, MemtableEntry::Tombstone);
                    }
                }
            }
            let wal_len = fs::metadata(&wal_path)?.len();
            if recovery.valid_len < wal_len {
                truncate_wal(&wal_path, recovery.valid_len)?;
            }
        }

        let wal = WalWriter::open(&wal_path)?;

        let inner = DbInner {
            path,
            memtable,
            wal,
            levels,
            next_sst_id,
            options,
            counters: AtomicCounters::default(),
            metrics: Metrics::default(),
            fault: FaultInjector::new(fault),
        };

        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
        })
    }

    pub fn set(&self, key: String, value: String) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.fault.hit(FaultPoint::BeforeWalAppend)?;
        inner.wal.append_record(&WalRecord::Set {
            key: key.clone(),
            value: value.clone(),
        })?;
        inner.fault.hit(FaultPoint::AfterWalAppend)?;
        inner.wal.sync()?;
        inner.fault.hit(FaultPoint::AfterWalFsync)?;
        inner.memtable.insert(key, MemtableEntry::Value(value));
        inner.fault.hit(FaultPoint::AfterMemtableUpdate)?;
        if inner.memtable.len() >= inner.options.flush_threshold {
            inner.flush()?;
        }
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let inner = self.inner.read().unwrap();
        match inner.memtable.get(key) {
            Some(MemtableEntry::Value(v)) => {
                inner.counters.memtable_hits.fetch_add(1, Ordering::Relaxed);
                return Some(v.clone());
            }
            Some(MemtableEntry::Tombstone) => return None,
            None => {}
        }
        for files in &inner.levels {
            for sst in files.iter().rev() {
                if !sst.bloom_might_contain(key) {
                    inner.counters.bloom_rejects.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                inner.counters.block_reads.fetch_add(1, Ordering::Relaxed);
                match sst.get(key) {
                    Some(SSTableEntry::Value(v)) => return Some(v),
                    Some(SSTableEntry::Tombstone) => return None,
                    None => {}
                }
            }
        }
        None
    }

    pub fn delete(&self, key: &str) -> Result<bool> {
        let mut inner = self.inner.write().unwrap();
        let existed = Self::get_inner(&inner, key).is_some();
        inner.fault.hit(FaultPoint::BeforeWalAppend)?;
        inner.wal.append_record(&WalRecord::Delete {
            key: key.to_string(),
        })?;
        inner.fault.hit(FaultPoint::AfterWalAppend)?;
        inner.wal.sync()?;
        inner.fault.hit(FaultPoint::AfterWalFsync)?;
        inner
            .memtable
            .insert(key.to_string(), MemtableEntry::Tombstone);
        inner.fault.hit(FaultPoint::AfterMemtableUpdate)?;
        if inner.memtable.len() >= inner.options.flush_threshold {
            inner.flush()?;
        }
        Ok(existed)
    }

    pub fn exists(&self, key: &str) -> bool {
        self.get(key).is_some()
    }

    pub fn range(&self, bounds: impl RangeBounds<String>) -> RangeIterator {
        let inner = self.inner.read().unwrap();
        inner.range_iterator(bounds.start_bound().cloned(), bounds.end_bound().cloned())
    }

    pub fn keys(&self) -> Vec<String> {
        self.range(..).map(|(key, _)| key).collect()
    }

    pub fn scan(&self) -> Vec<(String, String)> {
        self.range(..).collect()
    }

    pub fn compact(&self) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.compact_pending()
    }

    pub fn compact_full(&self) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.compact_full()
    }

    pub fn flush(&self) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.flush()?;
        Ok(())
    }

    pub fn stats(&self) -> DbStats {
        let inner = self.inner.read().unwrap();
        Self::stats_inner(&inner)
    }

    pub fn close(self) -> Result<()> {
        drop(self);
        Ok(())
    }

    fn get_inner(inner: &DbInner, key: &str) -> Option<String> {
        match inner.memtable.get(key) {
            Some(MemtableEntry::Value(v)) => return Some(v.clone()),
            Some(MemtableEntry::Tombstone) => return None,
            None => {}
        }
        for files in &inner.levels {
            for sst in files.iter().rev() {
                match sst.get(key) {
                    Some(SSTableEntry::Value(v)) => return Some(v),
                    Some(SSTableEntry::Tombstone) => return None,
                    None => {}
                }
            }
        }
        None
    }

    fn stats_inner(inner: &DbInner) -> DbStats {
        let memtable_bytes: usize = inner
            .memtable
            .iter()
            .map(|(k, v)| {
                k.len()
                    + match v {
                        MemtableEntry::Value(v) => v.len(),
                        MemtableEntry::Tombstone => 0,
                    }
            })
            .sum();

        let mut sst_sizes: Vec<(u64, usize)> = Vec::new();
        let mut levels: Vec<LevelStats> = Vec::new();
        for (level, files) in inner.levels.iter().enumerate() {
            if files.is_empty() {
                continue;
            }
            let bytes: usize = files.iter().map(|sst| sst.file_size()).sum();
            levels.push(LevelStats {
                level: level as u32,
                files: files.len(),
                bytes,
            });
            for sst in files {
                sst_sizes.push((sst.id, sst.file_size()));
            }
        }
        sst_sizes.sort_by_key(|(id, _)| *id);
        let total_sst_size: usize = sst_sizes.iter().map(|(_, size)| size).sum();

        let wal_path = inner.path.join("wal.log");
        let wal_size = fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);

        let metrics = &inner.metrics;
        let compactions_by_level: Vec<(u32, u64)> = metrics
            .compactions_by_level
            .iter()
            .enumerate()
            .filter(|(_, count)| **count > 0)
            .map(|(level, count)| (level as u32, *count))
            .collect();
        let write_amplification = (metrics.bytes_flushed > 0).then(|| {
            (metrics.bytes_flushed + metrics.bytes_written) as f64 / metrics.bytes_flushed as f64
        });

        DbStats {
            memtable_entries: inner.memtable.len(),
            memtable_bytes,
            sst_count: inner.sst_count(),
            total_sst_size,
            sst_sizes,
            wal_size,
            memtable_hits: inner.counters.memtable_hits.load(Ordering::Relaxed),
            bloom_rejects: inner.counters.bloom_rejects.load(Ordering::Relaxed),
            block_reads: inner.counters.block_reads.load(Ordering::Relaxed),
            compactions_completed: metrics.compactions,
            full_compactions: metrics.full_compactions,
            compactions_by_level,
            compaction_bytes_read: metrics.bytes_read,
            compaction_bytes_written: metrics.bytes_written,
            compaction_input_files: metrics.input_files,
            compaction_output_files: metrics.output_files,
            bytes_flushed: metrics.bytes_flushed,
            write_amplification,
            levels,
        }
    }
}

impl DbInner {
    fn range_iterator(&self, start: Bound<String>, end: Bound<String>) -> RangeIterator {
        let mut children: Vec<Box<dyn StorageIterator>> = Vec::with_capacity(self.sst_count() + 1);
        children.push(Box::new(MemtableIterator::new(&self.memtable)));
        for files in &self.levels {
            for sst in files.iter().rev() {
                children.push(Box::new(SSTableIterator::new(Arc::clone(sst))));
            }
        }
        RangeIterator::new(MergeIterator::new(children), start, end)
    }

    fn sst_count(&self) -> usize {
        self.levels.iter().map(|files| files.len()).sum()
    }

    fn ensure_level(&mut self, level: u32) {
        while self.levels.len() <= level as usize {
            self.levels.push(Vec::new());
        }
    }

    fn file_metas(&self) -> Vec<FileMeta> {
        let mut metas = Vec::new();
        for (level, files) in self.levels.iter().enumerate() {
            for sst in files {
                if let Some((smallest, largest)) = sst.key_range() {
                    metas.push(FileMeta {
                        id: sst.id,
                        level: level as u32,
                        smallest: smallest.to_string(),
                        largest: largest.to_string(),
                        size: sst.file_size(),
                    });
                }
            }
        }
        metas
    }

    fn write_level_file(
        &mut self,
        level: u32,
        entries: &BTreeMap<String, SSTableEntry>,
    ) -> Result<Arc<SSTable>> {
        let id = self.next_sst_id;
        self.next_sst_id += 1;
        let file_name = sstable_file_name(level, id);
        let tmp_path = self.path.join(format!("{}.tmp", file_name));
        let final_path = self.path.join(file_name);

        self.fault.hit(FaultPoint::BeforeSstableCreation)?;
        write_sstable(&tmp_path, entries)?;
        self.fault.hit(FaultPoint::AfterSstableWrite)?;
        self.fault.hit(FaultPoint::BeforeAtomicRename)?;
        fs::rename(&tmp_path, &final_path)?;
        self.fault.hit(FaultPoint::AfterAtomicRename)?;

        Ok(Arc::new(SSTable::load(id, &final_path)?))
    }

    fn flush(&mut self) -> Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        let mut entries: BTreeMap<String, SSTableEntry> = BTreeMap::new();
        for (key, entry) in &self.memtable {
            let sst_entry = match entry {
                MemtableEntry::Value(v) => SSTableEntry::Value(v.clone()),
                MemtableEntry::Tombstone => SSTableEntry::Tombstone,
            };
            entries.insert(key.clone(), sst_entry);
        }

        let sst = self.write_level_file(0, &entries)?;
        self.fault.hit(FaultPoint::BeforeWalTruncation)?;
        self.wal.truncate()?;
        self.fault.hit(FaultPoint::AfterWalTruncation)?;

        self.metrics.bytes_flushed += sst.file_size() as u64;
        self.fault.hit(FaultPoint::BeforeMetadataUpdate)?;
        self.levels[0].push(sst);
        self.memtable.clear();
        self.fault.hit(FaultPoint::AfterMetadataUpdate)?;

        self.compact_pending()?;
        Ok(())
    }

    fn compact_pending(&mut self) -> Result<()> {
        let config: LevelConfig = self.options.level_config();
        loop {
            let metas = self.file_metas();
            let Some(plan) = pick_compaction(&metas, &config) else {
                return Ok(());
            };
            let job = self.plan_job(&plan);
            let outcome = self.execute_job(job)?;
            self.metrics.record(Some(plan.level), &outcome);
        }
    }

    fn compact_full(&mut self) -> Result<()> {
        if self.sst_count() <= 1 {
            return Ok(());
        }

        let inputs: Vec<(u32, Arc<SSTable>)> = self
            .levels
            .iter()
            .enumerate()
            .flat_map(|(level, files)| files.iter().map(move |sst| (level as u32, Arc::clone(sst))))
            .collect();

        let job = CompactionJob {
            inputs,
            output_level: 1,
            drop_tombstones: true,
            target_file_size: usize::MAX,
        };
        let outcome = self.execute_job(job)?;
        self.metrics.record(None, &outcome);
        Ok(())
    }

    fn plan_job(&self, plan: &CompactionPlan) -> CompactionJob {
        let mut inputs = Vec::new();
        for (level, files) in self.levels.iter().enumerate() {
            let level = level as u32;
            let wanted = if level == plan.level {
                &plan.inputs
            } else if level == plan.level + 1 {
                &plan.overlaps
            } else {
                continue;
            };
            for sst in files {
                if wanted.contains(&sst.id) {
                    inputs.push((level, Arc::clone(sst)));
                }
            }
        }

        CompactionJob {
            inputs,
            output_level: plan.level + 1,
            drop_tombstones: plan.drop_tombstones,
            target_file_size: self.options.target_file_size,
        }
    }

    fn execute_job(&mut self, job: CompactionJob) -> Result<JobOutcome> {
        let bytes_read: u64 = job
            .inputs
            .iter()
            .map(|(_, sst)| sst.file_size() as u64)
            .sum();
        let input_files = job.inputs.len() as u64;

        let mut sources = job.inputs.clone();
        sources.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.id.cmp(&a.1.id)));

        let mut children: Vec<Box<dyn StorageIterator>> = Vec::with_capacity(sources.len());
        for (_, sst) in sources {
            children.push(Box::new(SSTableIterator::new(sst)));
        }
        let mut merged = MergeIterator::new_with_tombstones(children);

        let mut outputs: Vec<Arc<SSTable>> = Vec::new();
        let mut bytes_written = 0u64;
        let mut current: BTreeMap<String, SSTableEntry> = BTreeMap::new();
        let mut current_bytes = 0usize;

        while merged.valid() {
            let key = merged.key().unwrap().to_string();
            let entry = match merged.value() {
                Some(value) => SSTableEntry::Value(value.to_string()),
                None => SSTableEntry::Tombstone,
            };
            merged.advance();

            if job.drop_tombstones && entry == SSTableEntry::Tombstone {
                continue;
            }

            current_bytes += record_size(&key, &entry);
            current.insert(key, entry);
            if current_bytes >= job.target_file_size {
                let sst = self.write_level_file(job.output_level, &std::mem::take(&mut current))?;
                bytes_written += sst.file_size() as u64;
                outputs.push(sst);
                current_bytes = 0;
            }
        }
        if !current.is_empty() {
            let sst = self.write_level_file(job.output_level, &current)?;
            bytes_written += sst.file_size() as u64;
            outputs.push(sst);
        }

        self.fault.hit(FaultPoint::AfterCompactionOutputCreation)?;
        write_pending_merge(&self.path, &outputs, &job.inputs)?;
        for (_, sst) in &job.inputs {
            self.fault.hit(FaultPoint::BeforeObsoleteFileDelete)?;
            let _ = fs::remove_file(sst.path());
        }
        self.fault.hit(FaultPoint::AfterObsoleteFileDelete)?;

        self.fault.hit(FaultPoint::BeforeMetadataUpdate)?;
        for files in &mut self.levels {
            files.retain(|sst| !job.inputs.iter().any(|(_, input)| input.id == sst.id));
        }
        let output_files = outputs.len() as u64;
        self.ensure_level(job.output_level);
        let target = &mut self.levels[job.output_level as usize];
        target.extend(outputs);
        target.sort_by_key(|sst| sst.id);
        self.fault.hit(FaultPoint::AfterMetadataUpdate)?;
        clear_pending_merge(&self.path)?;

        Ok(JobOutcome {
            bytes_read,
            bytes_written,
            input_files,
            output_files,
        })
    }
}

fn record_size(key: &str, entry: &SSTableEntry) -> usize {
    let value_len = match entry {
        SSTableEntry::Value(value) => value.len(),
        SSTableEntry::Tombstone => 0,
    };
    1 + 4 + 4 + key.len() + value_len
}

fn sstable_file_name(level: u32, id: u64) -> String {
    format!("sstable_L{}_{:06}.sst", level, id)
}

fn parse_sstable_file_name(name: &str) -> Option<(u64, u32)> {
    let stem = name.strip_prefix("sstable_")?.strip_suffix(".sst")?;
    match stem.strip_prefix('L') {
        Some(rest) => {
            let (level, id) = rest.split_once('_')?;
            Some((id.parse().ok()?, level.parse().ok()?))
        }
        None => Some((stem.parse().ok()?, 0)),
    }
}

fn discover_sstables(path: &Path) -> Result<Vec<(u64, u32, PathBuf)>> {
    let mut sstables = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some((id, level)) = parse_sstable_file_name(&name) {
            sstables.push((id, level, entry.path()));
        }
    }
    sstables.sort_by_key(|(id, _, _)| *id);
    Ok(sstables)
}

fn truncate_wal(path: &Path, len: u64) -> Result<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(len)?;
    file.sync_all()?;
    Ok(())
}

fn pending_merge_path(path: &Path) -> PathBuf {
    path.join(MERGE_PENDING_FILE)
}

fn write_pending_merge(
    path: &Path,
    outputs: &[Arc<SSTable>],
    inputs: &[(u32, Arc<SSTable>)],
) -> Result<()> {
    let mut content = String::from("outputs: ");
    content.push_str(&join_ids(outputs.iter().map(|sst| sst.id)));
    content.push_str("\ninputs: ");
    content.push_str(&join_ids(inputs.iter().map(|(_, sst)| sst.id)));
    content.push('\n');

    let tmp = path.join(format!("{}.tmp", MERGE_PENDING_FILE));
    fs::write(&tmp, content)?;
    fs::rename(&tmp, pending_merge_path(path))?;
    Ok(())
}

fn clear_pending_merge(path: &Path) -> Result<()> {
    let marker = pending_merge_path(path);
    if marker.exists() {
        fs::remove_file(marker)?;
    }
    Ok(())
}

fn read_pending_merge(path: &Path) -> Result<Option<(Vec<u64>, Vec<u64>)>> {
    let marker = pending_merge_path(path);
    if !marker.exists() {
        return Ok(None);
    }
    let content = fs::read_to_string(&marker)?;
    let mut outputs = Vec::new();
    let mut inputs = Vec::new();
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("outputs:") {
            outputs = parse_ids(rest);
        } else if let Some(rest) = line.strip_prefix("inputs:") {
            inputs = parse_ids(rest);
        }
    }
    Ok(Some((outputs, inputs)))
}

fn join_ids(ids: impl Iterator<Item = u64>) -> String {
    ids.map(|id| id.to_string()).collect::<Vec<_>>().join(",")
}

fn parse_ids(text: &str) -> Vec<u64> {
    text.split(',')
        .filter_map(|id| id.trim().parse().ok())
        .collect()
}

fn finish_pending_merge(path: &Path) -> Result<()> {
    let Some((outputs, inputs)) = read_pending_merge(path)? else {
        return Ok(());
    };

    let mut files = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some((id, _)) = parse_sstable_file_name(&name) {
            files.push((id, entry.path()));
        }
    }

    let outputs_present = outputs
        .iter()
        .all(|id| files.iter().any(|(file_id, _)| file_id == id));
    if outputs_present {
        for (id, file) in &files {
            if inputs.contains(id) {
                fs::remove_file(file)?;
            }
        }
    }

    fs::remove_file(pending_merge_path(path))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{TempDir, tempdir};

    #[test]
    fn test_set_and_get() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        db.set("key".into(), "value".into()).unwrap();
        assert_eq!(db.get("key"), Some("value".into()));
    }

    #[test]
    fn test_overwrite_key() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        db.set("key".into(), "v1".into()).unwrap();
        db.set("key".into(), "v2".into()).unwrap();
        assert_eq!(db.get("key"), Some("v2".into()));
    }

    #[test]
    fn test_get_nonexistent() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        assert_eq!(db.get("nope"), None);
    }

    #[test]
    fn test_delete() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        db.set("key".into(), "val".into()).unwrap();
        let existed = db.delete("key").unwrap();
        assert!(existed);
        assert_eq!(db.get("key"), None);
    }

    #[test]
    fn test_delete_nonexistent() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        let existed = db.delete("nope").unwrap();
        assert!(!existed);
    }

    #[test]
    fn test_exists() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        db.set("key".into(), "val".into()).unwrap();
        assert!(db.exists("key"));
        assert!(!db.exists("nope"));
    }

    #[test]
    fn test_empty_database() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        assert_eq!(db.get("any"), None);
    }

    #[test]
    fn test_keys_sorted() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        db.set("c".into(), "3".into()).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        let keys = db.keys();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_multiple_operations_order() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        db.set("x".into(), "1".into()).unwrap();
        db.set("y".into(), "2".into()).unwrap();
        db.delete("x").unwrap();
        assert_eq!(db.get("x"), None);
        assert_eq!(db.get("y"), Some("2".into()));
    }

    #[test]
    fn test_reopen_persists_data() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_threshold(&path, 100).unwrap();
            db.set("hello".into(), "world".into()).unwrap();
        }
        {
            let db = PebbleDB::open(&path).unwrap();
            assert_eq!(db.get("hello"), Some("world".into()));
        }
    }

    #[test]
    fn test_reopen_after_delete() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_threshold(&path, 100).unwrap();
            db.set("key".into(), "val".into()).unwrap();
            db.delete("key").unwrap();
        }
        {
            let db = PebbleDB::open(&path).unwrap();
            assert_eq!(db.get("key"), None);
        }
    }

    #[test]
    fn test_flush_creates_sstable() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 2).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        let entries = fs::read_dir(dir.path().join("db")).unwrap();
        let ssts: Vec<_> = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".sst"))
            .collect();
        assert_eq!(ssts.len(), 1);
    }

    #[test]
    fn test_read_from_sstable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            db.set("a".into(), "1".into()).unwrap();
            db.set("b".into(), "2".into()).unwrap();
            db.set("c".into(), "3".into()).unwrap();
        }
        {
            let db = PebbleDB::open(&path).unwrap();
            assert_eq!(db.get("a"), Some("1".into()));
            assert_eq!(db.get("b"), Some("2".into()));
        }
    }

    #[test]
    fn test_memtable_overrides_sstable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            db.set("k".into(), "old".into()).unwrap();
            db.set("x".into(), "pad".into()).unwrap();
        }
        {
            let db = PebbleDB::open_with_threshold(&path, 100).unwrap();
            db.set("k".into(), "new".into()).unwrap();
            assert_eq!(db.get("k"), Some("new".into()));
        }
    }

    #[test]
    fn test_tombstone_overrides_sstable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            db.set("k".into(), "val".into()).unwrap();
            db.set("x".into(), "pad".into()).unwrap();
        }
        {
            let db = PebbleDB::open_with_threshold(&path, 100).unwrap();
            db.delete("k").unwrap();
            assert_eq!(db.get("k"), None);
        }
    }

    #[test]
    fn test_scan_ordering() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        db.set("c".into(), "3".into()).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        let scan = db.scan();
        assert_eq!(
            scan,
            vec![
                ("a".into(), "1".into()),
                ("b".into(), "2".into()),
                ("c".into(), "3".into()),
            ]
        );
    }

    #[test]
    fn test_scan_excludes_tombstones() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        db.delete("a").unwrap();
        let scan = db.scan();
        assert_eq!(scan, vec![("b".into(), "2".into())]);
    }

    #[test]
    fn test_compaction() {
        let dir = tempdir().unwrap();
        let db =
            PebbleDB::open_with_options(dir.path().join("db"), options(2, 2, usize::MAX)).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        db.set("c".into(), "3".into()).unwrap();
        db.set("d".into(), "4".into()).unwrap();
        db.compact().unwrap();
        assert_eq!(db.get("a"), Some("1".into()));
        assert_eq!(db.get("b"), Some("2".into()));
        assert_eq!(db.get("c"), Some("3".into()));
        assert_eq!(db.get("d"), Some("4".into()));
        assert_eq!(levels_of(&db), vec![(1, 1)]);
    }

    #[test]
    fn test_compaction_drops_tombstones() {
        let dir = tempdir().unwrap();
        let db =
            PebbleDB::open_with_options(dir.path().join("db"), options(2, 2, usize::MAX)).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        db.delete("a").unwrap();
        db.set("c".into(), "3".into()).unwrap();
        db.compact().unwrap();
        let keys = db.keys();
        assert_eq!(keys, vec!["b", "c"]);
        assert_eq!(tombstone_keys(&db), Vec::<String>::new());
    }

    #[test]
    fn test_compaction_newest_wins() {
        let dir = tempdir().unwrap();
        let db =
            PebbleDB::open_with_options(dir.path().join("db"), options(2, 2, usize::MAX)).unwrap();
        db.set("key".into(), "old".into()).unwrap();
        db.set("other".into(), "x".into()).unwrap();
        db.set("key".into(), "new".into()).unwrap();
        db.set("other2".into(), "y".into()).unwrap();
        db.compact().unwrap();
        assert_eq!(db.get("key"), Some("new".into()));
        assert_eq!(levels_of(&db), vec![(1, 1)]);
    }

    #[test]
    fn test_compaction_tombstone_shadows_older() {
        let dir = tempdir().unwrap();
        let db =
            PebbleDB::open_with_options(dir.path().join("db"), options(2, 2, usize::MAX)).unwrap();
        db.set("key".into(), "old".into()).unwrap();
        db.set("pad".into(), "x".into()).unwrap();
        db.delete("key").unwrap();
        db.set("pad2".into(), "y".into()).unwrap();
        db.compact().unwrap();
        assert_eq!(db.get("key"), None);
        assert_eq!(db.scan(), owned(&[("pad", "x"), ("pad2", "y")]));
    }

    #[test]
    fn test_wal_replay_after_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            db.set("a".into(), "1".into()).unwrap();
            db.set("b".into(), "2".into()).unwrap();
            db.set("c".into(), "3".into()).unwrap();
        }
        {
            let db = PebbleDB::open(&path).unwrap();
            assert_eq!(db.get("a"), Some("1".into()));
            assert_eq!(db.get("b"), Some("2".into()));
            assert_eq!(db.get("c"), Some("3".into()));
        }
    }

    #[test]
    fn test_crash_during_flush_recovery() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        fs::create_dir_all(&path).unwrap();
        {
            let db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            db.set("a".into(), "1".into()).unwrap();
            db.set("b".into(), "2".into()).unwrap();
        }
        let temp_file = path.join(".sstable_000000.sst.tmp");
        fs::write(&temp_file, b"garbage").unwrap();
        {
            let db = PebbleDB::open(&path).unwrap();
            assert_eq!(db.get("a"), Some("1".into()));
            assert_eq!(db.get("b"), Some("2".into()));
            assert!(!temp_file.exists());
        }
    }

    #[test]
    fn test_compaction_crash_before_input_cleanup_does_not_resurrect_deleted_keys() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");

        {
            let db = PebbleDB::open_with_options(&path, options(2, 2, usize::MAX)).unwrap();
            db.set("k".into(), "v1".into()).unwrap();
            db.set("a".into(), "1".into()).unwrap();
            db.set("b".into(), "2".into()).unwrap();
            db.set("c".into(), "3".into()).unwrap();
            assert_eq!(levels_of(&db), vec![(1, 1)]);
            assert_eq!(db.get("k"), Some("v1".into()));
        }

        {
            let fault = Fault::new(FaultPoint::BeforeObsoleteFileDelete, 3);
            let db = PebbleDB::open_with_options(&path, fault_options(2, 2, fault)).unwrap();
            db.delete("k").unwrap();
            db.set("d".into(), "4".into()).unwrap();
            db.set("e".into(), "5".into()).unwrap();
            assert!(db.set("f".into(), "6".into()).is_err());
            assert!(path.join(MERGE_PENDING_FILE).exists());
        }

        let db = PebbleDB::open_with_options(&path, options(2, 2, usize::MAX)).unwrap();
        assert!(!path.join(MERGE_PENDING_FILE).exists());
        assert_eq!(db.get("k"), None);
        assert_eq!(
            db.scan(),
            owned(&[
                ("a", "1"),
                ("b", "2"),
                ("c", "3"),
                ("d", "4"),
                ("e", "5"),
                ("f", "6")
            ])
        );
    }

    #[test]
    fn test_interrupted_merge_is_replayed_on_open() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");

        {
            let db = PebbleDB::open_with_options(&path, no_compaction(2)).unwrap();
            db.set("a".into(), "1".into()).unwrap();
            db.set("b".into(), "2".into()).unwrap();
            assert_eq!(levels_of(&db), vec![(0, 1)]);
        }

        let survivor = path.join(sstable_file_name(1, 9));
        {
            let mut merged: BTreeMap<String, SSTableEntry> = BTreeMap::new();
            let sst = SSTable::load(0, &path.join(sstable_file_name(0, 0))).unwrap();
            merged.extend(sst.entries());
            write_sstable(&survivor, &merged).unwrap();
        }
        fs::write(path.join(MERGE_PENDING_FILE), "outputs: 9\ninputs: 0\n").unwrap();

        let db = PebbleDB::open_with_options(&path, no_compaction(2)).unwrap();
        assert_eq!(levels_of(&db), vec![(1, 1)]);
        assert_eq!(file_names(&path), vec!["sstable_L1_000009.sst"]);
        assert_eq!(db.scan(), owned(&[("a", "1"), ("b", "2")]));
        assert!(!path.join(MERGE_PENDING_FILE).exists());
    }

    #[test]
    fn test_interrupted_merge_keeps_inputs_when_outputs_are_missing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");

        {
            let db = PebbleDB::open_with_options(&path, no_compaction(2)).unwrap();
            db.set("a".into(), "1".into()).unwrap();
            db.set("b".into(), "2".into()).unwrap();
        }

        fs::write(path.join(MERGE_PENDING_FILE), "outputs: 9\ninputs: 0\n").unwrap();

        let db = PebbleDB::open_with_options(&path, no_compaction(2)).unwrap();
        assert_eq!(levels_of(&db), vec![(0, 1)]);
        assert_eq!(db.scan(), owned(&[("a", "1"), ("b", "2")]));
        assert!(!path.join(MERGE_PENDING_FILE).exists());
    }

    #[test]
    fn test_stats() {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), 100).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        let stats = db.stats();
        assert_eq!(stats.memtable_entries, 2);
        assert_eq!(stats.sst_count, 0);
        assert!(stats.wal_size > 0);
        db.flush().unwrap();
        let stats = db.stats();
        assert_eq!(stats.memtable_entries, 0);
        assert_eq!(stats.sst_count, 1);
        assert!(stats.total_sst_size > 0);
    }

    #[test]
    fn test_concurrent_set_get() {
        use std::sync::Arc;
        use std::thread;

        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Arc::new(PebbleDB::open_with_threshold(&path, 10000).unwrap());

        let mut handles = Vec::new();
        for i in 0..4 {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("t{}_{}", i, j);
                    db.set(key.clone(), format!("val_{}", j)).unwrap();
                    let _ = db.get(&key);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let stats = db.stats();
        assert_eq!(stats.memtable_entries, 400);
    }

    #[test]
    fn test_bloom_filter_rejects() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            for i in 0..50 {
                db.set(format!("key_{:04}", i), format!("val_{}", i))
                    .unwrap();
            }
        }
        {
            let db = PebbleDB::open(&path).unwrap();
            let stats_before = db.stats();
            let rejects_before = stats_before.bloom_rejects;
            let _ = db.get("nonexistent_zzz");
            let stats_after = db.stats();
            assert!(stats_after.bloom_rejects > rejects_before || stats_after.sst_count > 0);
        }
    }

    fn db_with(threshold: usize, entries: &[(&str, Option<&str>)]) -> (TempDir, PebbleDB) {
        let dir = tempdir().unwrap();
        let db = PebbleDB::open_with_threshold(dir.path().join("db"), threshold).unwrap();
        for (key, value) in entries {
            match value {
                Some(value) => db.set((*key).to_string(), (*value).to_string()).unwrap(),
                None => {
                    db.delete(key).unwrap();
                }
            }
        }
        (dir, db)
    }

    fn range(db: &PebbleDB, start: Option<&str>, end: Option<&str>) -> Vec<(String, String)> {
        let bounds = (
            start.map_or(Bound::Unbounded, |key| Bound::Included(key.to_string())),
            end.map_or(Bound::Unbounded, |key| Bound::Excluded(key.to_string())),
        );
        db.range(bounds).collect()
    }

    fn owned(entries: &[(&str, &str)]) -> Vec<(String, String)> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn test_range_empty_database() {
        let (_dir, db) = db_with(100, &[]);
        assert_eq!(range(&db, None, None), Vec::new());
    }

    #[test]
    fn test_range_single_key() {
        let (_dir, db) = db_with(100, &[("only", Some("1"))]);
        assert_eq!(range(&db, None, None), owned(&[("only", "1")]));
        assert_eq!(range(&db, Some("only"), None), owned(&[("only", "1")]));
        assert_eq!(range(&db, None, Some("only")), Vec::new());
    }

    #[test]
    fn test_range_multiple_ordered_keys() {
        let (_dir, db) = db_with(100, &[("a", Some("1")), ("b", Some("2")), ("c", Some("3"))]);
        assert_eq!(
            range(&db, None, None),
            owned(&[("a", "1"), ("b", "2"), ("c", "3")])
        );
    }

    #[test]
    fn test_range_reverse_insertion_order() {
        let (_dir, db) = db_with(100, &[("c", Some("3")), ("b", Some("2")), ("a", Some("1"))]);
        assert_eq!(db.keys(), vec!["a", "b", "c"]);
        assert_eq!(
            range(&db, None, None),
            owned(&[("a", "1"), ("b", "2"), ("c", "3")])
        );
    }

    #[test]
    fn test_range_duplicate_updates() {
        let (_dir, db) = db_with(
            100,
            &[("a", Some("old")), ("b", Some("2")), ("a", Some("new"))],
        );
        assert_eq!(range(&db, None, None), owned(&[("a", "new"), ("b", "2")]));
    }

    #[test]
    fn test_range_excludes_deleted_keys() {
        let (_dir, db) = db_with(100, &[("a", Some("1")), ("b", Some("2")), ("a", None)]);
        assert_eq!(range(&db, None, None), owned(&[("b", "2")]));
    }

    #[test]
    fn test_range_memtable_only() {
        let (_dir, db) = db_with(
            100,
            &[("k1", Some("v1")), ("k2", Some("v2")), ("k3", Some("v3"))],
        );
        assert_eq!(db.stats().sst_count, 0);
        assert_eq!(
            range(&db, Some("k2"), None),
            owned(&[("k2", "v2"), ("k3", "v3")])
        );
    }

    #[test]
    fn test_range_sstable_only() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            db.set("a".into(), "1".into()).unwrap();
            db.set("b".into(), "2".into()).unwrap();
            db.set("c".into(), "3".into()).unwrap();
            db.set("d".into(), "4".into()).unwrap();
            assert_eq!(db.stats().memtable_entries, 0);
        }
        let db = PebbleDB::open(&path).unwrap();
        assert_eq!(db.stats().sst_count, 2);
        assert_eq!(
            range(&db, Some("b"), Some("d")),
            owned(&[("b", "2"), ("c", "3")])
        );
    }

    #[test]
    fn test_range_memtable_and_sstable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let db = PebbleDB::open_with_threshold(&path, 2).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        db.set("c".into(), "3".into()).unwrap();
        assert_eq!(db.stats().sst_count, 1);
        assert_eq!(db.stats().memtable_entries, 1);
        assert_eq!(
            range(&db, None, None),
            owned(&[("a", "1"), ("b", "2"), ("c", "3")])
        );
    }

    #[test]
    fn test_range_multiple_sstables() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_options(&path, no_compaction(2)).unwrap();
            for (key, value) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("e", "5")] {
                db.set(key.into(), value.into()).unwrap();
                db.set(format!("pad_{}", key), "x".into()).unwrap();
            }
            assert!(db.stats().sst_count >= 3);
        }
        let db = PebbleDB::open_with_options(&path, no_compaction(2)).unwrap();
        assert!(db.stats().sst_count >= 3);
        let scanned = range(&db, Some("c"), Some("e"));
        assert_eq!(scanned, owned(&[("c", "3"), ("d", "4")]));
    }

    #[test]
    fn test_range_overlapping_sstables() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            db.set("a".into(), "old".into()).unwrap();
            db.set("b".into(), "1".into()).unwrap();
        }
        {
            let db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            db.set("a".into(), "new".into()).unwrap();
            db.set("c".into(), "2".into()).unwrap();
            assert_eq!(db.stats().sst_count, 2);
            assert_eq!(
                range(&db, None, None),
                owned(&[("a", "new"), ("b", "1"), ("c", "2")])
            );
        }
    }

    #[test]
    fn test_range_deleted_key_shadows_older_sstable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            db.set("k".into(), "value".into()).unwrap();
            db.set("pad".into(), "x".into()).unwrap();
        }
        {
            let db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            db.delete("k").unwrap();
            db.set("pad2".into(), "y".into()).unwrap();
            assert_eq!(db.stats().sst_count, 2);
            assert_eq!(
                range(&db, None, None),
                owned(&[("pad", "x"), ("pad2", "y")])
            );
            assert_eq!(
                range(&db, Some("k"), None),
                owned(&[("pad", "x"), ("pad2", "y")])
            );
        }
    }

    #[test]
    fn test_range_skips_tombstone_between_live_keys() {
        let (_dir, db) = db_with(
            100,
            &[
                ("a", Some("1")),
                ("b", Some("2")),
                ("c", Some("3")),
                ("b", None),
                ("d", Some("4")),
            ],
        );
        assert_eq!(
            range(&db, None, None),
            owned(&[("a", "1"), ("c", "3"), ("d", "4")])
        );
    }

    #[test]
    fn test_range_unbounded_bounds() {
        let (_dir, db) = db_with(100, &[("a", Some("1")), ("b", Some("2")), ("c", Some("3"))]);
        assert_eq!(db.range(..).collect::<Vec<_>>().len(), 3);
        assert_eq!(
            db.range("b".to_string()..).collect::<Vec<_>>(),
            owned(&[("b", "2"), ("c", "3")])
        );
        assert_eq!(
            db.range(.."c".to_string()).collect::<Vec<_>>(),
            owned(&[("a", "1"), ("b", "2")])
        );
    }

    #[test]
    fn test_range_exact_single_key() {
        let (_dir, db) = db_with(100, &[("a", Some("1")), ("b", Some("2")), ("c", Some("3"))]);
        assert_eq!(
            db.range("b".to_string()..="b".to_string())
                .collect::<Vec<_>>(),
            owned(&[("b", "2")])
        );
        assert_eq!(
            db.range("b".to_string().."c".to_string())
                .collect::<Vec<_>>(),
            owned(&[("b", "2")])
        );
    }

    #[test]
    fn test_range_inclusive_end() {
        let (_dir, db) = db_with(100, &[("a", Some("1")), ("b", Some("2")), ("c", Some("3"))]);
        assert_eq!(
            db.range(..="b".to_string()).collect::<Vec<_>>(),
            owned(&[("a", "1"), ("b", "2")])
        );
    }

    #[test]
    fn test_range_empty_result() {
        let (_dir, db) = db_with(100, &[("a", Some("1")), ("b", Some("2")), ("c", Some("3"))]);
        assert_eq!(range(&db, Some("x"), Some("z")), Vec::new());
        assert_eq!(range(&db, Some("c"), Some("a")), Vec::new());
        assert_eq!(range(&db, Some("b"), Some("b")), Vec::new());
    }

    #[test]
    fn test_range_seek_to_existing_key() {
        let (_dir, db) = db_with(100, &[("a", Some("1")), ("b", Some("2")), ("c", Some("3"))]);
        let mut iter = db.range(..);
        iter.seek("b");
        assert_eq!(iter.key(), Some("b"));
        assert_eq!(iter.value(), Some("2"));
        assert_eq!(iter.next(), Some(("b".to_string(), "2".to_string())));
        assert_eq!(iter.next(), Some(("c".to_string(), "3".to_string())));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_range_seek_between_keys() {
        let (_dir, db) = db_with(100, &[("a", Some("1")), ("c", Some("3")), ("e", Some("5"))]);
        let mut iter = db.range(..);
        iter.seek("b");
        assert_eq!(iter.key(), Some("c"));
        iter.seek("d");
        assert_eq!((iter.key(), iter.value()), (Some("e"), Some("5")));
    }

    #[test]
    fn test_range_seek_beyond_final_key() {
        let (_dir, db) = db_with(100, &[("a", Some("1")), ("b", Some("2"))]);
        let mut iter = db.range(..);
        iter.seek("z");
        assert!(!iter.valid());
        assert_eq!(iter.key(), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_range_seek_respects_end_bound() {
        let (_dir, db) = db_with(100, &[("a", Some("1")), ("b", Some("2")), ("c", Some("3"))]);
        let mut iter = db.range(.."c".to_string());
        iter.seek("c");
        assert!(!iter.valid());
        iter.seek("b");
        assert_eq!(iter.key(), Some("b"));
    }

    #[test]
    fn test_scan_and_keys_match_range() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_threshold(&path, 2).unwrap();
            db.set("a".into(), "1".into()).unwrap();
            db.set("b".into(), "2".into()).unwrap();
            db.set("c".into(), "3".into()).unwrap();
            db.set("d".into(), "4".into()).unwrap();
        }
        let db = PebbleDB::open_with_threshold(&path, 100).unwrap();
        db.set("e".into(), "5".into()).unwrap();
        db.delete("b").unwrap();
        assert_eq!(db.stats().sst_count, 2);
        assert_eq!(db.stats().memtable_entries, 2);

        let expected = owned(&[("a", "1"), ("c", "3"), ("d", "4"), ("e", "5")]);
        assert_eq!(db.scan(), expected);
        assert_eq!(db.range(..).collect::<Vec<_>>(), expected);
        assert_eq!(db.keys(), vec!["a", "c", "d", "e"]);
        assert_eq!(
            range(&db, Some("b"), Some("e")),
            owned(&[("c", "3"), ("d", "4")])
        );
        assert_eq!(db.get("b"), None);
    }

    #[test]
    fn test_range_after_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_threshold(&path, 100).unwrap();
            db.set("b".into(), "2".into()).unwrap();
            db.set("a".into(), "1".into()).unwrap();
            db.set("c".into(), "3".into()).unwrap();
        }
        let db = PebbleDB::open(&path).unwrap();
        assert_eq!(
            range(&db, None, None),
            owned(&[("a", "1"), ("b", "2"), ("c", "3")])
        );
        assert_eq!(
            range(&db, Some("b"), None),
            owned(&[("b", "2"), ("c", "3")])
        );
    }

    fn no_compaction(flush_threshold: usize) -> DbOptions {
        DbOptions {
            flush_threshold,
            l0_compaction_threshold: usize::MAX,
            level_size_multiplier: 10,
            target_level_size: usize::MAX,
            target_file_size: 2 * 1024 * 1024,
            fault: None,
        }
    }

    fn options(
        flush_threshold: usize,
        l0_compaction_threshold: usize,
        target_level_size: usize,
    ) -> DbOptions {
        DbOptions {
            flush_threshold,
            l0_compaction_threshold,
            level_size_multiplier: 2,
            target_level_size,
            target_file_size: 2 * 1024 * 1024,
            fault: None,
        }
    }

    fn fault_options(
        flush_threshold: usize,
        l0_compaction_threshold: usize,
        fault: crate::fault::Fault,
    ) -> DbOptions {
        DbOptions {
            flush_threshold,
            l0_compaction_threshold,
            level_size_multiplier: 2,
            target_level_size: usize::MAX,
            target_file_size: 2 * 1024 * 1024,
            fault: Some(fault),
        }
    }

    fn levels_of(db: &PebbleDB) -> Vec<(u32, usize)> {
        db.stats()
            .levels
            .iter()
            .map(|ls| (ls.level, ls.files))
            .collect()
    }

    fn file_names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".sst"))
            .collect();
        names.sort();
        names
    }

    fn file_ranges(db: &PebbleDB, level: u32) -> Vec<(String, String)> {
        let inner = db.inner.read().unwrap();
        inner.levels[level as usize]
            .iter()
            .filter_map(|sst| {
                sst.key_range()
                    .map(|(first, last)| (first.to_string(), last.to_string()))
            })
            .collect()
    }

    fn tombstone_keys(db: &PebbleDB) -> Vec<String> {
        let children: Vec<Box<dyn StorageIterator>> = {
            let inner = db.inner.read().unwrap();
            inner
                .levels
                .iter()
                .flat_map(|level| {
                    level.iter().rev().map(|sst| {
                        Box::new(SSTableIterator::new(Arc::clone(sst))) as Box<dyn StorageIterator>
                    })
                })
                .collect()
        };
        let mut iter = MergeIterator::new_with_tombstones(children);
        let mut keys: Vec<String> = Vec::new();
        while iter.valid() {
            if iter.value().is_none() {
                keys.push(iter.key().unwrap().to_string());
            }
            iter.advance();
        }
        keys
    }

    #[test]
    fn test_flush_writes_level_zero_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let db = PebbleDB::open_with_options(&path, no_compaction(2)).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        assert_eq!(levels_of(&db), vec![(0, 1)]);
        assert_eq!(file_names(&path), vec!["sstable_L0_000000.sst"]);
    }

    #[test]
    fn test_l0_compaction_triggered_by_threshold() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let db = PebbleDB::open_with_options(&path, options(2, 2, usize::MAX)).unwrap();

        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        assert_eq!(levels_of(&db), vec![(0, 1)]);

        db.set("c".into(), "3".into()).unwrap();
        db.set("d".into(), "4".into()).unwrap();
        assert_eq!(levels_of(&db), vec![(1, 1)]);
        assert_eq!(file_names(&path), vec!["sstable_L1_000002.sst"]);

        for (key, value) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")] {
            assert_eq!(db.get(key), Some(value.to_string()));
        }
        let stats = db.stats();
        assert_eq!(stats.compactions_completed, 1);
        assert_eq!(stats.compactions_by_level, vec![(0, 1)]);
        assert_eq!(stats.compaction_input_files, 2);
        assert_eq!(stats.compaction_output_files, 1);
    }

    #[test]
    fn test_l0_files_may_overlap() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let db = PebbleDB::open_with_options(&path, no_compaction(2)).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("c".into(), "3".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        db.set("d".into(), "4".into()).unwrap();

        assert_eq!(levels_of(&db), vec![(0, 2)]);
        let ranges = file_ranges(&db, 0);
        assert_eq!(
            ranges,
            vec![
                ("a".to_string(), "c".to_string()),
                ("b".to_string(), "d".to_string())
            ]
        );
        assert!(ranges[0].0 <= ranges[1].1 && ranges[1].0 <= ranges[0].1);
        assert_eq!(
            db.scan(),
            owned(&[("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")])
        );
    }

    #[test]
    fn test_l1_files_do_not_overlap() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let mut config = options(2, 2, usize::MAX);
        config.target_file_size = 40;
        let db = PebbleDB::open_with_options(&path, config).unwrap();

        for i in 0..8 {
            db.set(format!("k{}", i), format!("v{}", i)).unwrap();
        }

        let ranges = file_ranges(&db, 1);
        assert!(ranges.len() > 1, "expected a split output: {:?}", ranges);
        for pair in ranges.windows(2) {
            assert!(
                pair[0].1 < pair[1].0,
                "level 1 files overlap: {:?} and {:?}",
                pair[0],
                pair[1]
            );
        }
        assert_eq!(db.keys().len(), 8);
        assert_eq!(db.get("k7"), Some("v7".into()));
    }

    #[test]
    fn test_multi_level_compaction() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let db = PebbleDB::open_with_options(&path, options(2, 2, 1)).unwrap();

        for i in 0..6 {
            db.set(format!("k{}", i), format!("v{}", i)).unwrap();
        }

        let levels = levels_of(&db);
        assert!(levels.iter().any(|(level, _)| *level >= 2));
        for i in 0..6 {
            assert_eq!(db.get(&format!("k{}", i)), Some(format!("v{}", i)));
        }
        let stats = db.stats();
        assert!(stats.compactions_completed >= 3);
        assert!(
            stats
                .compactions_by_level
                .iter()
                .any(|(level, _)| *level == 0)
        );
        assert!(
            stats
                .compactions_by_level
                .iter()
                .any(|(level, _)| *level >= 1)
        );
    }

    fn seed_deep_level(path: &Path) {
        let db = PebbleDB::open_with_options(path, options(2, 2, 1)).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        db.set("c".into(), "3".into()).unwrap();
        db.set("d".into(), "4".into()).unwrap();
        db.compact().unwrap();
        let levels = levels_of(&db);
        assert!(levels[0].0 >= 2, "expected a deep level: {:?}", levels);
    }

    #[test]
    fn test_tombstones_kept_while_a_deeper_level_exists() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        seed_deep_level(&path);

        let db = PebbleDB::open_with_options(&path, options(2, 2, usize::MAX)).unwrap();
        db.delete("a").unwrap();
        db.set("e".into(), "5".into()).unwrap();
        db.delete("b").unwrap();
        db.set("f".into(), "6".into()).unwrap();

        assert_eq!(levels_of(&db).first(), Some(&(1, 1)));
        assert_eq!(tombstone_keys(&db), vec!["a".to_string(), "b".to_string()]);
        assert_eq!(db.get("a"), None);
        assert_eq!(db.get("b"), None);
        assert_eq!(db.get("c"), Some("3".into()));
        assert_eq!(db.get("d"), Some("4".into()));
        assert_eq!(db.get("e"), Some("5".into()));
        assert_eq!(db.get("f"), Some("6".into()));
        assert_eq!(
            db.scan(),
            owned(&[("c", "3"), ("d", "4"), ("e", "5"), ("f", "6")])
        );
    }

    #[test]
    fn test_tombstones_removed_at_the_deepest_level() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        seed_deep_level(&path);

        {
            let db = PebbleDB::open_with_options(&path, options(2, 2, usize::MAX)).unwrap();
            db.delete("a").unwrap();
            db.set("e".into(), "5".into()).unwrap();
            db.delete("b").unwrap();
            db.set("f".into(), "6".into()).unwrap();
            assert_eq!(tombstone_keys(&db).len(), 2);
        }

        let db = PebbleDB::open_with_options(&path, options(2, 2, 1)).unwrap();
        db.compact().unwrap();

        assert_eq!(tombstone_keys(&db), Vec::<String>::new());
        assert_eq!(levels_of(&db).len(), 1);
        assert_eq!(db.get("a"), None);
        assert_eq!(db.get("b"), None);
        assert_eq!(db.get("c"), Some("3".into()));
        assert_eq!(db.get("d"), Some("4".into()));
        assert_eq!(db.get("e"), Some("5".into()));
        assert_eq!(db.get("f"), Some("6".into()));
    }

    #[test]
    fn test_recovery_after_compaction() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_options(&path, options(2, 2, usize::MAX)).unwrap();
            for i in 0..4 {
                db.set(format!("k{}", i), format!("v{}", i)).unwrap();
            }
            assert_eq!(levels_of(&db), vec![(1, 1)]);
        }

        let db = PebbleDB::open_with_options(&path, options(2, 2, usize::MAX)).unwrap();
        assert_eq!(levels_of(&db), vec![(1, 1)]);
        for i in 0..4 {
            assert_eq!(db.get(&format!("k{}", i)), Some(format!("v{}", i)));
        }

        db.set("z".into(), "1".into()).unwrap();
        db.set("y".into(), "2".into()).unwrap();
        assert_eq!(db.get("z"), Some("1".into()));
        assert_eq!(levels_of(&db), vec![(0, 1), (1, 1)]);
    }

    #[test]
    fn test_legacy_sstables_are_read_as_level_zero() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_options(&path, no_compaction(2)).unwrap();
            db.set("a".into(), "1".into()).unwrap();
            db.set("b".into(), "2".into()).unwrap();
            assert_eq!(file_names(&path), vec!["sstable_L0_000000.sst"]);
        }

        fs::rename(
            path.join("sstable_L0_000000.sst"),
            path.join("sstable_000000.sst"),
        )
        .unwrap();

        {
            let db = PebbleDB::open_with_options(&path, no_compaction(2)).unwrap();
            assert_eq!(levels_of(&db), vec![(0, 1)]);
            assert_eq!(db.get("a"), Some("1".into()));
            assert_eq!(db.scan(), owned(&[("a", "1"), ("b", "2")]));
        }

        let db = PebbleDB::open_with_options(&path, options(2, 1, usize::MAX)).unwrap();
        db.compact().unwrap();
        assert_eq!(levels_of(&db), vec![(1, 1)]);
        assert_eq!(db.scan(), owned(&[("a", "1"), ("b", "2")]));
    }

    #[test]
    fn test_partial_compaction_duplicates_are_tolerated() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_options(&path, no_compaction(2)).unwrap();
            db.set("a".into(), "1".into()).unwrap();
            db.set("b".into(), "2".into()).unwrap();
            db.set("c".into(), "3".into()).unwrap();
            db.set("d".into(), "4".into()).unwrap();
            assert_eq!(levels_of(&db), vec![(0, 2)]);
        }

        let mut merged: BTreeMap<String, SSTableEntry> = BTreeMap::new();
        for id in [0u64, 1] {
            let sst = SSTable::load(id, &path.join(sstable_file_name(0, id))).unwrap();
            for (key, entry) in sst.entries() {
                merged.insert(key, entry);
            }
        }
        write_sstable(&path.join(sstable_file_name(1, 2)), &merged).unwrap();

        let db = PebbleDB::open_with_options(&path, options(2, 2, usize::MAX)).unwrap();
        assert_eq!(levels_of(&db), vec![(0, 2), (1, 1)]);
        assert_eq!(db.get("a"), Some("1".into()));
        assert_eq!(db.get("d"), Some("4".into()));
        assert_eq!(
            db.scan(),
            owned(&[("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")])
        );

        db.compact().unwrap();
        assert_eq!(levels_of(&db), vec![(1, 1)]);
        assert_eq!(file_names(&path), vec!["sstable_L1_000003.sst"]);
        assert_eq!(
            db.scan(),
            owned(&[("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")])
        );
    }

    #[test]
    fn test_crash_during_compaction_temp_file_cleanup() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        {
            let db = PebbleDB::open_with_options(&path, no_compaction(2)).unwrap();
            db.set("a".into(), "1".into()).unwrap();
            db.set("b".into(), "2".into()).unwrap();
        }

        let temp_file = path.join("sstable_L1_000005.sst.tmp");
        fs::write(&temp_file, b"garbage").unwrap();

        let db = PebbleDB::open_with_options(&path, no_compaction(2)).unwrap();
        assert!(!temp_file.exists());
        assert_eq!(levels_of(&db), vec![(0, 1)]);
        assert_eq!(db.get("a"), Some("1".into()));
    }

    #[test]
    fn test_range_and_updates_across_levels() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        seed_deep_level(&path);

        let db = PebbleDB::open_with_options(&path, options(3, 2, usize::MAX)).unwrap();
        db.delete("b").unwrap();
        db.set("e".into(), "5".into()).unwrap();
        db.set("f".into(), "6".into()).unwrap();
        db.set("g".into(), "7".into()).unwrap();
        db.set("c".into(), "updated".into()).unwrap();
        db.set("h".into(), "8".into()).unwrap();
        db.set("i".into(), "9".into()).unwrap();

        assert_eq!(db.stats().memtable_entries, 1);
        assert_eq!(db.stats().levels.len(), 2);
        assert!(db.stats().levels.iter().any(|level| level.level == 1));
        assert!(db.stats().levels.iter().any(|level| level.level >= 2));

        assert_eq!(db.get("a"), Some("1".into()));
        assert_eq!(db.get("b"), None);
        assert_eq!(db.get("c"), Some("updated".into()));
        assert_eq!(
            db.scan(),
            owned(&[
                ("a", "1"),
                ("c", "updated"),
                ("d", "4"),
                ("e", "5"),
                ("f", "6"),
                ("g", "7"),
                ("h", "8"),
                ("i", "9"),
            ])
        );
        assert_eq!(
            range(&db, Some("c"), Some("g")),
            owned(&[("c", "updated"), ("d", "4"), ("e", "5"), ("f", "6")])
        );
        assert_eq!(
            db.range(.."b".to_string()).collect::<Vec<_>>(),
            owned(&[("a", "1")])
        );
    }

    #[test]
    fn test_compaction_metrics() {
        let dir = tempdir().unwrap();
        let db =
            PebbleDB::open_with_options(dir.path().join("db"), options(2, 2, usize::MAX)).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        db.set("c".into(), "3".into()).unwrap();
        db.set("d".into(), "4".into()).unwrap();

        let stats = db.stats();
        assert_eq!(stats.compactions_completed, 1);
        assert_eq!(stats.full_compactions, 0);
        assert_eq!(stats.compaction_input_files, 2);
        assert_eq!(stats.compaction_output_files, 1);
        assert!(stats.compaction_bytes_read > 0);
        assert!(stats.compaction_bytes_written > 0);
        assert!(stats.bytes_flushed > 0);

        let expected = (stats.bytes_flushed + stats.compaction_bytes_written) as f64
            / stats.bytes_flushed as f64;
        let write_amplification = stats.write_amplification.unwrap();
        assert!((write_amplification - expected).abs() < 1e-9);
        assert!(write_amplification >= 1.0);

        db.compact().unwrap();
        assert_eq!(db.stats().compactions_completed, 1);
    }

    #[test]
    fn test_full_compaction_merges_every_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let db = PebbleDB::open_with_options(&path, no_compaction(2)).unwrap();
        db.set("a".into(), "1".into()).unwrap();
        db.set("b".into(), "2".into()).unwrap();
        db.set("c".into(), "3".into()).unwrap();
        db.set("d".into(), "4".into()).unwrap();
        db.delete("b").unwrap();
        db.set("e".into(), "5".into()).unwrap();

        assert_eq!(levels_of(&db), vec![(0, 3)]);
        assert_eq!(tombstone_keys(&db), vec!["b".to_string()]);

        db.compact_full().unwrap();

        assert_eq!(levels_of(&db), vec![(1, 1)]);
        assert_eq!(file_names(&path), vec!["sstable_L1_000003.sst"]);
        assert_eq!(tombstone_keys(&db), Vec::<String>::new());
        assert_eq!(
            db.scan(),
            owned(&[("a", "1"), ("c", "3"), ("d", "4"), ("e", "5")])
        );
        let stats = db.stats();
        assert_eq!(stats.full_compactions, 1);
        assert_eq!(stats.compactions_completed, 1);
        assert_eq!(stats.compaction_input_files, 3);
        assert_eq!(stats.compaction_output_files, 1);
    }

    #[test]
    fn test_concurrent_reads_while_compacting() {
        use std::sync::atomic::AtomicBool;
        use std::thread;

        let dir = tempdir().unwrap();
        let path = dir.path().join("db");
        let db = Arc::new(PebbleDB::open_with_options(&path, options(4, 2, 1)).unwrap());
        for i in 0..4 {
            db.set(format!("k{:02}", i), format!("v{}", i)).unwrap();
        }

        let stop = Arc::new(AtomicBool::new(false));
        let mut readers = Vec::new();
        for _ in 0..3 {
            let db = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            readers.push(thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    for i in 0..4 {
                        let _ = db.get(&format!("k{:02}", i));
                    }
                    let scan = db.scan();
                    assert!(scan.windows(2).all(|pair| pair[0].0 < pair[1].0));
                    let _ = db.range("k".to_string().."l".to_string()).count();
                }
            }));
        }

        for i in 4..64 {
            db.set(format!("k{:02}", i), format!("v{}", i)).unwrap();
            if i % 7 == 0 {
                db.delete(&format!("k{:02}", i)).unwrap();
            }
        }
        stop.store(true, Ordering::Relaxed);
        for reader in readers {
            reader.join().unwrap();
        }

        assert_eq!(db.keys().len(), 55);
        assert_eq!(db.get("k63"), None);
        assert_eq!(db.get("k62"), Some("v62".into()));
        assert_eq!(db.get("k00"), Some("v0".into()));
    }
}
