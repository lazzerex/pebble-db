use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use crate::db::MemtableEntry;
use crate::sstable::{SSTable, SSTableEntry};

/// Ordered cursor over a single key-space source served in ascending key order.
///
/// A tombstone is reported as a valid key with no value; use [`StorageIterator::valid`]
/// to distinguish an exhausted iterator from a deleted entry.
pub trait StorageIterator {
    fn seek_to_first(&mut self);
    fn seek(&mut self, target: &str);
    fn advance(&mut self);
    fn valid(&self) -> bool;
    fn key(&self) -> Option<&str>;
    fn value(&self) -> Option<&str>;
}

pub struct MemtableIterator {
    entries: Vec<(String, Option<String>)>,
    pos: Option<usize>,
}

impl MemtableIterator {
    pub fn new(memtable: &BTreeMap<String, MemtableEntry>) -> Self {
        let entries = memtable
            .iter()
            .map(|(key, entry)| {
                let value = match entry {
                    MemtableEntry::Value(value) => Some(value.clone()),
                    MemtableEntry::Tombstone => None,
                };
                (key.clone(), value)
            })
            .collect();
        Self { entries, pos: None }
    }
}

impl StorageIterator for MemtableIterator {
    fn seek_to_first(&mut self) {
        self.pos = (!self.entries.is_empty()).then_some(0);
    }

    fn seek(&mut self, target: &str) {
        let idx = self
            .entries
            .partition_point(|(key, _)| key.as_str() < target);
        self.pos = (idx < self.entries.len()).then_some(idx);
    }

    fn advance(&mut self) {
        self.pos = match self.pos {
            Some(pos) if pos + 1 < self.entries.len() => Some(pos + 1),
            _ => None,
        };
    }

    fn valid(&self) -> bool {
        self.pos.is_some()
    }

    fn key(&self) -> Option<&str> {
        self.pos
            .and_then(|pos| self.entries.get(pos))
            .map(|(key, _)| key.as_str())
    }

    fn value(&self) -> Option<&str> {
        self.pos
            .and_then(|pos| self.entries.get(pos))
            .and_then(|(_, value)| value.as_deref())
    }
}

pub struct SSTableIterator {
    sst: Arc<SSTable>,
    block: usize,
    entries: Vec<(String, SSTableEntry)>,
    index: usize,
}

impl SSTableIterator {
    pub fn new(sst: Arc<SSTable>) -> Self {
        Self {
            sst,
            block: 0,
            entries: Vec::new(),
            index: 0,
        }
    }

    fn load_block(&mut self, block: usize) {
        self.block = block;
        self.entries = self.sst.read_block(block).unwrap_or_default();
        self.index = 0;
    }

    /// Moves to the next block while the current one is exhausted.
    fn ensure_entry(&mut self) -> bool {
        while self.index >= self.entries.len() {
            if self.block + 1 >= self.sst.block_count() {
                return false;
            }
            self.load_block(self.block + 1);
        }
        true
    }
}

impl StorageIterator for SSTableIterator {
    fn seek_to_first(&mut self) {
        self.load_block(0);
        self.ensure_entry();
    }

    fn seek(&mut self, target: &str) {
        self.load_block(self.sst.find_block(target).unwrap_or(0));
        self.index = self
            .entries
            .partition_point(|(key, _)| key.as_str() < target);
        self.ensure_entry();
    }

    fn advance(&mut self) {
        if self.index < self.entries.len() {
            self.index += 1;
        }
        self.ensure_entry();
    }

    fn valid(&self) -> bool {
        self.index < self.entries.len()
    }

    fn key(&self) -> Option<&str> {
        self.entries.get(self.index).map(|(key, _)| key.as_str())
    }

    fn value(&self) -> Option<&str> {
        match self.entries.get(self.index) {
            Some((_, SSTableEntry::Value(value))) => Some(value.as_str()),
            _ => None,
        }
    }
}

/// Merges several sources ordered from newest to oldest.
///
/// Keys are emitted once, in ascending order, using the value of the newest source
/// that contains them. A tombstone in the newest source hides older values and is
/// never emitted, so a merged iterator only yields live entries.
pub struct MergeIterator {
    children: Vec<Box<dyn StorageIterator>>,
    current: Option<CurrentEntry>,
}

struct CurrentEntry {
    key: String,
    source: usize,
}

impl MergeIterator {
    /// Builds the merged iterator positioned on the smallest live key.
    pub fn new(mut children: Vec<Box<dyn StorageIterator>>) -> Self {
        for child in &mut children {
            child.seek_to_first();
        }
        let mut merged = Self {
            children,
            current: None,
        };
        merged.normalize();
        merged
    }

    fn normalize(&mut self) {
        loop {
            let Some((key, source)) = self.smallest() else {
                self.current = None;
                return;
            };
            if self.children[source].value().is_some() {
                self.current = Some(CurrentEntry { key, source });
                return;
            }
            self.skip_key(&key);
        }
    }

    fn smallest(&self) -> Option<(String, usize)> {
        let mut best: Option<(String, usize)> = None;
        for (source, child) in self.children.iter().enumerate() {
            let Some(key) = child.key() else { continue };
            match &best {
                Some((best_key, _)) if best_key.as_str() <= key => {}
                _ => best = Some((key.to_string(), source)),
            }
        }
        best
    }

    fn skip_key(&mut self, key: &str) {
        for child in &mut self.children {
            if child.key() == Some(key) {
                child.advance();
            }
        }
    }
}

impl StorageIterator for MergeIterator {
    fn seek_to_first(&mut self) {
        for child in &mut self.children {
            child.seek_to_first();
        }
        self.normalize();
    }

    fn seek(&mut self, target: &str) {
        for child in &mut self.children {
            child.seek(target);
        }
        self.normalize();
    }

    fn advance(&mut self) {
        let Some(current) = self.current.take() else {
            return;
        };
        self.skip_key(&current.key);
        self.normalize();
    }

    fn valid(&self) -> bool {
        self.current.is_some()
    }

    fn key(&self) -> Option<&str> {
        self.current.as_ref().map(|current| current.key.as_str())
    }

    fn value(&self) -> Option<&str> {
        self.current
            .as_ref()
            .and_then(|current| self.children[current.source].value())
    }
}

/// Ascending iterator over the live keys of a database between two bounds.
pub struct RangeIterator {
    merged: MergeIterator,
    end: Bound<String>,
}

impl RangeIterator {
    /// Positions the iterator on the first live key that satisfies `start`.
    pub fn new(merged: MergeIterator, start: Bound<String>, end: Bound<String>) -> Self {
        let mut iter = Self { merged, end };
        match start {
            Bound::Unbounded => {}
            Bound::Included(key) => iter.merged.seek(&key),
            Bound::Excluded(key) => {
                iter.merged.seek(&key);
                if iter.merged.key() == Some(key.as_str()) {
                    iter.merged.advance();
                }
            }
        }
        iter
    }

    fn within_end(&self) -> bool {
        let Some(key) = self.merged.key() else {
            return false;
        };
        match &self.end {
            Bound::Unbounded => true,
            Bound::Included(end) => key <= end.as_str(),
            Bound::Excluded(end) => key < end.as_str(),
        }
    }
}

impl StorageIterator for RangeIterator {
    fn seek_to_first(&mut self) {
        self.merged.seek_to_first();
    }

    /// Seeks to the first live key greater than or equal to `target`.
    fn seek(&mut self, target: &str) {
        self.merged.seek(target);
    }

    fn advance(&mut self) {
        self.merged.advance();
    }

    fn valid(&self) -> bool {
        self.merged.valid() && self.within_end()
    }

    fn key(&self) -> Option<&str> {
        if self.valid() {
            self.merged.key()
        } else {
            None
        }
    }

    fn value(&self) -> Option<&str> {
        if self.valid() {
            self.merged.value()
        } else {
            None
        }
    }
}

impl Iterator for RangeIterator {
    type Item = (String, String);

    fn next(&mut self) -> Option<(String, String)> {
        if !self.valid() {
            return None;
        }
        let key = self.key()?.to_string();
        let value = self.value()?.to_string();
        self.advance();
        Some((key, value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::tempdir;

    fn pairs(entries: &[(&str, Option<&str>)]) -> Vec<(String, Option<String>)> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.map(str::to_string)))
            .collect()
    }

    fn memtable(entries: &[(&str, Option<&str>)]) -> BTreeMap<String, MemtableEntry> {
        entries
            .iter()
            .map(|(key, value)| {
                let entry = match value {
                    Some(value) => MemtableEntry::Value((*value).to_string()),
                    None => MemtableEntry::Tombstone,
                };
                ((*key).to_string(), entry)
            })
            .collect()
    }

    fn memtable_iter(entries: &[(&str, Option<&str>)]) -> MemtableIterator {
        MemtableIterator::new(&memtable(entries))
    }

    fn sstable_iterator(
        path: &Path,
        block_size: usize,
        entries: &[(&str, Option<&str>)],
    ) -> SSTableIterator {
        let map: BTreeMap<String, SSTableEntry> = entries
            .iter()
            .map(|(key, value)| {
                let entry = match value {
                    Some(value) => SSTableEntry::Value((*value).to_string()),
                    None => SSTableEntry::Tombstone,
                };
                ((*key).to_string(), entry)
            })
            .collect();
        SSTable::write_sstable_with_block_size(path, &map, block_size).unwrap();
        SSTableIterator::new(Arc::new(SSTable::load(0, path).unwrap()))
    }

    fn drain(iter: &mut impl StorageIterator) -> Vec<(String, Option<String>)> {
        let mut result = Vec::new();
        while iter.valid() {
            result.push((
                iter.key().unwrap().to_string(),
                iter.value().map(str::to_string),
            ));
            iter.advance();
        }
        result
    }

    #[test]
    fn memtable_iterates_in_key_order() {
        let mut iter = memtable_iter(&[("c", Some("3")), ("a", Some("1")), ("b", Some("2"))]);
        iter.seek_to_first();
        assert_eq!(
            drain(&mut iter),
            pairs(&[("a", Some("1")), ("b", Some("2")), ("c", Some("3"))])
        );
    }

    #[test]
    fn memtable_keeps_only_latest_value_per_key() {
        let mut iter = memtable_iter(&[("a", Some("old")), ("a", Some("new")), ("b", Some("2"))]);
        iter.seek_to_first();
        assert_eq!(
            drain(&mut iter),
            pairs(&[("a", Some("new")), ("b", Some("2"))])
        );
    }

    #[test]
    fn memtable_reports_tombstone() {
        let mut iter = memtable_iter(&[("a", Some("1")), ("b", None)]);
        iter.seek_to_first();
        assert_eq!(drain(&mut iter), pairs(&[("a", Some("1")), ("b", None)]));
    }

    #[test]
    fn memtable_seek_variants() {
        let mut iter = memtable_iter(&[("b", Some("2")), ("d", Some("4"))]);

        iter.seek("b");
        assert_eq!((iter.key(), iter.value()), (Some("b"), Some("2")));

        iter.seek("c");
        assert_eq!((iter.key(), iter.value()), (Some("d"), Some("4")));

        iter.seek("a");
        assert_eq!(iter.key(), Some("b"));

        iter.seek("z");
        assert!(!iter.valid());
        assert_eq!(iter.key(), None);
        assert_eq!(iter.value(), None);
    }

    #[test]
    fn memtable_advance_past_end_stays_invalid() {
        let mut iter = memtable_iter(&[("a", Some("1"))]);
        iter.seek_to_first();
        iter.advance();
        assert!(!iter.valid());
        iter.advance();
        assert!(!iter.valid());
    }

    #[test]
    fn memtable_empty() {
        let mut iter = memtable_iter(&[]);
        iter.seek_to_first();
        assert!(!iter.valid());
        assert_eq!(iter.key(), None);
    }

    #[test]
    fn sstable_iterates_all_blocks() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multi.sst");
        let keys: Vec<String> = (0..40).map(|i| format!("key_{:03}", i)).collect();
        let boxed: Vec<(&str, Option<&str>)> = keys
            .iter()
            .map(|key| (key.as_str(), Some("value")))
            .collect();
        let mut iter = sstable_iterator(&path, 64, &boxed);
        assert!(iter.sst.block_count() > 1);
        iter.seek_to_first();
        let drained = drain(&mut iter);
        assert_eq!(drained.len(), 40);
        assert_eq!(drained.first().unwrap().0, "key_000");
        assert_eq!(drained.last().unwrap().0, "key_039");
    }

    #[test]
    fn sstable_seek_variants() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seek.sst");
        let mut iter = sstable_iterator(
            &path,
            64,
            &[
                ("a", Some("1")),
                ("c", Some("3")),
                ("e", Some("5")),
                ("g", Some("7")),
            ],
        );

        iter.seek("a");
        assert_eq!(iter.key(), Some("a"));

        iter.seek("d");
        assert_eq!(iter.key(), Some("e"));

        iter.seek("f");
        assert_eq!((iter.key(), iter.value()), (Some("g"), Some("7")));

        iter.seek("z");
        assert!(!iter.valid());

        iter.seek("b");
        assert_eq!(iter.key(), Some("c"));
        iter.advance();
        assert_eq!(iter.key(), Some("e"));
    }

    #[test]
    fn sstable_reports_tombstone() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("tombstone.sst");
        let mut iter = sstable_iterator(&path, 64, &[("a", Some("1")), ("b", None)]);
        iter.seek("b");
        assert!(iter.valid());
        assert_eq!(iter.key(), Some("b"));
        assert_eq!(iter.value(), None);
    }

    #[test]
    fn sstable_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.sst");
        let mut iter = sstable_iterator(&path, 64, &[]);
        iter.seek_to_first();
        assert!(!iter.valid());
        iter.seek("a");
        assert!(!iter.valid());
        iter.advance();
        assert!(!iter.valid());
        assert_eq!(iter.key(), None);
    }

    #[test]
    fn merge_prefers_newest_source() {
        let newest = memtable(&[("a", Some("new")), ("b", Some("2"))]);
        let older = memtable(&[("a", Some("old")), ("c", Some("3"))]);
        let children: Vec<Box<dyn StorageIterator>> = vec![
            Box::new(MemtableIterator::new(&newest)),
            Box::new(MemtableIterator::new(&older)),
        ];
        let mut iter = MergeIterator::new(children);
        assert_eq!(
            drain(&mut iter),
            pairs(&[("a", Some("new")), ("b", Some("2")), ("c", Some("3"))])
        );
    }

    #[test]
    fn merge_tombstone_hides_older_value() {
        let newest = memtable(&[("a", None), ("b", Some("2"))]);
        let older = memtable(&[("a", Some("old")), ("b", Some("stale"))]);
        let children: Vec<Box<dyn StorageIterator>> = vec![
            Box::new(MemtableIterator::new(&newest)),
            Box::new(MemtableIterator::new(&older)),
        ];
        let mut iter = MergeIterator::new(children);
        assert_eq!(drain(&mut iter), pairs(&[("b", Some("2"))]));
    }

    #[test]
    fn merge_hides_key_deleted_in_every_source() {
        let newest = memtable(&[("a", None)]);
        let older = memtable(&[("a", None)]);
        let children: Vec<Box<dyn StorageIterator>> = vec![
            Box::new(MemtableIterator::new(&newest)),
            Box::new(MemtableIterator::new(&older)),
        ];
        let mut iter = MergeIterator::new(children);
        assert!(!iter.valid());
        assert_eq!(drain(&mut iter), pairs(&[]));
    }

    #[test]
    fn merge_orders_interleaved_sources() {
        let dir = tempdir().unwrap();
        let newest = memtable(&[("b", Some("b"))]);
        let middle = sstable_iterator(
            &dir.path().join("middle.sst"),
            64,
            &[("a", Some("a")), ("c", Some("middle")), ("e", Some("e"))],
        );
        let oldest = sstable_iterator(
            &dir.path().join("oldest.sst"),
            64,
            &[("c", Some("oldest")), ("d", Some("d"))],
        );
        let children: Vec<Box<dyn StorageIterator>> = vec![
            Box::new(MemtableIterator::new(&newest)),
            Box::new(middle),
            Box::new(oldest),
        ];
        let mut iter = MergeIterator::new(children);
        assert_eq!(
            drain(&mut iter),
            pairs(&[
                ("a", Some("a")),
                ("b", Some("b")),
                ("c", Some("middle")),
                ("d", Some("d")),
                ("e", Some("e")),
            ])
        );
    }

    #[test]
    fn merge_seek_positions_every_source() {
        let newest = memtable(&[("b", Some("b")), ("d", Some("d"))]);
        let older = memtable(&[("a", Some("a")), ("c", Some("c"))]);
        let children: Vec<Box<dyn StorageIterator>> = vec![
            Box::new(MemtableIterator::new(&newest)),
            Box::new(MemtableIterator::new(&older)),
        ];
        let mut iter = MergeIterator::new(children);

        iter.seek("c");
        assert_eq!((iter.key(), iter.value()), (Some("c"), Some("c")));
        iter.advance();
        assert_eq!(iter.key(), Some("d"));
        iter.advance();
        assert!(!iter.valid());
    }

    #[test]
    fn merge_tombstone_shadowing_sstable_does_not_stop_iteration() {
        let dir = tempdir().unwrap();
        let newest = memtable(&[("a", None)]);
        let older = sstable_iterator(
            &dir.path().join("older.sst"),
            64,
            &[("a", Some("gone")), ("b", Some("kept"))],
        );
        let children: Vec<Box<dyn StorageIterator>> =
            vec![Box::new(MemtableIterator::new(&newest)), Box::new(older)];
        let mut iter = MergeIterator::new(children);
        assert_eq!(drain(&mut iter), pairs(&[("b", Some("kept"))]));
    }

    fn range_iter(
        entries: &[(&str, Option<&str>)],
        start: Bound<String>,
        end: Bound<String>,
    ) -> RangeIterator {
        let source = memtable(entries);
        let children: Vec<Box<dyn StorageIterator>> =
            vec![Box::new(MemtableIterator::new(&source))];
        RangeIterator::new(MergeIterator::new(children), start, end)
    }

    fn bounds(start: Option<&str>, end: Option<&str>) -> (Bound<String>, Bound<String>) {
        (
            start.map_or(Bound::Unbounded, |key| Bound::Included(key.to_string())),
            end.map_or(Bound::Unbounded, |key| Bound::Excluded(key.to_string())),
        )
    }

    #[test]
    fn range_applies_start_and_end_bounds() {
        let entries = [
            ("a", Some("1")),
            ("b", Some("2")),
            ("c", Some("3")),
            ("d", Some("4")),
        ];

        let (start, end) = bounds(Some("b"), Some("d"));
        let iter = range_iter(&entries, start, end);
        assert_eq!(
            iter.collect::<Vec<_>>(),
            vec![
                ("b".to_string(), "2".to_string()),
                ("c".to_string(), "3".to_string()),
            ]
        );

        let (start, end) = bounds(Some("b"), None);
        let iter = range_iter(&entries, start, end);
        assert_eq!(iter.count(), 3);

        let (start, end) = bounds(None, Some("c"));
        let iter = range_iter(&entries, start, end);
        assert_eq!(iter.count(), 2);

        let (start, end) = bounds(None, None);
        let iter = range_iter(&entries, start, end);
        assert_eq!(iter.count(), 4);
    }

    #[test]
    fn range_excluded_start_skips_the_key() {
        let entries = [("a", Some("1")), ("b", Some("2")), ("c", Some("3"))];
        let iter = range_iter(&entries, Bound::Excluded("a".to_string()), Bound::Unbounded);
        assert_eq!(
            iter.collect::<Vec<_>>(),
            vec![
                ("b".to_string(), "2".to_string()),
                ("c".to_string(), "3".to_string()),
            ]
        );
    }

    #[test]
    fn range_included_end_keeps_the_key() {
        let entries = [("a", Some("1")), ("b", Some("2")), ("c", Some("3"))];
        let iter = range_iter(&entries, Bound::Unbounded, Bound::Included("b".to_string()));
        assert_eq!(
            iter.collect::<Vec<_>>(),
            vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "2".to_string()),
            ]
        );
    }

    #[test]
    fn range_seek_moves_within_bounds() {
        let entries = [
            ("a", Some("1")),
            ("b", Some("2")),
            ("c", Some("3")),
            ("d", Some("4")),
        ];
        let mut iter = range_iter(&entries, Bound::Unbounded, Bound::Excluded("d".to_string()));
        iter.seek("c");
        assert_eq!(iter.key(), Some("c"));
        assert_eq!(iter.next(), Some(("c".to_string(), "3".to_string())));
        assert_eq!(iter.next(), None);
    }
}
