#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LevelConfig {
    pub(crate) l0_compaction_threshold: usize,
    pub(crate) level_size_multiplier: u64,
    pub(crate) target_level_size: u64,
}

impl LevelConfig {
    pub(crate) fn new(
        l0_compaction_threshold: usize,
        level_size_multiplier: usize,
        target_level_size: usize,
    ) -> Self {
        Self {
            l0_compaction_threshold: l0_compaction_threshold.max(1),
            level_size_multiplier: (level_size_multiplier as u64).max(2),
            target_level_size: (target_level_size as u64).max(1),
        }
    }

    fn level_capacity(&self, level: u32) -> u64 {
        let mut capacity = self.target_level_size;
        for _ in 1..level {
            capacity = capacity.saturating_mul(self.level_size_multiplier);
        }
        capacity
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FileMeta {
    pub(crate) id: u64,
    pub(crate) level: u32,
    pub(crate) smallest: String,
    pub(crate) largest: String,
    pub(crate) size: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CompactionPlan {
    pub(crate) level: u32,
    pub(crate) inputs: Vec<u64>,
    pub(crate) overlaps: Vec<u64>,
    pub(crate) drop_tombstones: bool,
}

pub(crate) fn pick_compaction(files: &[FileMeta], config: &LevelConfig) -> Option<CompactionPlan> {
    let deepest = files.iter().map(|file| file.level).max()?;

    let l0: Vec<&FileMeta> = files.iter().filter(|file| file.level == 0).collect();
    if l0.len() >= config.l0_compaction_threshold {
        let (smallest, largest) = union_range(&l0);
        let overlaps = overlapping(files, 1, &smallest, &largest);
        return Some(CompactionPlan {
            level: 0,
            inputs: ids(&l0),
            overlaps: ids(&overlaps),
            drop_tombstones: deepest <= 1,
        });
    }

    for level in 1..=deepest {
        let level_files: Vec<&FileMeta> = files.iter().filter(|file| file.level == level).collect();
        if level_files.is_empty() {
            continue;
        }
        let size: u64 = level_files.iter().map(|file| file.size as u64).sum();
        if size <= config.level_capacity(level) {
            continue;
        }

        let input = level_files.iter().min_by_key(|file| file.id).unwrap();
        let overlaps = overlapping(files, level + 1, &input.smallest, &input.largest);
        return Some(CompactionPlan {
            level,
            inputs: vec![input.id],
            overlaps: ids(&overlaps),
            drop_tombstones: deepest <= level + 1,
        });
    }

    None
}

fn union_range(files: &[&FileMeta]) -> (String, String) {
    let smallest = files
        .iter()
        .map(|file| file.smallest.as_str())
        .min()
        .unwrap()
        .to_string();
    let largest = files
        .iter()
        .map(|file| file.largest.as_str())
        .max()
        .unwrap()
        .to_string();
    (smallest, largest)
}

fn overlapping<'a>(
    files: &'a [FileMeta],
    level: u32,
    smallest: &str,
    largest: &str,
) -> Vec<&'a FileMeta> {
    files
        .iter()
        .filter(|file| {
            file.level == level
                && file.smallest.as_str() <= largest
                && smallest <= file.largest.as_str()
        })
        .collect()
}

fn ids(files: &[&FileMeta]) -> Vec<u64> {
    let mut ids: Vec<u64> = files.iter().map(|file| file.id).collect();
    ids.sort_unstable_by(|a, b| b.cmp(a));
    ids
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(id: u64, level: u32, smallest: &str, largest: &str, size: usize) -> FileMeta {
        FileMeta {
            id,
            level,
            smallest: smallest.to_string(),
            largest: largest.to_string(),
            size,
        }
    }

    fn level_config(l0_threshold: usize, target_level_size: usize) -> LevelConfig {
        LevelConfig::new(l0_threshold, 10, target_level_size)
    }

    #[test]
    fn no_files_means_no_work() {
        assert_eq!(pick_compaction(&[], &level_config(4, 1000)), None);
    }

    #[test]
    fn l0_below_threshold_is_left_alone() {
        let files = vec![meta(1, 0, "a", "b", 10), meta(2, 0, "c", "d", 10)];
        assert_eq!(pick_compaction(&files, &level_config(4, 1000)), None);
    }

    #[test]
    fn small_levels_are_left_alone() {
        let files = vec![meta(1, 1, "a", "m", 400), meta(2, 1, "n", "z", 400)];
        assert_eq!(pick_compaction(&files, &level_config(4, 1000)), None);
    }

    #[test]
    fn l0_threshold_compacts_all_l0_files_with_overlapping_l1() {
        let files = vec![
            meta(3, 0, "b", "d", 10),
            meta(1, 0, "a", "c", 10),
            meta(5, 1, "c", "e", 10),
            meta(6, 1, "x", "z", 10),
        ];
        let plan = pick_compaction(&files, &level_config(2, 1000)).unwrap();
        assert_eq!(plan.level, 0);
        assert_eq!(plan.inputs, vec![3, 1]);
        assert_eq!(plan.overlaps, vec![5]);
        assert!(plan.drop_tombstones);
    }

    #[test]
    fn l0_plan_keeps_tombstones_when_a_deeper_level_exists() {
        let files = vec![
            meta(1, 0, "a", "b", 10),
            meta(2, 0, "c", "d", 10),
            meta(3, 2, "a", "d", 10),
        ];
        let plan = pick_compaction(&files, &level_config(2, 1000)).unwrap();
        assert_eq!(plan.level, 0);
        assert!(!plan.drop_tombstones);
    }

    #[test]
    fn oversized_level_compacts_its_oldest_file_with_overlaps() {
        let files = vec![
            meta(7, 2, "a", "c", 10),
            meta(2, 1, "c", "o", 900),
            meta(5, 1, "a", "b", 900),
            meta(9, 2, "x", "z", 10),
            meta(11, 3, "a", "z", 10),
        ];
        let plan = pick_compaction(&files, &level_config(100, 1000)).unwrap();
        assert_eq!(plan.level, 1);
        assert_eq!(plan.inputs, vec![2]);
        assert_eq!(plan.overlaps, vec![7]);
        assert!(!plan.drop_tombstones);
    }

    #[test]
    fn deepest_level_compaction_drops_tombstones() {
        let files = vec![meta(1, 1, "a", "z", 5000)];
        let plan = pick_compaction(&files, &level_config(100, 1000)).unwrap();
        assert_eq!(plan.level, 1);
        assert_eq!(plan.overlaps, Vec::new());
        assert!(plan.drop_tombstones);
    }

    #[test]
    fn level_capacity_grows_with_the_multiplier() {
        let config = LevelConfig::new(4, 10, 100);
        assert_eq!(config.level_capacity(1), 100);
        assert_eq!(config.level_capacity(2), 1000);
        assert_eq!(config.level_capacity(3), 10_000);
    }

    #[test]
    fn config_is_clamped_to_terminating_values() {
        let config = LevelConfig::new(0, 0, 0);
        assert_eq!(config.l0_compaction_threshold, 1);
        assert_eq!(config.level_size_multiplier, 2);
        assert_eq!(config.target_level_size, 1);
    }

    #[test]
    fn picker_is_deterministic() {
        let files = vec![meta(3, 0, "b", "d", 10), meta(1, 0, "a", "c", 10)];
        let config = level_config(2, 1000);
        assert_eq!(
            pick_compaction(&files, &config),
            pick_compaction(&files, &config)
        );
    }
}
