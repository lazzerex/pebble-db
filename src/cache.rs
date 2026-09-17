use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::db::{DbOptions, PebbleDB};
use crate::error::{PebbleError, Result};

pub const KEY_PREFIX: &str = "cache:";
const NO_EXPIRATION: u64 = 0;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub sets: u64,
    pub deletes: u64,
    pub expired: u64,
    pub clears: u64,
}

pub struct PebbleCache {
    db: PebbleDB,
    stats: Mutex<CacheStats>,
}

impl PebbleCache {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, DbOptions::default())
    }

    pub fn open_with_options(path: impl AsRef<Path>, options: DbOptions) -> Result<Self> {
        Ok(Self {
            db: PebbleDB::open_with_options(path, options)?,
            stats: Mutex::new(CacheStats::default()),
        })
    }

    pub fn set(&self, key: &str, value: &str, ttl: Duration) -> Result<()> {
        let expires_at = if ttl.is_zero() {
            NO_EXPIRATION
        } else {
            now_ms() + ttl.as_millis() as u64
        };
        self.db.set(cache_key(key), encode(expires_at, value))?;
        self.record(|stats| stats.sets += 1);
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<String>> {
        let stored_key = cache_key(key);
        let Some(raw) = self.db.get(&stored_key) else {
            self.record(|stats| stats.misses += 1);
            return Ok(None);
        };

        let (expires_at, value) = decode(&raw)?;
        if is_expired(expires_at) {
            self.db.delete(&stored_key)?;
            self.record(|stats| {
                stats.misses += 1;
                stats.expired += 1;
            });
            return Ok(None);
        }

        self.record(|stats| stats.hits += 1);
        Ok(Some(value))
    }

    pub fn delete(&self, key: &str) -> Result<bool> {
        let existed = self.db.delete(&cache_key(key))?;
        self.record(|stats| stats.deletes += 1);
        Ok(existed)
    }

    pub fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    pub fn clear(&self) -> Result<()> {
        let mut keys = Vec::new();
        for (key, _) in self.db.range(KEY_PREFIX.to_string()..) {
            if !key.starts_with(KEY_PREFIX) {
                break;
            }
            keys.push(key);
        }

        for key in &keys {
            self.db.delete(key)?;
        }
        self.record(|stats| {
            stats.deletes += keys.len() as u64;
            stats.clears += 1;
        });
        Ok(())
    }

    pub fn stats(&self) -> CacheStats {
        *self.stats.lock().unwrap()
    }

    pub fn close(self) -> Result<()> {
        self.db.close()
    }

    fn record(&self, update: impl FnOnce(&mut CacheStats)) {
        update(&mut self.stats.lock().unwrap());
    }
}

fn cache_key(key: &str) -> String {
    format!("{}{}", KEY_PREFIX, key)
}

fn encode(expires_at: u64, value: &str) -> String {
    format!("{}:{}", expires_at, value)
}

fn decode(raw: &str) -> Result<(u64, String)> {
    let Some((expires_at, value)) = raw.split_once(':') else {
        return Err(PebbleError::CacheCorruption(format!(
            "entry has no expiration field: {}",
            raw
        )));
    };
    let expires_at = expires_at.parse::<u64>().map_err(|_| {
        PebbleError::CacheCorruption(format!("invalid expiration field: {}", expires_at))
    })?;
    Ok((expires_at, value.to_string()))
}

fn is_expired(expires_at: u64) -> bool {
    expires_at != NO_EXPIRATION && expires_at <= now_ms()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use tempfile::tempdir;

    fn cache(threshold: usize) -> (tempfile::TempDir, PebbleCache) {
        let dir = tempdir().unwrap();
        let options = DbOptions {
            flush_threshold: threshold,
            ..DbOptions::default()
        };
        let cache = PebbleCache::open_with_options(dir.path().join("cache"), options).unwrap();
        (dir, cache)
    }

    #[test]
    fn set_and_get_returns_the_stored_value() {
        let (_dir, cache) = cache(100);
        cache.set("greeting", "hello", Duration::ZERO).unwrap();
        assert_eq!(cache.get("greeting").unwrap(), Some("hello".into()));
    }

    #[test]
    fn get_of_unknown_key_is_a_miss() {
        let (_dir, cache) = cache(100);
        assert_eq!(cache.get("missing").unwrap(), None);
        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 0);
    }

    #[test]
    fn set_overwrites_the_previous_value() {
        let (_dir, cache) = cache(100);
        cache.set("k", "old", Duration::ZERO).unwrap();
        cache.set("k", "new", Duration::ZERO).unwrap();
        assert_eq!(cache.get("k").unwrap(), Some("new".into()));
        assert_eq!(cache.stats().sets, 2);
    }

    #[test]
    fn values_may_contain_colons() {
        let (_dir, cache) = cache(100);
        cache
            .set("url", "http://example.com", Duration::ZERO)
            .unwrap();
        assert_eq!(cache.get("url").unwrap(), Some("http://example.com".into()));
    }

    #[test]
    fn delete_removes_the_entry() {
        let (_dir, cache) = cache(100);
        cache.set("k", "v", Duration::ZERO).unwrap();
        assert!(cache.delete("k").unwrap());
        assert_eq!(cache.get("k").unwrap(), None);
        assert_eq!(cache.stats().deletes, 1);
    }

    #[test]
    fn delete_of_unknown_key_reports_false() {
        let (_dir, cache) = cache(100);
        assert!(!cache.delete("missing").unwrap());
    }

    #[test]
    fn exists_follows_get() {
        let (_dir, cache) = cache(100);
        cache.set("k", "v", Duration::ZERO).unwrap();
        assert!(cache.exists("k").unwrap());
        cache.delete("k").unwrap();
        assert!(!cache.exists("k").unwrap());
    }

    #[test]
    fn entries_without_ttl_never_expire() {
        let (_dir, cache) = cache(100);
        cache.db.set(cache_key("k"), "0:still-here".into()).unwrap();
        assert_eq!(cache.get("k").unwrap(), Some("still-here".into()));
        assert_eq!(cache.stats().expired, 0);
    }

    #[test]
    fn expired_entry_is_removed_on_get() {
        let (_dir, cache) = cache(100);
        cache.db.set(cache_key("k"), "1:stale".into()).unwrap();

        assert_eq!(cache.get("k").unwrap(), None);
        assert_eq!(cache.db.get(&cache_key("k")), None);
        let stats = cache.stats();
        assert_eq!(stats.expired, 1);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn ttl_expires_with_real_time() {
        let (_dir, cache) = cache(100);
        cache.set("k", "v", Duration::from_millis(20)).unwrap();
        assert_eq!(cache.get("k").unwrap(), Some("v".into()));
        thread::sleep(Duration::from_millis(40));
        assert_eq!(cache.get("k").unwrap(), None);
        assert_eq!(cache.stats().expired, 1);
    }

    #[test]
    fn expired_entry_is_invisible_to_exists() {
        let (_dir, cache) = cache(100);
        cache.db.set(cache_key("k"), "1:stale".into()).unwrap();
        assert!(!cache.exists("k").unwrap());
        assert_eq!(cache.db.get(&cache_key("k")), None);
    }

    #[test]
    fn malformed_entry_reports_corruption() {
        let (_dir, cache) = cache(100);
        cache.db.set(cache_key("k"), "not-an-entry".into()).unwrap();
        assert!(matches!(
            cache.get("k"),
            Err(PebbleError::CacheCorruption(_))
        ));
    }

    #[test]
    fn statistics_track_each_operation() {
        let (_dir, cache) = cache(100);
        cache.set("a", "1", Duration::ZERO).unwrap();
        cache.set("b", "2", Duration::ZERO).unwrap();
        assert_eq!(cache.get("a").unwrap(), Some("1".into()));
        assert_eq!(cache.get("nope").unwrap(), None);
        cache.delete("b").unwrap();

        let stats = cache.stats();
        assert_eq!(stats.sets, 2);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.deletes, 1);
    }

    #[test]
    fn clear_removes_cache_entries_but_not_ordinary_keys() {
        let (_dir, cache) = cache(100);
        cache.set("a", "1", Duration::ZERO).unwrap();
        cache.set("b", "2", Duration::ZERO).unwrap();
        cache.db.set("regular".into(), "kept".into()).unwrap();
        cache.db.set("cachez:other".into(), "kept".into()).unwrap();

        cache.clear().unwrap();

        assert_eq!(cache.get("a").unwrap(), None);
        assert_eq!(cache.get("b").unwrap(), None);
        assert_eq!(cache.db.get("regular"), Some("kept".into()));
        assert_eq!(cache.db.get("cachez:other"), Some("kept".into()));
        assert_eq!(cache.stats().clears, 1);
    }

    #[test]
    fn cache_entries_live_under_the_expected_prefix() {
        let (_dir, cache) = cache(100);
        cache.set("key", "value", Duration::ZERO).unwrap();
        assert_eq!(cache.db.get("key"), None);
        assert!(cache.db.get("cache:key").is_some());
    }

    #[test]
    fn cache_survives_reopening_the_database() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("cache");

        {
            let cache = PebbleCache::open(&path).unwrap();
            cache.set("persisted", "value", Duration::ZERO).unwrap();
            cache
                .set("temporary", "value", Duration::from_millis(5))
                .unwrap();
        }

        {
            let cache = PebbleCache::open(&path).unwrap();
            assert_eq!(cache.get("persisted").unwrap(), Some("value".into()));
            thread::sleep(Duration::from_millis(20));
            assert_eq!(cache.get("temporary").unwrap(), None);
        }
    }

    #[test]
    fn cache_survives_a_flush_to_sstable() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("cache");
        let cache = PebbleCache::open_with_options(
            &path,
            DbOptions {
                flush_threshold: 2,
                ..DbOptions::default()
            },
        )
        .unwrap();

        cache.set("a", "1", Duration::ZERO).unwrap();
        cache.set("b", "2", Duration::ZERO).unwrap();
        assert_eq!(cache.db.stats().sst_count, 1);
        assert_eq!(cache.get("a").unwrap(), Some("1".into()));

        cache.close().unwrap();

        let reopened = PebbleCache::open(&path).unwrap();
        assert_eq!(reopened.get("a").unwrap(), Some("1".into()));
        assert_eq!(reopened.get("b").unwrap(), Some("2".into()));
    }

    #[test]
    fn concurrent_access_is_safe() {
        let (_dir, cache) = cache(10_000);
        let cache = Arc::new(cache);

        let mut handles = Vec::new();
        for thread_id in 0..4 {
            let cache = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                for i in 0..50 {
                    let key = format!("t{}_{}", thread_id, i);
                    cache.set(&key, "value", Duration::ZERO).unwrap();
                    assert_eq!(cache.get(&key).unwrap(), Some("value".into()));
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        let stats = cache.stats();
        assert_eq!(stats.sets, 200);
        assert_eq!(stats.hits, 200);
    }
}
