use std::time::Duration;

use pebbledb::cache::PebbleCache;

const DAY: Duration = Duration::from_secs(24 * 60 * 60);

fn main() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cache");

    println!("=== PebbleCache demo ===");
    println!("database directory: {}\n", path.display());

    let cache = PebbleCache::open(&path).unwrap();

    println!("First request");
    let answer = request(&cache, "what-is-stow", DAY);
    println!("  answer: {}\n", answer);

    println!("Second request");
    let answer = request(&cache, "what-is-stow", DAY);
    println!("  answer: {}\n", answer);

    println!("Statistics after two requests");
    print_stats(&cache);

    println!("\nExpiration with a short TTL");
    cache
        .set("session:42", "token", Duration::from_millis(200))
        .unwrap();
    println!("  SET session:42 (ttl 200ms)");
    println!("  GET session:42 -> {:?}", cache.get("session:42").unwrap());
    std::thread::sleep(Duration::from_millis(400));
    println!("  ... 400ms later ...");
    println!("  GET session:42 -> {:?}", cache.get("session:42").unwrap());
    println!(
        "  EXISTS session:42 -> {}",
        cache.exists("session:42").unwrap()
    );

    println!("\nPersistence across a restart");
    cache.set("config:theme", "dark", Duration::ZERO).unwrap();
    println!("  SET config:theme (no ttl)");
    cache.close().unwrap();
    println!("  database closed");

    let cache = PebbleCache::open(&path).unwrap();
    println!("  database reopened");
    println!(
        "  GET config:theme -> {:?}",
        cache.get("config:theme").unwrap()
    );
    println!("  counters live in memory, so the new instance starts from zero:");
    print_stats(&cache);

    println!("\nInvalidation");
    println!(
        "  DELETE config:theme -> {}",
        cache.delete("config:theme").unwrap()
    );
    println!("  CLEAR cache");
    cache.clear().unwrap();
    println!(
        "  GET what-is-stow -> {:?}",
        cache.get("what-is-stow").unwrap()
    );

    println!("\nStatistics of the reopened cache");
    print_stats(&cache);
}

fn request(cache: &PebbleCache, key: &str, ttl: Duration) -> String {
    match cache.get(key).unwrap() {
        Some(value) => {
            println!("  GET {} -> HIT", key);
            value
        }
        None => {
            println!("  GET {} -> MISS", key);
            let value = expensive_lookup(key);
            cache.set(key, &value, ttl).unwrap();
            println!("  SET {} (ttl {:?})", key, ttl);
            value
        }
    }
}

fn expensive_lookup(key: &str) -> String {
    println!("  [slow source] fetching {} ...", key);
    std::thread::sleep(Duration::from_millis(50));
    format!("{} -> a small educational Rust key-value database", key)
}

fn print_stats(cache: &PebbleCache) {
    let stats = cache.stats();
    println!("  hits:     {}", stats.hits);
    println!("  misses:   {}", stats.misses);
    println!("  sets:     {}", stats.sets);
    println!("  deletes:  {}", stats.deletes);
    println!("  expired:  {}", stats.expired);
    println!("  clears:   {}", stats.clears);
}
