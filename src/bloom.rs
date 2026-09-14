use crate::wal::crc32;

pub struct BloomFilter {
    bits: Vec<u64>,
    num_bits: usize,
    num_hashes: usize,
}

impl BloomFilter {
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let num_bits = Self::optimal_bits(expected_items, false_positive_rate);
        let num_hashes = Self::optimal_hashes(num_bits, expected_items);
        let words = num_bits.div_ceil(64);
        Self {
            bits: vec![0u64; words],
            num_bits,
            num_hashes,
        }
    }

    pub fn from_entries<'a, I>(entries: I) -> Self
    where
        I: Iterator<Item = &'a str>,
    {
        let keys: Vec<&str> = entries.collect();
        let n = keys.len().max(1);
        let mut filter = Self::new(n, 0.01);
        for key in keys {
            filter.insert(key);
        }
        filter
    }

    pub fn insert(&mut self, key: &str) {
        let hashes = self.key_hashes(key);
        for i in 0..self.num_hashes {
            let bit =
                (hashes[0].wrapping_add((i as u64).wrapping_mul(hashes[1]))) % self.num_bits as u64;
            let word = bit / 64;
            let offset = bit % 64;
            self.bits[word as usize] |= 1u64 << offset;
        }
    }

    pub fn might_contain(&self, key: &str) -> bool {
        let hashes = self.key_hashes(key);
        for i in 0..self.num_hashes {
            let bit =
                (hashes[0].wrapping_add((i as u64).wrapping_mul(hashes[1]))) % self.num_bits as u64;
            let word = bit / 64;
            let offset = bit % 64;
            if self.bits[word as usize] & (1u64 << offset) == 0 {
                return false;
            }
        }
        true
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.num_bits as u32).to_le_bytes());
        buf.extend_from_slice(&(self.num_hashes as u32).to_le_bytes());
        for word in &self.bits {
            buf.extend_from_slice(&word.to_le_bytes());
        }
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        let num_bits = u32::from_le_bytes(data[0..4].try_into().ok()?) as usize;
        let num_hashes = u32::from_le_bytes(data[4..8].try_into().ok()?) as usize;
        let words = num_bits.div_ceil(64);
        let expected = 8 + words * 8;
        if data.len() < expected {
            return None;
        }
        let mut bits = Vec::with_capacity(words);
        for i in 0..words {
            let offset = 8 + i * 8;
            bits.push(u64::from_le_bytes(
                data[offset..offset + 8].try_into().ok()?,
            ));
        }
        Some(Self {
            bits,
            num_bits,
            num_hashes,
        })
    }

    fn key_hashes(&self, key: &str) -> [u64; 2] {
        let h1 = crc32(key.as_bytes());
        let h2 = crc32(
            key.as_bytes()
                .iter()
                .rev()
                .copied()
                .collect::<Vec<u8>>()
                .as_slice(),
        );
        [h1 as u64, h2 as u64]
    }

    fn optimal_bits(n: usize, fp_rate: f64) -> usize {
        let ln2 = std::f64::consts::LN_2;
        let m = -(n as f64 * fp_rate.ln()) / (ln2 * ln2);
        (m.ceil() as usize).max(64)
    }

    fn optimal_hashes(m: usize, n: usize) -> usize {
        let k = (m as f64 / n as f64) * std::f64::consts::LN_2;
        (k.round() as usize).clamp(1, 30)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_insert_and_check() {
        let mut filter = BloomFilter::new(100, 0.01);
        filter.insert("apple");
        filter.insert("banana");
        assert!(filter.might_contain("apple"));
        assert!(filter.might_contain("banana"));
    }

    #[test]
    fn test_bloom_false_positive_possible() {
        let mut filter = BloomFilter::new(1000, 0.01);
        for i in 0..500 {
            filter.insert(&format!("key_{}", i));
        }
        let mut false_positives = 0;
        for i in 500..1500 {
            if filter.might_contain(&format!("key_{}", i)) {
                false_positives += 1;
            }
        }
        assert!(
            false_positives < 50,
            "too many false positives: {}",
            false_positives
        );
    }

    #[test]
    fn test_bloom_encode_decode() {
        let mut filter = BloomFilter::new(100, 0.01);
        filter.insert("hello");
        filter.insert("world");
        let encoded = filter.encode();
        let decoded = BloomFilter::decode(&encoded).unwrap();
        assert!(decoded.might_contain("hello"));
        assert!(decoded.might_contain("world"));
    }

    #[test]
    fn test_bloom_encode_decode_empty() {
        let filter = BloomFilter::new(100, 0.01);
        let encoded = filter.encode();
        let decoded = BloomFilter::decode(&encoded).unwrap();
        assert!(!decoded.might_contain("anything"));
    }

    #[test]
    fn test_bloom_from_entries() {
        let keys = vec!["x", "y", "z"];
        let filter = BloomFilter::from_entries(keys.iter().copied());
        assert!(filter.might_contain("x"));
        assert!(filter.might_contain("y"));
        assert!(filter.might_contain("z"));
        assert!(!filter.might_contain("w"));
    }
}
