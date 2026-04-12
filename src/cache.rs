//! File-based cache with TTL support

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Cache entry wrapper with timestamp
#[derive(Debug, Serialize, Deserialize)]
struct CacheEntry<T> {
    timestamp: u64,
    data: T,
}

/// File-based cache with time-to-live support
pub struct Cache {
    cache_dir: PathBuf,
    ttl: Duration,
}

impl Cache {
    /// Create a new cache with the specified directory and TTL in hours
    pub fn new(cache_dir: PathBuf, ttl_hours: u64) -> Self {
        Self {
            cache_dir,
            ttl: Duration::from_secs(ttl_hours * 3600),
        }
    }

    /// Create a cache in the default location (~/.cache/ncbi_poketext/)
    pub fn default_location(ttl_hours: u64) -> io::Result<Self> {
        let cache_dir = dirs::cache_dir()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, "Could not find cache directory")
            })?
            .join("ncbi_poketext");

        fs::create_dir_all(&cache_dir)?;

        Ok(Self::new(cache_dir, ttl_hours))
    }

    /// Get a cached value by key, returning None if not found or expired
    pub fn get<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        let path = self.key_to_path(key);

        let content = fs::read(&path).ok()?;
        let entry: CacheEntry<T> = serde_json::from_slice(&content).ok()?;

        // Check TTL
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();

        if now - entry.timestamp > self.ttl.as_secs() {
            // Entry expired, remove it
            let _ = fs::remove_file(&path);
            return None;
        }

        Some(entry.data)
    }

    /// Store a value in the cache
    pub fn set<T: Serialize>(&self, key: &str, value: &T) -> io::Result<()> {
        // Ensure cache directory exists
        fs::create_dir_all(&self.cache_dir)?;

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_secs();

        let entry = CacheEntry {
            timestamp,
            data: value,
        };

        let content = serde_json::to_vec(&entry)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let path = self.key_to_path(key);
        fs::write(&path, content)
    }

    /// Remove a cached entry
    pub fn invalidate(&self, key: &str) -> io::Result<()> {
        let path = self.key_to_path(key);
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }

    /// Convert a cache key to a file path
    fn key_to_path(&self, key: &str) -> PathBuf {
        // Sanitize key for use as filename
        let safe_key: String = key
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        self.cache_dir.join(format!("{}.json", safe_key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_cache_roundtrip() {
        let temp_dir = env::temp_dir().join("ncbi_poketext_test_cache");
        let cache = Cache::new(temp_dir.clone(), 1);

        cache.set("test_key", &"test_value".to_string()).unwrap();
        let result: Option<String> = cache.get("test_key");
        assert_eq!(result, Some("test_value".to_string()));

        // Cleanup
        let _ = fs::remove_dir_all(temp_dir);
    }
}
