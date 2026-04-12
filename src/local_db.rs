//! Local SQLite database for caching species data, images, and taxonomy names
//!
//! Provides fast offline access to previously viewed species and enables
//! offline search through the GBIF backbone taxonomy.

use crate::api::wikipedia::{WikiArticle, WikiLifeHistoryFallback};
use crate::curated_animals::CURATED_ANIMAL_SPECIES;
use crate::species::UnifiedSpecies;
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const SCHEMA_VERSION: i32 = 5;
const DEFAULT_CACHE_TTL_SECS: i64 = 60 * 60 * 24 * 30; // 30 days
const MAP_CACHE_VERSION: u32 = 2;

fn curated_species_sql_filter() -> String {
    let names = CURATED_ANIMAL_SPECIES
        .iter()
        .map(|name| format!("'{}'", name.to_ascii_lowercase().replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "lower(json_extract(data_json, '$.scientific_name')) IN ({})",
        names
    )
}

pub struct LocalDatabase {
    conn: Connection,
    cache_ttl_secs: i64,
}

#[derive(Debug, Clone)]
pub struct CachedSpecies {
    pub species: UnifiedSpecies,
    pub species_image: Option<Vec<u8>>,
    pub map_image: Option<Vec<u8>>,
    pub cached_at: i64,
}

#[derive(Debug, Clone, Default)]
pub struct CachedMedia {
    pub species_image: Option<Vec<u8>>,
    pub map_image: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct TaxonName {
    pub gbif_key: u64,
    pub scientific_name: String,
    pub canonical_name: Option<String>,
    pub rank: String,
    pub kingdom: Option<String>,
    pub phylum: Option<String>,
    pub class: Option<String>,
    pub order: Option<String>,
    pub family: Option<String>,
    pub genus: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct DatabaseStats {
    pub species_count: u64,
    pub rich_species_count: u64,
    pub taxon_names_count: u64,
    pub images_count: u64,
    pub total_size_bytes: u64,
}

impl LocalDatabase {
    pub fn open() -> rusqlite::Result<Self> {
        let db_path = Self::db_path();

        // Ensure parent directory exists
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }

        let conn = Connection::open(&db_path)?;
        Self::configure_connection(&conn, true)?;
        let mut db = Self {
            conn,
            cache_ttl_secs: DEFAULT_CACHE_TTL_SECS,
        };

        db.init_schema()?;
        Ok(db)
    }

    #[cfg(test)]
    pub fn open_in_memory() -> rusqlite::Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::configure_connection(&conn, false)?;
        let mut db = Self {
            conn,
            cache_ttl_secs: DEFAULT_CACHE_TTL_SECS,
        };
        db.init_schema()?;
        Ok(db)
    }

    fn db_path() -> PathBuf {
        dirs::data_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("ncbi_poketext")
            .join("species_cache.db")
    }

    fn configure_connection(conn: &Connection, enable_wal: bool) -> rusqlite::Result<()> {
        conn.busy_timeout(Duration::from_secs(2))?;

        if enable_wal {
            conn.execute_batch(
                "PRAGMA journal_mode = WAL;
                 PRAGMA synchronous = NORMAL;
                 PRAGMA temp_store = MEMORY;
                 PRAGMA cache_size = -8192;",
            )?;
        } else {
            conn.execute_batch(
                "PRAGMA synchronous = NORMAL;
                 PRAGMA temp_store = MEMORY;
                 PRAGMA cache_size = -4096;",
            )?;
        }

        Ok(())
    }

    fn init_schema(&mut self) -> rusqlite::Result<()> {
        // Check schema version
        let version: i32 = self
            .conn
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap_or(0);

        if version < SCHEMA_VERSION {
            self.create_tables()?;
            self.conn
                .execute(&format!("PRAGMA user_version = {}", SCHEMA_VERSION), [])?;
        }

        self.cleanup_legacy_map_cache()?;

        Ok(())
    }

    fn create_tables(&self) -> rusqlite::Result<()> {
        // Species data cache
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS species (
                scientific_name TEXT PRIMARY KEY,
                data_json TEXT NOT NULL,
                cached_at INTEGER NOT NULL,
                last_accessed INTEGER NOT NULL
            )",
            [],
        )?;

        // Image cache
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS images (
                url TEXT PRIMARY KEY,
                image_data BLOB NOT NULL,
                content_type TEXT,
                cached_at INTEGER NOT NULL
            )",
            [],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS wiki_articles (
                title TEXT PRIMARY KEY,
                extract TEXT NOT NULL,
                wikitext TEXT NOT NULL,
                cached_at INTEGER NOT NULL
            )",
            [],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS wiki_life_history (
                title TEXT PRIMARY KEY,
                data_json TEXT NOT NULL,
                cached_at INTEGER NOT NULL
            )",
            [],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS rich_species (
                scientific_name TEXT PRIMARY KEY,
                data_json TEXT NOT NULL,
                enriched_at INTEGER NOT NULL
            )",
            [],
        )?;

        // GBIF backbone taxonomy names for offline search
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS taxon_names (
                gbif_key INTEGER PRIMARY KEY,
                scientific_name TEXT NOT NULL,
                canonical_name TEXT,
                rank TEXT NOT NULL,
                kingdom TEXT,
                phylum TEXT,
                class TEXT,
                order_name TEXT,
                family TEXT,
                genus TEXT
            )",
            [],
        )?;

        // Create indexes for fast searching
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_taxon_scientific ON taxon_names(scientific_name COLLATE NOCASE)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_taxon_canonical ON taxon_names(canonical_name COLLATE NOCASE)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_taxon_rank ON taxon_names(rank)",
            [],
        )?;

        // Indexes for fast sibling lookups at each taxonomic level
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_taxon_genus ON taxon_names(genus, rank)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_taxon_family ON taxon_names(family, rank)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_taxon_order ON taxon_names(order_name, rank)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_taxon_class ON taxon_names(class, rank)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_taxon_phylum ON taxon_names(phylum, rank)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_taxon_kingdom ON taxon_names(kingdom, rank)",
            [],
        )?;

        // NCBI taxonomy with common names (for instant species info)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS ncbi_taxonomy (
                tax_id INTEGER PRIMARY KEY,
                scientific_name TEXT NOT NULL,
                common_name TEXT,
                rank TEXT
            )",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ncbi_scientific ON ncbi_taxonomy(scientific_name COLLATE NOCASE)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ncbi_common ON ncbi_taxonomy(common_name COLLATE NOCASE)",
            [],
        )?;

        // User metrics and stats
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS user_stats (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
            [],
        )?;

        // View history
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS view_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scientific_name TEXT NOT NULL,
                viewed_at INTEGER NOT NULL
            )",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_history_time ON view_history(viewed_at DESC)",
            [],
        )?;

        // Favorites
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS favorites (
                scientific_name TEXT PRIMARY KEY,
                added_at INTEGER NOT NULL,
                notes TEXT
            )",
            [],
        )?;

        Ok(())
    }

    fn current_timestamp() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    // ==================== Species Cache ====================

    /// Get cached species data if not stale
    pub fn get_species(&self, scientific_name: &str) -> rusqlite::Result<Option<CachedSpecies>> {
        let now = Self::current_timestamp();
        let cutoff = now - self.cache_ttl_secs;

        let result: Option<(String, i64)> = self
            .conn
            .query_row(
                "SELECT data_json, cached_at FROM species
             WHERE scientific_name = ?1 AND cached_at > ?2",
                params![scientific_name.to_lowercase(), cutoff],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        if let Some((json, cached_at)) = result {
            // Update last accessed
            self.conn.execute(
                "UPDATE species SET last_accessed = ?1 WHERE scientific_name = ?2",
                params![now, scientific_name.to_lowercase()],
            )?;

            // Parse JSON
            if let Ok(species) = serde_json::from_str::<UnifiedSpecies>(&json) {
                // Get cached images
                let species_image = self.get_species_image(&species)?;
                let map_image = self.get_map_image(&species)?;

                return Ok(Some(CachedSpecies {
                    species,
                    species_image,
                    map_image,
                    cached_at,
                }));
            }
        }

        Ok(None)
    }

    /// Cache species data
    pub fn cache_species(&self, species: &UnifiedSpecies) -> rusqlite::Result<()> {
        let now = Self::current_timestamp();
        let json = serde_json::to_string(species).unwrap_or_default();

        self.conn.execute(
            "INSERT OR REPLACE INTO species (scientific_name, data_json, cached_at, last_accessed)
             VALUES (?1, ?2, ?3, ?3)",
            params![species.scientific_name.to_lowercase(), json, now],
        )?;

        // Record in view history
        self.conn.execute(
            "INSERT INTO view_history (scientific_name, viewed_at) VALUES (?1, ?2)",
            params![species.scientific_name, now],
        )?;

        Ok(())
    }

    pub fn get_rich_species(
        &self,
        scientific_name: &str,
    ) -> rusqlite::Result<Option<UnifiedSpecies>> {
        let data_json: Option<String> = self
            .conn
            .query_row(
                "SELECT data_json FROM rich_species WHERE scientific_name = ?1",
                params![scientific_name.to_lowercase()],
                |row| row.get(0),
            )
            .optional()?;

        Ok(data_json.and_then(|json| serde_json::from_str(&json).ok()))
    }

    pub fn cache_rich_species(&self, species: &UnifiedSpecies) -> rusqlite::Result<()> {
        let now = Self::current_timestamp();
        let json = serde_json::to_string(species).unwrap_or_default();

        self.conn.execute(
            "INSERT OR REPLACE INTO rich_species (scientific_name, data_json, enriched_at)
             VALUES (?1, ?2, ?3)",
            params![species.scientific_name.to_lowercase(), json, now],
        )?;

        Ok(())
    }

    /// Get cached species image
    fn get_species_image(&self, species: &UnifiedSpecies) -> rusqlite::Result<Option<Vec<u8>>> {
        if let Some(url) = species.preferred_image_url() {
            self.get_image(url)
        } else {
            Ok(None)
        }
    }

    /// Get cached map image
    fn get_map_image(&self, species: &UnifiedSpecies) -> rusqlite::Result<Option<Vec<u8>>> {
        if let Some(gbif_key) = species.ids.gbif_key {
            self.get_map_image_by_key(gbif_key)
        } else {
            Ok(None)
        }
    }

    pub fn get_cached_media(
        &self,
        species_image_url: Option<&str>,
        gbif_key: Option<u64>,
    ) -> rusqlite::Result<CachedMedia> {
        let species_image = match species_image_url {
            Some(url) => self.get_image(url)?,
            None => None,
        };
        let map_image = match gbif_key {
            Some(key) => self.get_map_image_by_key(key)?,
            None => None,
        };

        Ok(CachedMedia {
            species_image,
            map_image,
        })
    }

    fn get_map_image_by_key(&self, gbif_key: u64) -> rusqlite::Result<Option<Vec<u8>>> {
        let url = Self::map_cache_key(gbif_key);
        if let Some(image) = self.get_image(&url)? {
            return Ok(Some(image));
        }

        // Drop old transparent overlays from pre-composite cache versions.
        self.delete_image(&Self::legacy_map_cache_key(gbif_key))?;
        Ok(None)
    }

    // ==================== Image Cache ====================

    /// Get cached image by URL
    pub fn get_image(&self, url: &str) -> rusqlite::Result<Option<Vec<u8>>> {
        self.conn
            .query_row(
                "SELECT image_data FROM images WHERE url = ?1",
                params![url],
                |row| row.get(0),
            )
            .optional()
    }

    /// Cache image data
    pub fn cache_image(
        &self,
        url: &str,
        data: &[u8],
        content_type: Option<&str>,
    ) -> rusqlite::Result<()> {
        let now = Self::current_timestamp();
        self.conn.execute(
            "INSERT OR REPLACE INTO images (url, image_data, content_type, cached_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![url, data, content_type, now],
        )?;
        Ok(())
    }

    pub fn delete_image(&self, url: &str) -> rusqlite::Result<()> {
        self.conn
            .execute("DELETE FROM images WHERE url = ?1", params![url])?;
        Ok(())
    }

    /// Cache species image
    pub fn cache_species_image(
        &self,
        species: &UnifiedSpecies,
        data: &[u8],
    ) -> rusqlite::Result<()> {
        if let Some(url) = species.preferred_image_url() {
            self.cache_image(url, data, Some("image/jpeg"))?;
        }
        Ok(())
    }

    /// Cache map image
    pub fn cache_map_image(&self, gbif_key: u64, data: &[u8]) -> rusqlite::Result<()> {
        let url = Self::map_cache_key(gbif_key);
        self.cache_image(&url, data, Some("image/png"))
    }

    pub fn invalidate_map_image(&self, gbif_key: u64) -> rusqlite::Result<()> {
        self.delete_image(&Self::map_cache_key(gbif_key))?;
        self.delete_image(&Self::legacy_map_cache_key(gbif_key))?;
        Ok(())
    }

    pub fn get_wiki_article(&self, title: &str) -> rusqlite::Result<Option<WikiArticle>> {
        let cutoff = Self::current_timestamp() - self.cache_ttl_secs;
        self.conn
            .query_row(
                "SELECT extract, wikitext FROM wiki_articles
                 WHERE title = ?1 AND cached_at > ?2",
                params![title.to_lowercase(), cutoff],
                |row| {
                    Ok(WikiArticle {
                        extract: row.get(0)?,
                        wikitext: row.get(1)?,
                    })
                },
            )
            .optional()
    }

    pub fn cache_wiki_article(&self, title: &str, article: &WikiArticle) -> rusqlite::Result<()> {
        let now = Self::current_timestamp();
        self.conn.execute(
            "INSERT OR REPLACE INTO wiki_articles (title, extract, wikitext, cached_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                title.to_lowercase(),
                &article.extract,
                &article.wikitext,
                now
            ],
        )?;
        Ok(())
    }

    pub fn get_wiki_life_history(
        &self,
        title: &str,
    ) -> rusqlite::Result<Option<WikiLifeHistoryFallback>> {
        let cutoff = Self::current_timestamp() - self.cache_ttl_secs;
        let data_json: Option<String> = self
            .conn
            .query_row(
                "SELECT data_json FROM wiki_life_history
                 WHERE title = ?1 AND cached_at > ?2",
                params![title.to_lowercase(), cutoff],
                |row| row.get(0),
            )
            .optional()?;

        Ok(data_json.and_then(|json| serde_json::from_str(&json).ok()))
    }

    pub fn cache_wiki_life_history(
        &self,
        title: &str,
        fallback: &WikiLifeHistoryFallback,
    ) -> rusqlite::Result<()> {
        let now = Self::current_timestamp();
        let data_json = serde_json::to_string(fallback).unwrap_or_default();
        self.conn.execute(
            "INSERT OR REPLACE INTO wiki_life_history (title, data_json, cached_at)
             VALUES (?1, ?2, ?3)",
            params![title.to_lowercase(), data_json, now],
        )?;
        Ok(())
    }

    fn map_cache_key(gbif_key: u64) -> String {
        format!("gbif_map_v{}_{}", MAP_CACHE_VERSION, gbif_key)
    }

    fn legacy_map_cache_key(gbif_key: u64) -> String {
        format!("gbif_map_{}", gbif_key)
    }

    fn cleanup_legacy_map_cache(&self) -> rusqlite::Result<()> {
        self.conn.execute(
            "DELETE FROM images
             WHERE url GLOB 'gbif_map_[0-9]*'
               AND url NOT GLOB 'gbif_map_v*'",
            [],
        )?;
        Ok(())
    }

    // ==================== Taxonomy Names (Offline Search) ====================

    /// Search curated species and taxonomy names for offline autocomplete.
    pub fn search_taxon_names(&self, query: &str, limit: u32) -> rusqlite::Result<Vec<TaxonName>> {
        let normalized = query.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return Ok(Vec::new());
        }

        let sql = format!(
            "SELECT data_json FROM rich_species WHERE {}",
            curated_species_sql_filter()
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;

        let mut matches: HashMap<(String, String), (u8, usize, TaxonName)> = HashMap::new();

        let push_match = |matches: &mut HashMap<(String, String), (u8, usize, TaxonName)>,
                          name: String,
                          rank: &str,
                          priority: u8,
                          taxonomy: &crate::species::Taxonomy,
                          scientific_name: &str| {
            let key = (name.to_ascii_lowercase(), rank.to_ascii_uppercase());
            let candidate = TaxonName {
                gbif_key: 0,
                scientific_name: name.clone(),
                canonical_name: Some(name.clone()),
                rank: rank.to_ascii_uppercase(),
                kingdom: taxonomy.kingdom.clone(),
                phylum: taxonomy.phylum.clone(),
                class: taxonomy.class.clone(),
                order: taxonomy.order.clone(),
                family: taxonomy.family.clone(),
                genus: taxonomy.genus.clone(),
            };
            let length = scientific_name.len().min(name.len());
            match matches.get(&key) {
                Some((existing_priority, existing_length, _))
                    if (*existing_priority, *existing_length) <= (priority, length) => {}
                _ => {
                    matches.insert(key, (priority, length, candidate));
                }
            }
        };

        for row in rows {
            let json = row?;
            let species: UnifiedSpecies = match serde_json::from_str(&json) {
                Ok(species) => species,
                Err(_) => continue,
            };

            let scientific = species.scientific_name.clone();
            let scientific_lower = scientific.to_ascii_lowercase();
            let common_match = species
                .common_names
                .iter()
                .any(|name| name.to_ascii_lowercase().contains(&normalized));
            let scientific_match = scientific_lower.contains(&normalized);
            if scientific_match || common_match {
                let priority = if scientific_lower.starts_with(&normalized) {
                    0
                } else if species
                    .common_names
                    .iter()
                    .any(|name| name.to_ascii_lowercase().starts_with(&normalized))
                {
                    1
                } else {
                    2
                };
                push_match(
                    &mut matches,
                    scientific.clone(),
                    "SPECIES",
                    priority,
                    &species.taxonomy,
                    &scientific,
                );
            }

            for (rank, value) in [
                ("KINGDOM", species.taxonomy.kingdom.as_deref()),
                ("PHYLUM", species.taxonomy.phylum.as_deref()),
                ("CLASS", species.taxonomy.class.as_deref()),
                ("ORDER", species.taxonomy.order.as_deref()),
                ("FAMILY", species.taxonomy.family.as_deref()),
                ("GENUS", species.taxonomy.genus.as_deref()),
            ] {
                let Some(value) = value else {
                    continue;
                };
                let lower = value.to_ascii_lowercase();
                if lower.contains(&normalized) {
                    let priority = if lower.starts_with(&normalized) { 0 } else { 2 };
                    push_match(
                        &mut matches,
                        value.to_string(),
                        rank,
                        priority,
                        &species.taxonomy,
                        &scientific,
                    );
                }
            }
        }

        let mut results = matches.into_values().collect::<Vec<_>>();
        results.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then(a.1.cmp(&b.1))
                .then_with(|| a.2.scientific_name.cmp(&b.2.scientific_name))
                .then_with(|| a.2.rank.cmp(&b.2.rank))
        });
        results.truncate(limit as usize);
        Ok(results.into_iter().map(|(_, _, taxon)| taxon).collect())
    }

    /// Get count of taxon names in database
    pub fn taxon_names_count(&self) -> rusqlite::Result<u64> {
        self.conn
            .query_row("SELECT COUNT(*) FROM taxon_names", [], |row| row.get(0))
    }

    pub fn species_rank_count(&self) -> rusqlite::Result<u64> {
        self.conn.query_row(
            "SELECT COUNT(*) FROM taxon_names WHERE rank = 'SPECIES'",
            [],
            |row| row.get(0),
        )
    }

    /// Insert taxon names in batch (for importing GBIF backbone)
    pub fn insert_taxon_names_batch(&self, names: &[TaxonName]) -> rusqlite::Result<()> {
        let tx = self.conn.unchecked_transaction()?;

        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO taxon_names
                 (gbif_key, scientific_name, canonical_name, rank, kingdom, phylum, class, order_name, family, genus)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)"
            )?;

            for name in names {
                stmt.execute(params![
                    name.gbif_key,
                    name.scientific_name,
                    name.canonical_name,
                    name.rank,
                    name.kingdom,
                    name.phylum,
                    name.class,
                    name.order,
                    name.family,
                    name.genus,
                ])?;
            }
        }

        tx.commit()
    }

    /// Check if GBIF backbone is imported
    pub fn has_backbone(&self) -> bool {
        self.taxon_names_count().unwrap_or(0) > 100_000
    }

    /// Check if NCBI taxonomy is imported
    pub fn has_ncbi_taxonomy(&self) -> bool {
        self.ncbi_taxonomy_count().unwrap_or(0) > 100_000
    }

    /// Get count of NCBI taxonomy entries
    pub fn ncbi_taxonomy_count(&self) -> rusqlite::Result<u64> {
        self.conn
            .query_row("SELECT COUNT(*) FROM ncbi_taxonomy", [], |row| row.get(0))
    }

    /// Insert NCBI taxonomy entries in batch
    pub fn insert_ncbi_taxonomy_batch(
        &self,
        entries: &[crate::bulk_import::NcbiTaxonEntry],
    ) -> rusqlite::Result<()> {
        let tx = self.conn.unchecked_transaction()?;

        {
            let mut stmt = tx.prepare(
                "INSERT OR REPLACE INTO ncbi_taxonomy (tax_id, scientific_name, common_name, rank)
                 VALUES (?1, ?2, ?3, ?4)",
            )?;

            for entry in entries {
                stmt.execute(params![
                    entry.tax_id,
                    entry.scientific_name,
                    entry.common_name,
                    entry.rank,
                ])?;
            }
        }

        tx.commit()
    }

    /// Get exact taxon match by scientific name
    pub fn get_taxon_by_name(&self, name: &str) -> rusqlite::Result<Option<TaxonName>> {
        self.conn
            .query_row(
                "SELECT gbif_key, scientific_name, canonical_name, rank,
                    kingdom, phylum, class, order_name, family, genus
             FROM taxon_names
             WHERE scientific_name = ?1 COLLATE NOCASE
                OR canonical_name = ?1 COLLATE NOCASE
             LIMIT 1",
                params![name],
                |row| {
                    Ok(TaxonName {
                        gbif_key: row.get(0)?,
                        scientific_name: row.get(1)?,
                        canonical_name: row.get(2)?,
                        rank: row.get(3)?,
                        kingdom: row.get(4)?,
                        phylum: row.get(5)?,
                        class: row.get(6)?,
                        order: row.get(7)?,
                        family: row.get(8)?,
                        genus: row.get(9)?,
                    })
                },
            )
            .optional()
    }

    pub fn get_species_batch_after(
        &self,
        after_gbif_key: u64,
        limit: u32,
    ) -> rusqlite::Result<Vec<TaxonName>> {
        let mut stmt = self.conn.prepare(
            "SELECT gbif_key, scientific_name, canonical_name, rank,
                    kingdom, phylum, class, order_name, family, genus
             FROM taxon_names
             WHERE rank = 'SPECIES'
               AND gbif_key > ?1
               AND (EXISTS (SELECT 1 FROM rich_species rs WHERE rs.scientific_name = IFNULL(taxon_names.canonical_name, taxon_names.scientific_name) COLLATE NOCASE)
                    OR EXISTS (SELECT 1 FROM species sp WHERE sp.scientific_name = IFNULL(taxon_names.canonical_name, taxon_names.scientific_name) COLLATE NOCASE))
             ORDER BY gbif_key
             LIMIT ?2",
        )?;

        let rows = stmt.query_map(params![after_gbif_key, limit], |row| {
            Ok(TaxonName {
                gbif_key: row.get(0)?,
                scientific_name: row.get(1)?,
                canonical_name: row.get(2)?,
                rank: row.get(3)?,
                kingdom: row.get(4)?,
                phylum: row.get(5)?,
                class: row.get(6)?,
                order: row.get(7)?,
                family: row.get(8)?,
                genus: row.get(9)?,
            })
        })?;

        rows.collect()
    }

    pub fn get_cached_kingdoms(&self) -> rusqlite::Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            &format!(
                "SELECT DISTINCT json_extract(data_json, '$.taxonomy.kingdom')\n                 FROM rich_species\n                 WHERE json_extract(data_json, '$.taxonomy.kingdom') IS NOT NULL\n                   AND {}\n                 ORDER BY 1 COLLATE NOCASE",
                curated_species_sql_filter()
            ),
        )?;

        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect()
    }

    pub fn get_cached_parent_taxon(
        &self,
        child_rank: &str,
        child_value: &str,
    ) -> rusqlite::Result<Option<(String, String)>> {
        let parent_rank = match child_rank.to_ascii_uppercase().as_str() {
            "GENUS" => "FAMILY",
            "FAMILY" => "ORDER",
            "ORDER" => "CLASS",
            "CLASS" => "PHYLUM",
            "PHYLUM" => "KINGDOM",
            _ => return Ok(None),
        };

        let child_path = match child_rank.to_ascii_uppercase().as_str() {
            "KINGDOM" => "$.taxonomy.kingdom",
            "PHYLUM" => "$.taxonomy.phylum",
            "CLASS" => "$.taxonomy.class",
            "ORDER" => "$.taxonomy.order",
            "FAMILY" => "$.taxonomy.family",
            "GENUS" => "$.taxonomy.genus",
            _ => return Ok(None),
        };
        let parent_path = match parent_rank {
            "KINGDOM" => "$.taxonomy.kingdom",
            "PHYLUM" => "$.taxonomy.phylum",
            "CLASS" => "$.taxonomy.class",
            "ORDER" => "$.taxonomy.order",
            "FAMILY" => "$.taxonomy.family",
            _ => return Ok(None),
        };

        let sql = format!(
            "SELECT DISTINCT json_extract(data_json, '{}')
             FROM rich_species
             WHERE json_extract(data_json, '{}') = ?1
               AND json_extract(data_json, '{}') IS NOT NULL
               AND {}
             LIMIT 1",
            parent_path,
            child_path,
            parent_path,
            curated_species_sql_filter()
        );

        self.conn
            .query_row(&sql, params![child_value], |row| row.get::<_, String>(0))
            .optional()
            .map(|value| value.map(|name| (name, parent_rank.to_string())))
    }

    /// Get siblings at a specific taxonomic rank (taxa sharing the same parent)
    /// Returns cached taxa of the specified child_rank that share the same parent_value
    pub fn get_siblings(
        &self,
        parent_rank: &str,
        parent_value: &str,
        child_rank: &str,
        limit: u32,
    ) -> rusqlite::Result<Vec<TaxonName>> {
        let parent_path = match parent_rank.to_ascii_uppercase().as_str() {
            "KINGDOM" => "$.taxonomy.kingdom",
            "PHYLUM" => "$.taxonomy.phylum",
            "CLASS" => "$.taxonomy.class",
            "ORDER" => "$.taxonomy.order",
            "FAMILY" => "$.taxonomy.family",
            "GENUS" => "$.taxonomy.genus",
            _ => return Ok(Vec::new()),
        };

        let target_rank = if child_rank.is_empty() {
            match parent_rank.to_ascii_uppercase().as_str() {
                "KINGDOM" => "PHYLUM",
                "PHYLUM" => "CLASS",
                "CLASS" => "ORDER",
                "ORDER" => "FAMILY",
                "FAMILY" => "GENUS",
                "GENUS" => "SPECIES",
                _ => return Ok(Vec::new()),
            }
        } else {
            child_rank
        };

        let child_path = match target_rank.to_ascii_uppercase().as_str() {
            "KINGDOM" => "$.taxonomy.kingdom",
            "PHYLUM" => "$.taxonomy.phylum",
            "CLASS" => "$.taxonomy.class",
            "ORDER" => "$.taxonomy.order",
            "FAMILY" => "$.taxonomy.family",
            "GENUS" => "$.taxonomy.genus",
            "SPECIES" => "$.scientific_name",
            _ => return Ok(Vec::new()),
        };

        let sql = format!(
            "SELECT DISTINCT json_extract(data_json, '{child_path}')
             FROM rich_species
             WHERE json_extract(data_json, '{parent_path}') = ?1
               AND json_extract(data_json, '{child_path}') IS NOT NULL
               AND {}
             ORDER BY 1 COLLATE NOCASE
             LIMIT ?2",
            curated_species_sql_filter()
        );

        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params![parent_value, limit], |row| {
            let name: String = row.get(0)?;
            Ok(TaxonName {
                gbif_key: 0,
                scientific_name: name.clone(),
                canonical_name: Some(name),
                rank: target_rank.to_ascii_uppercase(),
                kingdom: None,
                phylum: None,
                class: None,
                order: None,
                family: None,
                genus: None,
            })
        })?;

        rows.collect()
    }

    /// Get all species in a genus (fast sibling lookup for species)
    pub fn get_species_in_genus(
        &self,
        genus: &str,
        limit: u32,
    ) -> rusqlite::Result<Vec<TaxonName>> {
        self.get_siblings("GENUS", genus, "SPECIES", limit)
    }

    /// Get all genera in a family
    pub fn get_genera_in_family(
        &self,
        family: &str,
        limit: u32,
    ) -> rusqlite::Result<Vec<TaxonName>> {
        self.get_siblings("FAMILY", family, "GENUS", limit)
    }

    // ==================== User Stats & History ====================

    /// Get database statistics
    pub fn get_stats(&self) -> rusqlite::Result<DatabaseStats> {
        let species_count: u64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM species", [], |row| row.get(0))?;

        let rich_species_count: u64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM rich_species", [], |row| row.get(0))?;

        let taxon_names_count: u64 =
            self.conn
                .query_row("SELECT COUNT(*) FROM taxon_names", [], |row| row.get(0))?;

        let images_count: u64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM images", [], |row| row.get(0))?;

        // Get database file size
        let db_path = Self::db_path();
        let total_size_bytes = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);

        Ok(DatabaseStats {
            species_count,
            rich_species_count,
            taxon_names_count,
            images_count,
            total_size_bytes,
        })
    }

    pub fn get_user_stat(&self, key: &str) -> rusqlite::Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT value FROM user_stats WHERE key = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()
    }

    pub fn set_user_stat(&self, key: &str, value: &str) -> rusqlite::Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO user_stats (key, value) VALUES (?1, ?2)",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn delete_user_stat(&self, key: &str) -> rusqlite::Result<()> {
        self.conn
            .execute("DELETE FROM user_stats WHERE key = ?1", params![key])?;
        Ok(())
    }

    /// Get recent view history
    pub fn get_recent_history(&self, limit: u32) -> rusqlite::Result<Vec<(String, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT scientific_name, MAX(viewed_at) as last_viewed
             FROM view_history
             GROUP BY scientific_name
             ORDER BY last_viewed DESC
             LIMIT ?1",
        )?;

        let rows = stmt.query_map(params![limit], |row| Ok((row.get(0)?, row.get(1)?)))?;

        rows.collect()
    }

    /// Add to favorites
    pub fn add_favorite(&self, scientific_name: &str, notes: Option<&str>) -> rusqlite::Result<()> {
        let now = Self::current_timestamp();
        self.conn.execute(
            "INSERT OR REPLACE INTO favorites (scientific_name, added_at, notes)
             VALUES (?1, ?2, ?3)",
            params![scientific_name, now, notes],
        )?;
        Ok(())
    }

    /// Remove from favorites
    pub fn remove_favorite(&self, scientific_name: &str) -> rusqlite::Result<()> {
        self.conn.execute(
            "DELETE FROM favorites WHERE scientific_name = ?1",
            params![scientific_name],
        )?;
        Ok(())
    }

    /// Check if species is favorited
    pub fn is_favorite(&self, scientific_name: &str) -> rusqlite::Result<bool> {
        let count: u64 = self.conn.query_row(
            "SELECT COUNT(*) FROM favorites WHERE scientific_name = ?1",
            params![scientific_name],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Get all favorites
    pub fn get_favorites(&self) -> rusqlite::Result<Vec<(String, i64, Option<String>)>> {
        let mut stmt = self.conn.prepare(
            "SELECT scientific_name, added_at, notes FROM favorites ORDER BY added_at DESC",
        )?;

        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;

        rows.collect()
    }

    // ==================== Maintenance ====================

    /// Force refresh a specific species (delete from cache)
    pub fn invalidate_species(&self, scientific_name: &str) -> rusqlite::Result<()> {
        self.conn.execute(
            "DELETE FROM species WHERE scientific_name = ?1",
            params![scientific_name.to_lowercase()],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_creation() {
        let db = LocalDatabase::open_in_memory().unwrap();
        let stats = db.get_stats().unwrap();
        assert_eq!(stats.species_count, 0);
    }

    #[test]
    fn test_map_cache_key_is_versioned() {
        assert_eq!(LocalDatabase::map_cache_key(5219408), "gbif_map_v2_5219408");
    }

    #[test]
    fn test_legacy_map_cache_key() {
        assert_eq!(
            LocalDatabase::legacy_map_cache_key(5219408),
            "gbif_map_5219408"
        );
    }

    #[test]
    fn gets_cached_media_without_loading_species_json() {
        let db = LocalDatabase::open_in_memory().unwrap();
        db.cache_image(
            "https://example.com/inat.jpg",
            b"species-bytes",
            Some("image/jpeg"),
        )
        .unwrap();
        db.cache_map_image(42, b"map-bytes").unwrap();

        let media = db
            .get_cached_media(Some("https://example.com/inat.jpg"), Some(42))
            .unwrap();

        assert_eq!(media.species_image, Some(b"species-bytes".to_vec()));
        assert_eq!(media.map_image, Some(b"map-bytes".to_vec()));
    }

    #[test]
    fn caches_wiki_article_and_life_history() {
        let db = LocalDatabase::open_in_memory().unwrap();
        let article = WikiArticle {
            extract: "Lion extract".to_string(),
            wikitext: "Lion wikitext".to_string(),
        };
        let fallback = WikiLifeHistoryFallback {
            lifespan_years: Some(20.0),
            length_meters: Some(1.8),
            height_meters: None,
            mass_kilograms: Some(188.0),
            reproduction_modes: vec!["Sexual".to_string()],
        };

        db.cache_wiki_article("Panthera leo", &article).unwrap();
        db.cache_wiki_life_history("Panthera leo", &fallback)
            .unwrap();

        assert_eq!(
            db.get_wiki_article("Panthera leo")
                .unwrap()
                .unwrap()
                .extract,
            "Lion extract"
        );
        assert_eq!(
            db.get_wiki_life_history("Panthera leo")
                .unwrap()
                .unwrap()
                .lifespan_years,
            Some(20.0)
        );
    }

    #[test]
    fn caches_rich_species_without_ttl_gate() {
        let db = LocalDatabase::open_in_memory().unwrap();
        let species = UnifiedSpecies {
            scientific_name: "Panthera leo".to_string(),
            common_names: vec!["Lion".to_string()],
            rank: "species".to_string(),
            taxonomy: crate::species::Taxonomy::default(),
            ids: crate::species::ExternalIds::default(),
            genome: crate::species::GenomeStats::default(),
            life_history: crate::species::LifeHistory {
                extraction_version: crate::species::CURRENT_LIFE_HISTORY_VERSION,
                ..crate::species::LifeHistory::default()
            },
            description: Some("Big cat".to_string()),
            wikipedia_extract: None,
            wikipedia_url: None,
            conservation_status: None,
            iucn_status: None,
            observations_count: None,
            gbif_occurrences: None,
            top_countries: Vec::new(),
            distribution: crate::species::Distribution::default(),
            images: Vec::new(),
        };

        db.cache_rich_species(&species).unwrap();
        let cached = db.get_rich_species("Panthera leo").unwrap().unwrap();
        assert_eq!(cached.scientific_name, "Panthera leo");
        assert_eq!(cached.common_names, vec!["Lion"]);
    }

    #[test]
    fn search_taxon_names_stays_within_curated_species() {
        let db = LocalDatabase::open_in_memory().unwrap();

        let curated = UnifiedSpecies {
            scientific_name: "Panthera leo".to_string(),
            common_names: vec!["Lion".to_string()],
            rank: "species".to_string(),
            taxonomy: crate::species::Taxonomy {
                kingdom: Some("Animalia".to_string()),
                phylum: Some("Chordata".to_string()),
                class: Some("Mammalia".to_string()),
                order: Some("Carnivora".to_string()),
                family: Some("Felidae".to_string()),
                genus: Some("Panthera".to_string()),
                division: None,
                lineage: Vec::new(),
            },
            ids: crate::species::ExternalIds::default(),
            genome: crate::species::GenomeStats::default(),
            life_history: crate::species::LifeHistory {
                extraction_version: crate::species::CURRENT_LIFE_HISTORY_VERSION,
                ..crate::species::LifeHistory::default()
            },
            description: Some("Big cat".to_string()),
            wikipedia_extract: None,
            wikipedia_url: None,
            conservation_status: None,
            iucn_status: None,
            observations_count: None,
            gbif_occurrences: None,
            top_countries: Vec::new(),
            distribution: crate::species::Distribution::default(),
            images: Vec::new(),
        };

        let non_curated = UnifiedSpecies {
            scientific_name: "Dicentrarchus labrax".to_string(),
            common_names: vec!["European Seabass".to_string()],
            rank: "species".to_string(),
            taxonomy: crate::species::Taxonomy {
                kingdom: Some("Animalia".to_string()),
                phylum: Some("Chordata".to_string()),
                class: Some("Actinopteri".to_string()),
                order: Some("Perciformes".to_string()),
                family: Some("Moronidae".to_string()),
                genus: Some("Dicentrarchus".to_string()),
                division: None,
                lineage: Vec::new(),
            },
            ids: crate::species::ExternalIds::default(),
            genome: crate::species::GenomeStats::default(),
            life_history: crate::species::LifeHistory {
                extraction_version: crate::species::CURRENT_LIFE_HISTORY_VERSION,
                ..crate::species::LifeHistory::default()
            },
            description: Some("Seabass".to_string()),
            wikipedia_extract: None,
            wikipedia_url: None,
            conservation_status: None,
            iucn_status: None,
            observations_count: None,
            gbif_occurrences: None,
            top_countries: Vec::new(),
            distribution: crate::species::Distribution::default(),
            images: Vec::new(),
        };

        db.cache_rich_species(&curated).unwrap();
        db.cache_rich_species(&non_curated).unwrap();

        let panthera_hits = db.search_taxon_names("panthera", 20).unwrap();
        assert!(panthera_hits
            .iter()
            .any(|row| row.scientific_name == "Panthera leo"));
        assert!(panthera_hits
            .iter()
            .any(|row| row.scientific_name == "Panthera" && row.rank == "GENUS"));

        let seabass_hits = db.search_taxon_names("seabass", 20).unwrap();
        assert!(seabass_hits.is_empty());
    }
}
