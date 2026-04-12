//! Species aggregation service that fetches from multiple APIs

use crate::api::{
    ensembl::EnsemblClient,
    gbif::GbifClient,
    inat::InatClient,
    ncbi::NcbiClient,
    ollama::OllamaClient,
    wikipedia::{WikiArticle, WikiLifeHistoryFallback, WikipediaClient},
    ApiError,
};
use crate::cache::Cache;
use crate::curated_animals::apply_curated_animal_supplement;
use crate::db_worker::DbWorker;
use crate::local_db::{CachedMedia, CachedSpecies, TaxonName};
use crate::species::{
    BoundingBox, CountryOccurrence, Distribution, ExternalIds, GenomeStats, ImageInfo, LifeHistory,
    LineageEntry, Taxonomy, UnifiedSpecies, CURRENT_LIFE_HISTORY_VERSION,
};

/// Service for aggregating species data from multiple sources
pub struct SpeciesService {
    ncbi: NcbiClient,
    inat: InatClient,
    gbif: GbifClient,
    wikipedia: WikipediaClient,
    ollama: OllamaClient,
    ensembl: EnsemblClient,
    db: DbWorker,
    cache: Cache,
}

impl SpeciesService {
    /// Create a new species service with default cache settings (24 hour TTL)
    pub fn new() -> Result<Self, std::io::Error> {
        Ok(Self {
            ncbi: NcbiClient::new(),
            inat: InatClient::new(),
            gbif: GbifClient::new(),
            wikipedia: WikipediaClient::new(),
            ollama: OllamaClient::new(),
            ensembl: EnsemblClient::new(),
            db: DbWorker::new()?,
            cache: Cache::default_location(24)?,
        })
    }

    /// Create a species service with a custom cache
    pub fn with_cache(cache: Cache) -> Result<Self, std::io::Error> {
        Ok(Self {
            ncbi: NcbiClient::new(),
            inat: InatClient::new(),
            gbif: GbifClient::new(),
            wikipedia: WikipediaClient::new(),
            ollama: OllamaClient::new(),
            ensembl: EnsemblClient::new(),
            db: DbWorker::new()?,
            cache,
        })
    }

    /// Look up a species by name, checking local cache first
    pub async fn lookup(&self, name: &str) -> Result<UnifiedSpecies, ApiError> {
        self.lookup_with_options(name, false).await
    }

    /// Look up a species with option to force refresh
    pub async fn lookup_with_options(
        &self,
        name: &str,
        force_refresh: bool,
    ) -> Result<UnifiedSpecies, ApiError> {
        let lookup_span = crate::perf::start_span();
        let name_owned = name.to_string();

        // Check local SQLite database first (unless forcing refresh)
        if !force_refresh {
            let cached = self.db.get_species(name_owned).await;

            if let Some(mut cached) = cached {
                apply_curated_animal_supplement(&mut cached.species);
                cached.species.taxonomy.lineage = cached
                    .species
                    .taxonomy
                    .build_display_lineage(&cached.species.scientific_name, &cached.species.rank);
                crate::perf::log_value("lookup.cache_hit", &cached.species.scientific_name);
                crate::perf::log_elapsed("lookup.total", lookup_span);
                return Ok(cached.species);
            }

            if let Some(mut rich_species) = self.db.get_rich_species(name.to_string()).await {
                apply_curated_animal_supplement(&mut rich_species);
                rich_species.taxonomy.lineage = rich_species
                    .taxonomy
                    .build_display_lineage(&rich_species.scientific_name, &rich_species.rank);
                self.db.cache_species_detached(rich_species.clone());
                crate::perf::log_value("lookup.rich_cache_hit", &rich_species.scientific_name);
                crate::perf::log_elapsed("lookup.total", lookup_span);
                return Ok(rich_species);
            }

            if let Some(taxon) = self.db.get_taxon_by_name(name.to_string()).await {
                let mut species = build_local_species_profile(&taxon);
                apply_curated_animal_supplement(&mut species);
                self.db.cache_species_detached(species.clone());
                self.db.cache_rich_species_detached(species.clone());
                crate::perf::log_value("lookup.local_taxonomy_hit", &species.scientific_name);
                crate::perf::log_elapsed("lookup.total", lookup_span);
                return Ok(species);
            }

            crate::perf::log_elapsed("lookup.total", lookup_span);
            return Err(ApiError::NotFound(name.to_string()));
        }

        let previous_species = if force_refresh {
            let previous_species = self
                .db
                .get_species(name_owned.clone())
                .await
                .map(|cached| cached.species);
            let previous_species = match previous_species {
                Some(species) => Some(species),
                None => self.db.get_rich_species(name.to_string()).await,
            };
            // Invalidate cache entry if forcing refresh
            self.db.invalidate_species(name.to_string()).await;
            previous_species
        } else {
            None
        };

        // Fetch from APIs
        let mut species = self.fetch_from_apis(name, force_refresh).await?;
        if let Some(previous) = previous_species.as_ref() {
            merge_species_missing_fields(&mut species, previous);
        }
        apply_curated_animal_supplement(&mut species);
        species.taxonomy.lineage = species
            .taxonomy
            .build_display_lineage(&species.scientific_name, &species.rank);

        // Cache in local database
        self.db.cache_species_detached(species.clone());
        self.db.cache_rich_species_detached(species.clone());
        crate::perf::log_elapsed("lookup.total", lookup_span);

        Ok(species)
    }

    /// Get cached species data and images (for TUI)
    pub async fn get_cached_with_images(&self, name: &str) -> Option<CachedSpecies> {
        self.db
            .get_species(name.to_string())
            .await
            .map(|mut cached| {
                apply_curated_animal_supplement(&mut cached.species);
                cached.species.taxonomy.lineage = cached
                    .species
                    .taxonomy
                    .build_display_lineage(&cached.species.scientific_name, &cached.species.rank);
                cached
            })
    }

    pub async fn get_rich_species(&self, name: &str) -> Option<UnifiedSpecies> {
        self.db
            .get_rich_species(name.to_string())
            .await
            .map(|mut species| {
                apply_curated_animal_supplement(&mut species);
                species.taxonomy.lineage = species
                    .taxonomy
                    .build_display_lineage(&species.scientific_name, &species.rank);
                species
            })
    }

    pub async fn get_cached_media(&self, species: &UnifiedSpecies) -> CachedMedia {
        self.db
            .get_cached_media(
                species.preferred_image_url().map(str::to_string),
                species.ids.gbif_key,
            )
            .await
    }

    async fn get_cached_wiki_article(&self, title: &str) -> Result<WikiArticle, ApiError> {
        if let Some(article) = self.db.get_wiki_article(title.to_string()).await {
            crate::perf::log_value("wiki.article_cache_hit", title);
            return Ok(article);
        }

        let article = self.wikipedia.get_article_content(title).await?;
        self.db
            .cache_wiki_article_detached(title.to_string(), article.clone());
        crate::perf::log_value("wiki.article_cached", title);
        Ok(article)
    }

    async fn get_cached_wiki_life_history(&self, title: &str) -> Option<WikiLifeHistoryFallback> {
        if let Some(fallback) = self.db.get_wiki_life_history(title.to_string()).await {
            crate::perf::log_value("wiki.life_history_cache_hit", title);
            return Some(fallback);
        }

        let article = self.get_cached_wiki_article(title).await.ok()?;
        let mut fallback = self.wikipedia.extract_life_history_from_article(&article);

        if fallback.needs_completion() {
            if let Ok(llm_fallback) = self.ollama.extract_life_history(title, &article).await {
                fallback.fill_missing_from(llm_fallback);
                crate::perf::log_value("wiki.life_history_ollama", title);
            }
        }

        if !fallback.has_any_stats() {
            return None;
        }

        self.db
            .cache_wiki_life_history_detached(title.to_string(), fallback.clone());
        Some(fallback)
    }

    /// Cache image data
    pub async fn cache_species_image(&self, species: &UnifiedSpecies, data: &[u8]) {
        self.db
            .cache_species_image(species.clone(), data.to_vec())
            .await;
    }

    pub fn cache_species_image_detached(&self, species: &UnifiedSpecies, data: Vec<u8>) {
        if let Some(url) = species.preferred_image_url() {
            self.db
                .cache_image_detached(url.to_string(), data, Some("image/jpeg"));
        }
    }

    /// Cache map image data
    pub async fn cache_map_image(&self, gbif_key: u64, data: &[u8]) {
        self.db.cache_map_image(gbif_key, data.to_vec()).await;
    }

    pub fn cache_map_image_detached(&self, gbif_key: u64, data: Vec<u8>) {
        self.db.cache_map_image_detached(gbif_key, data);
    }

    pub fn cache_rich_species_detached(&self, species: UnifiedSpecies) {
        self.db.cache_rich_species_detached(species);
    }

    pub async fn flush_cache_writes(&self) {
        self.db.flush().await;
    }

    pub async fn invalidate_map_image(&self, gbif_key: u64) {
        self.db.invalidate_map_image(gbif_key).await;
    }

    /// Search taxon names offline
    pub async fn search_offline(&self, query: &str, limit: u32) -> Vec<TaxonName> {
        self.db.search_taxon_names(query.to_string(), limit).await
    }

    /// Check if offline search is available
    pub async fn has_offline_search(&self) -> bool {
        self.db.has_backbone().await
    }

    pub async fn species_rank_count(&self) -> u64 {
        self.db.species_rank_count().await
    }

    pub async fn get_species_batch_after(&self, after_gbif_key: u64, limit: u32) -> Vec<TaxonName> {
        self.db.get_species_batch_after(after_gbif_key, limit).await
    }

    pub async fn get_cached_kingdoms(&self) -> Vec<String> {
        self.db.get_cached_kingdoms().await
    }

    pub async fn get_cached_parent_taxon(
        &self,
        child_rank: &str,
        child_value: &str,
    ) -> Option<(String, String)> {
        self.db
            .get_cached_parent_taxon(child_rank.to_string(), child_value.to_string())
            .await
    }

    pub async fn get_user_stat(&self, key: &str) -> Option<String> {
        self.db.get_user_stat(key.to_string()).await
    }

    pub async fn set_user_stat(&self, key: &str, value: impl Into<String>) {
        self.db.set_user_stat(key.to_string(), value.into()).await;
    }

    pub async fn delete_user_stat(&self, key: &str) {
        self.db.delete_user_stat(key.to_string()).await;
    }

    /// Toggle favorite status for a species
    pub async fn toggle_favorite(&self, name: &str) -> bool {
        self.db.toggle_favorite(name.to_string()).await
    }

    /// Check if species is a favorite
    pub async fn is_favorite(&self, name: &str) -> bool {
        self.db.is_favorite(name.to_string()).await
    }

    /// Get siblings at a taxonomic rank from local database (instant, no API call)
    pub async fn get_siblings_local(
        &self,
        parent_rank: &str,
        parent_value: &str,
        child_rank: &str,
        limit: u32,
    ) -> Vec<TaxonName> {
        self.db
            .get_siblings(
                parent_rank.to_string(),
                parent_value.to_string(),
                child_rank.to_string(),
                limit,
            )
            .await
    }

    /// Get all species in a genus (instant local lookup)
    pub async fn get_species_in_genus(&self, genus: &str, limit: u32) -> Vec<TaxonName> {
        self.db.get_species_in_genus(genus.to_string(), limit).await
    }

    /// Get all genera in a family (instant local lookup)
    pub async fn get_genera_in_family(&self, family: &str, limit: u32) -> Vec<TaxonName> {
        self.db
            .get_genera_in_family(family.to_string(), limit)
            .await
    }

    /// Get taxon by name (instant local lookup)
    pub async fn get_taxon_by_name(&self, name: &str) -> Option<TaxonName> {
        self.db.get_taxon_by_name(name.to_string()).await
    }

    /// Fetch species data from APIs (internal)
    async fn fetch_from_apis(
        &self,
        name: &str,
        bypass_file_cache: bool,
    ) -> Result<UnifiedSpecies, ApiError> {
        let fetch_span = crate::perf::start_span();
        // Check file cache first
        let cache_key = format!("species_{}", name.to_lowercase().replace(' ', "_"));
        let mut stale_file_cache = None;
        if !bypass_file_cache {
            if let Some(mut cached) = self.cache.get::<UnifiedSpecies>(&cache_key) {
                cached.taxonomy.lineage = cached
                    .taxonomy
                    .build_display_lineage(&cached.scientific_name, &cached.rank);
                if cached.life_history.is_current() {
                    crate::perf::log_value("fetch.file_cache_hit", &cached.scientific_name);
                    crate::perf::log_elapsed("fetch.total", fetch_span);
                    return Ok(cached);
                }

                stale_file_cache = Some(cached);
            }
        }

        // Fetch taxonomy APIs in parallel
        let source_fetch_span = crate::perf::start_span();
        let (ncbi_result, inat_result, gbif_result, wiki_result, wikidata_result) = tokio::join!(
            self.ncbi.get_taxonomy(name),
            self.inat.search_species(name),
            self.gbif.match_species(name),
            self.wikipedia.get_summary(name),
            self.wikipedia.get_taxon_wikidata(name),
        );
        crate::perf::log_elapsed("fetch.sources", source_fetch_span);

        let has_taxon_match = ncbi_result.is_ok()
            || inat_result.is_ok()
            || gbif_result.is_ok()
            || wikidata_result.is_ok();
        let ncbi_tax_id = ncbi_result.as_ref().ok().map(|record| record.tax_id);
        let genome_fetch_span = crate::perf::start_span();
        let (genome_result, ensembl_result) = if has_taxon_match {
            tokio::join!(
                async {
                    // Delay before NCBI genome stats to avoid rate limiting
                    // (NCBI allows 3 requests/sec without API key)
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    if let Some(tax_id) = ncbi_tax_id {
                        self.ncbi.get_genome_stats_by_tax_id(tax_id, name).await
                    } else {
                        self.ncbi.get_genome_stats(name).await
                    }
                },
                self.ensembl.get_genome_info(name),
            )
        } else {
            (
                Err(ApiError::NotFound(name.to_string())),
                Err(ApiError::NotFound(name.to_string())),
            )
        };
        crate::perf::log_elapsed("fetch.genome_ensembl", genome_fetch_span);

        // Start building the unified species
        let mut scientific_name = name.to_string();
        let mut common_names: Vec<String> = Vec::new();
        let mut rank = String::from("species");
        let mut taxonomy = Taxonomy::default();
        let mut ids = ExternalIds::default();
        let mut description = None;
        let mut wikipedia_extract = None;
        let mut wikipedia_url = None;
        let mut life_history = LifeHistory {
            extraction_version: CURRENT_LIFE_HISTORY_VERSION,
            ..LifeHistory::default()
        };
        let mut conservation_status = None;
        let mut iucn_status = None;
        let mut observations_count = None;
        let mut gbif_occurrences = None;
        let mut top_countries: Vec<CountryOccurrence> = Vec::new();
        let mut distribution = Distribution::default();
        let mut images: Vec<ImageInfo> = Vec::new();

        // Process GBIF result FIRST - it has the most reliable Linnaean taxonomy
        // GBIF is specifically designed for taxonomic classification
        if let Ok(gbif) = &gbif_result {
            ids.gbif_key = Some(gbif.key);

            // GBIF provides authoritative Linnaean taxonomy
            taxonomy.kingdom = gbif.kingdom.clone();
            taxonomy.phylum = gbif.phylum.clone();
            taxonomy.class = gbif.class.clone();
            taxonomy.order = gbif.order.clone();
            taxonomy.family = gbif.family.clone();
            taxonomy.genus = gbif.genus.clone();

            if let Some(vernacular) = &gbif.vernacular_name {
                if !common_names.contains(vernacular) {
                    common_names.push(vernacular.clone());
                }
            }

            let gbif_enrichment_span = crate::perf::start_span();
            let (count_result, countries_result, continents_result, bbox_result) = tokio::join!(
                self.gbif.get_occurrence_count(gbif.key),
                self.gbif.get_country_counts(gbif.key),
                self.gbif.get_continents(gbif.key),
                self.gbif.get_bounding_box(gbif.key),
            );
            crate::perf::log_elapsed("fetch.gbif_enrichment", gbif_enrichment_span);

            if let Ok(count) = count_result {
                gbif_occurrences = Some(count);
            }

            if let Ok(countries) = countries_result {
                top_countries = countries
                    .into_iter()
                    .take(10)
                    .map(|c| CountryOccurrence {
                        country: c.country_name,
                        count: c.count,
                    })
                    .collect();
            }

            if let Ok(continents) = continents_result {
                distribution.continents = continents;
            }

            if let Ok((min_lat, max_lat, min_lon, max_lon)) = bbox_result {
                distribution.bounding_box = Some(BoundingBox {
                    min_latitude: min_lat,
                    max_latitude: max_lat,
                    min_longitude: min_lon,
                    max_longitude: max_lon,
                });
            }
        }

        // Process NCBI result - provides detailed lineage and molecular taxonomy
        if let Ok(ncbi) = ncbi_result {
            scientific_name = ncbi.scientific_name;
            rank = ncbi.rank.clone();
            ids.ncbi_tax_id = Some(ncbi.tax_id);
            taxonomy.division = Some(ncbi.division);
            // Preserve NCBI lineage long enough to backfill missing canonical ranks.
            taxonomy.lineage = ncbi.lineage.into_iter().map(LineageEntry::from).collect();

            if let Some(common) = ncbi.common_name {
                if !common_names.contains(&common) {
                    common_names.push(common);
                }
            }

            // Only fill in taxonomy from NCBI if GBIF didn't provide it
            for entry in &taxonomy.lineage {
                match entry.rank.to_lowercase().as_str() {
                    "kingdom" if taxonomy.kingdom.is_none() => {
                        taxonomy.kingdom = Some(entry.name.clone())
                    }
                    "phylum" if taxonomy.phylum.is_none() => {
                        taxonomy.phylum = Some(entry.name.clone())
                    }
                    "class" if taxonomy.class.is_none() => {
                        taxonomy.class = Some(entry.name.clone())
                    }
                    "order" if taxonomy.order.is_none() => {
                        taxonomy.order = Some(entry.name.clone())
                    }
                    "family" if taxonomy.family.is_none() => {
                        taxonomy.family = Some(entry.name.clone())
                    }
                    "genus" if taxonomy.genus.is_none() => {
                        taxonomy.genus = Some(entry.name.clone())
                    }
                    _ => {}
                }
            }
        }

        // Process iNaturalist result
        if let Ok(inat) = inat_result {
            ids.inat_id = Some(inat.id);
            observations_count = Some(inat.observations_count);

            if let Some(common) = inat.preferred_common_name {
                if !common_names.contains(&common) {
                    common_names.insert(0, common); // Prefer iNat common name
                }
            }

            if let Some(status) = inat.conservation_status {
                conservation_status = status.status_name;
            }

            if let Some(photo) = inat.default_photo {
                if let Some(url) = photo.medium_url.or(photo.url) {
                    // Upgrade to original resolution for best quality
                    // iNaturalist URLs: .../square.jpg, .../medium.jpg, .../large.jpg, .../original.jpg
                    let high_res_url = url
                        .replace("/medium.", "/original.")
                        .replace("/square.", "/original.")
                        .replace("/large.", "/original.");
                    images.push(ImageInfo {
                        url: high_res_url,
                        source: "iNaturalist".to_string(),
                        attribution: photo.attribution,
                    });
                }
            }

            // Fill in any still-missing taxonomy from iNat ancestors
            for ancestor in inat.ancestors {
                match ancestor.rank.to_lowercase().as_str() {
                    "kingdom" if taxonomy.kingdom.is_none() => {
                        taxonomy.kingdom = Some(ancestor.name)
                    }
                    "phylum" if taxonomy.phylum.is_none() => taxonomy.phylum = Some(ancestor.name),
                    "class" if taxonomy.class.is_none() => taxonomy.class = Some(ancestor.name),
                    "order" if taxonomy.order.is_none() => taxonomy.order = Some(ancestor.name),
                    "family" if taxonomy.family.is_none() => taxonomy.family = Some(ancestor.name),
                    "genus" if taxonomy.genus.is_none() => taxonomy.genus = Some(ancestor.name),
                    _ => {}
                }
            }
        }

        // Process Wikipedia result
        if let Ok(wiki) = wiki_result {
            wikipedia_extract = Some(wiki.extract);
            wikipedia_url = Some(wiki.page_url);
            description = wiki.description;

            if let Some(thumb) = wiki.thumbnail_url {
                images.push(ImageInfo {
                    url: thumb,
                    source: "Wikipedia".to_string(),
                    attribution: Some("Wikimedia Commons".to_string()),
                });
            }
        }

        // Process Wikidata result
        if let Ok(wikidata) = wikidata_result {
            if ids.ncbi_tax_id.is_none() {
                if let Some(taxon_rank) = wikidata.taxon_rank.clone() {
                    rank = taxon_rank;
                }
            }

            ids.wikidata_id = Some(wikidata.id);
            iucn_status = wikidata.iucn_status;
            life_history.lifespan_years = wikidata.life_expectancy_years;
            life_history.length_meters = wikidata.length_meters;
            life_history.height_meters = wikidata.height_meters;
            life_history.mass_kilograms = wikidata.mass_kilograms;
            life_history.reproduction_modes = wikidata.reproduction_modes;

            if description.is_none() {
                description = wikidata.description;
            }

            // Add aliases as common names
            for alias in wikidata.aliases {
                if !common_names.contains(&alias) {
                    common_names.push(alias);
                }
            }

            if let Some(img_url) = wikidata.image_url {
                // Avoid duplicates
                if !images.iter().any(|i| i.url == img_url) {
                    images.push(ImageInfo {
                        url: img_url,
                        source: "Wikidata".to_string(),
                        attribution: Some("Wikimedia Commons".to_string()),
                    });
                }
            }
        }

        if life_history.lifespan_years.is_none()
            || (life_history.length_meters.is_none() && life_history.height_meters.is_none())
            || life_history.mass_kilograms.is_none()
            || life_history.reproduction_modes.is_empty()
        {
            if let Some(fallback) = self.get_cached_wiki_life_history(&scientific_name).await {
                if life_history.lifespan_years.is_none() {
                    life_history.lifespan_years = fallback.lifespan_years;
                }
                if life_history.length_meters.is_none() {
                    life_history.length_meters = fallback.length_meters;
                }
                if life_history.height_meters.is_none() {
                    life_history.height_meters = fallback.height_meters;
                }
                if life_history.mass_kilograms.is_none() {
                    life_history.mass_kilograms = fallback.mass_kilograms;
                }
                if life_history.reproduction_modes.is_empty() {
                    life_history.reproduction_modes = fallback.reproduction_modes;
                }
            }
        }

        // Check that we got data from at least one source
        if ids.ncbi_tax_id.is_none()
            && ids.inat_id.is_none()
            && ids.gbif_key.is_none()
            && ids.wikidata_id.is_none()
        {
            if let Some(cached) = stale_file_cache {
                crate::perf::log_value("fetch.stale_cache_fallback", &cached.scientific_name);
                crate::perf::log_elapsed("fetch.total", fetch_span);
                return Ok(cached);
            }
            crate::perf::log_elapsed("fetch.total", fetch_span);
            return Err(ApiError::NotFound(name.to_string()));
        }

        // Process genome stats from NCBI
        let mut genome = genome_result.map(GenomeStats::from).unwrap_or_default();

        // Fetch Ensembl data (includes gene counts)
        if let Ok(ensembl_info) = ensembl_result {
            ids.ensembl_id = Some(ensembl_info.species.clone());
            genome.merge_ensembl(&ensembl_info);
        }

        // Build a clean lineage for the TUI and navigation.
        taxonomy.lineage = taxonomy.build_display_lineage(&scientific_name, &rank);

        let unified = UnifiedSpecies {
            scientific_name,
            common_names,
            rank,
            taxonomy,
            ids,
            genome,
            life_history,
            description,
            wikipedia_extract,
            wikipedia_url,
            conservation_status,
            iucn_status,
            observations_count,
            gbif_occurrences,
            top_countries,
            distribution,
            images,
        };

        // Cache the result in file cache
        let _ = self.cache.set(&cache_key, &unified);
        crate::perf::log_elapsed("fetch.total", fetch_span);

        Ok(unified)
    }
}

fn merge_species_missing_fields(species: &mut UnifiedSpecies, previous: &UnifiedSpecies) {
    for common_name in &previous.common_names {
        if !species
            .common_names
            .iter()
            .any(|current| current.eq_ignore_ascii_case(common_name))
        {
            species.common_names.push(common_name.clone());
        }
    }

    for image in &previous.images {
        if !species
            .images
            .iter()
            .any(|current| current.url == image.url)
        {
            species.images.push(image.clone());
        }
    }

    if species.taxonomy.kingdom.is_none() {
        species.taxonomy.kingdom = previous.taxonomy.kingdom.clone();
    }
    if species.taxonomy.phylum.is_none() {
        species.taxonomy.phylum = previous.taxonomy.phylum.clone();
    }
    if species.taxonomy.class.is_none() {
        species.taxonomy.class = previous.taxonomy.class.clone();
    }
    if species.taxonomy.order.is_none() {
        species.taxonomy.order = previous.taxonomy.order.clone();
    }
    if species.taxonomy.family.is_none() {
        species.taxonomy.family = previous.taxonomy.family.clone();
    }
    if species.taxonomy.genus.is_none() {
        species.taxonomy.genus = previous.taxonomy.genus.clone();
    }
    if species.taxonomy.division.is_none() {
        species.taxonomy.division = previous.taxonomy.division.clone();
    }

    if species.ids.ncbi_tax_id.is_none() {
        species.ids.ncbi_tax_id = previous.ids.ncbi_tax_id;
    }
    if species.ids.inat_id.is_none() {
        species.ids.inat_id = previous.ids.inat_id;
    }
    if species.ids.gbif_key.is_none() {
        species.ids.gbif_key = previous.ids.gbif_key;
    }
    if species.ids.wikidata_id.is_none() {
        species.ids.wikidata_id = previous.ids.wikidata_id.clone();
    }
    if species.ids.ensembl_id.is_none() {
        species.ids.ensembl_id = previous.ids.ensembl_id.clone();
    }

    if species.life_history.lifespan_years.is_none() {
        species.life_history.lifespan_years = previous.life_history.lifespan_years;
    }
    if species.life_history.length_meters.is_none() {
        species.life_history.length_meters = previous.life_history.length_meters;
    }
    if species.life_history.height_meters.is_none() {
        species.life_history.height_meters = previous.life_history.height_meters;
    }
    if species.life_history.mass_kilograms.is_none() {
        species.life_history.mass_kilograms = previous.life_history.mass_kilograms;
    }
    if species.life_history.reproduction_modes.is_empty() {
        species.life_history.reproduction_modes = previous.life_history.reproduction_modes.clone();
    }

    if species.description.is_none() {
        species.description = previous.description.clone();
    }
    if species.wikipedia_extract.is_none() {
        species.wikipedia_extract = previous.wikipedia_extract.clone();
    }
    if species.wikipedia_url.is_none() {
        species.wikipedia_url = previous.wikipedia_url.clone();
    }
    if species.conservation_status.is_none() {
        species.conservation_status = previous.conservation_status.clone();
    }
    if species.iucn_status.is_none() {
        species.iucn_status = previous.iucn_status.clone();
    }

    if species.observations_count.is_none() {
        species.observations_count = previous.observations_count;
    }
    if species.gbif_occurrences.is_none() {
        species.gbif_occurrences = previous.gbif_occurrences;
    }
    if species.top_countries.is_empty() {
        species.top_countries = previous.top_countries.clone();
    }
    if species.distribution.continents.is_empty() {
        species.distribution.continents = previous.distribution.continents.clone();
    }
    if species.distribution.bounding_box.is_none() {
        species.distribution.bounding_box = previous.distribution.bounding_box.clone();
    }
    if species.distribution.native_range.is_none() {
        species.distribution.native_range = previous.distribution.native_range.clone();
    }

    merge_genome_missing_fields(&mut species.genome, &previous.genome);

    species.taxonomy.lineage = species
        .taxonomy
        .build_display_lineage(&species.scientific_name, &species.rank);
}

fn merge_genome_missing_fields(genome: &mut GenomeStats, previous: &GenomeStats) {
    if genome.assembly_accession.is_none() {
        genome.assembly_accession = previous.assembly_accession.clone();
    }
    if genome.assembly_name.is_none() {
        genome.assembly_name = previous.assembly_name.clone();
    }
    if genome.genome_size_bp.is_none() {
        genome.genome_size_bp = previous.genome_size_bp;
    }
    if genome.chromosome_count.is_none() {
        genome.chromosome_count = previous.chromosome_count;
    }
    if genome.scaffold_count.is_none() {
        genome.scaffold_count = previous.scaffold_count;
    }
    if genome.contig_count.is_none() {
        genome.contig_count = previous.contig_count;
    }
    if genome.scaffold_n50.is_none() {
        genome.scaffold_n50 = previous.scaffold_n50;
    }
    if genome.contig_n50.is_none() {
        genome.contig_n50 = previous.contig_n50;
    }
    if genome.gc_percent.is_none() {
        genome.gc_percent = previous.gc_percent;
    }
    if genome.assembly_level.is_none() {
        genome.assembly_level = previous.assembly_level.clone();
    }
    if genome.mito_genome_size_bp.is_none() {
        genome.mito_genome_size_bp = previous.mito_genome_size_bp;
    }
    if genome.coding_genes.is_none() {
        genome.coding_genes = previous.coding_genes;
    }
    if genome.noncoding_genes.is_none() {
        genome.noncoding_genes = previous.noncoding_genes;
    }
    if genome.pseudogenes.is_none() {
        genome.pseudogenes = previous.pseudogenes;
    }
    if genome.genebuild.is_none() {
        genome.genebuild = previous.genebuild.clone();
    }
    if !genome.is_reference {
        genome.is_reference = previous.is_reference;
    }
}

pub fn build_local_species_profile(taxon: &TaxonName) -> UnifiedSpecies {
    let taxonomy = Taxonomy {
        kingdom: taxon.kingdom.clone(),
        phylum: taxon.phylum.clone(),
        class: taxon.class.clone(),
        order: taxon.order.clone(),
        family: taxon.family.clone(),
        genus: taxon.genus.clone(),
        division: None,
        lineage: Vec::new(),
    };
    let scientific_name = taxon.scientific_name.clone();
    let rank = taxon.rank.clone();
    let taxonomy = Taxonomy {
        lineage: taxonomy.build_display_lineage(&scientific_name, &rank),
        ..taxonomy
    };

    UnifiedSpecies {
        scientific_name,
        common_names: Vec::new(),
        rank,
        taxonomy,
        ids: ExternalIds {
            gbif_key: Some(taxon.gbif_key),
            ..ExternalIds::default()
        },
        genome: GenomeStats::default(),
        life_history: LifeHistory {
            extraction_version: CURRENT_LIFE_HISTORY_VERSION,
            ..LifeHistory::default()
        },
        description: Some("Locally materialized from the offline taxonomy cache.".to_string()),
        wikipedia_extract: None,
        wikipedia_url: None,
        conservation_status: None,
        iucn_status: None,
        observations_count: None,
        gbif_occurrences: None,
        top_countries: Vec::new(),
        distribution: Distribution::default(),
        images: Vec::new(),
    }
}

impl Default for SpeciesService {
    fn default() -> Self {
        Self::new().expect("Failed to create SpeciesService with default cache")
    }
}
