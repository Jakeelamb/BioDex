mod api;
mod backbone_import;
mod bulk_import;
mod cache;
mod curated_animals;
mod db_worker;
mod demo;
mod local_db;
mod perf;
mod service;
mod species;
mod tui;
mod world_map;

use api::gbif::GbifClient;
use local_db::{CachedMedia, LocalDatabase};
use service::{build_local_species_profile, SpeciesService};
use species::UnifiedSpecies;
use std::collections::{BTreeSet, HashSet};
use std::env;
use std::sync::Arc;
use tokio::sync::mpsc;
use tui::TuiUpdate;

const DEFAULT_INITIAL_TAXON: &str = "Animalia";
const HOT_SEED_VERSION_KEY: &str = "hot_seed.version";
const HOT_SEED_VERSION: &str = "1";
const HOT_SEED_CONCURRENCY: usize = 3;
const RICH_CACHE_LAST_KEY: &str = "rich_cache.last_gbif_key";
const RICH_CACHE_PROCESSED: &str = "rich_cache.processed";
const RICH_CACHE_ENRICHED: &str = "rich_cache.enriched";
const RICH_CACHE_FALLBACK: &str = "rich_cache.fallback";
const RICH_CACHE_BATCH_SIZE: u32 = 32;
const RICH_CACHE_CONCURRENCY: usize = 2;
const CURATED_ANIMAL_PREFETCH_CONCURRENCY: usize = 3;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    let mut text_mode = false;
    let mut import_backbone = false;
    let mut import_all = false;
    let mut show_stats = false;
    let mut prefetch_mode = false;
    let mut seed_mode = false;
    let mut prefetch_animals = false;
    let mut audit_animals = false;
    let mut cache_all_rich = false;
    let mut force_refresh = false;
    let mut species_parts = Vec::new();

    for arg in args.iter().skip(1) {
        match arg.as_str() {
            "--text" | "-t" => text_mode = true,
            "--import-backbone" | "--import" => import_backbone = true,
            "--import-all" => import_all = true,
            "--stats" | "-s" => show_stats = true,
            "--prefetch" | "-p" => prefetch_mode = true,
            "--seed" => seed_mode = true,
            "--prefetch-animals" => prefetch_animals = true,
            "--audit-animals" => audit_animals = true,
            "--cache-all-rich" => cache_all_rich = true,
            "--force-refresh" | "--force" => force_refresh = true,
            "--help" | "-h" => {
                print_help();
                return Ok(());
            }
            _ => species_parts.push(arg.as_str()),
        }
    }

    // Handle full import (backbone + NCBI taxonomy)
    if import_all {
        return run_full_import().await;
    }

    // Handle backbone import
    if import_backbone {
        return run_backbone_import().await;
    }

    // Handle stats display
    if show_stats {
        return show_database_stats();
    }

    // Handle bulk prefetch
    if prefetch_mode || seed_mode {
        return run_bulk_prefetch(force_refresh).await;
    }

    if prefetch_animals {
        return run_curated_animal_prefetch(force_refresh).await;
    }

    if audit_animals {
        return run_curated_animal_audit().await;
    }

    if cache_all_rich {
        return run_cache_all_rich().await;
    }

    let species_name = if species_parts.is_empty() {
        DEFAULT_INITIAL_TAXON.to_string()
    } else {
        species_parts.join(" ")
    };

    let service = Arc::new(SpeciesService::new()?);
    let gbif = Arc::new(GbifClient::new());

    if text_mode {
        run_text_mode(&service, &species_name).await
    } else {
        run_tui_mode(service, gbif, &species_name).await
    }
}

fn print_help() {
    println!("ncbi_poketext - Species Database TUI");
    println!();
    println!("USAGE:");
    println!("    ncbi_poketext [OPTIONS] [SPECIES_NAME]");
    println!();
    println!("OPTIONS:");
    println!("    -t, --text              Text-only output (no TUI)");
    println!("    --import-all            Download ALL taxonomy data for maximum offline speed");
    println!("    --import-backbone       Download GBIF backbone for offline search (~200MB)");
    println!("    -p, --prefetch          Seed the hot cache for default browsing species");
    println!("    --seed                  Alias for --prefetch");
    println!("    --prefetch-animals      Refresh curated Animalia candidates and cache media");
    println!("    --audit-animals         Audit curated Animalia completeness");
    println!("    --force-refresh         Re-fetch all rows for a prefetch command");
    println!("    --cache-all-rich        Sweep all species into the durable rich cache");
    println!("    -s, --stats             Show local database statistics");
    println!("    -h, --help              Show this help message");
    println!();
    println!("TUI KEYBINDINGS:");
    println!("    j/k, ↑/↓                Navigate lineage/siblings");
    println!("    l, →                    Show sibling taxa at rank");
    println!("    h, ←                    Close siblings panel");
    println!("    Enter                   View selected taxon");
    println!("    /                       Search for species");
    println!("    r                       Refresh from live sources");
    println!("    f                       Toggle favorite");
    println!("    ?                       Show help");
    println!("    q, Esc                  Quit/close panel");
    println!();
    println!("EXAMPLES:");
    println!("    ncbi_poketext                    # Default: Animalia");
    println!("    ncbi_poketext \"Homo sapiens\"     # Search for humans");
    println!("    ncbi_poketext --import-backbone  # Download offline search data");
    println!("    ncbi_poketext --prefetch         # Materialize the hot cache");
    println!("    ncbi_poketext --cache-all-rich   # Long-running resumable rich cache sweep");
}

async fn run_full_import() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Full Taxonomy Import ===");
    println!();
    println!("This will download and import ALL taxonomy data for maximum offline speed:");
    println!("  1. GBIF Backbone (~200MB) - 3M+ taxa with taxonomy hierarchy");
    println!("  2. NCBI Taxonomy (~60MB) - Common names and detailed lineage");
    println!();

    let db = LocalDatabase::open()?;

    // Import GBIF backbone if not present
    if !db.has_backbone() {
        println!("Step 1/2: Importing GBIF Backbone...");
        backbone_import::import_backbone(&db, None::<fn(u64, u64)>).await?;
        println!();
    } else {
        println!("Step 1/2: GBIF Backbone already imported (skipping)");
        println!();
    }

    // Import NCBI taxonomy
    if !db.has_ncbi_taxonomy() {
        println!("Step 2/2: Importing NCBI Taxonomy...");
        bulk_import::import_ncbi_taxonomy(&db).await?;
        println!();
    } else {
        println!("Step 2/2: NCBI Taxonomy already imported (skipping)");
        println!();
    }

    // Show final stats
    let stats = db.get_stats()?;
    let ncbi_count = db.ncbi_taxonomy_count().unwrap_or(0);

    println!("=== Import Complete ===");
    println!();
    println!("Database now contains:");
    println!("  GBIF taxa:      {:>12}", stats.taxon_names_count);
    println!("  NCBI entries:   {:>12}", ncbi_count);
    println!(
        "  Total size:     {:>12}",
        format_bytes(stats.total_size_bytes)
    );
    println!();
    println!("You now have maximum offline speed!");
    println!("Navigation and search will be instant.");

    Ok(())
}

async fn run_backbone_import() -> Result<(), Box<dyn std::error::Error>> {
    let db = LocalDatabase::open()?;

    if db.has_backbone() {
        let count = db.taxon_names_count()?;
        println!("GBIF backbone already imported ({} taxa).", count);
        println!("To re-import, delete the database and run again.");
        return Ok(());
    }

    println!("=== GBIF Backbone Taxonomy Import ===");
    println!();
    println!("This will download the GBIF backbone taxonomy (~200MB compressed)");
    println!("and import ~3-4 million species/taxa names for offline search.");
    println!();
    println!("The database will be stored at:");
    println!("  ~/.local/share/ncbi_poketext/species_cache.db");
    println!();

    backbone_import::import_backbone(&db, None::<fn(u64, u64)>).await?;

    println!();
    println!("Import complete! You can now search offline with '/' in the TUI.");

    Ok(())
}

#[derive(Debug)]
struct HotSeedAuditRow {
    gaps: Vec<&'static str>,
}

#[derive(Debug)]
struct HotSeedRefreshOutcome {
    requested_name: String,
    scientific_name: Option<String>,
    ready: bool,
    failed: bool,
    gaps: Vec<&'static str>,
}

#[derive(Debug, Default)]
struct HotSeedSummary {
    total: usize,
    ready: usize,
    refreshed: usize,
    failed: usize,
    media_missing: usize,
}

async fn run_bulk_prefetch(force_refresh: bool) -> Result<(), Box<dyn std::error::Error>> {
    use indicatif::{ProgressBar, ProgressStyle};

    println!("=== Hot Cache Seed ===");
    println!();
    println!("This materializes the default browsing pack for instant opens.");
    println!("It covers the startup taxonomy, the demo species set, and the curated animals pack.");
    println!();

    let service = Arc::new(SpeciesService::new()?);
    let gbif = Arc::new(GbifClient::new());
    let species_names = hot_seed_species_names();
    let total = species_names.len();

    let pb = ProgressBar::new(total as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("#>-"),
    );

    let summary =
        run_hot_seed_sweep(service.clone(), gbif, force_refresh, Some(pb.clone()), true).await;

    pb.finish_with_message(format!(
        "ready={} refreshed={} failed={}",
        summary.ready, summary.refreshed, summary.failed
    ));

    println!();
    println!("Hot seed complete:");
    println!("  Target rows:      {}", summary.total);
    println!("  Ready now:        {}", summary.ready);
    println!("  Refreshed rows:   {}", summary.refreshed);
    println!("  Failed lookups:   {}", summary.failed);
    println!("  Missing media:    {}", summary.media_missing);
    println!();
    println!("The default taxonomy view and hot species pack should now open from cache.");

    Ok(())
}

fn hot_seed_species_names() -> Vec<&'static str> {
    let mut seen = HashSet::new();
    let mut names = Vec::new();

    for name in std::iter::once(DEFAULT_INITIAL_TAXON)
        .chain(demo::DEMO_SPECIES.iter().copied())
        .chain(curated_animals::CURATED_ANIMAL_SPECIES.iter().copied())
    {
        if seen.insert(name.to_ascii_lowercase()) {
            names.push(name);
        }
    }

    names
}

async fn run_hot_seed_sweep(
    service: Arc<SpeciesService>,
    gbif: Arc<GbifClient>,
    force_refresh: bool,
    progress: Option<indicatif::ProgressBar>,
    persist_version: bool,
) -> HotSeedSummary {
    use futures::{stream, StreamExt};

    let species_names = hot_seed_species_names();
    let mut ready = 0usize;
    let mut to_refresh: Vec<String> = Vec::new();

    if force_refresh {
        to_refresh.extend(species_names.iter().map(|name| (*name).to_string()));
    } else {
        for &requested_name in &species_names {
            let audit = audit_hot_seed_entry(&service, requested_name).await;
            if audit.gaps.is_empty() {
                ready += 1;
            } else {
                to_refresh.push(requested_name.to_string());
            }
        }
    }

    if let Some(pb) = progress.as_ref() {
        pb.set_position(ready as u64);
        pb.set_message(format!(
            "cache hits={} refreshing={}",
            ready,
            to_refresh.len()
        ));
    }

    let refresh_stream = stream::iter(to_refresh.into_iter().map(|requested_name| {
        let service = service.clone();
        let gbif = gbif.clone();
        async move { refresh_hot_seed_entry(service, gbif, requested_name, force_refresh).await }
    }))
    .buffer_unordered(HOT_SEED_CONCURRENCY);

    tokio::pin!(refresh_stream);

    let mut refreshed = 0usize;
    let mut failed = 0usize;
    let mut media_missing = 0usize;

    while let Some(outcome) = refresh_stream.next().await {
        if outcome.failed {
            failed += 1;
            if let Some(pb) = progress.as_ref() {
                pb.set_message(format!("failed {}", outcome.requested_name));
                pb.inc(1);
            }
            continue;
        }

        refreshed += 1;
        if outcome.ready {
            ready += 1;
        }
        if outcome
            .gaps
            .iter()
            .any(|gap| *gap == "local-image" || *gap == "local-map")
        {
            media_missing += 1;
        }

        if let Some(pb) = progress.as_ref() {
            let label = outcome
                .scientific_name
                .as_deref()
                .unwrap_or(&outcome.requested_name);
            pb.set_message(format!(
                "ready={} {}{}",
                ready,
                label,
                if outcome.gaps.is_empty() {
                    ""
                } else {
                    " media pending"
                }
            ));
            pb.inc(1);
        }
    }

    service.flush_cache_writes().await;

    if persist_version {
        if failed == 0 {
            service
                .set_user_stat(HOT_SEED_VERSION_KEY, HOT_SEED_VERSION)
                .await;
        } else {
            service.delete_user_stat(HOT_SEED_VERSION_KEY).await;
        }
    }

    HotSeedSummary {
        total: species_names.len(),
        ready,
        refreshed,
        failed,
        media_missing,
    }
}

async fn audit_hot_seed_entry(service: &SpeciesService, requested_name: &str) -> HotSeedAuditRow {
    match service.get_cached_with_images(requested_name).await {
        Some(cached) => HotSeedAuditRow {
            gaps: hot_seed_gaps(
                &cached.species,
                &CachedMedia {
                    species_image: cached.species_image,
                    map_image: cached.map_image,
                },
            ),
        },
        None => HotSeedAuditRow {
            gaps: vec!["profile"],
        },
    }
}

async fn refresh_hot_seed_entry(
    service: Arc<SpeciesService>,
    gbif: Arc<GbifClient>,
    requested_name: String,
    force_refresh: bool,
) -> HotSeedRefreshOutcome {
    match service
        .lookup_with_options(&requested_name, force_refresh)
        .await
    {
        Ok(species) => {
            let _ = tokio::join!(
                download_species_image(&species, &service),
                download_map_image(&gbif, &species, &service),
            );
            service.flush_cache_writes().await;

            let audit = audit_hot_seed_entry(&service, &requested_name).await;
            HotSeedRefreshOutcome {
                requested_name,
                scientific_name: Some(species.scientific_name),
                ready: audit.gaps.is_empty(),
                failed: false,
                gaps: audit.gaps,
            }
        }
        Err(_) => HotSeedRefreshOutcome {
            requested_name,
            scientific_name: None,
            ready: false,
            failed: true,
            gaps: vec!["profile"],
        },
    }
}

fn hot_seed_gaps(species: &UnifiedSpecies, media: &CachedMedia) -> Vec<&'static str> {
    let mut gaps = Vec::new();

    if species.preferred_image_url().is_some() && media.species_image.is_none() {
        gaps.push("local-image");
    }
    if species.ids.gbif_key.is_some() && media.map_image.is_none() {
        gaps.push("local-map");
    }

    gaps
}

#[derive(Debug)]
struct AnimalAuditRow {
    requested_name: String,
    scientific_name: Option<String>,
    class_name: Option<String>,
    complete: bool,
    gaps: Vec<&'static str>,
}

#[derive(Debug)]
struct AnimalRefreshOutcome {
    requested_name: String,
    scientific_name: Option<String>,
    complete: bool,
    failed: bool,
    gaps: Vec<&'static str>,
}

async fn run_curated_animal_prefetch(
    force_refresh: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use futures::{stream, StreamExt};
    use indicatif::{ProgressBar, ProgressStyle};

    println!("=== Curated Animalia Prefetch ===");
    println!();
    println!(
        "Target: {} complete animals from {} diverse candidates.",
        curated_animals::CURATED_ANIMAL_TARGET,
        curated_animals::CURATED_ANIMAL_SPECIES.len()
    );
    println!("Completeness requires taxonomy, common names, life history, genome data, and local image/map media.");
    println!();

    let service = Arc::new(SpeciesService::new()?);
    let gbif = Arc::new(GbifClient::new());

    let mut complete = 0usize;
    let mut to_refresh = Vec::new();
    if force_refresh {
        to_refresh.extend(curated_animals::CURATED_ANIMAL_SPECIES.iter().copied());
    } else {
        for &requested_name in curated_animals::CURATED_ANIMAL_SPECIES {
            let existing = audit_curated_animal(&service, requested_name).await;
            if existing.complete {
                complete += 1;
            } else {
                to_refresh.push(requested_name);
            }
        }
    }

    let pb = ProgressBar::new(to_refresh.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("#>-"),
    );

    let mut refreshed = 0usize;
    let mut failed = 0usize;

    let refresh_stream = stream::iter(to_refresh.into_iter().map(|requested_name| {
        let service = service.clone();
        let gbif = gbif.clone();
        async move { refresh_curated_animal(service, gbif, requested_name).await }
    }))
    .buffer_unordered(CURATED_ANIMAL_PREFETCH_CONCURRENCY);

    tokio::pin!(refresh_stream);

    while let Some(outcome) = refresh_stream.next().await {
        if outcome.failed {
            failed += 1;
            pb.set_message(format!("failed {}", outcome.requested_name));
        } else {
            refreshed += 1;
            if outcome.complete {
                complete += 1;
            }
            pb.set_message(format!(
                "complete={} {} {}",
                complete,
                outcome
                    .scientific_name
                    .as_deref()
                    .unwrap_or(&outcome.requested_name),
                if outcome.complete {
                    "ready"
                } else {
                    "gaps remain"
                }
            ));
            if !outcome.complete {
                pb.set_message(format!(
                    "complete={} {} gaps={}",
                    complete,
                    outcome
                        .scientific_name
                        .as_deref()
                        .unwrap_or(&outcome.requested_name),
                    outcome.gaps.len()
                ));
            }
        }

        pb.inc(1);
    }

    service.flush_cache_writes().await;
    pb.finish_with_message(format!(
        "complete={} refreshed={} failed={}",
        complete, refreshed, failed
    ));

    println!();
    println!("Prefetch pass complete:");
    println!("  Complete animals: {}", complete);
    println!("  Refreshed rows:    {}", refreshed);
    println!("  Failed lookups:    {}", failed);
    println!();
    println!("Run `ncbi_poketext --audit-animals` for the detailed gap report.");

    Ok(())
}

async fn refresh_curated_animal(
    service: Arc<SpeciesService>,
    gbif: Arc<GbifClient>,
    requested_name: &'static str,
) -> AnimalRefreshOutcome {
    match service.lookup_with_options(requested_name, true).await {
        Ok(species) => {
            let _ = tokio::join!(
                download_species_image(&species, &service),
                download_map_image(&gbif, &species, &service),
            );
            service.flush_cache_writes().await;

            let media = service.get_cached_media(&species).await;
            let gaps = curated_animal_gaps(&species, &media);
            AnimalRefreshOutcome {
                requested_name: requested_name.to_string(),
                scientific_name: Some(species.scientific_name),
                complete: gaps.is_empty(),
                failed: false,
                gaps,
            }
        }
        Err(_) => AnimalRefreshOutcome {
            requested_name: requested_name.to_string(),
            scientific_name: None,
            complete: false,
            failed: true,
            gaps: vec!["profile"],
        },
    }
}

async fn run_curated_animal_audit() -> Result<(), Box<dyn std::error::Error>> {
    let service = SpeciesService::new()?;
    let rows = audit_curated_animals(&service).await;
    print_curated_animal_audit(&rows);
    Ok(())
}

async fn audit_curated_animals(service: &SpeciesService) -> Vec<AnimalAuditRow> {
    let mut rows = Vec::with_capacity(curated_animals::CURATED_ANIMAL_SPECIES.len());
    for &name in curated_animals::CURATED_ANIMAL_SPECIES {
        rows.push(audit_curated_animal(service, name).await);
    }
    rows
}

async fn audit_curated_animal(service: &SpeciesService, requested_name: &str) -> AnimalAuditRow {
    let species = cached_curated_species(service, requested_name).await;
    let media = match species.as_ref() {
        Some(species) => service.get_cached_media(species).await,
        None => CachedMedia::default(),
    };

    let gaps = match species.as_ref() {
        Some(species) => curated_animal_gaps(species, &media),
        None => vec!["profile"],
    };

    AnimalAuditRow {
        requested_name: requested_name.to_string(),
        scientific_name: species
            .as_ref()
            .map(|species| species.scientific_name.clone()),
        class_name: species
            .as_ref()
            .and_then(|species| species.taxonomy.class.clone()),
        complete: gaps.is_empty(),
        gaps,
    }
}

async fn cached_curated_species(
    service: &SpeciesService,
    requested_name: &str,
) -> Option<UnifiedSpecies> {
    if let Some(cached) = service.get_cached_with_images(requested_name).await {
        return Some(cached.species);
    }

    service.get_rich_species(requested_name).await
}

fn curated_animal_gaps(species: &UnifiedSpecies, media: &CachedMedia) -> Vec<&'static str> {
    let mut gaps = Vec::new();

    if species.taxonomy.kingdom.as_deref() != Some("Animalia") {
        gaps.push("animalia");
    }
    if !matches!(
        species.rank.to_ascii_lowercase().as_str(),
        "species" | "subspecies"
    ) {
        gaps.push("species-rank");
    }
    if species.common_names.is_empty() {
        gaps.push("common-name");
    }
    if species.taxonomy.phylum.is_none()
        || species.taxonomy.class.is_none()
        || species.taxonomy.order.is_none()
        || species.taxonomy.family.is_none()
        || species.taxonomy.genus.is_none()
    {
        gaps.push("taxonomy");
    }
    if species.ids.ncbi_tax_id.is_none()
        && species.ids.inat_id.is_none()
        && species.ids.gbif_key.is_none()
        && species.ids.wikidata_id.is_none()
        && species.ids.ensembl_id.is_none()
    {
        gaps.push("external-id");
    }
    if species.description.is_none() && species.wikipedia_extract.is_none() {
        gaps.push("description");
    }
    if !life_history_complete(species) {
        gaps.push("life-history");
    }
    if !genome_data_complete(species) {
        gaps.push("genome");
    }
    if species.images.is_empty() {
        gaps.push("image-metadata");
    }
    if media.species_image.is_none() {
        gaps.push("local-image");
    }
    if species.ids.gbif_key.is_none() {
        gaps.push("gbif-key");
    }
    if media.map_image.is_none() {
        gaps.push("local-map");
    }

    gaps
}

fn life_history_complete(species: &UnifiedSpecies) -> bool {
    let life = &species.life_history;
    life.lifespan_years.is_some()
        && (life.length_meters.is_some() || life.height_meters.is_some())
        && life.mass_kilograms.is_some()
        && !life.reproduction_modes.is_empty()
}

fn genome_data_complete(species: &UnifiedSpecies) -> bool {
    let genome = &species.genome;
    genome.assembly_accession.is_some()
        || genome.assembly_name.is_some()
        || genome.genome_size_bp.is_some()
        || genome.chromosome_count.is_some()
        || genome.scaffold_count.is_some()
        || genome.contig_count.is_some()
        || genome.coding_genes.is_some()
        || genome.noncoding_genes.is_some()
        || genome.mito_genome_size_bp.is_some()
}

fn print_curated_animal_audit(rows: &[AnimalAuditRow]) {
    let complete = rows.iter().filter(|row| row.complete).count();
    let cached_profiles = rows
        .iter()
        .filter(|row| row.scientific_name.is_some())
        .count();
    let classes = rows
        .iter()
        .filter(|row| row.complete)
        .filter_map(|row| row.class_name.as_deref())
        .collect::<BTreeSet<_>>();

    println!("=== Curated Animalia Audit ===");
    println!();
    println!("Candidates:        {:>4}", rows.len());
    println!("Cached profiles:   {:>4}", cached_profiles);
    println!("Complete animals:  {:>4}", complete);
    println!(
        "Target:            {:>4}",
        curated_animals::CURATED_ANIMAL_TARGET
    );
    println!("Complete classes:  {:>4}", classes.len());
    println!();

    println!("Gap counts:");
    for gap in [
        "profile",
        "animalia",
        "species-rank",
        "common-name",
        "taxonomy",
        "external-id",
        "description",
        "life-history",
        "genome",
        "image-metadata",
        "local-image",
        "gbif-key",
        "local-map",
    ] {
        let count = rows.iter().filter(|row| row.gaps.contains(&gap)).count();
        if count > 0 {
            println!("  {:<14} {}", gap, count);
        }
    }

    println!();
    println!("Complete rows:");
    for row in rows.iter().filter(|row| row.complete).take(120) {
        println!(
            "  - {} ({})",
            row.scientific_name
                .as_deref()
                .unwrap_or(&row.requested_name),
            row.class_name.as_deref().unwrap_or("unknown class")
        );
    }

    let incomplete = rows.iter().filter(|row| !row.complete).take(40);
    println!();
    println!("First incomplete rows:");
    for row in incomplete {
        println!(
            "  - {}: {}",
            row.scientific_name
                .as_deref()
                .unwrap_or(&row.requested_name),
            row.gaps.join(", ")
        );
    }
}

async fn run_cache_all_rich() -> Result<(), Box<dyn std::error::Error>> {
    use futures::{stream, StreamExt};
    use indicatif::{ProgressBar, ProgressStyle};

    println!("=== Durable Rich Species Cache Sweep ===");
    println!();
    println!("This sweep is resumable and writes into the non-expiring rich cache.");
    println!("It will take a very long time for the full species set.");
    println!();

    let service = Arc::new(SpeciesService::new()?);

    if !service.has_offline_search().await {
        return Err("GBIF backbone is required before running --cache-all-rich".into());
    }

    let total_species = service.species_rank_count().await;
    let mut last_key = service
        .get_user_stat(RICH_CACHE_LAST_KEY)
        .await
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let mut processed = service
        .get_user_stat(RICH_CACHE_PROCESSED)
        .await
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let mut enriched = service
        .get_user_stat(RICH_CACHE_ENRICHED)
        .await
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let mut fallback = service
        .get_user_stat(RICH_CACHE_FALLBACK)
        .await
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);

    let pb = ProgressBar::new(total_species);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
        .unwrap()
        .progress_chars("#>-"));
    pb.set_position(processed.min(total_species));
    pb.set_message(format!(
        "resume key={last_key} enriched={enriched} local_fallback={fallback}"
    ));

    loop {
        let batch = service
            .get_species_batch_after(last_key, RICH_CACHE_BATCH_SIZE)
            .await;
        if batch.is_empty() {
            break;
        }

        let batch_stream = stream::iter(batch.into_iter().map(|taxon| {
            let svc = service.clone();
            async move {
                let name = taxon.scientific_name.clone();
                match svc.lookup(&name).await {
                    Ok(species) => {
                        svc.cache_rich_species_detached(species);
                        (taxon.gbif_key, name, true)
                    }
                    Err(_) => {
                        svc.cache_rich_species_detached(build_local_species_profile(&taxon));
                        (taxon.gbif_key, name, false)
                    }
                }
            }
        }))
        .buffered(RICH_CACHE_CONCURRENCY);

        tokio::pin!(batch_stream);

        while let Some((gbif_key, name, was_enriched)) = batch_stream.next().await {
            last_key = gbif_key;
            processed += 1;
            if was_enriched {
                enriched += 1;
            } else {
                fallback += 1;
            }

            pb.inc(1);
            pb.set_message(format!(
                "{} | enriched={} local_fallback={}",
                name, enriched, fallback
            ));
        }

        service
            .set_user_stat(RICH_CACHE_LAST_KEY, last_key.to_string())
            .await;
        service
            .set_user_stat(RICH_CACHE_PROCESSED, processed.to_string())
            .await;
        service
            .set_user_stat(RICH_CACHE_ENRICHED, enriched.to_string())
            .await;
        service
            .set_user_stat(RICH_CACHE_FALLBACK, fallback.to_string())
            .await;
    }

    pb.finish_with_message(format!(
        "Rich cache sweep complete: enriched={} local_fallback={}",
        enriched, fallback
    ));

    let stats = LocalDatabase::open()?.get_stats()?;
    println!();
    println!("Rich cache rows:     {:>10}", stats.rich_species_count);
    println!("Hot cache rows:      {:>10}", stats.species_count);
    println!("Species in backbone: {:>10}", total_species);

    Ok(())
}

fn show_database_stats() -> Result<(), Box<dyn std::error::Error>> {
    let db = LocalDatabase::open()?;
    let stats = db.get_stats()?;
    let ncbi_count = db.ncbi_taxonomy_count().unwrap_or(0);

    println!("=== Local Database Statistics ===");
    println!();
    println!("Species cached:      {:>10}", stats.species_count);
    println!("Rich species cached: {:>10}", stats.rich_species_count);
    println!("GBIF taxa:           {:>10}", stats.taxon_names_count);
    println!("NCBI taxonomy:       {:>10}", ncbi_count);
    println!("Images cached:       {:>10}", stats.images_count);
    println!(
        "Database size:       {:>10}",
        format_bytes(stats.total_size_bytes)
    );
    println!();

    if stats.taxon_names_count > 100_000 && ncbi_count > 100_000 {
        println!("Offline mode:        FULL (maximum speed)");
    } else if stats.taxon_names_count > 100_000 {
        println!("Offline search:      AVAILABLE");
        println!("Common names:        NOT AVAILABLE");
        println!("  Run: ncbi_poketext --import-all");
    } else {
        println!("Offline search:      NOT AVAILABLE");
        println!("  Run: ncbi_poketext --import-all");
    }

    // Show recent history
    if let Ok(history) = db.get_recent_history(5) {
        if !history.is_empty() {
            println!();
            println!("Recent species viewed:");
            for (name, _timestamp) in history {
                println!("  - {}", name);
            }
        }
    }

    // Show favorites
    if let Ok(favorites) = db.get_favorites() {
        if !favorites.is_empty() {
            println!();
            println!("Favorites:");
            for (name, _added, _notes) in favorites {
                println!("  ★ {}", name);
            }
        }
    }

    Ok(())
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.2} GB", bytes as f64 / 1_000_000_000.0)
    } else if bytes >= 1_000_000 {
        format!("{:.2} MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.2} KB", bytes as f64 / 1_000.0)
    } else {
        format!("{} bytes", bytes)
    }
}

async fn run_text_mode(
    service: &SpeciesService,
    species_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Loading {}...", species_name);
    let lookup_span = crate::perf::start_span();
    match service.lookup(species_name).await {
        Ok(species) => {
            crate::perf::log_elapsed("text.lookup_total", lookup_span);
            print_text_output(&species);
            Ok(())
        }
        Err(e) => {
            crate::perf::log_elapsed("text.lookup_total", lookup_span);
            eprintln!("Error: Species not found: {}", species_name);
            eprintln!("{}", e);
            Ok(())
        }
    }
}

async fn run_tui_mode(
    service: Arc<SpeciesService>,
    gbif: Arc<GbifClient>,
    initial_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let startup_span = crate::perf::start_span();

    // Initial load with simple loading message
    println!("Loading {}...", initial_name);

    let lookup_span = crate::perf::start_span();
    let (species, species_image, map_image) =
        if let Some(cached) = service.get_cached_with_images(initial_name).await {
            let species = cached.species;
            let (species_image, map_image) = decode_cached_media(
                &service,
                &species,
                CachedMedia {
                    species_image: cached.species_image,
                    map_image: cached.map_image,
                },
            )
            .await;
            crate::perf::log_value("tui.startup.cached_species_hit", &species.scientific_name);
            crate::perf::log_elapsed("tui.lookup_total", lookup_span);
            (species, species_image, map_image)
        } else {
            let species = match service.lookup(initial_name).await {
                Ok(s) => s,
                Err(e) => {
                    crate::perf::log_elapsed("tui.lookup_total", lookup_span);
                    eprintln!("Error: Species not found: {}", initial_name);
                    eprintln!("{}", e);
                    return Ok(());
                }
            };

            crate::perf::log_elapsed("tui.lookup_total", lookup_span);
            let media_span = crate::perf::start_span();
            let (species_image, map_image) = load_cached_media(&service, &species).await;
            crate::perf::log_elapsed("tui.initial_media_cache", media_span);
            (species, species_image, map_image)
        };

    let meta_span = crate::perf::start_span();
    let (is_favorite, has_offline_search) = tokio::join!(
        service.is_favorite(&species.scientific_name),
        service.has_offline_search(),
    );
    crate::perf::log_elapsed("tui.startup_meta", meta_span);

    spawn_hot_seed_background(service.clone(), gbif.clone());

    // Create channel for TUI updates
    let (update_tx, update_rx) = mpsc::channel::<TuiUpdate>(32);

    // Run the TUI event loop
    let result = tui::run_tui_loop(
        tui::TuiBootstrap {
            species,
            species_image,
            map_image,
            is_favorite,
            has_offline_search,
        },
        tui::TuiRuntime {
            update_tx,
            update_rx,
            service,
            gbif,
        },
    )
    .await;
    crate::perf::log_elapsed("tui.startup_total", startup_span);
    result
}

fn spawn_hot_seed_background(service: Arc<SpeciesService>, gbif: Arc<GbifClient>) {
    tokio::spawn(async move {
        if service.get_user_stat(HOT_SEED_VERSION_KEY).await.as_deref() == Some(HOT_SEED_VERSION) {
            return;
        }

        let summary = run_hot_seed_sweep(service, gbif, false, None, true).await;
        crate::perf::log_value("hot_seed.ready", summary.ready);
        crate::perf::log_value("hot_seed.refreshed", summary.refreshed);
        crate::perf::log_value("hot_seed.failed", summary.failed);
    });
}

pub async fn load_cached_media(
    service: &SpeciesService,
    species: &UnifiedSpecies,
) -> (Option<image::DynamicImage>, Option<image::DynamicImage>) {
    let cache_span = crate::perf::start_span();
    let cached = service.get_cached_media(species).await;
    let decoded = decode_cached_media(service, species, cached).await;
    crate::perf::log_elapsed("image.cached_media_lookup", cache_span);
    decoded
}

pub async fn decode_cached_media(
    service: &SpeciesService,
    species: &UnifiedSpecies,
    cached: CachedMedia,
) -> (Option<image::DynamicImage>, Option<image::DynamicImage>) {
    let species_image = cached
        .species_image
        .and_then(|data| image::load_from_memory(&data).ok());

    let map_image = match cached.map_image {
        Some(data) => match image::load_from_memory(&data).ok() {
            Some(img) if image_has_transparency(&img) => {
                if let Some(gbif_key) = species.ids.gbif_key {
                    service.invalidate_map_image(gbif_key).await;
                    crate::perf::log_value("image.map.cache_rejected", gbif_key);
                }
                None
            }
            Some(img) => Some(world_map::normalize_for_tui(&img)),
            None => {
                if let Some(gbif_key) = species.ids.gbif_key {
                    service.invalidate_map_image(gbif_key).await;
                }
                None
            }
        },
        None => None,
    };

    (species_image, map_image)
}

/// Download species image asynchronously and cache it
pub async fn download_species_image(
    species: &UnifiedSpecies,
    service: &SpeciesService,
) -> Option<image::DynamicImage> {
    let image_span = crate::perf::start_span();

    // Check local cache first
    let cached = service.get_cached_media(species).await;
    if let Some(data) = cached.species_image {
        if let Ok(img) = image::load_from_memory(&data) {
            crate::perf::log_elapsed("image.species.cache_hit", image_span);
            return Some(img);
        }
    }

    // Download from URL
    let url = species.preferred_image_url()?;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
    let response = client
        .get(url)
        .header("User-Agent", "ncbi_poketext/0.1")
        .send()
        .await
        .ok()?;

    let bytes = response.bytes().await.ok()?.to_vec();
    let image = image::load_from_memory(&bytes).ok();
    service.cache_species_image_detached(species, bytes);
    crate::perf::log_elapsed("image.species.download", image_span);
    image
}

/// Download GBIF distribution map image and cache it
pub async fn download_map_image(
    gbif: &GbifClient,
    species: &UnifiedSpecies,
    service: &SpeciesService,
) -> Option<image::DynamicImage> {
    download_map_image_with_options(gbif, species, service, false).await
}

/// Download GBIF distribution map image with optional force refresh
pub async fn download_map_image_with_options(
    gbif: &GbifClient,
    species: &UnifiedSpecies,
    service: &SpeciesService,
    force_refresh: bool,
) -> Option<image::DynamicImage> {
    let map_span = crate::perf::start_span();
    let gbif_key = species.ids.gbif_key?;

    // Check local cache first (unless forcing refresh)
    if !force_refresh {
        let cached = service.get_cached_media(species).await;
        if let Some(data) = cached.map_image {
            if let Ok(img) = image::load_from_memory(&data) {
                if image_has_transparency(&img) {
                    service.invalidate_map_image(gbif_key).await;
                    crate::perf::log_value("image.map.cache_rejected", gbif_key);
                } else {
                    crate::perf::log_elapsed("image.map.cache_hit", map_span);
                    return Some(world_map::normalize_for_tui(&img));
                }
            } else {
                service.invalidate_map_image(gbif_key).await;
            }
        }
    }

    // Download occurrence layer from GBIF
    let bytes = gbif.get_map_image(gbif_key).await.ok()?;

    // Load the occurrence layer (transparent PNG with dots)
    let occurrence_layer = image::load_from_memory(&bytes).ok()?;

    // Composite onto world map background
    let composited = world_map::composite_with_background(&occurrence_layer);
    let presentation_map = world_map::normalize_for_tui(&composited);

    // Encode composited image for caching
    let mut cache_bytes: Vec<u8> = Vec::new();
    if presentation_map
        .write_to(
            &mut std::io::Cursor::new(&mut cache_bytes),
            image::ImageFormat::Png,
        )
        .is_err()
    {
        // If encoding fails, cache the original
        service.cache_map_image_detached(gbif_key, bytes.to_vec());
        crate::perf::log_elapsed("image.map.download", map_span);
        return Some(presentation_map);
    }

    // Cache the composited image
    service.cache_map_image_detached(gbif_key, cache_bytes);

    crate::perf::log_elapsed("image.map.download", map_span);
    Some(presentation_map)
}

fn image_has_transparency(image: &image::DynamicImage) -> bool {
    image
        .as_rgba8()
        .map(|rgba| rgba.pixels().any(|pixel| pixel.0[3] < 250))
        .unwrap_or(false)
}

fn print_text_output(species: &UnifiedSpecies) {
    println!("=== {} ===", species.scientific_name);

    if !species.common_names.is_empty() {
        println!("Common names: {}", species.common_names.join(", "));
    }

    println!("\n--- Taxonomy ---");
    if let Some(ref k) = species.taxonomy.kingdom {
        println!("Kingdom: {}", k);
    }
    if let Some(ref p) = species.taxonomy.phylum {
        println!("Phylum: {}", p);
    }
    if let Some(ref c) = species.taxonomy.class {
        println!("Class: {}", c);
    }
    if let Some(ref o) = species.taxonomy.order {
        println!("Order: {}", o);
    }
    if let Some(ref f) = species.taxonomy.family {
        println!("Family: {}", f);
    }
    if let Some(ref g) = species.taxonomy.genus {
        println!("Genus: {}", g);
    }

    if species.life_history.has_any_stats() {
        println!("\n--- Life History ---");
        if let Some(years) = species.life_history.lifespan_years {
            println!("Lifespan: {}", format_lifespan(years));
        }
        if let Some(length) = species.life_history.length_meters {
            println!("Length: {}", format_length(length));
        }
        if let Some(height) = species.life_history.height_meters {
            println!("Height: {}", format_length(height));
        }
        if let Some(mass) = species.life_history.mass_kilograms {
            println!("Mass: {}", format_mass(mass));
        }
        if !species.life_history.reproduction_modes.is_empty() {
            println!(
                "Reproduction: {}",
                species.life_history.reproduction_modes.join(", ")
            );
        }
    }

    println!("\n--- Genome ---");
    let g = &species.genome;
    if let Some(size) = g.genome_size_bp {
        println!("Size: {:.2} Gb", size as f64 / 1_000_000_000.0);
    }
    if let Some(chr) = g.chromosome_count {
        println!("Chromosomes: {}", chr);
    }
    if let Some(mito) = g.mito_genome_size_bp {
        println!("Mito: {:.1} kb", mito as f64 / 1000.0);
    }

    if let Some(ref desc) = species.description {
        println!("\n{}", desc);
    }
}

fn format_lifespan(years: f64) -> String {
    if years >= 1.0 {
        format!("{} years", format_measurement_value(years))
    } else if years * 12.0 >= 1.0 {
        format!("{} months", format_measurement_value(years * 12.0))
    } else {
        format!("{} days", format_measurement_value(years * 365.25))
    }
}

fn format_length(meters: f64) -> String {
    if meters >= 1.0 {
        format!("{} m", format_measurement_value(meters))
    } else if meters >= 0.01 {
        format!("{} cm", format_measurement_value(meters * 100.0))
    } else {
        format!("{} mm", format_measurement_value(meters * 1_000.0))
    }
}

fn format_mass(kilograms: f64) -> String {
    if kilograms >= 1_000.0 {
        format!("{} t", format_measurement_value(kilograms / 1_000.0))
    } else if kilograms >= 1.0 {
        format!("{} kg", format_measurement_value(kilograms))
    } else {
        format!("{} g", format_measurement_value(kilograms * 1_000.0))
    }
}

fn format_measurement_value(value: f64) -> String {
    if value >= 10.0 || (value.round() - value).abs() < 0.05 {
        format!("{:.0}", value)
    } else {
        format!("{:.1}", value)
    }
}
