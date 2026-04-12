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
use std::collections::BTreeSet;
use std::env;
use std::sync::Arc;
use tokio::sync::mpsc;
use tui::TuiUpdate;

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
    if prefetch_mode {
        return run_bulk_prefetch().await;
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
        "Panthera leo".to_string()
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
    println!("    -p, --prefetch          Bulk prefetch popular species for instant access");
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
    println!("    ncbi_poketext                    # Default: Panthera leo");
    println!("    ncbi_poketext \"Homo sapiens\"     # Search for humans");
    println!("    ncbi_poketext --import-backbone  # Download offline search data");
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

async fn run_bulk_prefetch() -> Result<(), Box<dyn std::error::Error>> {
    use indicatif::{ProgressBar, ProgressStyle};

    println!("=== Bulk Species Prefetch ===");
    println!();
    println!("This will cache popular species for instant access.");
    println!("The process fetches data from multiple APIs and may take a while.");
    println!();

    let service = Arc::new(SpeciesService::new()?);
    let gbif = Arc::new(GbifClient::new());

    // Popular species across different taxonomic groups
    let popular_species = demo::DEMO_SPECIES;

    let total = popular_species.len();
    let pb = ProgressBar::new(total as u64);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
        .unwrap()
        .progress_chars("#>-"));

    let mut cached = 0;
    let mut failed = 0;

    // Process in batches of 3 for parallel fetching (respects API rate limits)
    for chunk in popular_species.chunks(3) {
        let mut handles = Vec::new();

        for &species_name in chunk {
            let svc = service.clone();
            let gb = gbif.clone();
            let name = species_name.to_string();
            handles.push(tokio::spawn(async move {
                match svc.lookup(&name).await {
                    Ok(species) => {
                        let _ = tokio::join!(
                            download_species_image(&species, &svc),
                            download_map_image(&gb, &species, &svc),
                        );
                        true
                    }
                    Err(_) => false,
                }
            }));
        }

        // Wait for batch to complete
        for (i, handle) in handles.into_iter().enumerate() {
            if let Ok(success) = handle.await {
                if success {
                    cached += 1;
                } else {
                    failed += 1;
                }
            }
            pb.set_message(chunk.get(i).unwrap_or(&"").to_string());
            pb.inc(1);
        }

        // Small delay between batches to respect rate limits
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    pb.finish_with_message("Done!");

    println!();
    println!("Prefetch complete:");
    println!("  Cached: {} species", cached);
    println!("  Failed: {} species", failed);
    println!();
    println!("These species and any available media will now load from cache in the TUI.");

    Ok(())
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
    let species = match service.lookup(initial_name).await {
        Ok(s) => {
            crate::perf::log_elapsed("tui.lookup_total", lookup_span);
            s
        }
        Err(e) => {
            crate::perf::log_elapsed("tui.lookup_total", lookup_span);
            eprintln!("Error: Species not found: {}", initial_name);
            eprintln!("{}", e);
            return Ok(());
        }
    };

    // Start from local media cache so the TUI can open before network fetches finish.
    let media_span = crate::perf::start_span();
    let ((species_image, map_image), is_favorite, has_offline_search) = tokio::join!(
        load_cached_media(&service, &species),
        service.is_favorite(&species.scientific_name),
        service.has_offline_search(),
    );
    crate::perf::log_elapsed("tui.initial_media_cache", media_span);

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

pub async fn load_cached_media(
    service: &SpeciesService,
    species: &UnifiedSpecies,
) -> (Option<image::DynamicImage>, Option<image::DynamicImage>) {
    let cache_span = crate::perf::start_span();
    let cached = service.get_cached_media(species).await;

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

    crate::perf::log_elapsed("image.cached_media_lookup", cache_span);
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
