//! GBIF Backbone Taxonomy Importer
//!
//! Downloads and imports the GBIF backbone taxonomy for offline search.
//! Uses rayon for parallel processing and batched inserts for performance.

use crate::local_db::{LocalDatabase, TaxonName};
use flate2::read::GzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::io::{BufRead, BufReader, Read};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const BACKBONE_URL: &str =
    "https://hosted-datasets.gbif.org/datasets/backbone/current/simple.txt.gz";
const BATCH_SIZE: usize = 50_000;
const PARSE_CHUNK_SIZE: usize = 10_000;

/// Download and import the GBIF backbone taxonomy
pub async fn import_backbone(
    db: &LocalDatabase,
    _progress_callback: Option<impl Fn(u64, u64)>,
) -> Result<u64, Box<dyn std::error::Error>> {
    println!("Downloading GBIF Backbone Taxonomy...");
    println!("This is a one-time download of ~200MB that enables offline search.");
    println!();

    // Download the file
    let client = reqwest::Client::new();
    let response = client
        .get(BACKBONE_URL)
        .header("User-Agent", "biodex/0.1")
        .send()
        .await?;

    let total_size = response.content_length().unwrap_or(200_000_000);

    let pb = ProgressBar::new(total_size);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
        .unwrap()
        .progress_chars("#>-"));

    // Download to memory
    let bytes = response.bytes().await?;
    pb.finish_with_message("Download complete");

    println!(
        "Decompressing and importing using {} CPU cores...",
        num_cpus::get()
    );

    // Decompress
    let decoder = GzDecoder::new(&bytes[..]);
    let reader = BufReader::with_capacity(1024 * 1024, decoder); // 1MB buffer

    // Parse and import
    let count = import_from_reader_parallel(db, reader)?;

    println!("Imported {} taxa for offline search", count);

    Ok(count)
}

/// Import backbone data from a reader using parallel processing
pub fn import_from_reader_parallel<R: Read>(
    db: &LocalDatabase,
    reader: BufReader<R>,
) -> Result<u64, Box<dyn std::error::Error>> {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );

    let total_imported = Arc::new(AtomicU64::new(0));
    let lines_processed = Arc::new(AtomicU64::new(0));

    // Collect lines in chunks for parallel processing
    let mut line_buffer: Vec<String> = Vec::with_capacity(PARSE_CHUNK_SIZE);
    let mut batch_buffer: Vec<TaxonName> = Vec::with_capacity(BATCH_SIZE);

    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => continue,
        };

        // Skip empty lines
        if line.is_empty() {
            continue;
        }

        line_buffer.push(line);

        // Process chunk in parallel when buffer is full
        if line_buffer.len() >= PARSE_CHUNK_SIZE {
            let parsed: Vec<TaxonName> = line_buffer
                .par_iter()
                .filter_map(|line| parse_backbone_line(line))
                .filter(|taxon| is_valid_rank(&taxon.rank))
                .collect();

            lines_processed.fetch_add(line_buffer.len() as u64, Ordering::Relaxed);
            line_buffer.clear();

            batch_buffer.extend(parsed);

            // Insert batch when full
            if batch_buffer.len() >= BATCH_SIZE {
                db.insert_taxon_names_batch(&batch_buffer)?;
                let count = total_imported.fetch_add(batch_buffer.len() as u64, Ordering::Relaxed);
                pb.set_message(format!(
                    "Imported {} taxa...",
                    count + batch_buffer.len() as u64
                ));
                batch_buffer.clear();
            }
        }
    }

    // Process remaining lines
    if !line_buffer.is_empty() {
        let parsed: Vec<TaxonName> = line_buffer
            .par_iter()
            .filter_map(|line| parse_backbone_line(line))
            .filter(|taxon| is_valid_rank(&taxon.rank))
            .collect();

        batch_buffer.extend(parsed);
    }

    // Insert remaining batch
    if !batch_buffer.is_empty() {
        db.insert_taxon_names_batch(&batch_buffer)?;
        total_imported.fetch_add(batch_buffer.len() as u64, Ordering::Relaxed);
    }

    let final_count = total_imported.load(Ordering::Relaxed);
    pb.finish_with_message(format!("Imported {} taxa", final_count));

    Ok(final_count)
}

/// Check if rank should be included (species and higher)
fn is_valid_rank(rank: &str) -> bool {
    matches!(
        rank.to_uppercase().as_str(),
        "SPECIES" | "GENUS" | "FAMILY" | "ORDER" | "CLASS" | "PHYLUM" | "KINGDOM"
    )
}

fn parse_backbone_line(line: &str) -> Option<TaxonName> {
    let fields: Vec<&str> = line.split('\t').collect();

    // GBIF simple backbone format (no header):
    // 0: taxonID (GBIF key)
    // 1: parentKey
    // 2: acceptedKey
    // 3: isSynonym (t/f)
    // 4: status (ACCEPTED, SYNONYM, DOUBTFUL)
    // 5: rank (GENUS, SPECIES, etc.)
    // ...
    // 18: scientificName
    // 19: canonicalName (genus)
    // 20: genericName
    // 21: specificEpithet

    if fields.len() < 20 {
        return None;
    }

    let gbif_key: u64 = fields.first()?.parse().ok()?;
    let status = fields.get(4)?;
    let rank = fields.get(5)?.to_string();
    let scientific_name = fields.get(18)?.to_string();
    let canonical_name = fields
        .get(19)
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty() && *s != "\\N");

    // Skip if no valid name
    if scientific_name.is_empty() || scientific_name == "\\N" {
        return None;
    }

    // Skip synonyms - only keep ACCEPTED and DOUBTFUL
    if *status != "ACCEPTED" && *status != "DOUBTFUL" {
        return None;
    }

    // Extract genus from canonical name or field 20
    let genus = fields
        .get(20)
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty() && *s != "\\N");

    Some(TaxonName {
        gbif_key,
        scientific_name,
        canonical_name,
        rank,
        kingdom: None, // Not in simple format
        phylum: None,
        class: None,
        order: None,
        family: None,
        genus,
    })
}
