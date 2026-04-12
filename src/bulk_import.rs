//! Bulk import of taxonomy databases for maximum offline speed
//!
//! Downloads and imports NCBI taxonomy dump for comprehensive offline access.

use crate::local_db::LocalDatabase;
use flate2::read::GzDecoder;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read};
use tar::Archive;

const NCBI_TAXDUMP_URL: &str = "https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz";

/// Download and import NCBI taxonomy for comprehensive offline access
pub async fn import_ncbi_taxonomy(db: &LocalDatabase) -> Result<u64, Box<dyn std::error::Error>> {
    println!("Downloading NCBI Taxonomy Database...");
    println!("This provides common names and detailed lineage for all species.");
    println!();

    // Download the taxdump
    let client = reqwest::Client::new();
    let response = client
        .get(NCBI_TAXDUMP_URL)
        .header("User-Agent", "biodex/0.1")
        .send()
        .await?;

    let total_size = response.content_length().unwrap_or(60_000_000);

    let pb = ProgressBar::new(total_size);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
        .unwrap()
        .progress_chars("#>-"));

    let bytes = response.bytes().await?;
    pb.finish_with_message("Download complete");

    println!("Extracting and importing taxonomy data...");

    // Decompress and extract tar
    let decoder = GzDecoder::new(&bytes[..]);
    let mut archive = Archive::new(decoder);

    let mut names_data: Option<Vec<u8>> = None;
    let mut nodes_data: Option<Vec<u8>> = None;

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.to_string_lossy().to_string();

        if path == "names.dmp" {
            let mut data = Vec::new();
            entry.read_to_end(&mut data)?;
            names_data = Some(data);
        } else if path == "nodes.dmp" {
            let mut data = Vec::new();
            entry.read_to_end(&mut data)?;
            nodes_data = Some(data);
        }
    }

    let names_data = names_data.ok_or("names.dmp not found in archive")?;
    let nodes_data = nodes_data.ok_or("nodes.dmp not found in archive")?;

    // Parse nodes.dmp to get rank info
    println!("Parsing taxonomy nodes...");
    let nodes = parse_nodes(&nodes_data)?;

    // Parse names.dmp and import
    println!("Importing taxonomy names...");
    let count = import_names(db, &names_data, &nodes)?;

    println!("Imported {} taxonomy entries with common names", count);

    Ok(count)
}

/// Taxonomy node info from nodes.dmp
struct TaxonNode {
    rank: String,
}

/// Parse nodes.dmp file
fn parse_nodes(data: &[u8]) -> Result<HashMap<u64, TaxonNode>, Box<dyn std::error::Error>> {
    let reader = BufReader::new(data);
    let mut nodes = HashMap::new();

    for line in reader.lines() {
        let line = line?;
        let fields: Vec<&str> = line.split("\t|\t").collect();

        if fields.len() >= 3 {
            if let Ok(tax_id) = fields[0].trim().parse::<u64>() {
                let rank = fields[2].trim().to_string();
                nodes.insert(tax_id, TaxonNode { rank });
            }
        }
    }

    Ok(nodes)
}

/// Parse names.dmp and import into database
fn import_names(
    db: &LocalDatabase,
    data: &[u8],
    nodes: &HashMap<u64, TaxonNode>,
) -> Result<u64, Box<dyn std::error::Error>> {
    let reader = BufReader::new(data);

    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );

    let mut entries: HashMap<u64, NcbiTaxonEntry> = HashMap::new();

    // First pass: collect all names
    for line in reader.lines() {
        let line = line?;
        let fields: Vec<&str> = line.split("\t|\t").collect();

        if fields.len() >= 4 {
            if let Ok(tax_id) = fields[0].trim().parse::<u64>() {
                let name = fields[1].trim().to_string();
                let name_class = fields[3].trim().trim_end_matches("\t|").to_string();

                let entry = entries.entry(tax_id).or_insert_with(|| NcbiTaxonEntry {
                    tax_id,
                    scientific_name: String::new(),
                    common_name: None,
                    rank: nodes
                        .get(&tax_id)
                        .map(|n| n.rank.clone())
                        .unwrap_or_default(),
                });

                match name_class.as_str() {
                    "scientific name" => entry.scientific_name = name,
                    "genbank common name" | "common name" => {
                        if entry.common_name.is_none() {
                            entry.common_name = Some(name);
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    // Import in batches
    let entries: Vec<_> = entries
        .into_values()
        .filter(|e| !e.scientific_name.is_empty())
        .collect();

    let total = entries.len();
    pb.set_message(format!("Importing {} entries...", total));

    let batch_size = 50_000;
    let mut imported = 0u64;

    for chunk in entries.chunks(batch_size) {
        db.insert_ncbi_taxonomy_batch(chunk)?;
        imported += chunk.len() as u64;
        pb.set_message(format!("Imported {}/{} entries...", imported, total));
    }

    pb.finish_with_message(format!("Imported {} entries", imported));

    Ok(imported)
}

/// NCBI taxonomy entry for import
pub struct NcbiTaxonEntry {
    pub tax_id: u64,
    pub scientific_name: String,
    pub common_name: Option<String>,
    pub rank: String,
}
