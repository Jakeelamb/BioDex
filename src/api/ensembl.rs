//! Ensembl REST API client for genome and gene data
//!
//! Documentation: https://rest.ensembl.org/

use super::{ApiError, Result};
use serde::Deserialize;

const BASE_URL: &str = "https://rest.ensembl.org";

pub struct EnsemblClient {
    client: reqwest::Client,
}

/// Species genome information from Ensembl
#[derive(Debug, Clone, Default)]
pub struct EnsemblGenomeInfo {
    /// Species name
    pub species: String,
    /// Display name
    pub display_name: String,
    /// Assembly name (e.g., GRCh38)
    pub assembly_name: Option<String>,
    /// Assembly accession (e.g., GCA_000001405.28)
    pub assembly_accession: Option<String>,
    /// Ensembl database version
    pub db_version: Option<String>,
    /// Genebuild version/date
    pub genebuild: Option<String>,
    /// Taxonomy ID
    pub taxonomy_id: Option<u64>,
    /// Total base pairs
    pub base_pairs: Option<u64>,
    /// Number of coding genes
    pub coding_genes: Option<u64>,
    /// Number of non-coding genes
    pub noncoding_genes: Option<u64>,
    /// Number of pseudogenes
    pub pseudogenes: Option<u64>,
    /// Golden path length (total assembled length)
    pub golden_path: Option<u64>,
    /// Has genome alignments
    pub has_genome_alignments: bool,
    /// Has variation data
    pub has_variations: bool,
}

#[derive(Debug, Deserialize)]
struct AssemblyInfo {
    assembly_name: Option<String>,
    assembly_accession: Option<String>,
    genebuild_last_geneset_update: Option<String>,
    golden_path: Option<u64>,
    #[serde(default)]
    top_level_region: Vec<TopLevelRegion>,
}

#[derive(Debug, Deserialize)]
struct TopLevelRegion {
    length: u64,
    coord_system: String,
}

#[derive(Debug, Deserialize)]
struct InfoData {
    #[serde(default)]
    species: Vec<SpeciesData>,
}

#[derive(Debug, Deserialize)]
struct SpeciesData {
    name: String,
    display_name: Option<String>,
}

impl EnsemblClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        }
    }

    /// Search for a species by name and get genome info
    pub async fn get_genome_info(&self, species_name: &str) -> Result<EnsemblGenomeInfo> {
        // First, find the species in Ensembl's database
        let species_id = self.find_species(species_name).await?;

        // Get detailed assembly info
        let url = format!(
            "{}/info/assembly/{}?content-type=application/json",
            BASE_URL, species_id
        );

        let response = self
            .client
            .get(&url)
            .header("Accept", "application/json")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(ApiError::NotFound(species_name.to_string()));
        }

        let assembly: AssemblyInfo = response.json().await?;

        // Ensembl commonly reports the assembled span directly as golden_path.
        // Fall back to summing assembled top-level regions when it is absent.
        let total_bp = assembled_base_pairs(&assembly);

        // Get gene statistics
        let gene_stats = self.get_gene_stats(&species_id).await.unwrap_or_default();

        Ok(EnsemblGenomeInfo {
            species: species_id.clone(),
            display_name: species_id.replace('_', " "),
            assembly_name: assembly.assembly_name,
            assembly_accession: assembly.assembly_accession,
            db_version: None,
            genebuild: assembly.genebuild_last_geneset_update,
            taxonomy_id: None,
            base_pairs: total_bp,
            coding_genes: gene_stats.0,
            noncoding_genes: gene_stats.1,
            pseudogenes: gene_stats.2,
            golden_path: total_bp,
            has_genome_alignments: false,
            has_variations: false,
        })
    }

    /// Find the Ensembl species identifier for a given name
    async fn find_species(&self, name: &str) -> Result<String> {
        let url = format!("{}/info/species?content-type=application/json", BASE_URL);

        let response = self
            .client
            .get(&url)
            .header("Accept", "application/json")
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(ApiError::Api("Failed to get species list".to_string()));
        }

        let info: InfoData = response.json().await?;

        // Search for matching species
        let name_lower = name.to_lowercase();
        let name_parts: Vec<&str> = name_lower.split_whitespace().collect();

        for species in info.species {
            // Check exact name match
            if species.name.to_lowercase() == name_lower.replace(' ', "_") {
                return Ok(species.name);
            }

            // Check display name
            if let Some(ref display) = species.display_name {
                if display.to_lowercase() == name_lower {
                    return Ok(species.name);
                }
            }

            // Check if species name contains all parts of search query
            let species_lower = species.name.to_lowercase();
            if name_parts.len() >= 2 {
                let genus = name_parts[0];
                let species_part = name_parts[1];
                if species_lower.starts_with(genus) && species_lower.contains(species_part) {
                    return Ok(species.name);
                }
            }
        }

        Err(ApiError::NotFound(name.to_string()))
    }

    /// Get gene statistics for a species
    async fn get_gene_stats(
        &self,
        species_id: &str,
    ) -> Result<(Option<u64>, Option<u64>, Option<u64>)> {
        let url = format!(
            "{}/info/data/{}?content-type=application/json",
            BASE_URL, species_id
        );

        let response = self
            .client
            .get(&url)
            .header("Accept", "application/json")
            .send()
            .await?;

        if !response.status().is_success() {
            return Ok((None, None, None));
        }

        // Parse the response - structure varies by species
        let data: serde_json::Value = response.json().await?;

        // Try to extract gene counts from various possible locations
        let coding = data
            .get("coding_cnt")
            .or_else(|| data.get("core").and_then(|c| c.get("coding_cnt")))
            .and_then(|v| v.as_u64());

        let noncoding = data
            .get("noncoding_cnt")
            .or_else(|| data.get("core").and_then(|c| c.get("noncoding_cnt")))
            .and_then(|v| v.as_u64());

        let pseudogene = data
            .get("pseudogene_cnt")
            .or_else(|| data.get("core").and_then(|c| c.get("pseudogene_cnt")))
            .and_then(|v| v.as_u64());

        Ok((coding, noncoding, pseudogene))
    }
}

impl Default for EnsemblClient {
    fn default() -> Self {
        Self::new()
    }
}

fn assembled_base_pairs(assembly: &AssemblyInfo) -> Option<u64> {
    assembly.golden_path.or_else(|| {
        let total: u64 = assembly
            .top_level_region
            .iter()
            .filter(|r| matches!(r.coord_system.as_str(), "primary_assembly" | "chromosome"))
            .map(|r| r.length)
            .sum();
        (total > 0).then_some(total)
    })
}

#[cfg(test)]
mod tests {
    use super::{assembled_base_pairs, AssemblyInfo, TopLevelRegion};

    #[test]
    fn prefers_reported_golden_path() {
        let assembly = AssemblyInfo {
            assembly_name: None,
            assembly_accession: None,
            genebuild_last_geneset_update: None,
            golden_path: Some(123),
            top_level_region: vec![
                TopLevelRegion {
                    length: 50,
                    coord_system: "chromosome".to_string(),
                },
                TopLevelRegion {
                    length: 75,
                    coord_system: "primary_assembly".to_string(),
                },
            ],
        };

        assert_eq!(assembled_base_pairs(&assembly), Some(123));
    }

    #[test]
    fn sums_primary_assembly_regions_when_golden_path_missing() {
        let assembly = AssemblyInfo {
            assembly_name: None,
            assembly_accession: None,
            genebuild_last_geneset_update: None,
            golden_path: None,
            top_level_region: vec![
                TopLevelRegion {
                    length: 100,
                    coord_system: "primary_assembly".to_string(),
                },
                TopLevelRegion {
                    length: 200,
                    coord_system: "chromosome".to_string(),
                },
                TopLevelRegion {
                    length: 999,
                    coord_system: "scaffold".to_string(),
                },
            ],
        };

        assert_eq!(assembled_base_pairs(&assembly), Some(300));
    }
}
