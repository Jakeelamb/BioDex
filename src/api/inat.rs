//! iNaturalist API client for observations and species data
//!
//! Documentation: https://api.inaturalist.org/v1/docs/

use super::{ApiError, Result};
use serde::Deserialize;

const BASE_URL: &str = "https://api.inaturalist.org/v1";

pub struct InatClient {
    client: reqwest::Client,
}

#[derive(Debug, Clone)]
pub struct Taxon {
    pub id: u64,
    pub name: String,
    pub observations_count: u64,
    pub preferred_common_name: Option<String>,
    pub default_photo: Option<Photo>,
    pub conservation_status: Option<ConservationStatus>,
    pub ancestors: Vec<AncestorTaxon>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Photo {
    pub url: Option<String>,
    pub medium_url: Option<String>,
    pub attribution: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConservationStatus {
    pub status_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AncestorTaxon {
    pub name: String,
    pub rank: String,
}

// API response structures
#[derive(Debug, Deserialize)]
struct TaxaResponse {
    results: Vec<TaxonResult>,
}

#[derive(Debug, Deserialize)]
struct TaxonResult {
    id: u64,
    name: String,
    #[serde(default)]
    observations_count: u64,
    preferred_common_name: Option<String>,
    default_photo: Option<Photo>,
    conservation_status: Option<ConservationStatus>,
    #[serde(default)]
    ancestors: Vec<AncestorResult>,
}

#[derive(Debug, Deserialize)]
struct AncestorResult {
    name: String,
    rank: String,
}

impl InatClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        }
    }

    /// Search for taxa by name with rank filter (e.g., "species", "genus")
    pub async fn search_taxa_by_rank(&self, query: &str, rank: &str) -> Result<Vec<Taxon>> {
        let url = format!(
            "{}/taxa?q={}&rank={}&per_page=10",
            BASE_URL,
            urlencoding::encode(query),
            rank
        );

        let response: TaxaResponse = self.client.get(&url).send().await?.json().await?;

        if response.results.is_empty() {
            return Err(ApiError::NotFound(query.to_string()));
        }

        Ok(response
            .results
            .into_iter()
            .map(Self::convert_taxon)
            .collect())
    }

    /// Search specifically for a species by scientific name
    pub async fn search_species(&self, query: &str) -> Result<Taxon> {
        // Try species rank filter
        let mut taxa = self.search_taxa_by_rank(query, "species").await?;

        // Find exact match index on name
        let exact_idx = taxa
            .iter()
            .position(|t| t.name.to_lowercase() == query.to_lowercase());

        // Return exact match if found, otherwise first result
        if let Some(idx) = exact_idx {
            Ok(taxa.swap_remove(idx))
        } else {
            Ok(taxa.swap_remove(0))
        }
    }

    fn convert_taxon(t: TaxonResult) -> Taxon {
        Taxon {
            id: t.id,
            name: t.name,
            observations_count: t.observations_count,
            preferred_common_name: t.preferred_common_name,
            default_photo: t.default_photo,
            conservation_status: t.conservation_status,
            ancestors: t
                .ancestors
                .into_iter()
                .map(|a| AncestorTaxon {
                    name: a.name,
                    rank: a.rank,
                })
                .collect(),
        }
    }
}

impl Default for InatClient {
    fn default() -> Self {
        Self::new()
    }
}
