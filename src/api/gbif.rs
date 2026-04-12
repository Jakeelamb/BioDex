//! GBIF (Global Biodiversity Information Facility) API client
//!
//! Documentation: https://www.gbif.org/developer/summary

use super::{ApiError, Result};
use serde::Deserialize;

const BASE_URL: &str = "https://api.gbif.org/v1";
const MAP_URL: &str = "https://api.gbif.org/v2/map/occurrence/density";

pub struct GbifClient {
    client: reqwest::Client,
}

#[derive(Debug, Clone)]
pub struct Species {
    pub key: u64,
    pub nub_key: Option<u64>,
    pub scientific_name: String,
    pub canonical_name: Option<String>,
    pub vernacular_name: Option<String>,
    pub rank: String,
    pub status: String,
    pub kingdom: Option<String>,
    pub phylum: Option<String>,
    pub class: Option<String>,
    pub order: Option<String>,
    pub family: Option<String>,
    pub genus: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Occurrence {
    pub key: u64,
    pub species: Option<String>,
    pub country: Option<String>,
    pub country_code: Option<String>,
    pub decimal_latitude: Option<f64>,
    pub decimal_longitude: Option<f64>,
    pub event_date: Option<String>,
    pub basis_of_record: String,
    pub institution_code: Option<String>,
    pub collection_code: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CountryCount {
    pub country_code: String,
    pub country_name: String,
    pub count: u64,
}

#[derive(Debug, Clone)]
pub struct SpeciesSuggestion {
    pub key: u64,
    pub scientific_name: String,
    pub canonical_name: Option<String>,
    pub rank: String,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct YearCount {
    pub year: u32,
    pub count: u64,
}

// API response structures
#[derive(Debug, Deserialize)]
struct SpeciesSearchResponse {
    results: Vec<SpeciesResult>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SpeciesResult {
    #[serde(alias = "usageKey")]
    key: Option<u64>,
    nub_key: Option<u64>,
    scientific_name: Option<String>,
    canonical_name: Option<String>,
    vernacular_name: Option<String>,
    rank: Option<String>,
    #[serde(alias = "status")]
    taxonomic_status: Option<String>,
    kingdom: Option<String>,
    phylum: Option<String>,
    class: Option<String>,
    order: Option<String>,
    family: Option<String>,
    genus: Option<String>,
    species_key: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct OccurrenceSearchResponse {
    results: Vec<OccurrenceResult>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OccurrenceResult {
    key: u64,
    species: Option<String>,
    country: Option<String>,
    country_code: Option<String>,
    decimal_latitude: Option<f64>,
    decimal_longitude: Option<f64>,
    event_date: Option<String>,
    basis_of_record: Option<String>,
    institution_code: Option<String>,
    collection_code: Option<String>,
}

impl GbifClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        }
    }

    /// Search for species by name
    pub async fn search_species(&self, query: &str) -> Result<Vec<Species>> {
        let url = format!(
            "{}/species/search?q={}&limit=10",
            BASE_URL,
            urlencoding::encode(query)
        );

        let response: SpeciesSearchResponse = self.client.get(&url).send().await?.json().await?;

        if response.results.is_empty() {
            return Err(ApiError::NotFound(query.to_string()));
        }

        Ok(response
            .results
            .into_iter()
            .map(Self::convert_species)
            .collect())
    }

    /// Get species by GBIF key
    pub async fn get_species(&self, species_key: u64) -> Result<Species> {
        let url = format!("{}/species/{}", BASE_URL, species_key);

        let response: SpeciesResult = self.client.get(&url).send().await?.json().await?;

        Ok(Self::convert_species(response))
    }

    /// Match a species name to GBIF backbone taxonomy
    pub async fn match_species(&self, name: &str) -> Result<Species> {
        let url = format!(
            "{}/species/match?name={}",
            BASE_URL,
            urlencoding::encode(name)
        );

        let response: SpeciesResult = self.client.get(&url).send().await?.json().await?;

        Ok(Self::convert_species(response))
    }

    /// Get occurrences for a species
    pub async fn get_occurrences(&self, species_key: u64, limit: u32) -> Result<Vec<Occurrence>> {
        let url = format!(
            "{}/occurrence/search?taxonKey={}&limit={}&hasCoordinate=true",
            BASE_URL, species_key, limit
        );

        let response: OccurrenceSearchResponse = self.client.get(&url).send().await?.json().await?;

        Ok(response
            .results
            .into_iter()
            .map(Self::convert_occurrence)
            .collect())
    }

    /// Get occurrence count by country for a species
    pub async fn get_country_counts(&self, species_key: u64) -> Result<Vec<CountryCount>> {
        let url = format!(
            "{}/occurrence/search?taxonKey={}&limit=0&facet=country&facetLimit=100",
            BASE_URL, species_key
        );

        let response: serde_json::Value = self.client.get(&url).send().await?.json().await?;

        let mut counts = Vec::new();

        if let Some(facets) = response.get("facets").and_then(|f| f.as_array()) {
            for facet in facets {
                if facet.get("field").and_then(|f| f.as_str()) == Some("COUNTRY") {
                    if let Some(facet_counts) = facet.get("counts").and_then(|c| c.as_array()) {
                        for fc in facet_counts {
                            if let (Some(code), Some(count)) = (
                                fc.get("name").and_then(|n| n.as_str()),
                                fc.get("count").and_then(|c| c.as_u64()),
                            ) {
                                counts.push(CountryCount {
                                    country_code: code.to_string(),
                                    country_name: Self::country_code_to_name(code),
                                    count,
                                });
                            }
                        }
                    }
                }
            }
        }

        Ok(counts)
    }

    /// Get continents where a species occurs
    pub async fn get_continents(&self, species_key: u64) -> Result<Vec<String>> {
        let url = format!(
            "{}/occurrence/search?taxonKey={}&limit=0&facet=continent&facetLimit=10",
            BASE_URL, species_key
        );

        let response: serde_json::Value = self.client.get(&url).send().await?.json().await?;

        let mut continents = Vec::new();

        if let Some(facets) = response.get("facets").and_then(|f| f.as_array()) {
            for facet in facets {
                if facet.get("field").and_then(|f| f.as_str()) == Some("CONTINENT") {
                    if let Some(facet_counts) = facet.get("counts").and_then(|c| c.as_array()) {
                        for fc in facet_counts {
                            if let Some(continent) = fc.get("name").and_then(|n| n.as_str()) {
                                continents.push(Self::continent_code_to_name(continent));
                            }
                        }
                    }
                }
            }
        }

        Ok(continents)
    }

    /// Get geographic bounding box for a species
    pub async fn get_bounding_box(&self, species_key: u64) -> Result<(f64, f64, f64, f64)> {
        // Get a sample of occurrences with coordinates to determine range
        let url = format!(
            "{}/occurrence/search?taxonKey={}&hasCoordinate=true&limit=300",
            BASE_URL, species_key
        );

        let response: serde_json::Value = self.client.get(&url).send().await?.json().await?;

        let mut min_lat = 90.0_f64;
        let mut max_lat = -90.0_f64;
        let mut min_lon = 180.0_f64;
        let mut max_lon = -180.0_f64;
        let mut found = false;

        if let Some(results) = response.get("results").and_then(|r| r.as_array()) {
            for result in results {
                if let (Some(lat), Some(lon)) = (
                    result.get("decimalLatitude").and_then(|v| v.as_f64()),
                    result.get("decimalLongitude").and_then(|v| v.as_f64()),
                ) {
                    found = true;
                    min_lat = min_lat.min(lat);
                    max_lat = max_lat.max(lat);
                    min_lon = min_lon.min(lon);
                    max_lon = max_lon.max(lon);
                }
            }
        }

        if found {
            Ok((min_lat, max_lat, min_lon, max_lon))
        } else {
            Err(ApiError::NotFound("No coordinates found".to_string()))
        }
    }

    fn continent_code_to_name(code: &str) -> String {
        match code {
            "AFRICA" => "Africa",
            "ANTARCTICA" => "Antarctica",
            "ASIA" => "Asia",
            "EUROPE" => "Europe",
            "NORTH_AMERICA" => "North America",
            "OCEANIA" => "Oceania",
            "SOUTH_AMERICA" => "South America",
            _ => code,
        }
        .to_string()
    }

    /// Get occurrence count by year for a species
    pub async fn get_year_counts(&self, species_key: u64) -> Result<Vec<YearCount>> {
        let url = format!(
            "{}/occurrence/search?taxonKey={}&limit=0&facet=year&facetLimit=100",
            BASE_URL, species_key
        );

        let response: serde_json::Value = self.client.get(&url).send().await?.json().await?;

        let mut counts = Vec::new();

        if let Some(facets) = response.get("facets").and_then(|f| f.as_array()) {
            for facet in facets {
                if facet.get("field").and_then(|f| f.as_str()) == Some("YEAR") {
                    if let Some(facet_counts) = facet.get("counts").and_then(|c| c.as_array()) {
                        for fc in facet_counts {
                            if let (Some(year_str), Some(count)) = (
                                fc.get("name").and_then(|n| n.as_str()),
                                fc.get("count").and_then(|c| c.as_u64()),
                            ) {
                                if let Ok(year) = year_str.parse::<u32>() {
                                    counts.push(YearCount { year, count });
                                }
                            }
                        }
                    }
                }
            }
        }

        counts.sort_by(|a, b| a.year.cmp(&b.year));
        Ok(counts)
    }

    /// Get total occurrence count for a species
    pub async fn get_occurrence_count(&self, species_key: u64) -> Result<u64> {
        let url = format!(
            "{}/occurrence/search?taxonKey={}&limit=0",
            BASE_URL, species_key
        );

        let response: serde_json::Value = self.client.get(&url).send().await?.json().await?;

        response
            .get("count")
            .and_then(|c| c.as_u64())
            .ok_or_else(|| ApiError::Api("Missing count in response".to_string()))
    }

    /// Suggest species names for autocomplete
    pub async fn suggest_species(&self, query: &str, limit: u32) -> Result<Vec<SpeciesSuggestion>> {
        let url = format!(
            "{}/species/suggest?q={}&limit={}",
            BASE_URL,
            urlencoding::encode(query),
            limit
        );

        let response: serde_json::Value = self.client.get(&url).send().await?.json().await?;

        let mut suggestions = Vec::new();

        if let Some(results) = response.as_array() {
            for result in results {
                let suggestion = SpeciesSuggestion {
                    key: result.get("key").and_then(|k| k.as_u64()).unwrap_or(0),
                    scientific_name: result
                        .get("scientificName")
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .to_string(),
                    canonical_name: result
                        .get("canonicalName")
                        .and_then(|s| s.as_str())
                        .map(|s| s.to_string()),
                    rank: result
                        .get("rank")
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .to_string(),
                    status: result
                        .get("status")
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .to_string(),
                };
                suggestions.push(suggestion);
            }
        }

        Ok(suggestions)
    }

    /// Get a map image URL for species occurrences
    /// Returns a URL to a PNG image showing global distribution
    pub fn get_map_image_url(&self, species_key: u64) -> String {
        // Zoom level 0 = entire world in one tile
        // @2x for retina/high-res
        // style options: classic.point, purpleYellow.point, fire.point, glacier.point
        format!(
            "{}/0/0/0@2x.png?taxonKey={}&style=classic.point",
            MAP_URL, species_key
        )
    }

    /// Download map image bytes for a species
    pub async fn get_map_image(&self, species_key: u64) -> Result<Vec<u8>> {
        let url = self.get_map_image_url(species_key);

        let response = self.client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(ApiError::Api(format!(
                "Map request failed: {}",
                response.status()
            )));
        }

        let bytes = response.bytes().await?;
        Ok(bytes.to_vec())
    }

    /// Get the parent taxon key for a species at a specific rank level
    /// For example, to get siblings at "family" rank, this returns the "order" key
    pub async fn get_parent_key_for_rank(
        &self,
        species_key: u64,
        target_rank: &str,
    ) -> Result<u64> {
        let url = format!("{}/species/{}", BASE_URL, species_key);

        let response: serde_json::Value = self.client.get(&url).send().await?.json().await?;

        // Map target rank to the parent rank's key field
        // To get siblings at a rank, we need the parent (one level up)
        let parent_key_field = match target_rank.to_uppercase().as_str() {
            "SPECIES" => "genusKey",
            "GENUS" => "familyKey",
            "FAMILY" => "orderKey",
            "ORDER" => "classKey",
            "CLASS" => "phylumKey",
            "PHYLUM" => "kingdomKey",
            // For non-standard ranks, try to use the direct parent
            _ => "parentKey",
        };

        response
            .get(parent_key_field)
            .and_then(|k| k.as_u64())
            .ok_or_else(|| {
                ApiError::NotFound(format!("No parent key found for rank {}", target_rank))
            })
    }

    /// Get children taxa of a parent taxon, optionally filtered by rank
    pub async fn get_children(&self, parent_key: u64, rank: &str) -> Result<Vec<Species>> {
        let url = format!("{}/species/{}/children?limit=100", BASE_URL, parent_key);

        let response: serde_json::Value = self.client.get(&url).send().await?.json().await?;

        let mut children = Vec::new();

        if let Some(results) = response.get("results").and_then(|r| r.as_array()) {
            for result in results {
                // Filter by rank if specified
                let result_rank = result.get("rank").and_then(|r| r.as_str()).unwrap_or("");
                if !rank.is_empty() && !result_rank.eq_ignore_ascii_case(rank) {
                    continue;
                }

                let species = Species {
                    key: result.get("key").and_then(|k| k.as_u64()).unwrap_or(0),
                    nub_key: result.get("nubKey").and_then(|k| k.as_u64()),
                    scientific_name: result
                        .get("scientificName")
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .to_string(),
                    canonical_name: result
                        .get("canonicalName")
                        .and_then(|s| s.as_str())
                        .map(|s| s.to_string()),
                    vernacular_name: result
                        .get("vernacularName")
                        .and_then(|s| s.as_str())
                        .map(|s| s.to_string()),
                    rank: result_rank.to_string(),
                    status: result
                        .get("taxonomicStatus")
                        .and_then(|s| s.as_str())
                        .unwrap_or("UNKNOWN")
                        .to_string(),
                    kingdom: result
                        .get("kingdom")
                        .and_then(|s| s.as_str())
                        .map(|s| s.to_string()),
                    phylum: result
                        .get("phylum")
                        .and_then(|s| s.as_str())
                        .map(|s| s.to_string()),
                    class: result
                        .get("class")
                        .and_then(|s| s.as_str())
                        .map(|s| s.to_string()),
                    order: result
                        .get("order")
                        .and_then(|s| s.as_str())
                        .map(|s| s.to_string()),
                    family: result
                        .get("family")
                        .and_then(|s| s.as_str())
                        .map(|s| s.to_string()),
                    genus: result
                        .get("genus")
                        .and_then(|s| s.as_str())
                        .map(|s| s.to_string()),
                };
                children.push(species);
            }
        }

        Ok(children)
    }

    fn convert_species(s: SpeciesResult) -> Species {
        Species {
            key: s.key.or(s.species_key).unwrap_or(0),
            nub_key: s.nub_key,
            scientific_name: s.scientific_name.unwrap_or_default(),
            canonical_name: s.canonical_name,
            vernacular_name: s.vernacular_name,
            rank: s.rank.unwrap_or_else(|| "UNKNOWN".to_string()),
            status: s.taxonomic_status.unwrap_or_else(|| "UNKNOWN".to_string()),
            kingdom: s.kingdom,
            phylum: s.phylum,
            class: s.class,
            order: s.order,
            family: s.family,
            genus: s.genus,
        }
    }

    fn convert_occurrence(o: OccurrenceResult) -> Occurrence {
        Occurrence {
            key: o.key,
            species: o.species,
            country: o.country,
            country_code: o.country_code,
            decimal_latitude: o.decimal_latitude,
            decimal_longitude: o.decimal_longitude,
            event_date: o.event_date,
            basis_of_record: o.basis_of_record.unwrap_or_else(|| "UNKNOWN".to_string()),
            institution_code: o.institution_code,
            collection_code: o.collection_code,
        }
    }

    fn country_code_to_name(code: &str) -> String {
        // Common country codes - extend as needed
        match code {
            "US" => "United States",
            "CA" => "Canada",
            "MX" => "Mexico",
            "GB" => "United Kingdom",
            "DE" => "Germany",
            "FR" => "France",
            "ES" => "Spain",
            "IT" => "Italy",
            "AU" => "Australia",
            "NZ" => "New Zealand",
            "BR" => "Brazil",
            "AR" => "Argentina",
            "CN" => "China",
            "JP" => "Japan",
            "IN" => "India",
            "ZA" => "South Africa",
            "KE" => "Kenya",
            "EG" => "Egypt",
            "RU" => "Russia",
            "SE" => "Sweden",
            "NO" => "Norway",
            "FI" => "Finland",
            "DK" => "Denmark",
            "NL" => "Netherlands",
            "BE" => "Belgium",
            "CH" => "Switzerland",
            "AT" => "Austria",
            "PL" => "Poland",
            "PT" => "Portugal",
            "GR" => "Greece",
            "TR" => "Turkey",
            "IE" => "Ireland",
            "CL" => "Chile",
            "CO" => "Colombia",
            "PE" => "Peru",
            "VE" => "Venezuela",
            "EC" => "Ecuador",
            "CR" => "Costa Rica",
            "PA" => "Panama",
            "TH" => "Thailand",
            "VN" => "Vietnam",
            "PH" => "Philippines",
            "ID" => "Indonesia",
            "MY" => "Malaysia",
            "SG" => "Singapore",
            "TZ" => "Tanzania",
            "BW" => "Botswana",
            "NA" => "Namibia",
            "ZW" => "Zimbabwe",
            "ZM" => "Zambia",
            "MZ" => "Mozambique",
            "AO" => "Angola",
            "UG" => "Uganda",
            "ET" => "Ethiopia",
            "NG" => "Nigeria",
            "GH" => "Ghana",
            "SN" => "Senegal",
            "CM" => "Cameroon",
            "CD" => "DR Congo",
            "CG" => "Congo",
            "MW" => "Malawi",
            "RW" => "Rwanda",
            "BI" => "Burundi",
            "MG" => "Madagascar",
            "MU" => "Mauritius",
            "SC" => "Seychelles",
            "IS" => "Iceland",
            "GL" => "Greenland",
            "HK" => "Hong Kong",
            "TW" => "Taiwan",
            "KR" => "South Korea",
            "NP" => "Nepal",
            "PK" => "Pakistan",
            "BD" => "Bangladesh",
            "LK" => "Sri Lanka",
            "MM" => "Myanmar",
            "KH" => "Cambodia",
            "LA" => "Laos",
            "HU" => "Hungary",
            "CZ" => "Czech Republic",
            "SK" => "Slovakia",
            "RO" => "Romania",
            "BG" => "Bulgaria",
            "UA" => "Ukraine",
            "BY" => "Belarus",
            "LT" => "Lithuania",
            "LV" => "Latvia",
            "EE" => "Estonia",
            _ => code,
        }
        .to_string()
    }
}

impl Default for GbifClient {
    fn default() -> Self {
        Self::new()
    }
}
