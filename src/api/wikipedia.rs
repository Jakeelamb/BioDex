//! Wikipedia/Wikidata API client for species descriptions
//!
//! Documentation: https://www.mediawiki.org/wiki/API:Main_page

use super::{ApiError, Result};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::sync::OnceLock;

const WIKIPEDIA_API: &str = "https://en.wikipedia.org/api/rest_v1";
const WIKIPEDIA_QUERY_API: &str = "https://en.wikipedia.org/w/api.php";
const WIKIDATA_API: &str = "https://www.wikidata.org/w/api.php";
const P_IUCN_STATUS: &str = "P141";
const P_TAXON_RANK: &str = "P105";
const P_IMAGE: &str = "P18";
const P_LIFE_EXPECTANCY: &str = "P2250";
const P_LENGTH: &str = "P2043";
const P_HEIGHT: &str = "P2048";
const P_MASS: &str = "P2067";
const P_REPRODUCTION_MODE: &str = "P13318";

pub struct WikipediaClient {
    client: reqwest::Client,
}

#[derive(Debug, Clone)]
pub struct WikiSummary {
    pub extract: String,
    pub description: Option<String>,
    pub thumbnail_url: Option<String>,
    pub page_url: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WikiArticle {
    pub extract: String,
    pub wikitext: String,
}

#[derive(Debug, Clone)]
pub struct WikidataEntity {
    pub id: String,
    pub description: Option<String>,
    pub aliases: Vec<String>,
    pub iucn_status: Option<String>,
    pub taxon_rank: Option<String>,
    pub image_url: Option<String>,
    pub life_expectancy_years: Option<f64>,
    pub length_meters: Option<f64>,
    pub height_meters: Option<f64>,
    pub mass_kilograms: Option<f64>,
    pub reproduction_modes: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WikiLifeHistoryFallback {
    pub lifespan_years: Option<f64>,
    pub length_meters: Option<f64>,
    pub height_meters: Option<f64>,
    pub mass_kilograms: Option<f64>,
    pub reproduction_modes: Vec<String>,
}

impl WikiArticle {
    pub fn is_empty(&self) -> bool {
        self.extract.trim().is_empty() && self.wikitext.trim().is_empty()
    }

    pub fn plain_text(&self) -> String {
        let extract = self.extract.trim();
        let simplified = simplify_wikitext(&self.wikitext);
        let simplified = simplified.trim();

        match (extract.is_empty(), simplified.is_empty()) {
            (true, true) => String::new(),
            (false, true) => extract.to_string(),
            (true, false) => simplified.to_string(),
            (false, false) if simplified.contains(extract) => simplified.to_string(),
            (false, false) => format!("{extract}\n\n{simplified}"),
        }
    }

    pub fn llm_context(&self, max_chars: usize) -> String {
        let plain_text = self.plain_text();
        if plain_text.chars().count() <= max_chars {
            return plain_text;
        }

        let mut truncated = plain_text.chars().take(max_chars).collect::<String>();
        truncated.push_str("\n\n[truncated]");
        truncated
    }
}

impl WikiLifeHistoryFallback {
    pub fn has_any_stats(&self) -> bool {
        self.lifespan_years.is_some()
            || self.length_meters.is_some()
            || self.height_meters.is_some()
            || self.mass_kilograms.is_some()
            || !self.reproduction_modes.is_empty()
    }

    pub fn needs_completion(&self) -> bool {
        self.lifespan_years.is_none()
            || (self.length_meters.is_none() && self.height_meters.is_none())
            || self.mass_kilograms.is_none()
            || self.reproduction_modes.is_empty()
    }

    pub fn fill_missing_from(&mut self, other: Self) {
        if self.lifespan_years.is_none() {
            self.lifespan_years = other.lifespan_years;
        }
        if self.length_meters.is_none() {
            self.length_meters = other.length_meters;
        }
        if self.height_meters.is_none() {
            self.height_meters = other.height_meters;
        }
        if self.mass_kilograms.is_none() {
            self.mass_kilograms = other.mass_kilograms;
        }
        if self.reproduction_modes.is_empty() {
            self.reproduction_modes = other.reproduction_modes;
        } else if !other.reproduction_modes.is_empty() {
            let mut merged = self.reproduction_modes.clone();
            merged.extend(other.reproduction_modes);
            self.reproduction_modes = Self::normalize_reproduction_modes(merged);
        }
    }

    pub fn normalize_reproduction_modes(values: Vec<String>) -> Vec<String> {
        normalize_reproduction_modes(values)
    }
}

// API response structures
#[derive(Debug, Deserialize)]
struct SummaryResponse {
    extract: Option<String>,
    description: Option<String>,
    thumbnail: Option<Thumbnail>,
    content_urls: Option<ContentUrls>,
}

#[derive(Debug, Deserialize)]
struct Thumbnail {
    source: String,
}

#[derive(Debug, Deserialize)]
struct ContentUrls {
    desktop: Option<DesktopUrl>,
}

#[derive(Debug, Deserialize)]
struct DesktopUrl {
    page: Option<String>,
}

#[derive(Debug, Deserialize)]
struct QueryExtractResponse {
    query: QueryPages,
}

#[derive(Debug, Deserialize)]
struct QueryPages {
    pages: HashMap<String, QueryPage>,
}

#[derive(Debug, Deserialize)]
struct QueryPage {
    extract: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ParseWikitextResponse {
    parse: ParseWikitextData,
}

#[derive(Debug, Deserialize)]
struct ParseWikitextData {
    wikitext: String,
}

#[derive(Debug, Deserialize)]
struct WikidataSearchResponse {
    search: Vec<WikidataSearchResult>,
}

#[derive(Debug, Deserialize)]
struct WikidataSearchResult {
    id: String,
}

#[derive(Debug, Deserialize)]
struct WikidataEntityResponse {
    entities: std::collections::HashMap<String, WikidataEntityData>,
}

#[derive(Debug, Deserialize)]
struct WikidataEntityData {
    id: String,
    labels: Option<std::collections::HashMap<String, LabelValue>>,
    descriptions: Option<std::collections::HashMap<String, LabelValue>>,
    aliases: Option<std::collections::HashMap<String, Vec<LabelValue>>>,
    claims: Option<std::collections::HashMap<String, Vec<Claim>>>,
}

#[derive(Debug, Deserialize)]
struct LabelValue {
    value: String,
}

#[derive(Debug, Deserialize)]
struct Claim {
    mainsnak: Option<MainSnak>,
    qualifiers: Option<HashMap<String, Vec<QualifierSnak>>>,
}

#[derive(Debug, Deserialize)]
struct MainSnak {
    datavalue: Option<DataValue>,
}

#[derive(Debug, Deserialize)]
struct DataValue {
    value: serde_json::Value,
}

#[derive(Debug, Clone)]
struct QuantityClaim {
    amount: f64,
    unit_id: Option<String>,
    qualifier_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct QualifierSnak {
    datavalue: Option<DataValue>,
}

impl WikipediaClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        }
    }

    /// Get Wikipedia summary for a species by name
    pub async fn get_summary(&self, title: &str) -> Result<WikiSummary> {
        // Replace spaces with underscores for Wikipedia API
        let formatted_title = title.replace(' ', "_");
        let url = format!(
            "{}/page/summary/{}",
            WIKIPEDIA_API,
            urlencoding::encode(&formatted_title)
        );

        let response = self
            .client
            .get(&url)
            .header("User-Agent", "ncbi_poketext/0.1 (biodiversity TUI app)")
            .send()
            .await?;

        if response.status() == 404 {
            return Err(ApiError::NotFound(title.to_string()));
        }

        let summary: SummaryResponse = response.json().await?;

        Ok(WikiSummary {
            extract: summary.extract.unwrap_or_default(),
            description: summary.description,
            thumbnail_url: summary.thumbnail.map(|t| t.source),
            page_url: summary
                .content_urls
                .and_then(|c| c.desktop)
                .and_then(|d| d.page)
                .unwrap_or_else(|| format!("https://en.wikipedia.org/wiki/{}", formatted_title)),
        })
    }

    pub async fn get_article_content(&self, title: &str) -> Result<WikiArticle> {
        let (extract_result, wikitext_result) = tokio::join!(
            self.get_article_extract(title),
            self.get_article_wikitext(title),
        );

        let article = WikiArticle {
            extract: extract_result.unwrap_or_default(),
            wikitext: wikitext_result.unwrap_or_default(),
        };

        if article.is_empty() {
            return Err(ApiError::NotFound(title.to_string()));
        }

        Ok(article)
    }

    pub fn extract_life_history_from_article(
        &self,
        article: &WikiArticle,
    ) -> WikiLifeHistoryFallback {
        let plain_text = article.plain_text();

        WikiLifeHistoryFallback {
            lifespan_years: extract_lifespan_years_from_wikitext(&article.wikitext)
                .or_else(|| extract_lifespan_years_from_text(&plain_text)),
            length_meters: extract_length_meters_from_wikitext(&article.wikitext)
                .or_else(|| extract_length_meters_from_text(&plain_text)),
            height_meters: extract_height_meters_from_wikitext(&article.wikitext)
                .or_else(|| extract_height_meters_from_text(&plain_text)),
            mass_kilograms: extract_mass_kilograms_from_wikitext(&article.wikitext)
                .or_else(|| extract_mass_kilograms_from_text(&plain_text)),
            reproduction_modes: infer_reproduction_modes_from_text(&plain_text),
        }
    }

    /// Search Wikidata for a taxon
    pub async fn search_wikidata(&self, query: &str) -> Result<Vec<String>> {
        let url = format!(
            "{}?action=wbsearchentities&search={}&language=en&type=item&format=json",
            WIKIDATA_API,
            urlencoding::encode(query)
        );

        let response: WikidataSearchResponse = self
            .client
            .get(&url)
            .header("User-Agent", "ncbi_poketext/0.1 (biodiversity TUI app)")
            .send()
            .await?
            .json()
            .await?;

        if response.search.is_empty() {
            return Err(ApiError::NotFound(query.to_string()));
        }

        Ok(response.search.into_iter().map(|r| r.id).collect())
    }

    /// Get Wikidata entity by ID
    pub async fn get_wikidata_entity(&self, entity_id: &str) -> Result<WikidataEntity> {
        let url = format!(
            "{}?action=wbgetentities&ids={}&languages=en&format=json",
            WIKIDATA_API, entity_id
        );

        let response: WikidataEntityResponse = self
            .client
            .get(&url)
            .header("User-Agent", "ncbi_poketext/0.1 (biodiversity TUI app)")
            .send()
            .await?
            .json()
            .await?;

        let entity = response
            .entities
            .get(entity_id)
            .ok_or_else(|| ApiError::NotFound(entity_id.to_string()))?;

        let description = entity
            .descriptions
            .as_ref()
            .and_then(|d| d.get("en"))
            .map(|d| d.value.clone());

        let aliases = entity
            .aliases
            .as_ref()
            .and_then(|a| a.get("en"))
            .map(|a| a.iter().map(|v| v.value.clone()).collect())
            .unwrap_or_default();

        let iucn_status_id = self.extract_claim_entity_id(entity, P_IUCN_STATUS);
        let taxon_rank_id = self.extract_claim_entity_id(entity, P_TAXON_RANK);
        let life_expectancy_claims = self.extract_claim_quantities(entity, P_LIFE_EXPECTANCY);
        let length_claims = self.extract_claim_quantities(entity, P_LENGTH);
        let height_claims = self.extract_claim_quantities(entity, P_HEIGHT);
        let mass_claims = self.extract_claim_quantities(entity, P_MASS);
        let reproduction_ids = self.extract_claim_entity_ids(entity, P_REPRODUCTION_MODE);

        let mut referenced_ids = Vec::new();
        referenced_ids.extend(iucn_status_id.iter().cloned());
        referenced_ids.extend(taxon_rank_id.iter().cloned());
        referenced_ids.extend(reproduction_ids.iter().cloned());
        referenced_ids.extend(
            [
                &life_expectancy_claims,
                &length_claims,
                &height_claims,
                &mass_claims,
            ]
            .into_iter()
            .flat_map(|claims| {
                claims.iter().flat_map(|quantity| {
                    quantity
                        .unit_id
                        .iter()
                        .cloned()
                        .chain(quantity.qualifier_ids.iter().cloned())
                })
            }),
        );

        let entity_labels = self
            .get_entity_labels(&referenced_ids)
            .await
            .unwrap_or_default();
        let iucn_status = iucn_status_id
            .as_deref()
            .and_then(|id| entity_labels.get(id))
            .map(|label| normalize_iucn_status(label));
        let taxon_rank = taxon_rank_id
            .as_deref()
            .and_then(|id| entity_labels.get(id))
            .map(|label| label.to_ascii_lowercase());
        let life_expectancy_years = select_best_quantity(&life_expectancy_claims, &entity_labels)
            .and_then(|quantity| quantity_to_years(quantity, &entity_labels));
        let length_meters = select_best_quantity(&length_claims, &entity_labels)
            .and_then(|quantity| quantity_to_meters(quantity, &entity_labels));
        let height_meters = select_best_quantity(&height_claims, &entity_labels)
            .and_then(|quantity| quantity_to_meters(quantity, &entity_labels));
        let mass_kilograms = select_best_quantity(&mass_claims, &entity_labels)
            .and_then(|quantity| quantity_to_kilograms(quantity, &entity_labels));
        let reproduction_modes = normalize_reproduction_modes(
            reproduction_ids
                .iter()
                .filter_map(|id| entity_labels.get(id))
                .cloned()
                .collect(),
        );

        // Extract image (P18)
        let image_url = entity
            .claims
            .as_ref()
            .and_then(|c| c.get(P_IMAGE))
            .and_then(|claims| claims.first())
            .and_then(|c| c.mainsnak.as_ref())
            .and_then(|s| s.datavalue.as_ref())
            .and_then(|d| d.value.as_str())
            .map(|filename| {
                let encoded = urlencoding::encode(filename);
                format!(
                    "https://commons.wikimedia.org/wiki/Special:FilePath/{}",
                    encoded
                )
            });

        Ok(WikidataEntity {
            id: entity.id.clone(),
            description,
            aliases,
            iucn_status,
            taxon_rank,
            image_url,
            life_expectancy_years,
            length_meters,
            height_meters,
            mass_kilograms,
            reproduction_modes,
        })
    }

    /// Get Wikidata entity for a taxon by name
    pub async fn get_taxon_wikidata(&self, name: &str) -> Result<WikidataEntity> {
        let ids = self.search_wikidata(name).await?;
        self.get_wikidata_entity(&ids[0]).await
    }

    async fn get_article_extract(&self, title: &str) -> Result<String> {
        let url = format!(
            "{}?action=query&prop=extracts&explaintext=1&redirects=1&titles={}&format=json",
            WIKIPEDIA_QUERY_API,
            urlencoding::encode(title)
        );

        let response: QueryExtractResponse = self
            .client
            .get(&url)
            .header("User-Agent", "ncbi_poketext/0.1 (biodiversity TUI app)")
            .send()
            .await?
            .json()
            .await?;

        Ok(response
            .query
            .pages
            .into_values()
            .find_map(|page| page.extract)
            .unwrap_or_default())
    }

    async fn get_article_wikitext(&self, title: &str) -> Result<String> {
        let url = format!(
            "{}?action=parse&page={}&prop=wikitext&formatversion=2&redirects=1&format=json",
            WIKIPEDIA_QUERY_API,
            urlencoding::encode(title)
        );

        let response: ParseWikitextResponse = self
            .client
            .get(&url)
            .header("User-Agent", "ncbi_poketext/0.1 (biodiversity TUI app)")
            .send()
            .await?
            .json()
            .await?;

        Ok(response.parse.wikitext)
    }

    async fn get_entity_labels(&self, ids: &[String]) -> Result<HashMap<String, String>> {
        let unique = ids
            .iter()
            .map(|id| id.trim())
            .filter(|id| !id.is_empty())
            .map(ToOwned::to_owned)
            .collect::<BTreeSet<_>>();

        if unique.is_empty() {
            return Ok(HashMap::new());
        }

        let url = format!(
            "{}?action=wbgetentities&ids={}&props=labels&languages=en&format=json",
            WIKIDATA_API,
            unique.into_iter().collect::<Vec<_>>().join("|")
        );

        let response: WikidataEntityResponse = self
            .client
            .get(&url)
            .header("User-Agent", "ncbi_poketext/0.1 (biodiversity TUI app)")
            .send()
            .await?
            .json()
            .await?;

        let mut labels = HashMap::new();
        for (id, entity) in response.entities {
            if let Some(label) = entity
                .labels
                .as_ref()
                .and_then(|all_labels| all_labels.get("en"))
                .map(|value| value.value.clone())
            {
                labels.insert(id, label);
            }
        }

        Ok(labels)
    }

    fn extract_claim_entity_id(
        &self,
        entity: &WikidataEntityData,
        property: &str,
    ) -> Option<String> {
        self.extract_claim_entity_ids(entity, property)
            .into_iter()
            .next()
    }

    fn extract_claim_entity_ids(&self, entity: &WikidataEntityData, property: &str) -> Vec<String> {
        entity
            .claims
            .as_ref()
            .and_then(|c| c.get(property))
            .map(|claims| {
                claims
                    .iter()
                    .filter_map(|claim| {
                        claim
                            .mainsnak
                            .as_ref()
                            .and_then(|snak| snak.datavalue.as_ref())
                            .and_then(|value| extract_value_entity_id(&value.value))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn extract_claim_quantities(
        &self,
        entity: &WikidataEntityData,
        property: &str,
    ) -> Vec<QuantityClaim> {
        entity
            .claims
            .as_ref()
            .and_then(|c| c.get(property))
            .map(|claims| {
                claims
                    .iter()
                    .filter_map(|claim| {
                        let value = claim
                            .mainsnak
                            .as_ref()
                            .and_then(|snak| snak.datavalue.as_ref())?;
                        parse_quantity_claim(&value.value, claim.qualifiers.as_ref())
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

fn extract_value_entity_id(value: &serde_json::Value) -> Option<String> {
    value
        .as_object()
        .and_then(|object| object.get("id"))
        .and_then(|id| id.as_str())
        .map(ToOwned::to_owned)
}

fn parse_quantity_claim(
    value: &serde_json::Value,
    qualifiers: Option<&HashMap<String, Vec<QualifierSnak>>>,
) -> Option<QuantityClaim> {
    let object = value.as_object()?;
    let amount = object.get("amount")?.as_str()?.parse::<f64>().ok()?;
    let unit_id = object
        .get("unit")
        .and_then(|unit| unit.as_str())
        .and_then(parse_unit_entity_id);
    let qualifier_ids = qualifiers
        .into_iter()
        .flat_map(|entries| entries.values())
        .flatten()
        .filter_map(|snak| snak.datavalue.as_ref())
        .filter_map(|value| extract_value_entity_id(&value.value))
        .collect();

    Some(QuantityClaim {
        amount,
        unit_id,
        qualifier_ids,
    })
}

fn parse_unit_entity_id(unit: &str) -> Option<String> {
    if unit == "1" || unit.trim().is_empty() {
        return None;
    }

    unit.rsplit('/').next().map(ToOwned::to_owned)
}

fn quantity_to_years(quantity: &QuantityClaim, labels: &HashMap<String, String>) -> Option<f64> {
    let unit = quantity_unit_label(quantity, labels)?;
    let normalized = normalize_unit_label(unit);
    let factor = if normalized.contains("millennium") {
        1_000.0
    } else if normalized.contains("centur") {
        100.0
    } else if normalized.contains("decade") {
        10.0
    } else if normalized.contains("year") || normalized.contains("annum") {
        1.0
    } else if normalized.contains("month") {
        1.0 / 12.0
    } else if normalized.contains("week") {
        1.0 / 52.1775
    } else if normalized.contains("day") {
        1.0 / 365.25
    } else if normalized.contains("hour") {
        1.0 / (365.25 * 24.0)
    } else {
        return None;
    };

    Some(quantity.amount * factor)
}

fn quantity_to_meters(quantity: &QuantityClaim, labels: &HashMap<String, String>) -> Option<f64> {
    let unit = quantity_unit_label(quantity, labels)?;
    let normalized = normalize_unit_label(unit);
    let factor = if normalized.contains("kilomet") {
        1_000.0
    } else if normalized.contains("centimet") {
        0.01
    } else if normalized.contains("millimet") {
        0.001
    } else if normalized.contains("micromet") {
        0.000_001
    } else if normalized.contains("foot") || normalized == "ft" {
        0.3048
    } else if normalized.contains("inch") || normalized == "in" {
        0.0254
    } else if normalized.contains("yard") {
        0.9144
    } else if normalized.contains("mile") {
        1_609.344
    } else if normalized.contains("metre") || normalized.contains("meter") {
        1.0
    } else {
        return None;
    };

    Some(quantity.amount * factor)
}

fn quantity_to_kilograms(
    quantity: &QuantityClaim,
    labels: &HashMap<String, String>,
) -> Option<f64> {
    let unit = quantity_unit_label(quantity, labels)?;
    let normalized = normalize_unit_label(unit);
    let factor = if normalized.contains("tonne") || normalized.contains("metric ton") {
        1_000.0
    } else if normalized.contains("kilogram") || normalized == "kg" {
        1.0
    } else if normalized.contains("milligram") {
        0.000_001
    } else if normalized.contains("gram") || normalized == "g" {
        0.001
    } else if normalized.contains("pound") || normalized == "lb" {
        0.453_592_37
    } else if normalized.contains("ounce") || normalized == "oz" {
        0.028_349_523_125
    } else {
        return None;
    };

    Some(quantity.amount * factor)
}

fn select_best_quantity<'a>(
    claims: &'a [QuantityClaim],
    labels: &HashMap<String, String>,
) -> Option<&'a QuantityClaim> {
    let best = claims.iter().max_by(|left, right| {
        let left_score = quantity_claim_score(left, labels);
        let right_score = quantity_claim_score(right, labels);

        left_score.cmp(&right_score).then_with(|| {
            left.amount
                .partial_cmp(&right.amount)
                .unwrap_or(Ordering::Equal)
        })
    })?;

    if quantity_claim_score(best, labels) < 0 {
        None
    } else {
        Some(best)
    }
}

fn quantity_claim_score(quantity: &QuantityClaim, labels: &HashMap<String, String>) -> i32 {
    let mut score = 0;

    for qualifier_id in &quantity.qualifier_ids {
        let Some(label) = labels.get(qualifier_id) else {
            continue;
        };
        let normalized = label.trim().to_ascii_lowercase();

        if normalized.contains("adult") {
            score += 40;
        }
        if normalized.contains("average") || normalized.contains("typical") {
            score += 15;
        }
        if normalized.contains("male") || normalized.contains("female") {
            score += 10;
        }
        if normalized.contains("birth")
            || normalized.contains("newborn")
            || normalized.contains("juvenile")
            || normalized.contains("hatchling")
            || normalized.contains("larva")
            || normalized.contains("pupa")
            || normalized.contains("egg")
        {
            score -= 100;
        }
    }

    score
}

fn quantity_unit_label<'a>(
    quantity: &QuantityClaim,
    labels: &'a HashMap<String, String>,
) -> Option<&'a str> {
    quantity
        .unit_id
        .as_deref()
        .and_then(|id| labels.get(id))
        .map(String::as_str)
}

fn normalize_unit_label(unit: &str) -> String {
    unit.trim().to_ascii_lowercase().replace(['-', '_'], " ")
}

fn normalize_iucn_status(label: &str) -> String {
    match label.trim().to_ascii_lowercase().as_str() {
        "least concern" => "LC".to_string(),
        "near threatened" => "NT".to_string(),
        "vulnerable" => "VU".to_string(),
        "endangered" => "EN".to_string(),
        "critically endangered" => "CR".to_string(),
        "extinct in the wild" => "EW".to_string(),
        "extinct" => "EX".to_string(),
        "data deficient" => "DD".to_string(),
        "not evaluated" => "NE".to_string(),
        other => other.to_ascii_uppercase(),
    }
}

fn normalize_reproduction_modes(values: Vec<String>) -> Vec<String> {
    let mut normalized = Vec::new();

    for value in values {
        let Some(mode) = normalize_reproduction_mode(&value) else {
            continue;
        };

        if normalized
            .iter()
            .any(|existing: &String| existing.eq_ignore_ascii_case(&mode))
        {
            continue;
        }

        normalized.push(mode);
    }

    normalized
}

fn normalize_reproduction_mode(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    let lower = trimmed.to_ascii_lowercase();
    let simplified = match lower.as_str() {
        "sexual reproduction" => "Sexual".to_string(),
        "asexual reproduction" => "Asexual".to_string(),
        "vegetative reproduction" | "vegetative propagation" => "Vegetative".to_string(),
        _ => {
            let stripped = lower.strip_suffix(" reproduction").unwrap_or(&lower);
            title_case_words(stripped)
        }
    };

    Some(simplified)
}

fn title_case_words(value: &str) -> String {
    value
        .split_whitespace()
        .map(|word| {
            let mut chars = word.chars();
            let Some(first) = chars.next() else {
                return String::new();
            };

            let mut titled = String::with_capacity(word.len());
            titled.extend(first.to_uppercase());
            titled.push_str(&chars.as_str().to_ascii_lowercase());
            titled
        })
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

#[derive(Copy, Clone)]
enum MeasurementKind {
    Length,
    Mass,
}

fn extract_length_meters_from_wikitext(wikitext: &str) -> Option<f64> {
    extract_measurement_from_section(
        wikitext,
        "Size",
        &["Head-and-body length", "Body length", "Length"],
        MeasurementKind::Length,
    )
    .or_else(|| {
        extract_measurement_from_infobox(
            wikitext,
            &["head and body length", "body length", "length"],
            MeasurementKind::Length,
        )
    })
}

fn extract_height_meters_from_wikitext(wikitext: &str) -> Option<f64> {
    extract_measurement_from_section(
        wikitext,
        "Size",
        &["Shoulder height", "Height"],
        MeasurementKind::Length,
    )
    .or_else(|| {
        extract_measurement_from_infobox(
            wikitext,
            &["shoulder height", "height"],
            MeasurementKind::Length,
        )
    })
}

fn extract_mass_kilograms_from_wikitext(wikitext: &str) -> Option<f64> {
    extract_measurement_from_section(wikitext, "Size", &["Weight", "Mass"], MeasurementKind::Mass)
        .or_else(|| {
            extract_measurement_from_infobox(wikitext, &["weight", "mass"], MeasurementKind::Mass)
        })
}

fn extract_lifespan_years_from_wikitext(wikitext: &str) -> Option<f64> {
    extract_text_from_infobox(wikitext, &["lifespan", "life span", "longevity"])
        .and_then(extract_lifespan_years_from_text)
}

fn extract_measurement_from_section(
    wikitext: &str,
    section_name: &str,
    row_labels: &[&str],
    kind: MeasurementKind,
) -> Option<f64> {
    let section = extract_wikitext_section(wikitext, section_name)?;
    let mut lines = section.lines().peekable();

    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if !trimmed.starts_with('|') {
            continue;
        }

        let label = trimmed.trim_start_matches('|').trim();
        if !matches_any_label(label, row_labels) {
            continue;
        }

        let mut measurements = extract_template_measurements(trimmed, kind);
        while let Some(next_line) = lines.peek() {
            let next_trimmed = next_line.trim();
            if next_trimmed.starts_with("|-")
                || next_trimmed.starts_with("|}")
                || section_heading_name(next_trimmed).is_some()
                || (next_trimmed.starts_with('|') && !next_trimmed.starts_with("||"))
            {
                break;
            }

            measurements.extend(extract_template_measurements(next_trimmed, kind));
            lines.next();
        }

        if let Some(value) = representative_measurement(&measurements) {
            return Some(value);
        }
    }

    None
}

fn extract_measurement_from_infobox(
    wikitext: &str,
    field_names: &[&str],
    kind: MeasurementKind,
) -> Option<f64> {
    for line in wikitext.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with('|') {
            continue;
        }

        let Some((field_name, value)) = trimmed.trim_start_matches('|').split_once('=') else {
            continue;
        };

        if !matches_any_label(field_name, field_names) {
            continue;
        }

        let measurements = extract_measurements_from_text(value, kind);
        if let Some(value) = representative_measurement(&measurements) {
            return Some(value);
        }
    }

    None
}

fn extract_text_from_infobox<'a>(wikitext: &'a str, field_names: &[&str]) -> Option<&'a str> {
    for line in wikitext.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with('|') {
            continue;
        }

        let Some((field_name, value)) = trimmed.trim_start_matches('|').split_once('=') else {
            continue;
        };

        if matches_any_label(field_name, field_names) {
            return Some(value.trim());
        }
    }

    None
}

fn representative_measurement(measurements: &[f64]) -> Option<f64> {
    if measurements.is_empty() {
        return None;
    }

    Some(measurements.iter().sum::<f64>() / measurements.len() as f64)
}

fn extract_measurements_from_text(text: &str, kind: MeasurementKind) -> Vec<f64> {
    let mut measurements = extract_template_measurements(text, kind);
    measurements.extend(extract_plain_measurements(text, kind));
    measurements
}

fn extract_template_measurements(text: &str, kind: MeasurementKind) -> Vec<f64> {
    let mut measurements = Vec::new();
    let mut remainder = text;

    while let Some(start) = remainder.find("{{") {
        let template_body = &remainder[start + 2..];
        let Some(end) = template_body.find("}}") else {
            break;
        };

        let template = &template_body[..end];
        if let Some(value) = parse_conversion_template(template, kind) {
            measurements.push(value);
        }

        remainder = &template_body[end + 2..];
    }

    measurements
}

fn extract_plain_measurements(text: &str, kind: MeasurementKind) -> Vec<f64> {
    let mut values = Vec::new();
    let mut saw_range = false;

    for captures in feet_inches_range_regex().captures_iter(text) {
        let Some(start_feet) = captures.get(1).and_then(|m| m.as_str().parse::<f64>().ok()) else {
            continue;
        };
        let Some(start_inches) = captures.get(2).and_then(|m| m.as_str().parse::<f64>().ok())
        else {
            continue;
        };
        let Some(end_feet) = captures.get(3).and_then(|m| m.as_str().parse::<f64>().ok()) else {
            continue;
        };
        let Some(end_inches) = captures.get(4).and_then(|m| m.as_str().parse::<f64>().ok()) else {
            continue;
        };

        let start = convert_feet_inches_to_meters(start_feet, start_inches);
        let end = convert_feet_inches_to_meters(end_feet, end_inches);
        match kind {
            MeasurementKind::Length => {
                values.push((start + end) / 2.0);
                saw_range = true;
            }
            MeasurementKind::Mass => {}
        }
    }

    for captures in range_measurement_regex().captures_iter(text) {
        let Some(start) = captures
            .get(1)
            .and_then(|m| parse_decimal_token(m.as_str()))
        else {
            continue;
        };
        let Some(end) = captures
            .get(2)
            .and_then(|m| parse_decimal_token(m.as_str()))
        else {
            continue;
        };
        let Some(unit) = captures.get(3).map(|m| m.as_str()) else {
            continue;
        };
        let Some(start) = convert_measurement_value(start, unit, kind) else {
            continue;
        };
        let Some(end) = convert_measurement_value(end, unit, kind) else {
            continue;
        };
        values.push((start + end) / 2.0);
        saw_range = true;
    }

    if !saw_range {
        for captures in feet_inches_regex().captures_iter(text) {
            let Some(feet) = captures.get(1).and_then(|m| m.as_str().parse::<f64>().ok()) else {
                continue;
            };
            let Some(inches) = captures.get(2).and_then(|m| m.as_str().parse::<f64>().ok()) else {
                continue;
            };
            match kind {
                MeasurementKind::Length => values.push(convert_feet_inches_to_meters(feet, inches)),
                MeasurementKind::Mass => {}
            }
        }
    }

    if !saw_range {
        for captures in single_measurement_regex().captures_iter(text) {
            let Some(value) = captures
                .get(1)
                .and_then(|m| parse_decimal_token(m.as_str()))
            else {
                continue;
            };
            let Some(unit) = captures.get(2).map(|m| m.as_str()) else {
                continue;
            };
            if let Some(value) = convert_measurement_value(value, unit, kind) {
                values.push(value);
            }
        }
    }

    values
}

fn feet_inches_range_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(
            r"(?i)\b(\d+(?:\.\d+)?)\s*ft\s*(\d+(?:\.\d+)?)?\s*in(?:ches)?\s*(?:-|–|to)\s*(\d+(?:\.\d+)?)\s*ft\s*(\d+(?:\.\d+)?)?\s*in",
        )
        .expect("valid feet/inches range regex")
    })
}

fn feet_inches_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?i)\b(\d+(?:\.\d+)?)\s*ft\s*(\d+(?:\.\d+)?)\s*in")
            .expect("valid feet/inches regex")
    })
}

fn range_measurement_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(
            r"(?i)(?:^|[^\d.])(\d+(?:\.\d+)?)\s*(?:-|–|to)\s*(\d+(?:\.\d+)?)\s*(km|kilomet(?:re|er)s?|m|met(?:re|er)s?|cm|centimet(?:re|er)s?|mm|millimet(?:re|er)s?|ft|feet|foot|in|inch(?:es)?|yd|yard(?:s)?|kg|kilogram(?:s)?|g|gram(?:s)?|mg|milligram(?:s)?|lb|lbs|pound(?:s)?|oz|ounce(?:s)?|tonne(?:s)?|metric tons?)\b",
        )
        .expect("valid range measurement regex")
    })
}

fn single_measurement_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(
            r"(?i)(?:^|[^\d.])(\d+(?:\.\d+)?)\s*(km|kilomet(?:re|er)s?|m|met(?:re|er)s?|cm|centimet(?:re|er)s?|mm|millimet(?:re|er)s?|ft|feet|foot|in|inch(?:es)?|yd|yard(?:s)?|kg|kilogram(?:s)?|g|gram(?:s)?|mg|milligram(?:s)?|lb|lbs|pound(?:s)?|oz|ounce(?:s)?|tonne(?:s)?|metric tons?)\b",
        )
        .expect("valid single measurement regex")
    })
}

fn convert_feet_inches_to_meters(feet: f64, inches: f64) -> f64 {
    feet * 0.3048 + inches * 0.0254
}

fn parse_conversion_template(template: &str, kind: MeasurementKind) -> Option<f64> {
    if !template.starts_with("cvt|") && !template.starts_with("convert|") {
        return None;
    }

    let mut first = None;
    let mut second = None;
    let mut unit = None;

    for part in template.split('|').skip(1) {
        let token = part.trim();
        if token.is_empty() || token.contains('=') {
            continue;
        }

        if first.is_none() {
            first = parse_decimal_token(token);
            continue;
        }

        if second.is_none() {
            if is_range_separator(token) {
                continue;
            }
            if let Some(value) = parse_decimal_token(token) {
                second = Some(value);
                continue;
            }
        }

        if unit.is_none() && is_supported_unit(token, kind) {
            unit = Some(token);
            break;
        }
    }

    let first = first?;
    let unit = unit?;
    let start = convert_measurement_value(first, unit, kind)?;
    let end = second
        .and_then(|value| convert_measurement_value(value, unit, kind))
        .unwrap_or(start);
    Some((start + end) / 2.0)
}

fn extract_length_meters_from_text(text: &str) -> Option<f64> {
    select_text_measurement(
        text,
        MeasurementKind::Length,
        false,
        &["length", "long", "head body", "body length", "total length"],
        &["tail", "wingspan", "horn", "skull"],
    )
}

fn extract_height_meters_from_text(text: &str) -> Option<f64> {
    select_text_measurement(
        text,
        MeasurementKind::Length,
        true,
        &["height", "tall", "stands", "shoulder"],
        &["tail", "wingspan"],
    )
}

fn extract_mass_kilograms_from_text(text: &str) -> Option<f64> {
    select_text_measurement(
        text,
        MeasurementKind::Mass,
        false,
        &["weigh", "weight", "mass"],
        &["brain", "egg"],
    )
}

fn select_text_measurement(
    text: &str,
    kind: MeasurementKind,
    prefer_smallest: bool,
    required_keywords: &[&str],
    penalty_keywords: &[&str],
) -> Option<f64> {
    let mut best: Option<(i32, f64)> = None;

    for sentence in split_into_sentences(text) {
        let normalized = normalize_lookup_text(sentence);
        if normalized.is_empty() {
            continue;
        }

        let mut score = 0;
        if required_keywords
            .iter()
            .any(|keyword| normalized.contains(&normalize_lookup_text(keyword)))
        {
            score += 30;
        } else {
            continue;
        }

        for keyword in penalty_keywords {
            if normalized.contains(&normalize_lookup_text(keyword)) {
                score -= 20;
            }
        }

        if normalized.contains("adult")
            || normalized.contains("male")
            || normalized.contains("female")
        {
            score += 5;
        }

        let measurements = extract_plain_measurements(sentence, kind);
        if let Some(value) = select_measurement_for_dimension(&measurements, prefer_smallest) {
            match best {
                Some((best_score, _)) if best_score >= score => {}
                _ => best = Some((score, value)),
            }
        }
    }

    best.map(|(_, value)| value)
}

fn select_measurement_for_dimension(measurements: &[f64], prefer_smallest: bool) -> Option<f64> {
    if prefer_smallest {
        measurements
            .iter()
            .copied()
            .min_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal))
    } else {
        measurements
            .iter()
            .copied()
            .max_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal))
    }
}

fn is_range_separator(token: &str) -> bool {
    matches!(
        token.trim().to_ascii_lowercase().as_str(),
        "-" | "–" | "to" | "and"
    )
}

fn parse_decimal_token(token: &str) -> Option<f64> {
    let cleaned = token
        .trim()
        .trim_matches(|ch: char| ch == ',' || ch == ';' || ch == '.')
        .replace(',', "");
    cleaned.parse::<f64>().ok()
}

fn is_supported_unit(token: &str, kind: MeasurementKind) -> bool {
    convert_measurement_value(1.0, token, kind).is_some()
}

fn convert_measurement_value(value: f64, unit: &str, kind: MeasurementKind) -> Option<f64> {
    let normalized = normalize_lookup_text(unit);
    match kind {
        MeasurementKind::Length => {
            let factor = match normalized.as_str() {
                "km" | "kilometre" | "kilometer" | "kilometres" | "kilometers" => 1_000.0,
                "m" | "metre" | "meter" | "metres" | "meters" => 1.0,
                "cm" | "centimetre" | "centimeter" | "centimetres" | "centimeters" => 0.01,
                "mm" | "millimetre" | "millimeter" | "millimetres" | "millimeters" => 0.001,
                "ft" | "foot" | "feet" => 0.3048,
                "in" | "inch" | "inches" => 0.0254,
                "yd" | "yard" | "yards" => 0.9144,
                _ => return None,
            };
            Some(value * factor)
        }
        MeasurementKind::Mass => {
            let factor = match normalized.as_str() {
                "kg" | "kilogram" | "kilograms" => 1.0,
                "g" | "gram" | "grams" => 0.001,
                "mg" | "milligram" | "milligrams" => 0.000_001,
                "lb" | "lbs" | "pound" | "pounds" => 0.453_592_37,
                "oz" | "ounce" | "ounces" => 0.028_349_523_125,
                "tonne" | "tonnes" | "metric ton" | "metric tons" => 1_000.0,
                _ => return None,
            };
            Some(value * factor)
        }
    }
}

fn extract_lifespan_years_from_text(text: &str) -> Option<f64> {
    split_into_sentences(text)
        .into_iter()
        .filter_map(score_lifespan_sentence)
        .max_by(|left, right| left.0.cmp(&right.0))
        .map(|(_, years)| years)
}

fn score_lifespan_sentence(sentence: &str) -> Option<(i32, f64)> {
    let normalized = normalize_lookup_text(sentence);
    if normalized.is_empty()
        || normalized.contains("years ago")
        || normalized.contains("million years")
        || normalized.contains("thousand years")
    {
        return None;
    }

    let mentions_lifespan = normalized.contains("lifespan")
        || normalized.contains("life span")
        || normalized.contains("life expectancy");
    let mentions_living = normalized.contains("can live")
        || normalized.contains("live up to")
        || normalized.contains("live over")
        || normalized.contains("live for")
        || normalized.contains("lives up to")
        || normalized.contains("lives for");

    if !mentions_lifespan && !mentions_living {
        return None;
    }

    let tokens = tokenize_search_text(&normalized);
    let mut years = None;
    for (index, token) in tokens.iter().enumerate() {
        if token == "year" || token == "years" || token == "yr" || token == "yrs" {
            years = extract_year_count(&tokens, index);
            if years.is_some() {
                break;
            }
        }
    }

    let years = years?;
    let mut score = 10;
    if mentions_lifespan {
        score += 30;
    }
    if normalized.contains(" in the wild") || normalized.contains(" wild ") {
        score += 15;
    }
    if normalized.contains(" in captivity") || normalized.contains(" captive ") {
        score += 10;
    }
    if normalized.contains("up to") || normalized.contains("over") {
        score -= 5;
    }

    Some((score, years))
}

fn extract_year_count(tokens: &[String], year_index: usize) -> Option<f64> {
    let start = year_index.saturating_sub(6);
    let window = &tokens[start..year_index];

    for end in (1..=window.len()).rev() {
        for len in (1..=2).rev() {
            if end < len {
                continue;
            }

            let Some(right) = parse_number_phrase(&window[end - len..end]) else {
                continue;
            };
            if end >= len + 2 {
                let separator = window[end - len - 1].as_str();
                if matches!(separator, "to" | "and" | "or") {
                    for left_len in (1..=2).rev() {
                        if end < len + 1 + left_len {
                            continue;
                        }

                        let left_start = end - len - 1 - left_len;
                        if let Some(left) =
                            parse_number_phrase(&window[left_start..left_start + left_len])
                        {
                            return Some((left + right) / 2.0);
                        }
                    }
                }
            }

            return Some(right);
        }
    }

    None
}

fn parse_number_phrase(tokens: &[String]) -> Option<f64> {
    if tokens.is_empty() {
        return None;
    }

    if tokens.len() == 1 {
        return parse_number_token(&tokens[0]);
    }

    let mut total = 0.0;
    let mut matched_any = false;
    for token in tokens {
        let value = parse_number_word(token)?;
        matched_any = true;
        if (value - 100.0).abs() < f64::EPSILON {
            if total == 0.0 {
                total = 100.0;
            } else {
                total *= 100.0;
            }
        } else {
            total += value;
        }
    }

    matched_any.then_some(total)
}

fn parse_number_token(token: &str) -> Option<f64> {
    if let Some(value) = parse_decimal_token(token) {
        return Some(value);
    }

    for separator in ['-', '–'] {
        if let Some((left, right)) = token.split_once(separator) {
            let left = parse_number_token(left.trim())?;
            let right = parse_number_token(right.trim())?;
            return Some((left + right) / 2.0);
        }
    }

    parse_number_word(token)
}

fn parse_number_word(token: &str) -> Option<f64> {
    let normalized = token.trim().to_ascii_lowercase();
    let value = match normalized.as_str() {
        "zero" => 0.0,
        "one" => 1.0,
        "two" => 2.0,
        "three" => 3.0,
        "four" => 4.0,
        "five" => 5.0,
        "six" => 6.0,
        "seven" => 7.0,
        "eight" => 8.0,
        "nine" => 9.0,
        "ten" => 10.0,
        "eleven" => 11.0,
        "twelve" => 12.0,
        "thirteen" => 13.0,
        "fourteen" => 14.0,
        "fifteen" => 15.0,
        "sixteen" => 16.0,
        "seventeen" => 17.0,
        "eighteen" => 18.0,
        "nineteen" => 19.0,
        "twenty" => 20.0,
        "thirty" => 30.0,
        "forty" => 40.0,
        "fifty" => 50.0,
        "sixty" => 60.0,
        "seventy" => 70.0,
        "eighty" => 80.0,
        "ninety" => 90.0,
        "hundred" => 100.0,
        _ => return None,
    };
    Some(value)
}

fn infer_reproduction_modes_from_text(text: &str) -> Vec<String> {
    let normalized = normalize_lookup_text(text);
    let mut modes = Vec::new();

    if normalized.contains("asexual reproduction")
        || normalized.contains("binary fission")
        || normalized.contains("budding")
        || normalized.contains("parthenogenesis")
    {
        modes.push("Asexual".to_string());
    }

    if normalized.contains("vegetative reproduction")
        || normalized.contains("vegetative propagation")
    {
        modes.push("Vegetative".to_string());
    }

    if normalized.contains("sexual reproduction")
        || normalized.contains(" mating ")
        || normalized.contains(" mate ")
        || normalized.contains(" mates ")
        || normalized.contains("copulation")
        || normalized.contains("gestation")
        || normalized.contains("ovulation")
        || normalized.contains("pregnan")
        || normalized.contains("gives birth")
        || normalized.contains("lays eggs")
        || normalized.contains("ovipar")
        || normalized.contains("vivipar")
        || normalized.contains("spawn")
        || normalized.contains("spawning")
        || normalized.contains("clutch")
        || normalized.contains("incubat")
        || normalized.contains("brood")
        || normalized.contains("breeding season")
    {
        modes.push("Sexual".to_string());
    }

    normalize_reproduction_modes(modes)
}

fn extract_wikitext_section<'a>(wikitext: &'a str, heading: &str) -> Option<&'a str> {
    let target = normalize_lookup_text(heading);
    let mut offset = 0;
    let mut section_start = None;

    for line in wikitext.split_inclusive('\n') {
        let trimmed = line.trim();
        if let Some(name) = section_heading_name(trimmed) {
            if let Some(start) = section_start {
                return Some(wikitext[start..offset].trim());
            }

            if normalize_lookup_text(name) == target {
                section_start = Some(offset + line.len());
            }
        }

        offset += line.len();
    }

    section_start.map(|start| wikitext[start..].trim())
}

fn section_heading_name(line: &str) -> Option<&str> {
    let trimmed = line.trim();
    let leading = trimmed.chars().take_while(|ch| *ch == '=').count();
    let trailing = trimmed.chars().rev().take_while(|ch| *ch == '=').count();
    if leading < 2 || trailing != leading || trimmed.len() <= leading + trailing {
        return None;
    }

    let name = trimmed[leading..trimmed.len() - trailing].trim();
    (!name.is_empty()).then_some(name)
}

fn split_into_sentences(text: &str) -> Vec<&str> {
    let mut sentences = Vec::new();
    let mut start = 0usize;
    let chars = text.char_indices().collect::<Vec<_>>();

    for (index, ch) in &chars {
        if !matches!(ch, '.' | '!' | '?') {
            continue;
        }

        let prev_is_digit = text[..*index]
            .chars()
            .next_back()
            .is_some_and(|prev| prev.is_ascii_digit());
        let next_is_digit = text[index + ch.len_utf8()..]
            .chars()
            .next()
            .is_some_and(|next| next.is_ascii_digit());
        if *ch == '.' && prev_is_digit && next_is_digit {
            continue;
        }

        let sentence = text[start..*index].trim();
        if !sentence.is_empty() {
            sentences.push(sentence);
        }
        start = index + ch.len_utf8();
    }

    let tail = text[start..].trim();
    if !tail.is_empty() {
        sentences.push(tail);
    }

    sentences
}

fn tokenize_search_text(text: &str) -> Vec<String> {
    text.split_whitespace().map(ToOwned::to_owned).collect()
}

fn normalize_lookup_text(value: &str) -> String {
    let mut normalized = String::with_capacity(value.len());
    let mut previous_space = true;

    for ch in value.chars() {
        let mapped = if ch.is_ascii_alphanumeric() || ch == '-' {
            ch.to_ascii_lowercase()
        } else {
            ' '
        };

        if mapped == ' ' {
            if !previous_space {
                normalized.push(mapped);
            }
            previous_space = true;
        } else {
            normalized.push(mapped);
            previous_space = false;
        }
    }

    normalized.trim().to_string()
}

fn matches_any_label(value: &str, labels: &[&str]) -> bool {
    let normalized = normalize_lookup_text(value);
    labels
        .iter()
        .any(|label| normalize_lookup_text(label) == normalized)
}

fn simplify_wikitext(wikitext: &str) -> String {
    let mut simplified = String::with_capacity(wikitext.len());
    let mut chars = wikitext.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '[' && chars.peek() == Some(&'[') {
            chars.next();
            let mut link_text = String::new();
            while let Some(next) = chars.next() {
                if next == ']' && chars.peek() == Some(&']') {
                    chars.next();
                    break;
                }
                link_text.push(next);
            }

            let display = link_text.rsplit('|').next().unwrap_or_default();
            simplified.push_str(display);
            simplified.push(' ');
            continue;
        }

        if ch == '\'' {
            continue;
        }

        simplified.push(ch);
    }

    normalized_spacing(&simplified)
}

fn normalized_spacing(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

impl Default for WikipediaClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        extract_length_meters_from_text, extract_length_meters_from_wikitext,
        extract_lifespan_years_from_text, extract_mass_kilograms_from_text,
        extract_plain_measurements, infer_reproduction_modes_from_text, normalize_iucn_status,
        normalize_reproduction_modes, parse_quantity_claim, quantity_to_kilograms,
        quantity_to_meters, quantity_to_years, select_best_quantity, DataValue, MeasurementKind,
        QualifierSnak,
    };
    use std::collections::HashMap;

    #[test]
    fn parses_quantity_claims_with_unit_entity_ids() {
        let value = serde_json::json!({
            "amount": "+14",
            "unit": "http://www.wikidata.org/entity/Q577"
        });

        let quantity = parse_quantity_claim(&value, None).expect("quantity");
        assert_eq!(quantity.amount, 14.0);
        assert_eq!(quantity.unit_id.as_deref(), Some("Q577"));
    }

    #[test]
    fn converts_quantities_to_canonical_units() {
        let years = parse_quantity_claim(
            &serde_json::json!({
                "amount": "+18",
                "unit": "http://www.wikidata.org/entity/Q577"
            }),
            None,
        )
        .unwrap();
        let length = parse_quantity_claim(
            &serde_json::json!({
                "amount": "+250",
                "unit": "http://www.wikidata.org/entity/Q174728"
            }),
            None,
        )
        .unwrap();
        let mass = parse_quantity_claim(
            &serde_json::json!({
                "amount": "+4200",
                "unit": "http://www.wikidata.org/entity/Q41803"
            }),
            None,
        )
        .unwrap();

        let labels = HashMap::from([
            ("Q577".to_string(), "year".to_string()),
            ("Q174728".to_string(), "centimetre".to_string()),
            ("Q41803".to_string(), "gram".to_string()),
        ]);

        assert_eq!(quantity_to_years(&years, &labels), Some(18.0));
        assert_eq!(quantity_to_meters(&length, &labels), Some(2.5));
        assert_eq!(quantity_to_kilograms(&mass, &labels), Some(4.2));
    }

    #[test]
    fn prefers_adult_weight_over_birth_weight() {
        let birth = parse_quantity_claim(
            &serde_json::json!({
                "amount": "+1.65",
                "unit": "http://www.wikidata.org/entity/Q11570"
            }),
            Some(&HashMap::from([(
                "P3831".to_string(),
                vec![QualifierSnak {
                    datavalue: Some(DataValue {
                        value: serde_json::json!({
                            "entity-type": "item",
                            "numeric-id": 4128476,
                            "id": "Q4128476"
                        }),
                    }),
                }],
            )])),
        )
        .unwrap();
        let adult = parse_quantity_claim(
            &serde_json::json!({
                "amount": "+188",
                "unit": "http://www.wikidata.org/entity/Q11570"
            }),
            Some(&HashMap::from([
                (
                    "P3831".to_string(),
                    vec![QualifierSnak {
                        datavalue: Some(DataValue {
                            value: serde_json::json!({
                                "entity-type": "item",
                                "numeric-id": 78101716,
                                "id": "Q78101716"
                            }),
                        }),
                    }],
                ),
                (
                    "P21".to_string(),
                    vec![QualifierSnak {
                        datavalue: Some(DataValue {
                            value: serde_json::json!({
                                "entity-type": "item",
                                "numeric-id": 44148,
                                "id": "Q44148"
                            }),
                        }),
                    }],
                ),
            ])),
        )
        .unwrap();

        let labels = HashMap::from([
            ("Q11570".to_string(), "kilogram".to_string()),
            ("Q4128476".to_string(), "birth weight".to_string()),
            ("Q78101716".to_string(), "adult weight".to_string()),
            ("Q44148".to_string(), "male organism".to_string()),
        ]);

        let claims = [birth, adult];
        let best = select_best_quantity(&claims, &labels).expect("best quantity");
        assert_eq!(quantity_to_kilograms(best, &labels), Some(188.0));
    }

    #[test]
    fn normalizes_entity_labels_for_display() {
        assert_eq!(normalize_iucn_status("least concern"), "LC");
        assert_eq!(
            normalize_reproduction_modes(vec![
                "sexual reproduction".to_string(),
                "Vegetative reproduction".to_string(),
                "sexual reproduction".to_string(),
            ]),
            vec!["Sexual".to_string(), "Vegetative".to_string()]
        );
    }

    #[test]
    fn extracts_length_from_wikipedia_size_table() {
        let wikitext = r#"
===Size===
{| class="wikitable"
! Average !!Female lions !!Male lions
|-
|Head-and-body length
||{{cvt|160|-|184|cm}}
||{{cvt|184|-|208|cm}}
|}
"#;

        let length = extract_length_meters_from_wikitext(wikitext).expect("length");
        assert!((length - 1.84).abs() < 0.001, "unexpected length {length}");
    }

    #[test]
    fn extracts_lifespan_from_descriptive_text() {
        let text =
            "Lions can live over twenty years in captivity. They diverged 165,000 years ago.";
        assert_eq!(extract_lifespan_years_from_text(text), Some(20.0));
    }

    #[test]
    fn infers_reproduction_mode_from_life_cycle_text() {
        let text = "A lioness may mate with more than one male. The average gestation period is around 110 days.";
        assert_eq!(
            infer_reproduction_modes_from_text(text),
            vec!["Sexual".to_string()]
        );
    }

    #[test]
    fn extracts_length_from_plain_text_sentence() {
        let text = "The tiger reaches 1.4-2.8 m (4 ft 7 in - 9 ft 2 in) in head-and-body length and stands 0.8-1.1 m at the shoulder.";
        let measurements = extract_plain_measurements(text, MeasurementKind::Length);
        assert!(
            measurements.iter().all(|value| *value < 4.0),
            "unexpected measurements {measurements:?}"
        );
        let length = extract_length_meters_from_text(text).expect("length from text");
        assert!((length - 2.1).abs() < 0.05, "unexpected length {length}");
    }

    #[test]
    fn extracts_mass_from_plain_text_sentence() {
        let text = "Male Bengal tigers weigh 200-260 kg (440-570 lb), and females weigh 100-160 kg (220-350 lb).";
        let mass = extract_mass_kilograms_from_text(text).expect("mass from text");
        assert!((mass - 230.0).abs() < 1.0, "unexpected mass {mass}");
    }
}
