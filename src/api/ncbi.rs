//! NCBI E-utilities API client for taxonomy and genome data
//!
//! Documentation: https://www.ncbi.nlm.nih.gov/books/NBK25499/

use super::{ApiError, Result};
use serde::Deserialize;

const BASE_URL: &str = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils";

pub struct NcbiClient {
    client: reqwest::Client,
}

#[derive(Debug, Clone)]
pub struct TaxonomyRecord {
    pub tax_id: u64,
    pub scientific_name: String,
    pub common_name: Option<String>,
    pub rank: String,
    pub division: String,
    pub lineage: Vec<LineageEntry>,
}

#[derive(Debug, Clone)]
pub struct LineageEntry {
    pub tax_id: u64,
    pub name: String,
    pub rank: String,
}

/// Genome assembly statistics from NCBI
#[derive(Debug, Clone, Default)]
pub struct GenomeStats {
    /// Assembly accession (e.g., GCF_000001405.40)
    pub assembly_accession: Option<String>,
    /// Assembly name (e.g., GRCh38.p14)
    pub assembly_name: Option<String>,
    /// Total genome size in base pairs
    pub genome_size_bp: Option<u64>,
    /// Number of chromosomes
    pub chromosome_count: Option<u32>,
    /// Number of scaffolds
    pub scaffold_count: Option<u32>,
    /// Number of contigs
    pub contig_count: Option<u32>,
    /// Scaffold N50 (quality metric)
    pub scaffold_n50: Option<u64>,
    /// Contig N50 (quality metric)
    pub contig_n50: Option<u64>,
    /// GC content percentage
    pub gc_percent: Option<f64>,
    /// Assembly level (Complete Genome, Chromosome, Scaffold, Contig)
    pub assembly_level: Option<String>,
    /// Mitochondrial genome size in base pairs
    pub mito_genome_size_bp: Option<u64>,
    /// Whether this is a reference genome
    pub is_reference: bool,
}

#[derive(Debug, Deserialize)]
struct ESearchResult {
    esearchresult: ESearchData,
}

#[derive(Debug, Deserialize)]
struct ESearchData {
    #[serde(default)]
    idlist: Vec<String>,
}

impl NcbiClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        }
    }

    /// Search for a taxon by name and return matching tax IDs
    pub async fn search_taxonomy(&self, query: &str) -> Result<Vec<u64>> {
        let url = format!(
            "{}/esearch.fcgi?db=taxonomy&term={}&retmode=json",
            BASE_URL,
            urlencoding::encode(query)
        );

        let response: ESearchResult = self.client.get(&url).send().await?.json().await?;

        let ids: Vec<u64> = response
            .esearchresult
            .idlist
            .iter()
            .filter_map(|id| id.parse().ok())
            .collect();

        if ids.is_empty() {
            return Err(ApiError::NotFound(query.to_string()));
        }

        Ok(ids)
    }

    /// Fetch detailed taxonomy information for a given tax ID
    pub async fn fetch_taxonomy(&self, tax_id: u64) -> Result<TaxonomyRecord> {
        let url = format!(
            "{}/efetch.fcgi?db=taxonomy&id={}&retmode=xml",
            BASE_URL, tax_id
        );

        let response = self.client.get(&url).send().await?.text().await?;

        self.parse_taxonomy_xml(&response, tax_id)
    }

    /// Search and fetch taxonomy in one call
    pub async fn get_taxonomy(&self, query: &str) -> Result<TaxonomyRecord> {
        let ids = self.search_taxonomy(query).await?;
        self.fetch_taxonomy(ids[0]).await
    }

    fn parse_taxonomy_xml(&self, xml: &str, tax_id: u64) -> Result<TaxonomyRecord> {
        // Parse XML manually since the NCBI format is complex
        let scientific_name = Self::extract_tag(xml, "ScientificName")
            .ok_or_else(|| ApiError::Api("Missing ScientificName".to_string()))?;

        let common_name = Self::extract_tag(xml, "CommonName");

        let rank = Self::extract_tag(xml, "Rank").unwrap_or_else(|| "unknown".to_string());

        let division = Self::extract_tag(xml, "Division").unwrap_or_else(|| "unknown".to_string());

        let lineage = self.parse_lineage(xml);

        Ok(TaxonomyRecord {
            tax_id,
            scientific_name,
            common_name,
            rank,
            division,
            lineage,
        })
    }

    fn extract_tag(xml: &str, tag: &str) -> Option<String> {
        let start_tag = format!("<{}>", tag);
        let end_tag = format!("</{}>", tag);

        let start = xml.find(&start_tag)? + start_tag.len();
        let end = xml[start..].find(&end_tag)? + start;

        Some(xml[start..end].trim().to_string())
    }

    fn parse_lineage(&self, xml: &str) -> Vec<LineageEntry> {
        let mut entries = Vec::new();

        // Find LineageEx section
        let lineage_start = match xml.find("<LineageEx>") {
            Some(pos) => pos,
            None => return entries,
        };
        let lineage_end = match xml[lineage_start..].find("</LineageEx>") {
            Some(pos) => lineage_start + pos,
            None => return entries,
        };
        let lineage_xml = &xml[lineage_start..lineage_end];

        // Parse each Taxon in lineage
        let mut pos = 0;
        while let Some(taxon_start) = lineage_xml[pos..].find("<Taxon>") {
            let taxon_start = pos + taxon_start;
            let taxon_end = match lineage_xml[taxon_start..].find("</Taxon>") {
                Some(end) => taxon_start + end,
                None => break,
            };
            let taxon_xml = &lineage_xml[taxon_start..taxon_end];

            if let (Some(id_str), Some(name), Some(rank)) = (
                Self::extract_tag(taxon_xml, "TaxId"),
                Self::extract_tag(taxon_xml, "ScientificName"),
                Self::extract_tag(taxon_xml, "Rank"),
            ) {
                if let Ok(tax_id) = id_str.parse() {
                    entries.push(LineageEntry { tax_id, name, rank });
                }
            }

            pos = taxon_end;
        }

        entries
    }

    /// Search for genome assemblies by organism name
    pub async fn search_assemblies(&self, organism: &str) -> Result<Vec<String>> {
        let query = format!("{}[Organism]", organism);
        self.search_assemblies_by_query(&query, organism).await
    }

    /// Search for genome assemblies by NCBI taxonomy id.
    pub async fn search_assemblies_by_tax_id(&self, tax_id: u64) -> Result<Vec<String>> {
        let query = format!("txid{}[Organism:exp]", tax_id);
        self.search_assemblies_by_query(&query, &tax_id.to_string())
            .await
    }

    async fn search_assemblies_by_query(&self, query: &str, label: &str) -> Result<Vec<String>> {
        let url = format!(
            "{}/esearch.fcgi?db=assembly&term={}&retmode=json&retmax=10",
            BASE_URL,
            urlencoding::encode(query)
        );

        let response: ESearchResult = self.client.get(&url).send().await?.json().await?;

        if response.esearchresult.idlist.is_empty() {
            return Err(ApiError::NotFound(label.to_string()));
        }

        Ok(response.esearchresult.idlist)
    }

    /// Get genome statistics for an organism
    pub async fn get_genome_stats(&self, organism: &str) -> Result<GenomeStats> {
        let assembly_ids = match self.search_assemblies(organism).await {
            Ok(ids) => ids,
            Err(_) => return Ok(GenomeStats::default()),
        };

        self.get_genome_stats_from_assemblies(&assembly_ids, organism)
            .await
    }

    /// Get genome statistics using an NCBI taxonomy id, falling back to organism name when needed.
    pub async fn get_genome_stats_by_tax_id(
        &self,
        tax_id: u64,
        organism: &str,
    ) -> Result<GenomeStats> {
        let assembly_ids = match self.search_assemblies_by_tax_id(tax_id).await {
            Ok(ids) => ids,
            Err(_) => return self.get_genome_stats(organism).await,
        };

        self.get_genome_stats_from_assemblies(&assembly_ids, organism)
            .await
    }

    async fn get_genome_stats_from_assemblies(
        &self,
        assembly_ids: &[String],
        organism: &str,
    ) -> Result<GenomeStats> {
        if assembly_ids.is_empty() {
            return Ok(GenomeStats::default());
        }

        // Small delay to avoid NCBI rate limiting (3 requests/sec without API key)
        tokio::time::sleep(std::time::Duration::from_millis(350)).await;

        // Fetch assembly summaries - try to get the best (reference) assembly
        let ids_param = assembly_ids.join(",");
        let url = format!(
            "{}/esummary.fcgi?db=assembly&id={}&retmode=json",
            BASE_URL, ids_param
        );

        let response: serde_json::Value = self.client.get(&url).send().await?.json().await?;

        // Parse assembly summaries and find the best one
        let result = response
            .get("result")
            .ok_or_else(|| ApiError::Api("Missing result in assembly response".to_string()))?;

        let mut best_stats: Option<GenomeStats> = None;
        let mut best_score = 0;

        for id in assembly_ids {
            if let Some(doc) = result.get(id) {
                let stats = self.parse_assembly_summary(doc);

                // Score assemblies: prefer reference genomes and higher assembly levels
                let mut score = 0;
                if stats.is_reference {
                    score += 100;
                }
                match stats.assembly_level.as_deref() {
                    Some("Complete Genome") => score += 50,
                    Some("Chromosome") => score += 40,
                    Some("Scaffold") => score += 20,
                    Some("Contig") => score += 10,
                    _ => {}
                }
                if stats.genome_size_bp.is_some() {
                    score += 5;
                }
                if stats.chromosome_count.is_some() {
                    score += 5;
                }

                if score > best_score {
                    best_score = score;
                    best_stats = Some(stats);
                }
            }
        }

        let mut stats = best_stats.unwrap_or_default();

        // Small delay before mito lookup
        tokio::time::sleep(std::time::Duration::from_millis(350)).await;

        // Try to get mitochondrial genome size
        if let Ok(mito_size) = self.get_mito_genome_size(organism).await {
            stats.mito_genome_size_bp = Some(mito_size);
        }

        Ok(stats)
    }

    /// Get mitochondrial genome size for an organism
    async fn get_mito_genome_size(&self, organism: &str) -> Result<u64> {
        // Search nuccore for complete mitochondrial genome
        let query = format!(
            "{}[Organism] AND mitochondrion[Title] AND complete genome[Title]",
            organism
        );
        let url = format!(
            "{}/esearch.fcgi?db=nuccore&term={}&retmode=json&retmax=1",
            BASE_URL,
            urlencoding::encode(&query)
        );

        let response: ESearchResult = self.client.get(&url).send().await?.json().await?;

        if response.esearchresult.idlist.is_empty() {
            return Err(ApiError::NotFound("mitochondrial genome".to_string()));
        }

        // Small delay before fetching summary
        tokio::time::sleep(std::time::Duration::from_millis(350)).await;

        // Get the sequence summary
        let nuc_id = &response.esearchresult.idlist[0];
        let url = format!(
            "{}/esummary.fcgi?db=nuccore&id={}&retmode=json",
            BASE_URL, nuc_id
        );

        let response: serde_json::Value = self.client.get(&url).send().await?.json().await?;

        // Extract sequence length
        response
            .get("result")
            .and_then(|r| r.get(nuc_id))
            .and_then(|doc| doc.get("slen"))
            .and_then(|v| v.as_u64())
            .ok_or_else(|| ApiError::Api("Could not parse mito genome size".to_string()))
    }

    fn parse_assembly_summary(&self, doc: &serde_json::Value) -> GenomeStats {
        let get_str = |key: &str| -> Option<String> {
            doc.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
        };

        let get_u32 = |key: &str| -> Option<u32> {
            doc.get(key).and_then(|v| {
                v.as_u64()
                    .map(|n| n as u32)
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
        };

        // Check if this is a reference genome
        let is_reference = doc
            .get("refseq_category")
            .and_then(|v| v.as_str())
            .map(|s| s.contains("reference"))
            .unwrap_or(false);

        // Parse chromosome count from "meta" field if available
        let chromosome_count = doc
            .get("meta")
            .and_then(|v| v.as_str())
            .and_then(|meta| {
                // Look for chromosome count in meta string
                // Format varies but often contains "<Stat category="chromosome_count" ...>N</Stat>"
                if let Some(start) = meta.find("chromosome_count") {
                    let after = &meta[start..];
                    if let Some(gt) = after.find('>') {
                        let after_gt = &after[gt + 1..];
                        if let Some(lt) = after_gt.find('<') {
                            return after_gt[..lt].trim().parse().ok();
                        }
                    }
                }
                None
            })
            .or_else(|| {
                // Try alternative: count from stats
                get_u32("chrcount")
            });

        // Parse total sequence length (genome size)
        let genome_size_bp = doc.get("meta").and_then(|v| v.as_str()).and_then(|meta| {
            // Look for total_length in meta
            if let Some(start) = meta.find("total_length") {
                let after = &meta[start..];
                if let Some(gt) = after.find('>') {
                    let after_gt = &after[gt + 1..];
                    if let Some(lt) = after_gt.find('<') {
                        return after_gt[..lt].trim().parse().ok();
                    }
                }
            }
            None
        });

        // Parse scaffold N50
        let scaffold_n50 = doc.get("meta").and_then(|v| v.as_str()).and_then(|meta| {
            if let Some(start) = meta.find("scaffold_n50") {
                let after = &meta[start..];
                if let Some(gt) = after.find('>') {
                    let after_gt = &after[gt + 1..];
                    if let Some(lt) = after_gt.find('<') {
                        return after_gt[..lt].trim().parse().ok();
                    }
                }
            }
            None
        });

        // Parse contig N50
        let contig_n50 = doc.get("meta").and_then(|v| v.as_str()).and_then(|meta| {
            if let Some(start) = meta.find("contig_n50") {
                let after = &meta[start..];
                if let Some(gt) = after.find('>') {
                    let after_gt = &after[gt + 1..];
                    if let Some(lt) = after_gt.find('<') {
                        return after_gt[..lt].trim().parse().ok();
                    }
                }
            }
            None
        });

        // Parse scaffold count
        let scaffold_count = doc.get("meta").and_then(|v| v.as_str()).and_then(|meta| {
            if let Some(start) = meta.find("scaffold_count") {
                let after = &meta[start..];
                if let Some(gt) = after.find('>') {
                    let after_gt = &after[gt + 1..];
                    if let Some(lt) = after_gt.find('<') {
                        return after_gt[..lt].trim().parse().ok();
                    }
                }
            }
            None
        });

        // Parse contig count
        let contig_count = doc.get("meta").and_then(|v| v.as_str()).and_then(|meta| {
            if let Some(start) = meta.find("contig_count") {
                let after = &meta[start..];
                if let Some(gt) = after.find('>') {
                    let after_gt = &after[gt + 1..];
                    if let Some(lt) = after_gt.find('<') {
                        return after_gt[..lt].trim().parse().ok();
                    }
                }
            }
            None
        });

        // Parse GC percent
        let gc_percent = doc.get("meta").and_then(|v| v.as_str()).and_then(|meta| {
            if let Some(start) = meta.find("gc_perc") {
                let after = &meta[start..];
                if let Some(gt) = after.find('>') {
                    let after_gt = &after[gt + 1..];
                    if let Some(lt) = after_gt.find('<') {
                        return after_gt[..lt].trim().parse().ok();
                    }
                }
            }
            None
        });

        GenomeStats {
            assembly_accession: get_str("assemblyaccession")
                .or_else(|| get_str("rsuid"))
                .or_else(|| get_str("gbuid")),
            assembly_name: get_str("assemblyname"),
            genome_size_bp,
            chromosome_count,
            scaffold_count,
            contig_count,
            scaffold_n50,
            contig_n50,
            gc_percent,
            assembly_level: get_str("assemblystatus"),
            mito_genome_size_bp: None, // Set separately
            is_reference,
        }
    }
}

impl Default for NcbiClient {
    fn default() -> Self {
        Self::new()
    }
}
