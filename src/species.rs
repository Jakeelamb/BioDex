//! Unified species types for aggregating data from multiple APIs

use serde::{Deserialize, Serialize};

pub const CURRENT_LIFE_HISTORY_VERSION: u8 = 3;

/// Aggregated species information from all data sources
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedSpecies {
    // Core identification
    pub scientific_name: String,
    pub common_names: Vec<String>,
    pub rank: String,

    // Taxonomy lineage
    pub taxonomy: Taxonomy,

    // External IDs for cross-referencing
    pub ids: ExternalIds,

    // Genome statistics
    pub genome: GenomeStats,

    // Life history statistics
    #[serde(default)]
    pub life_history: LifeHistory,

    // Descriptions
    pub description: Option<String>,
    pub wikipedia_extract: Option<String>,
    pub wikipedia_url: Option<String>,

    // Conservation
    pub conservation_status: Option<String>,
    pub iucn_status: Option<String>,

    // Occurrence data
    pub observations_count: Option<u64>,
    pub gbif_occurrences: Option<u64>,
    pub top_countries: Vec<CountryOccurrence>,

    // Geographic distribution
    pub distribution: Distribution,

    // Media
    pub images: Vec<ImageInfo>,
}

impl UnifiedSpecies {
    pub fn preferred_image_url(&self) -> Option<&str> {
        self.images
            .iter()
            .find(|img| img.source == "iNaturalist")
            .or_else(|| self.images.iter().find(|img| img.source == "Wikipedia"))
            .or_else(|| self.images.first())
            .map(|img| img.url.as_str())
    }
}

/// Taxonomic classification hierarchy
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Taxonomy {
    pub kingdom: Option<String>,
    pub phylum: Option<String>,
    pub class: Option<String>,
    pub order: Option<String>,
    pub family: Option<String>,
    pub genus: Option<String>,
    pub division: Option<String>,
    pub lineage: Vec<LineageEntry>,
}

/// External database identifiers
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExternalIds {
    pub ncbi_tax_id: Option<u64>,
    pub inat_id: Option<u64>,
    pub gbif_key: Option<u64>,
    pub wikidata_id: Option<String>,
    pub ensembl_id: Option<String>,
}

/// Genome assembly statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    /// Number of protein-coding genes
    pub coding_genes: Option<u64>,
    /// Number of non-coding genes
    pub noncoding_genes: Option<u64>,
    /// Number of pseudogenes
    pub pseudogenes: Option<u64>,
    /// Ensembl genebuild version/date
    pub genebuild: Option<String>,
}

/// Life history statistics from encyclopedic sources such as Wikidata
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct LifeHistory {
    /// Extraction/version marker for cached life-history stats
    pub extraction_version: u8,
    /// Typical or reported lifespan in years
    pub lifespan_years: Option<f64>,
    /// Typical body length in meters
    pub length_meters: Option<f64>,
    /// Typical body height in meters
    pub height_meters: Option<f64>,
    /// Typical body mass in kilograms
    pub mass_kilograms: Option<f64>,
    /// Reproduction strategies or modes
    pub reproduction_modes: Vec<String>,
}

impl From<crate::api::ncbi::GenomeStats> for GenomeStats {
    fn from(stats: crate::api::ncbi::GenomeStats) -> Self {
        Self {
            assembly_accession: stats.assembly_accession,
            assembly_name: stats.assembly_name,
            genome_size_bp: stats.genome_size_bp,
            chromosome_count: stats.chromosome_count,
            scaffold_count: stats.scaffold_count,
            contig_count: stats.contig_count,
            scaffold_n50: stats.scaffold_n50,
            contig_n50: stats.contig_n50,
            gc_percent: stats.gc_percent,
            assembly_level: stats.assembly_level,
            mito_genome_size_bp: stats.mito_genome_size_bp,
            is_reference: stats.is_reference,
            coding_genes: None,
            noncoding_genes: None,
            pseudogenes: None,
            genebuild: None,
        }
    }
}

impl GenomeStats {
    /// Merge Ensembl data into existing genome stats
    pub fn merge_ensembl(&mut self, ensembl: &crate::api::ensembl::EnsemblGenomeInfo) {
        // Only fill in missing data, prefer NCBI data
        if self.assembly_accession.is_none() {
            self.assembly_accession = ensembl.assembly_accession.clone();
        }
        if self.assembly_name.is_none() {
            self.assembly_name = ensembl.assembly_name.clone();
        }
        if self.genome_size_bp.is_none() {
            self.genome_size_bp = ensembl.base_pairs;
        }
        // Ensembl gene counts
        self.coding_genes = ensembl.coding_genes;
        self.noncoding_genes = ensembl.noncoding_genes;
        self.pseudogenes = ensembl.pseudogenes;
        self.genebuild = ensembl.genebuild.clone();
    }
}

impl Taxonomy {
    /// Build a clean, display-oriented lineage using canonical ranks.
    pub fn build_display_lineage(&self, scientific_name: &str, rank: &str) -> Vec<LineageEntry> {
        let mut lineage = Vec::with_capacity(7);

        self.push_display_entry(&mut lineage, self.kingdom.as_deref(), "Kingdom");
        self.push_display_entry(&mut lineage, self.phylum.as_deref(), "Phylum");
        self.push_display_entry(&mut lineage, self.class.as_deref(), "Class");
        self.push_display_entry(&mut lineage, self.order.as_deref(), "Order");
        self.push_display_entry(&mut lineage, self.family.as_deref(), "Family");
        self.push_display_entry(&mut lineage, self.genus.as_deref(), "Genus");
        self.push_current_taxon(&mut lineage, scientific_name, rank);

        lineage
    }

    fn push_display_entry(&self, lineage: &mut Vec<LineageEntry>, name: Option<&str>, rank: &str) {
        if let Some(name) = name {
            Self::push_unique_entry(lineage, name, rank);
        }
    }

    fn push_current_taxon(
        &self,
        lineage: &mut Vec<LineageEntry>,
        scientific_name: &str,
        rank: &str,
    ) {
        let normalized_rank = normalize_rank(rank);

        if scientific_name.trim().is_empty() {
            return;
        }

        match normalized_rank.as_str() {
            "Kingdom" | "Phylum" | "Class" | "Order" | "Family" | "Genus" => {
                if let Some(last) = lineage.last_mut() {
                    if last.rank == normalized_rank
                        && last.name.eq_ignore_ascii_case(scientific_name)
                    {
                        last.name = scientific_name.to_string();
                    }
                }
            }
            _ => Self::push_unique_entry(lineage, scientific_name, &normalized_rank),
        }
    }

    fn push_unique_entry(lineage: &mut Vec<LineageEntry>, name: &str, rank: &str) {
        if lineage.iter().any(|entry| {
            entry.rank.eq_ignore_ascii_case(rank) && entry.name.eq_ignore_ascii_case(name)
        }) {
            return;
        }

        lineage.push(LineageEntry {
            tax_id: 0,
            name: name.to_string(),
            rank: rank.to_string(),
        });
    }
}

impl LifeHistory {
    pub fn has_any_stats(&self) -> bool {
        self.lifespan_years.is_some()
            || self.length_meters.is_some()
            || self.height_meters.is_some()
            || self.mass_kilograms.is_some()
            || !self.reproduction_modes.is_empty()
    }

    pub fn is_current(&self) -> bool {
        self.extraction_version >= CURRENT_LIFE_HISTORY_VERSION
    }
}

fn normalize_rank(rank: &str) -> String {
    let trimmed = rank.trim();
    if trimmed.is_empty() {
        return "Species".to_string();
    }

    let mut chars = trimmed.chars();
    let Some(first) = chars.next() else {
        return "Species".to_string();
    };

    let mut normalized = String::with_capacity(trimmed.len());
    normalized.extend(first.to_uppercase());
    normalized.push_str(&chars.as_str().to_lowercase());
    normalized
}

/// A single entry in the taxonomic lineage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageEntry {
    pub tax_id: u64,
    pub name: String,
    pub rank: String,
}

/// Occurrence count by country
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountryOccurrence {
    pub country: String,
    pub count: u64,
}

/// Geographic distribution information
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Distribution {
    /// Continents where species occurs
    pub continents: Vec<String>,
    /// Geographic bounding box
    pub bounding_box: Option<BoundingBox>,
    /// Native range description (from Wikipedia/Wikidata)
    pub native_range: Option<String>,
}

/// Geographic bounding box
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundingBox {
    pub min_latitude: f64,
    pub max_latitude: f64,
    pub min_longitude: f64,
    pub max_longitude: f64,
}

/// Image information with source attribution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageInfo {
    pub url: String,
    pub source: String,
    pub attribution: Option<String>,
}

impl From<crate::api::ncbi::LineageEntry> for LineageEntry {
    fn from(entry: crate::api::ncbi::LineageEntry) -> Self {
        Self {
            tax_id: entry.tax_id,
            name: entry.name,
            rank: entry.rank,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_rank, ExternalIds, GenomeStats, ImageInfo, LifeHistory, Taxonomy, UnifiedSpecies,
        CURRENT_LIFE_HISTORY_VERSION,
    };

    fn test_species(images: Vec<ImageInfo>) -> UnifiedSpecies {
        UnifiedSpecies {
            scientific_name: "Panthera leo".to_string(),
            common_names: Vec::new(),
            rank: "species".to_string(),
            taxonomy: Taxonomy::default(),
            ids: ExternalIds::default(),
            genome: GenomeStats::default(),
            life_history: LifeHistory::default(),
            description: None,
            wikipedia_extract: None,
            wikipedia_url: None,
            conservation_status: None,
            iucn_status: None,
            observations_count: None,
            gbif_occurrences: None,
            top_countries: Vec::new(),
            distribution: super::Distribution::default(),
            images,
        }
    }

    #[test]
    fn builds_display_lineage_from_standard_taxonomy() {
        let taxonomy = Taxonomy {
            kingdom: Some("Animalia".to_string()),
            phylum: Some("Chordata".to_string()),
            class: Some("Mammalia".to_string()),
            order: Some("Carnivora".to_string()),
            family: Some("Felidae".to_string()),
            genus: Some("Panthera".to_string()),
            division: None,
            lineage: Vec::new(),
        };

        let lineage = taxonomy.build_display_lineage("Panthera leo", "species");
        let names = lineage
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>();
        let ranks = lineage
            .iter()
            .map(|entry| entry.rank.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            vec![
                "Animalia",
                "Chordata",
                "Mammalia",
                "Carnivora",
                "Felidae",
                "Panthera",
                "Panthera leo",
            ]
        );
        assert_eq!(
            ranks,
            vec!["Kingdom", "Phylum", "Class", "Order", "Family", "Genus", "Species",]
        );
    }

    #[test]
    fn avoids_duplicate_terminal_rank_entries() {
        let taxonomy = Taxonomy {
            kingdom: None,
            phylum: None,
            class: None,
            order: None,
            family: None,
            genus: Some("Panthera".to_string()),
            division: None,
            lineage: Vec::new(),
        };

        let lineage = taxonomy.build_display_lineage("Panthera", "genus");

        assert_eq!(lineage.len(), 1);
        assert_eq!(lineage[0].name, "Panthera");
        assert_eq!(lineage[0].rank, "Genus");
    }

    #[test]
    fn normalizes_rank_names_for_display() {
        assert_eq!(normalize_rank("species"), "Species");
        assert_eq!(normalize_rank("FAMILY"), "Family");
        assert_eq!(normalize_rank(""), "Species");
    }

    #[test]
    fn reports_whether_life_history_has_any_stats() {
        let mut life_history = LifeHistory::default();
        assert!(!life_history.has_any_stats());

        life_history.reproduction_modes.push("Sexual".to_string());
        assert!(life_history.has_any_stats());
    }

    #[test]
    fn tracks_life_history_version() {
        let mut life_history = LifeHistory::default();
        assert!(!life_history.is_current());

        life_history.extraction_version = CURRENT_LIFE_HISTORY_VERSION;
        assert!(life_history.is_current());
    }

    #[test]
    fn prefers_inaturalist_image_url() {
        let species = test_species(vec![
            ImageInfo {
                url: "https://example.com/wiki.jpg".to_string(),
                source: "Wikipedia".to_string(),
                attribution: None,
            },
            ImageInfo {
                url: "https://example.com/inat.jpg".to_string(),
                source: "iNaturalist".to_string(),
                attribution: None,
            },
        ]);

        assert_eq!(
            species.preferred_image_url(),
            Some("https://example.com/inat.jpg")
        );
    }
}
