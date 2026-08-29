//! Adapter for the curated 2014 Tree of Sex dataset.
//!
//! The shipped asset contains only source rows that match BioDex's starter
//! roster. Every claim retains the archive row, literature citation, version,
//! and DOI so presentation code never has to infer biological facts.

use crate::species::{
    EvidenceMethod, Karyotype, ReproductiveMode, SexDetermination, SexualSystem, TraitEvidence,
    TraitScope, TraitSource, UnifiedSpecies,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::OnceLock;

pub const DATASET_NAME: &str = "Tree of Sex";

#[derive(Debug, Deserialize)]
struct Supplement {
    dataset: Dataset,
    records: Vec<Record>,
}

#[derive(Debug, Clone, Deserialize)]
struct Dataset {
    name: String,
    version: String,
    url: String,
    citation: String,
}

#[derive(Debug, Deserialize)]
struct Record {
    scientific_name: String,
    record_id: String,
    #[serde(default)]
    fields: Fields,
    #[serde(default)]
    citations: HashMap<String, String>,
    #[serde(default)]
    notes: String,
}

#[derive(Debug, Default, Deserialize)]
struct Fields {
    sexual_system: Option<String>,
    karyotype: Option<String>,
    genotypic: Option<String>,
    haplodiploidy: Option<String>,
    environmental: Option<String>,
    polyfactorial: Option<String>,
}

struct Index {
    dataset: Dataset,
    records: HashMap<String, Vec<Record>>,
}

static INDEX: OnceLock<Index> = OnceLock::new();

fn index() -> &'static Index {
    INDEX.get_or_init(|| {
        let payload = include_str!("../assets/tree_of_sex_supplement.json");
        let supplement: Supplement =
            serde_json::from_str(payload).expect("valid Tree of Sex supplement");
        let mut records: HashMap<String, Vec<Record>> = HashMap::new();
        for record in supplement.records {
            records
                .entry(record.scientific_name.clone())
                .or_default()
                .push(record);
        }
        Index {
            dataset: supplement.dataset,
            records,
        }
    })
}

/// Merge curated reproductive claims for a covered starter taxon.
pub fn apply_tree_of_sex_supplement(species: &mut UnifiedSpecies) {
    let supplement = index();
    let Some(records) = supplement
        .records
        .get(&species.scientific_name)
        .or_else(|| {
            supplement
                .records
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(&species.scientific_name))
                .map(|(_, records)| records)
        })
    else {
        return;
    };

    for record in records {
        apply_record(species, &supplement.dataset, record);
    }
}

fn apply_record(species: &mut UnifiedSpecies, dataset: &Dataset, record: &Record) {
    let scope = TraitScope::for_taxon(&species.scientific_name);
    let traits = &mut species.life_history.reproductive_traits;

    if let Some(raw) = record.fields.sexual_system.as_deref() {
        let source = source_for(dataset, record, "sexual_system");
        let normalized = raw.trim().to_ascii_lowercase();
        if normalized == "apomictic" {
            traits.reproductive_modes.add_claim(TraitEvidence::new(
                ReproductiveMode::Asexual,
                source.clone(),
                EvidenceMethod::CuratedLiterature,
                scope.clone(),
            ));
        } else {
            traits.reproductive_modes.add_claim(TraitEvidence::new(
                ReproductiveMode::Sexual,
                source.clone(),
                EvidenceMethod::CuratedLiterature,
                scope.clone(),
            ));
        }
        traits.sexual_systems.add_claim(TraitEvidence::new(
            map_sexual_system(raw, &record.notes),
            source,
            EvidenceMethod::CuratedLiterature,
            scope.clone(),
        ));
    }

    if let Some(raw) = record.fields.karyotype.as_deref() {
        let source = source_for(dataset, record, "karyotype");
        traits.karyotypes.add_claim(TraitEvidence::new(
            map_karyotype(raw),
            source.clone(),
            EvidenceMethod::CuratedLiterature,
            scope.clone(),
        ));
        traits.sex_determination.add_claim(TraitEvidence::new(
            SexDetermination::GeneticChromosomal,
            source,
            EvidenceMethod::CuratedLiterature,
            scope.clone(),
        ));
    }

    if let Some(raw) = record.fields.genotypic.as_deref() {
        let source = source_for(dataset, record, "genotypic");
        let determination = match raw.trim().to_ascii_lowercase().as_str() {
            "male heterogametic" | "female heterogametic" => SexDetermination::GeneticChromosomal,
            _ => SexDetermination::GeneticOther,
        };
        traits.sex_determination.add_claim(TraitEvidence::new(
            determination,
            source,
            EvidenceMethod::CuratedLiterature,
            scope.clone(),
        ));
    }

    if record.fields.haplodiploidy.is_some() {
        let source = source_for(dataset, record, "haplodiploidy");
        traits.sex_determination.add_claim(TraitEvidence::new(
            SexDetermination::Haplodiploid,
            source.clone(),
            EvidenceMethod::CuratedLiterature,
            scope.clone(),
        ));
        traits.karyotypes.add_claim(TraitEvidence::new(
            Karyotype::Haplodiploid,
            source,
            EvidenceMethod::CuratedLiterature,
            scope.clone(),
        ));
    }

    if let Some(raw) = record.fields.environmental.as_deref() {
        let normalized = raw.trim().to_ascii_lowercase();
        let determination = if normalized.starts_with("tsd") {
            SexDetermination::EnvironmentalTemperature
        } else {
            SexDetermination::EnvironmentalOther
        };
        traits.sex_determination.add_claim(TraitEvidence::new(
            determination,
            source_for(dataset, record, "environmental"),
            EvidenceMethod::CuratedLiterature,
            scope.clone(),
        ));
    }

    if record
        .fields
        .polyfactorial
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("yes"))
    {
        traits.sex_determination.add_claim(TraitEvidence::new(
            SexDetermination::Other("polyfactorial".to_string()),
            source_for(dataset, record, "polyfactorial"),
            EvidenceMethod::CuratedLiterature,
            scope,
        ));
    }
}

fn source_for(dataset: &Dataset, record: &Record, field: &str) -> TraitSource {
    TraitSource {
        dataset: dataset.name.clone(),
        record_id: Some(format!("{}:{field}", record.record_id)),
        url: Some(dataset.url.clone()),
        citation: Some(
            record
                .citations
                .get(field)
                .cloned()
                .unwrap_or_else(|| dataset.citation.clone()),
        ),
        version: Some(dataset.version.clone()),
        retrieved_at_unix: None,
    }
}

fn map_sexual_system(raw: &str, notes: &str) -> SexualSystem {
    let normalized = raw.trim().to_ascii_lowercase();
    let notes = notes.to_ascii_lowercase();
    match normalized.as_str() {
        "gonochorous" | "dioecy" | "dioecious" => SexualSystem::SeparateSexes,
        "hermaphrodite" if notes.contains("protandrous") => SexualSystem::Protandrous,
        "hermaphrodite" if notes.contains("protogynous") => SexualSystem::Protogynous,
        "hermaphrodite" => SexualSystem::SimultaneousHermaphrodite,
        "monoecy" | "monoecious" => SexualSystem::Monoecious,
        "gynodioecy" => SexualSystem::Gynodioecious,
        "androdioecy" => SexualSystem::Androdioecious,
        "trioecy" => SexualSystem::Trioecious,
        _ => SexualSystem::Other(raw.trim().to_string()),
    }
}

fn map_karyotype(raw: &str) -> Karyotype {
    match raw.trim().to_ascii_lowercase().as_str() {
        "xy" => Karyotype::XxXy,
        "xo" => Karyotype::XxX0,
        "zw" => Karyotype::ZzZw,
        "zo" => Karyotype::ZzZ0,
        "complex xy" | "complex zw" => Karyotype::ComplexMultiple,
        "homomorphic" => Karyotype::HomomorphicUndifferentiated,
        _ => Karyotype::Other(raw.trim().to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{apply_tree_of_sex_supplement, DATASET_NAME};
    use crate::species::{
        Distribution, EvidenceMethod, EvidenceState, ExternalIds, GenomeStats, LifeHistory,
        Taxonomy, UnifiedSpecies,
    };

    fn species(name: &str) -> UnifiedSpecies {
        UnifiedSpecies {
            scientific_name: name.to_string(),
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
            distribution: Distribution::default(),
            images: Vec::new(),
        }
    }

    #[test]
    fn mallard_maps_to_zw_with_full_provenance() {
        let mut mallard = species("Anas platyrhynchos");
        apply_tree_of_sex_supplement(&mut mallard);

        assert_eq!(
            mallard
                .life_history
                .reproductive_traits
                .sex_determination_summary()
                .as_deref(),
            Some("ZZ/ZW · genetic")
        );
        let claims = mallard.life_history.reproductive_traits.karyotypes.claims();
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].source.dataset, DATASET_NAME);
        assert_eq!(claims[0].source.version.as_deref(), Some("2014-05-19"));
        assert!(claims[0]
            .source
            .record_id
            .as_deref()
            .unwrap_or_default()
            .contains("vertebrate:"));
        assert!(claims[0]
            .source
            .citation
            .as_deref()
            .unwrap_or_default()
            .contains("Rutkowska"));
        assert_eq!(claims[0].method, EvidenceMethod::CuratedLiterature);
    }

    #[test]
    fn environmental_and_unknown_paths_remain_distinct() {
        let mut alligator = species("Alligator mississippiensis");
        apply_tree_of_sex_supplement(&mut alligator);
        assert_eq!(
            alligator
                .life_history
                .reproductive_traits
                .sex_determination_summary()
                .as_deref(),
            Some("temperature")
        );

        let mut unknown = species("Specius notincurateddata");
        apply_tree_of_sex_supplement(&mut unknown);
        assert_eq!(
            unknown
                .life_history
                .reproductive_traits
                .sex_determination
                .state(),
            EvidenceState::Unknown
        );
    }

    #[test]
    fn duplicate_source_rows_preserve_evidence_without_false_variability() {
        let mut buffalo = species("Bubalus bubalis");
        apply_tree_of_sex_supplement(&mut buffalo);
        let karyotypes = &buffalo.life_history.reproductive_traits.karyotypes;
        assert_eq!(karyotypes.state(), EvidenceState::Known);
        assert_eq!(karyotypes.claims().len(), 2);
    }
}
