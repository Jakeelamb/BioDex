use crate::species::{ImageInfo, UnifiedSpecies, CURRENT_LIFE_HISTORY_VERSION};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::OnceLock;

pub const CURATED_ANIMAL_TARGET: usize = 100;

pub const CURATED_ANIMAL_SPECIES: &[&str] = &[
    // Mammals
    "Panthera leo",
    "Panthera tigris",
    "Panthera pardus",
    "Panthera onca",
    "Acinonyx jubatus",
    "Felis catus",
    "Canis lupus",
    "Lynx lynx",
    "Ursus americanus",
    "Pongo abelii",
    "Macaca mulatta",
    "Equus quagga",
    "Equus asinus",
    "Bison bison",
    "Bubalus bubalis",
    "Cervus elaphus",
    "Odocoileus virginianus",
    "Megaptera novaeangliae",
    "Macropus rufus",
    "Ornithorhynchus anatinus",
    "Monodelphis domestica",
    "Mus musculus",
    "Rattus norvegicus",
    "Oryctolagus cuniculus",
    "Cavia porcellus",
    "Marmota marmota",
    "Meles meles",
    "Lutra lutra",
    "Enhydra lutris",
    "Rangifer tarandus",
    "Alces alces",
    "Camelus dromedarius",
    "Camelus bactrianus",
    "Lama glama",
    "Cavia aperea",
    "Bos taurus",
    "Mustela putorius",
    "Vombatus ursinus",
    "Sarcophilus harrisii",
    "Erinaceus europaeus",
    "Leptailurus serval",
    "Hyaena hyaena",
    "Suricata suricatta",
    "Ailuropoda melanoleuca",
    "Homo sapiens",
    "Pteropus vampyrus",
    "Rousettus aegyptiacus",
    "Saimiri sciureus",
    "Callithrix jacchus",
    "Chlorocebus sabaeus",
    "Pan troglodytes",
    "Diceros bicornis",
    "Phacochoerus africanus",
    "Marmota monax",
    // Birds
    "Meleagris gallopavo",
    "Gallus gallus",
    "Corvus brachyrhynchos",
    "Spheniscus demersus",
    "Aquila chrysaetos",
    "Haliaeetus leucocephalus",
    "Bubo bubo",
    "Strix aluco",
    "Taeniopygia guttata",
    "Serinus canaria",
    "Apteryx mantelli",
    "Calypte anna",
    "Cygnus atratus",
    "Anas platyrhynchos",
    "Anser anser",
    "Anser cygnoides",
    "Buteo jamaicensis",
    "Falco tinnunculus",
    "Ara ararauna",
    "Nymphicus hollandicus",
    "Colinus virginianus",
    "Coturnix japonica",
    "Anser albifrons",
    "Pica pica",
    "Cyanistes caeruleus",
    "Sturnus vulgaris",
    "Vanellus vanellus",
    "Corvus corax",
    "Ciconia ciconia",
    // Reptiles and amphibians
    "Naja naja",
    "Eublepharis macularius",
    "Caiman crocodilus",
    "Alligator mississippiensis",
    "Pantherophis guttatus",
    "Pogona vitticeps",
    "Podarcis muralis",
    "Varanus salvator",
    "Xenopus laevis",
    // Fishes
    "Lepisosteus oculatus",
    "Oncorhynchus mykiss",
    "Esox lucius",
    "Somniosus microcephalus (Bloch & Schneider, 1801)",
    "Sparus aurata",
    // Non-chordates
    "Homarus americanus",
    "Sepia officinalis",
    "Tursiops truncatus",
];

#[derive(Debug, Default, Deserialize)]
struct CuratedAnimalSourceFile {
    #[serde(default)]
    species: Vec<CuratedAnimalSourceRecord>,
}

#[derive(Debug, Default, Deserialize)]
struct CuratedAnimalSourceRecord {
    scientific_name: String,
    #[serde(default)]
    common_names: Vec<String>,
    #[serde(default)]
    images: Vec<CuratedAnimalImage>,
    #[serde(default)]
    taxonomy: CuratedAnimalTaxonomy,
    #[serde(default)]
    life_history: CuratedAnimalLifeHistory,
    #[serde(default)]
    genome: CuratedAnimalGenome,
}

#[derive(Debug, Default, Deserialize)]
struct CuratedAnimalImage {
    url: String,
    source: String,
    attribution: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct CuratedAnimalGenome {
    assembly_accession: Option<String>,
    assembly_name: Option<String>,
    genome_size_bp: Option<u64>,
    chromosome_count: Option<u32>,
    scaffold_count: Option<u32>,
    contig_count: Option<u32>,
    scaffold_n50: Option<u64>,
    contig_n50: Option<u64>,
    assembly_level: Option<String>,
    mito_genome_size_bp: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct CuratedAnimalTaxonomy {
    kingdom: Option<String>,
    phylum: Option<String>,
    class: Option<String>,
    order: Option<String>,
    family: Option<String>,
    genus: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct CuratedAnimalLifeHistory {
    lifespan_years: Option<f64>,
    length_meters: Option<f64>,
    height_meters: Option<f64>,
    mass_kilograms: Option<f64>,
    #[serde(default)]
    reproduction_modes: Vec<String>,
}

static CURATED_ANIMAL_SOURCE_INDEX: OnceLock<HashMap<String, CuratedAnimalSourceRecord>> =
    OnceLock::new();

fn curated_animal_sources() -> &'static HashMap<String, CuratedAnimalSourceRecord> {
    CURATED_ANIMAL_SOURCE_INDEX.get_or_init(|| {
        let payload = include_str!("../assets/curated_animal_sources.json");
        let parsed: CuratedAnimalSourceFile =
            serde_json::from_str(payload).expect("valid curated animal source supplement");
        parsed
            .species
            .into_iter()
            .map(|record| (record.scientific_name.to_ascii_lowercase(), record))
            .collect()
    })
}

pub fn canonical_curated_species_name(name: &str) -> Option<&'static str> {
    CURATED_ANIMAL_SPECIES
        .iter()
        .copied()
        .find(|candidate| candidate.eq_ignore_ascii_case(name.trim()))
}

pub fn apply_curated_animal_supplement(species: &mut UnifiedSpecies) {
    let Some(supplement) =
        curated_animal_sources().get(&species.scientific_name.to_ascii_lowercase())
    else {
        return;
    };

    for common_name in &supplement.common_names {
        if !species
            .common_names
            .iter()
            .any(|current| current.eq_ignore_ascii_case(common_name))
        {
            species.common_names.push(common_name.clone());
        }
    }

    for image in &supplement.images {
        if !species
            .images
            .iter()
            .any(|current| current.url == image.url)
        {
            species.images.push(ImageInfo {
                url: image.url.clone(),
                source: image.source.clone(),
                attribution: image.attribution.clone(),
            });
        }
    }

    if species.taxonomy.kingdom.is_none() {
        species.taxonomy.kingdom = supplement.taxonomy.kingdom.clone();
    }
    if species.taxonomy.phylum.is_none() {
        species.taxonomy.phylum = supplement.taxonomy.phylum.clone();
    }
    if species.taxonomy.class.is_none() {
        species.taxonomy.class = supplement.taxonomy.class.clone();
    }
    if species.taxonomy.order.is_none() {
        species.taxonomy.order = supplement.taxonomy.order.clone();
    }
    if species.taxonomy.family.is_none() {
        species.taxonomy.family = supplement.taxonomy.family.clone();
    }
    if species.taxonomy.genus.is_none() {
        species.taxonomy.genus = supplement.taxonomy.genus.clone();
    }

    if species.life_history.lifespan_years.is_none() {
        species.life_history.lifespan_years = supplement.life_history.lifespan_years;
    }
    if species.life_history.length_meters.is_none() {
        species.life_history.length_meters = supplement.life_history.length_meters;
    }
    if species.life_history.height_meters.is_none() {
        species.life_history.height_meters = supplement.life_history.height_meters;
    }
    if species.life_history.mass_kilograms.is_none() {
        species.life_history.mass_kilograms = supplement.life_history.mass_kilograms;
    }
    for mode in &supplement.life_history.reproduction_modes {
        if !species
            .life_history
            .reproduction_modes
            .iter()
            .any(|current| current.eq_ignore_ascii_case(mode))
        {
            species.life_history.reproduction_modes.push(mode.clone());
        }
    }

    if species.genome.assembly_accession.is_none() {
        species.genome.assembly_accession = supplement.genome.assembly_accession.clone();
    }
    if species.genome.assembly_name.is_none() {
        species.genome.assembly_name = supplement.genome.assembly_name.clone();
    }
    if species.genome.genome_size_bp.is_none() {
        species.genome.genome_size_bp = supplement.genome.genome_size_bp;
    }
    if species.genome.chromosome_count.is_none() {
        species.genome.chromosome_count = supplement.genome.chromosome_count;
    }
    if species.genome.scaffold_count.is_none() {
        species.genome.scaffold_count = supplement.genome.scaffold_count;
    }
    if species.genome.contig_count.is_none() {
        species.genome.contig_count = supplement.genome.contig_count;
    }
    if species.genome.scaffold_n50.is_none() {
        species.genome.scaffold_n50 = supplement.genome.scaffold_n50;
    }
    if species.genome.contig_n50.is_none() {
        species.genome.contig_n50 = supplement.genome.contig_n50;
    }
    if species.genome.assembly_level.is_none() {
        species.genome.assembly_level = supplement.genome.assembly_level.clone();
    }
    if species.genome.mito_genome_size_bp.is_none() {
        species.genome.mito_genome_size_bp = supplement.genome.mito_genome_size_bp;
    }

    if species.life_history.extraction_version < CURRENT_LIFE_HISTORY_VERSION
        && (species.life_history.lifespan_years.is_some()
            || species.life_history.length_meters.is_some()
            || species.life_history.height_meters.is_some()
            || species.life_history.mass_kilograms.is_some()
            || !species.life_history.reproduction_modes.is_empty())
    {
        species.life_history.extraction_version = CURRENT_LIFE_HISTORY_VERSION;
    }
}

#[cfg(test)]
mod tests {
    use super::apply_curated_animal_supplement;
    use crate::species::{
        Distribution, ExternalIds, GenomeStats, LifeHistory, Taxonomy, UnifiedSpecies,
    };

    #[test]
    fn curated_supplement_can_add_species_images() {
        let mut species = UnifiedSpecies {
            scientific_name: "Somniosus microcephalus (Bloch & Schneider, 1801)".to_string(),
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
        };

        apply_curated_animal_supplement(&mut species);

        assert!(
            species
                .images
                .iter()
                .any(|image| image.source == "Wikimedia Commons"),
            "expected curated image fallback to be attached",
        );
    }
}
