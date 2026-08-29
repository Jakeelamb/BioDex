# Reproductive traits and field metrics

Research snapshot: 2026-08-28

## Recommendation

BioDex can responsibly show chromosome/mitochondrial assembly metrics now, and it can often show reproductive traits, but there is no current universal species API for sex-determination systems. The product should therefore:

1. Surface genome values on the main profile as **assembly-scoped** facts: `ASSEMBLY CHR`, `MT LENGTH`, assembly accession, and assembly level.
2. Add a provenance-aware reproductive-traits model rather than another optional string.
3. Use a curated, locally indexed dataset for sex systems (initially Tree of Sex, explicitly labeled as a 2014 literature snapshot), then layer current taxon-specific datasets such as HerpSexDet.
4. Keep Wikidata as opportunistic enrichment, not the primary source. Its relevant fields are structured but sparse.
5. Never infer a species' sex system or reproductive mode from occurrence-record sex, a Wikipedia paragraph, or the mere presence of X/Y/Z/W-named assembly sequences.

The main UI could use a compact block such as:

```text
REPRO      SEXUAL · OVIPAROUS
SEX SYS    ZZ/ZW · GENETIC              [TREE OF SEX · 2014]
MATURITY   UNKNOWN
ASSEMBLY   40 CHR · MT 16.6 kb          [GCF_…]
```

`UNKNOWN`, `VARIABLE`, `INHERITED`, and `NOT APPLICABLE` are scientifically meaningful states, not missing polish.

## What the existing upstreams can actually provide

| Upstream | Sex-determination system | Reproductive traits | Other useful structured metrics | Assessment |
|---|---|---|---|---|
| NCBI Taxonomy / Datasets | No species-level XY/ZW/XO/environmental field. The assembly report's `sex` is explicitly the **physical sex of the sampled organism**, not the species' sex system. | No general reproductive-mode fields. | Assembly accession/level, total sequence length, assembly chromosome count, contigs/scaffolds/N50, GC, annotated gene counts, BUSCO, and organelle length. | Best immediate source for genome cards, but all values must retain the chosen assembly accession. [NCBI assembly schema](https://www.ncbi.nlm.nih.gov/datasets/docs/v2/reference-docs/data-reports/genome-assembly/) |
| Ensembl | No declared species-level sex-system field. Chromosome and karyotype-band data describe the represented assembly. | None general. | Assembly accession/name, top-level sequences/chromosomes, cytogenetic bands, base count, genebuild, taxonomy ID, and variation/alignment availability. | Useful assembly cross-check/fallback, not reproductive evidence. [assembly endpoint](https://rest.ensembl.org/documentation/info/assembly_info), [genome endpoint](https://rest.ensembl.org/documentation/info/info_genome) |
| GBIF | `sex` is the sex of individuals represented by an occurrence. It cannot establish the species' possible sexes or determination mechanism. | `reproductiveCondition` is likewise an occurrence-level state, not oviparity/viviparity or sexual/asexual reproduction. | Georeferenced occurrences, observation/collection time, basis of record, life stage, country/range summaries. | Good field-observation signal. Never promote occurrence aggregation to a species trait. Darwin Core explicitly models sex, life stage, and reproductive condition as state that can differ between occurrences. [Darwin Core conceptual model](https://dwc.tdwg.org/cm/), [GBIF download fields](https://techdocs.gbif.org/en/data-use/download-formats) |
| iNaturalist | No taxon-level sex-determination field in the public taxon API. Observation annotations may describe individual observations. | No broadly structured taxon-level reproduction model. | Observation count, photos, common names, lineage, and conservation fields. | Keep for portrait and observation signal. Do not infer reproductive traits. [iNaturalist API](https://www.inaturalist.org/api) |
| Wikidata | It contains items for XY, ZW, and related systems, but no dedicated, well-populated taxon property was found for sex determination. | `mode of reproduction` (P13318) is the right structured property, including sexual/asexual and oviparity-style values, but its own instructions place general facts at high taxonomic ranks. It currently has only 61 uses, so direct species lookup will usually be empty. | Gestation (P3063), litter/clutch size (P7725), egg incubation (P7770), and the new age-at-sexual-maturity property (P12432). Current property pages report 578, 6,801, 2,417, and 19 total uses respectively: useful when present, not broad coverage. | Opportunistic enrichment with statement rank, qualifiers, references, retrieval time, and explicit `inherited from taxon` handling. [P13318 documentation and usage](https://www.wikidata.org/wiki/Property_talk:P13318), [P3063](https://www.wikidata.org/wiki/Property_talk:P3063), [P7725](https://www.wikidata.org/wiki/Property_talk:P7725), [P7770](https://www.wikidata.org/wiki/Property_talk:P7770), [P12432](https://www.wikidata.org/wiki/Property_talk:P12432) |
| Wikipedia | No stable structured schema for these traits. | Narrative text may mention modes, but extraction loses claim-level scope and evidence. | Descriptions and human-readable context. | Show prose as prose. Text-mined values should be labeled candidates and must not silently become facts. |

### Genome-labeling caution

NCBI defines `totalNumberOfChromosomes` as a count in the submitted assembly and says that it can include nuclear chromosomes, organelles, and plasmids. Its sequence report describes the actual assembly records and identifies `Chromosome`, `Mitochondrion`, and unknown molecules. These are not automatically the biological diploid number (`2n`). BioDex should rename the current generic `chromosome_count` presentation to **assembly chromosome count** unless a cytogenetic source explicitly supplies a karyotype. [NCBI assembly report](https://www.ncbi.nlm.nih.gov/datasets/docs/v2/reference-docs/data-reports/genome-assembly/), [NCBI sequence report](https://www.ncbi.nlm.nih.gov/datasets/docs/v2/reference-docs/data-reports/genome-sequence/)

The mitochondrial size should also come from the organelle record associated with the selected assembly where possible. NCBI's assembly schema includes `OrganelleInfo.totalSeqLength`, and its sequence report identifies mitochondrial records. A free-text search for the first "complete mitochondrial genome" can select a different isolate or population.

## Authoritative additions worth integrating

### 1. Tree of Sex: best broad starting snapshot

The Tree of Sex dataset directly models sexual system, sex-determination mechanism, sex-chromosome system/karyotype, chromosome number, and within-species variation. Its published snapshot covers 11,038 plants, 709 fish, 173 amphibians, 593 non-avian reptiles, 195 birds, 479 mammals, and 11,556 invertebrates. Records retain literature sources, and the compilers state that species traits were not inferred from higher taxa unless the source explicitly listed the species. [Dryad dataset and files](https://datadryad.org/dataset/doi:10.5061/dryad.v1908), [database methods and validation](https://www.nature.com/articles/sdata201415)

Limitations are important:

- the downloadable snapshot is from May 2014;
- coverage is taxonomically uneven and deliberately focused on variable groups;
- taxonomy has drifted since publication;
- multiple records can reflect real population variation or disagreement.

Recommendation: ship it as a versioned, offline enrichment table after reconciling scientific names through stable taxon IDs and synonyms. Display its snapshot date and original citation. Do not overwrite newer evidence.

### 2. HerpSexDet: stronger current authority for amphibians and reptiles

HerpSexDet contains genetic and temperature-dependent determination, sex reversal, polyploidy, taxonomy, and source references for 192 amphibian and 697 reptile species. Its TSV and separate references/metadata are designed for reuse, and entries were manually extracted and double-checked. It is a high-value taxon-specific override/addition to Tree of Sex, not a global provider. [HerpSexDet data description](https://pmc.ncbi.nlm.nih.gov/articles/PMC10264413/)

### 3. FishBase: rich reproduction model, fish only

FishBase's REPRODUCTION table has controlled values for dioecism, protandry, protogyny, true hermaphroditism, and parthenogenesis, plus fertilization location, spawning frequency, batch spawning, reproductive guild, parental care, and life-cycle text. Separate tables hold population-scoped age/size at maturity, spawning season, sex ratio, and fecundity. FishBase itself warns that missing reproduction records must not be assumed to mean dioecy. [FishBase REPRODUCTION schema](https://www.fishbase.se/manual/fishbasethe_reproduction_table.htm), [MATURITY table](https://www.fishbase.se/manual/English/fishbasethe_maturity_table.htm), [SPAWNING table](https://fishbase.se/manual/english/fishbasethe_SPAWNING_table.htm)

Recommendation: integrate only after confirming a stable machine-access contract and reuse terms. Preserve population/locality scope for maturity and spawning; those values are not necessarily species constants.

### 4. AnAge: easiest high-value life-history expansion

AnAge is a curated animal database with downloadable tabular data. Its field survey exposes adult weight, sexual maturity, gestation, litter/clutch size, and longevity. The current statistics page reports 4,645 species and 3,946 species with at least one life-history trait. It is not a sex-determination database, but it is probably the highest-value next source for the main profile. [AnAge overview](https://www.genomics.senescence.info/species/index.html), [coverage statistics](https://genomics.senescence.info/species/stats.php), [download page](https://www.genomics.senescence.info/download.html)

HAGR permits reuse, including commercial reuse, under CC BY 3.0 with attribution, and asks bulk users to use the download rather than scrape the site. [HAGR terms](https://genomics.senescence.info/legal.html)

### 5. EOL TraitBank: broad aggregator, later phase

EOL TraitBank aggregates structured organism attributes and ecological interactions with per-record provenance. Its structured API supports graph queries but requires a key; bulk trait archives are also available. This can add habitat, trophic and life-history traits across taxa, but BioDex must retain each content partner's attribution/license rather than citing EOL alone. [EOL data services](https://www.eol.org/docs/what-is-eol/data-services), [TraitBank model](https://www.eol.org/traitbank), [API terms](https://eol.org/docs/what-is-eol/terms-of-use-for-eol-application-programming-interfaces)

## Canonical data model

Do not flatten the domain into `reproduction_modes: Vec<String>`. These concepts are independent and can each vary:

```rust
struct ReproductiveTraits {
    reproductive_modes: EvidenceSet<ReproductiveMode>,
    sexual_systems: EvidenceSet<SexualSystem>,
    sex_determination: EvidenceSet<SexDetermination>,
    karyotypes: EvidenceSet<Karyotype>,
    offspring_development: EvidenceSet<OffspringDevelopment>,
    fertilization: EvidenceSet<FertilizationMode>,
    maturity_age: EvidenceSet<MeasuredRange>,
    gestation_or_incubation: EvidenceSet<MeasuredRange>,
    litter_or_clutch_size: EvidenceSet<MeasuredRange>,
}

enum Knowledge<T> {
    Known(T),
    Variable(Vec<ScopedValue<T>>),
    NotApplicable { reason: String },
    Unknown,
}

struct Evidence<T> {
    value: Knowledge<T>,
    scope: Scope,              // species, subspecies, population, sex, life stage
    source: SourceRef,         // dataset, record/statement ID, citation, URL
    method: EvidenceMethod,    // curated literature, karyotype, experiment, inherited, text-mined
    retrieved_at: Timestamp,
    source_version: Option<String>,
}
```

Suggested controlled values:

- `ReproductiveMode`: sexual, asexual, facultative sexual/asexual, alternation of generations, artificial/other.
- `SexualSystem`: gonochoric/dioecious, simultaneous hermaphrodite, protandrous, protogynous, bidirectional sex change, monoecious, gynodioecious, androdioecious, trioecious, other.
- `SexDetermination`: genetic-chromosomal, genetic-nonchromosomal, haplodiploid, environmental-temperature, environmental-other, cytoplasmic, mixed GSD/environmental, other.
- `Karyotype`: XX/XY, XX/X0, ZZ/ZW, ZZ/Z0, UV, haplodiploid, complex/multiple, homomorphic/undifferentiated, other; keep a verbatim value and cytogenetic chromosome number alongside the normalized class.
- `OffspringDevelopment`: oviparous, ovoviviparous/lecithotrophic viviparous, viviparous/matrotrophic, larviparous, spores, vegetative, other. Preserve the source's terminology because `ovoviviparous` is not used consistently across disciplines.
- `FertilizationMode`: external, internal, mixed/variable, not applicable, other.

An evidence set must permit multiple claims. A species can reproduce sexually and parthenogenetically, switch sex, differ among populations, or combine genetic and temperature effects. `Variable` must not be reduced to whichever source returned first.

## Provenance and confidence rules

1. Resolve identity using stable source IDs where available (NCBI Taxonomy ID, GBIF key, Wikidata QID) and retain the exact submitted/accepted scientific names.
2. Prefer directly cited, curated species records. An inherited higher-taxon claim is useful but must be visibly labeled `INHERITED` and must lose to a direct species record.
3. Record disagreement instead of choosing silently. Contradictory supported claims should produce `VARIABLE` or `CONFLICT`, with scope and citations.
4. Do not manufacture a numeric confidence score. Use evidence classes such as `DIRECT CURATED`, `DIRECT COMMUNITY`, `INHERITED`, and `TEXT CANDIDATE`; numeric confidence would require calibration data.
5. `Unknown` means no adequate evidence was found. It does not mean absence. `NotApplicable` requires positive evidence and a reason.
6. Keep measurement bounds and qualifiers. A mean, range, sex-specific measurement, captive record, and wild record are not interchangeable.
7. Every visible field should be able to open a provenance view showing source, record/accession, scope, retrieval time, and dataset version.

## Practical implementation order

1. **Now:** move `ASSEMBLY CHR` and `MT LENGTH` onto Variant A's profile; show accession/assembly level and remove the Genome tab. Correct mitochondrial retrieval to follow the selected assembly.
2. **Next:** introduce the canonical evidence model and ingest the Tree of Sex snapshot without guessing across missing taxa. Render `SEX SYS`, `SEXUAL SYSTEM`, and `REPRO` only when evidenced.
3. **Then:** add HerpSexDet overrides and AnAge life-history values. These deliver far more scientific value than more dashboard ornament.
4. **Opportunistically:** expand Wikidata extraction to P3063, P7725, P7770, and P12432, retaining references/qualifiers and showing its low-coverage nature honestly.
5. **Later:** evaluate FishBase and EOL integration contracts/licenses and add taxon-specific adapters behind the same evidence model.

This architecture also fits the fictional field device: the device does not pretend to know everything. It distinguishes a confirmed scan, an inherited model, a stale field archive, and an unresolved signal.
