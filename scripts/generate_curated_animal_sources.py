#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import subprocess
import zipfile
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

ANAGE_URL = "https://genomics.senescence.info/species/dataset.zip"
AMNIOTE_URL = "https://doi.org/10.6084/m9.figshare.3563457.v1"
AMPHIBIO_URL = "https://doi.org/10.6084/m9.figshare.4644424.v5"
EOL_URL = "https://doi.org/10.5281/zenodo.13305577"

EOL_MASS_PREDICATES = {
    "http://purl.obolibrary.org/obo/vt_0001259",
    "http://purl.obolibrary.org/obo/oba_vt0001259",
    "http://eol.org/schema/terms/bodymassdry",
    "http://eol.org/schema/terms/bodymasswet",
}
EOL_LENGTH_PREDICATES = {
    "http://purl.obolibrary.org/obo/vt_0001256",
    "http://purl.obolibrary.org/obo/oba_vt0001256",
    "http://purl.obolibrary.org/obo/cmo_0000013",
    "http://purl.org/obo/owlatol_0001660",
    "http://eol.org/schema/terms/headbodylength",
    "http://eol.org/schema/terms/bodylengthexclhead",
}
EOL_LIFESPAN_PREDICATES = {
    "http://purl.obolibrary.org/obo/vt_0001661",
}
EOL_REPRODUCTION_VALUES = {
    "http://purl.bioontology.org/ontology/mesh/d052286": "Viviparous",
    "http://purl.bioontology.org/ontology/mesh/d052287": "Oviparous",
    "http://www.marinespecies.org/traits/oviparous": "Oviparous",
    "http://www.marinespecies.org/traits/ovoviviparous": "Ovoviviparous",
    "http://www.marinespecies.org/traits/viviparous": "Viviparous",
    "http://purl.obolibrary.org/obo/go_0019953": "Sexual",
    "http://purl.obolibrary.org/obo/go_0019954": "Asexual",
}


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def normalize_species_name(value: str) -> str:
    value = re.sub(r"<[^>]+>", "", value)
    value = re.sub(r"\([^)]*\)", "", value)
    value = normalize_whitespace(value)
    parts = value.split()
    if len(parts) >= 3 and parts[2].islower():
        return " ".join(parts[:3])
    if len(parts) >= 2:
        return " ".join(parts[:2])
    return value


def normalize_csv_value(value: str | None) -> str:
    if value is None:
        return ""
    value = normalize_whitespace(value)
    if value in {"", "NA", "-999", "unknown"}:
        return ""
    return value


def parse_float(value: str | None) -> float | None:
    value = normalize_csv_value(value)
    if not value:
        return None
    try:
        number = float(value)
    except ValueError:
        return None
    if number <= 0:
        return None
    return number


def prefer_unique(values: list[str]) -> list[str]:
    seen = set()
    ordered = []
    for value in values:
        cleaned = normalize_csv_value(value)
        if not cleaned:
            continue
        key = cleaned.casefold()
        if key in seen:
            continue
        seen.add(key)
        ordered.append(cleaned)
    return ordered


def add_mode(modes: list[str], mode: str) -> list[str]:
    if not any(existing.casefold() == mode.casefold() for existing in modes):
        modes.append(mode)
    return modes


def choose_first(*values: float | None) -> float | None:
    for value in values:
        if value is not None:
            return value
    return None


def merge_unique_sources(existing: list[dict], new: list[dict]) -> list[dict]:
    merged = []
    seen = set()
    for source in [*existing, *new]:
        key = json.dumps(source, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        merged.append(source)
    return merged


def parse_roster(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    match = re.search(
        r"CURATED_ANIMAL_SPECIES:\s*&\[\s*&str\s*\]\s*=\s*&\[(?P<body>.*?)\];",
        text,
        re.S,
    )
    if not match:
        raise RuntimeError(f"could not locate CURATED_ANIMAL_SPECIES in {path}")
    return re.findall(r'"([^"]+)"', match.group("body"))


def make_record(scientific_name: str) -> dict:
    return {
        "scientific_name": scientific_name,
        "common_names": [],
        "taxonomy": {
            "kingdom": None,
            "phylum": None,
            "class": None,
            "order": None,
            "family": None,
            "genus": None,
        },
        "life_history": {
            "lifespan_years": None,
            "length_meters": None,
            "height_meters": None,
            "mass_kilograms": None,
            "reproduction_modes": [],
        },
        "genome": {
            "assembly_accession": None,
            "assembly_level": None,
            "assembly_name": None,
            "genome_size_bp": None,
            "mito_genome_size_bp": None,
        },
        "provenance": {
            "common_names": [],
            "taxonomy": [],
            "lifespan_years": [],
            "length_meters": [],
            "height_meters": [],
            "mass_kilograms": [],
            "reproduction_modes": [],
            "genome": [],
        },
    }


def set_common_names(record: dict, values: list[str], source: dict) -> None:
    merged = prefer_unique(record["common_names"] + values)
    if merged != record["common_names"]:
        record["common_names"] = merged
        record["provenance"]["common_names"].append(source)


def fill_taxonomy(record: dict, taxonomy: dict, source: dict) -> None:
    changed = False
    for key, value in taxonomy.items():
        if value and record["taxonomy"][key] is None:
            record["taxonomy"][key] = value
            changed = True
    if changed:
        record["provenance"]["taxonomy"].append(source)


def fill_numeric(record: dict, field: str, value: float | None, source: dict) -> None:
    if value is None:
        return
    if record["life_history"][field] is None:
        record["life_history"][field] = round(value, 6)
        record["provenance"][field].append(source)


def fill_reproduction(record: dict, modes: list[str], source: dict) -> None:
    if not modes:
        return
    changed = False
    for mode in modes:
        existing = record["life_history"]["reproduction_modes"]
        if not any(current.casefold() == mode.casefold() for current in existing):
            existing.append(mode)
            changed = True
    if changed:
        record["provenance"]["reproduction_modes"].append(source)


def grams_to_kilograms(value: float | None) -> float | None:
    if value is None:
        return None
    return value / 1000.0


def centimeters_to_meters(value: float | None) -> float | None:
    if value is None:
        return None
    return value / 100.0


def millimeters_to_meters(value: float | None) -> float | None:
    if value is None:
        return None
    return value / 1000.0


def convert_units(value: float, unit_blob: str, quantity: str) -> float | None:
    unit_blob = unit_blob.casefold()
    if quantity == "mass":
        if "kilogram" in unit_blob or unit_blob.endswith("/kg") or " kg" in unit_blob:
            return value
        if "milligram" in unit_blob or " mg" in unit_blob:
            return value / 1_000_000.0
        if "gram" in unit_blob or " g" in unit_blob:
            return grams_to_kilograms(value)
        if unit_blob == "":
            if value >= 100:
                return grams_to_kilograms(value)
            return value
        return None

    if quantity == "length":
        if "millimeter" in unit_blob or " mm" in unit_blob:
            return millimeters_to_meters(value)
        if "centimeter" in unit_blob or " cm" in unit_blob:
            return centimeters_to_meters(value)
        if "meter" in unit_blob or unit_blob.endswith("/m") or " m" in unit_blob:
            return value
        if unit_blob == "":
            if value >= 20:
                return millimeters_to_meters(value)
            if value >= 2:
                return centimeters_to_meters(value)
            return value
        return None

    if quantity == "lifespan":
        if "month" in unit_blob:
            return value / 12.0
        if "day" in unit_blob:
            return value / 365.25
        if "year" in unit_blob or unit_blob.endswith("/yr") or " yr" in unit_blob:
            return value
        return None

    return None


def load_anage(targets: set[str], records: dict[str, dict]) -> None:
    archive_path = ROOT / "data_sources" / "anage_dataset.zip"
    with zipfile.ZipFile(archive_path) as zf:
        member = zf.namelist()[0]
        with zf.open(member) as fh:
            reader = csv.DictReader(io.TextIOWrapper(fh, encoding="utf-8"), delimiter="\t")
            for row in reader:
                scientific_name = normalize_species_name(f"{row['Genus']} {row['Species']}")
                if scientific_name not in targets:
                    continue
                record = records[scientific_name]
                hagrid = normalize_csv_value(row.get("HAGRID"))
                refs = normalize_csv_value(row.get("References"))
                source = {
                    "dataset": "AnAge",
                    "url": ANAGE_URL,
                    "citation": f"AnAge dataset row {hagrid or scientific_name}" + (f"; references {refs}" if refs else ""),
                }
                set_common_names(record, [row.get("Common name", "")], source)
                fill_taxonomy(
                    record,
                    {
                        "kingdom": normalize_csv_value(row.get("Kingdom")),
                        "phylum": normalize_csv_value(row.get("Phylum")),
                        "class": normalize_csv_value(row.get("Class")),
                        "order": normalize_csv_value(row.get("Order")),
                        "family": normalize_csv_value(row.get("Family")),
                        "genus": normalize_csv_value(row.get("Genus")),
                    },
                    source,
                )
                fill_numeric(record, "lifespan_years", parse_float(row.get("Maximum longevity (yrs)")), source)
                fill_numeric(
                    record,
                    "mass_kilograms",
                    choose_first(
                        grams_to_kilograms(parse_float(row.get("Body mass (g)"))),
                        grams_to_kilograms(parse_float(row.get("Adult weight (g)"))),
                    ),
                    source,
                )


def load_amniote(targets: set[str], records: dict[str, dict]) -> None:
    path = ROOT / "data_sources" / "amniote_unzipped" / "Data_Files" / "Amniote_Database_Aug_2015.csv"
    with path.open(encoding="latin-1", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            scientific_name = normalize_species_name(f"{row['genus']} {row['species']}")
            if scientific_name not in targets:
                continue
            record = records[scientific_name]
            source = {
                "dataset": "Amniote Database",
                "url": AMNIOTE_URL,
                "citation": f"Amniote Database row for {scientific_name}",
            }
            set_common_names(record, [row.get("common_name", "")], source)
            fill_taxonomy(
                record,
                {
                    "kingdom": "Animalia",
                    "phylum": "Chordata",
                    "class": normalize_csv_value(row.get("class")),
                    "order": normalize_csv_value(row.get("order")),
                    "family": normalize_csv_value(row.get("family")),
                    "genus": normalize_csv_value(row.get("genus")),
                },
                source,
            )
            fill_numeric(
                record,
                "lifespan_years",
                choose_first(parse_float(row.get("maximum_longevity_y")), parse_float(row.get("longevity_y"))),
                source,
            )
            fill_numeric(
                record,
                "mass_kilograms",
                choose_first(
                    grams_to_kilograms(parse_float(row.get("adult_body_mass_g"))),
                    grams_to_kilograms(parse_float(row.get("female_body_mass_g"))),
                    grams_to_kilograms(parse_float(row.get("male_body_mass_g"))),
                    grams_to_kilograms(parse_float(row.get("no_sex_body_mass_g"))),
                ),
                source,
            )
            fill_numeric(
                record,
                "length_meters",
                choose_first(
                    centimeters_to_meters(parse_float(row.get("adult_svl_cm"))),
                    centimeters_to_meters(parse_float(row.get("female_svl_cm"))),
                    centimeters_to_meters(parse_float(row.get("male_svl_cm"))),
                    centimeters_to_meters(parse_float(row.get("no_sex_svl_cm"))),
                ),
                source,
            )


def load_amphibio(targets: set[str], records: dict[str, dict]) -> None:
    path = ROOT / "data_sources" / "amphibio_unzipped" / "AmphiBIO_v1.csv"
    with path.open(encoding="latin-1", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            scientific_name = normalize_species_name(f"{row['Genus']} {row['Species']}")
            if scientific_name not in targets:
                continue
            record = records[scientific_name]
            source = {
                "dataset": "AmphiBIO",
                "url": AMPHIBIO_URL,
                "citation": f"AmphiBIO row for {scientific_name}",
            }
            fill_taxonomy(
                record,
                {
                    "kingdom": "Animalia",
                    "phylum": "Chordata",
                    "class": "Amphibia",
                    "order": normalize_csv_value(row.get("Order")),
                    "family": normalize_csv_value(row.get("Family")),
                    "genus": normalize_csv_value(row.get("Genus")),
                },
                source,
            )
            fill_numeric(record, "lifespan_years", parse_float(row.get("Longevity_max_y")), source)
            fill_numeric(record, "mass_kilograms", grams_to_kilograms(parse_float(row.get("Body_mass_g"))), source)
            fill_numeric(record, "length_meters", millimeters_to_meters(parse_float(row.get("Body_size_mm"))), source)
            modes = ["Sexual"]
            if normalize_csv_value(row.get("Viv")) == "1":
                add_mode(modes, "Viviparous")
            elif normalize_csv_value(row.get("Dir")) == "1" or normalize_csv_value(row.get("Lar")) == "1":
                add_mode(modes, "Oviparous")
            fill_reproduction(record, modes, source)


def load_eol_page_ids(targets: set[str]) -> dict[str, str]:
    command = ["unzip", "-p", str(ROOT / "data_sources" / "eol_traits_all.zip"), "trait_bank/pages.csv"]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, text=True, encoding="utf-8", errors="replace")
    assert process.stdout is not None
    reader = csv.DictReader(process.stdout)
    page_ids = {}
    for row in reader:
        canonical = normalize_csv_value(row.get("canonical"))
        if canonical in targets and canonical not in page_ids:
            page_ids[canonical] = normalize_csv_value(row.get("page_id"))
            if len(page_ids) == len(targets):
                break
    process.stdout.close()
    return_code = process.wait()
    if return_code not in (0, -13):
        raise RuntimeError(f"EOL page scan failed with exit code {return_code}")
    return page_ids


def load_eol_inferred_traits(page_ids: dict[str, str]) -> dict[str, set[str]]:
    page_to_species = defaultdict(list)
    for species_name, page_id in page_ids.items():
        if page_id:
            page_to_species[page_id].append(species_name)

    command = ["unzip", "-p", str(ROOT / "data_sources" / "eol_traits_all.zip"), "trait_bank/inferred.csv"]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, text=True, encoding="utf-8", errors="replace")
    assert process.stdout is not None
    reader = csv.DictReader(process.stdout)
    inferred = defaultdict(set)
    for row in reader:
        page_id = normalize_csv_value(row.get("page_id"))
        if page_id not in page_to_species:
            continue
        trait_id = normalize_csv_value(row.get("inferred_trait"))
        if not trait_id:
            continue
        for species_name in page_to_species[page_id]:
            inferred[species_name].add(trait_id)
    process.stdout.close()
    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(f"EOL inferred scan failed with exit code {return_code}")
    return inferred


def apply_eol_trait(record: dict, row: dict, source: dict) -> None:
    predicate = normalize_csv_value(row.get("predicate")).casefold()
    value_uri = normalize_csv_value(row.get("value_uri")).casefold()
    number = parse_float(row.get("normal_measurement"))
    if number is None:
        number = parse_float(row.get("measurement"))
    unit_blob = " ".join(
        [
            normalize_csv_value(row.get("normal_units_uri")),
            normalize_csv_value(row.get("normal_units")),
            normalize_csv_value(row.get("units_uri")),
            normalize_csv_value(row.get("units")),
        ]
    )
    if predicate in EOL_MASS_PREDICATES and number is not None:
        fill_numeric(record, "mass_kilograms", convert_units(number, unit_blob, "mass"), source)
    elif predicate in EOL_LENGTH_PREDICATES and number is not None:
        fill_numeric(record, "length_meters", convert_units(number, unit_blob, "length"), source)
    elif predicate in EOL_LIFESPAN_PREDICATES and number is not None:
        fill_numeric(record, "lifespan_years", convert_units(number, unit_blob, "lifespan"), source)

    modes = []
    if value_uri in EOL_REPRODUCTION_VALUES:
        add_mode(modes, EOL_REPRODUCTION_VALUES[value_uri])
    name_en = normalize_csv_value(row.get("name_en")).casefold()
    literal = normalize_csv_value(row.get("literal")).casefold()
    predicate_text = normalize_csv_value(row.get("predicate")).casefold()
    for blob in (name_en, literal, predicate_text):
        if "ovovivip" in blob:
            add_mode(modes, "Ovoviviparous")
        elif "vivip" in blob:
            add_mode(modes, "Viviparous")
        elif "ovip" in blob:
            add_mode(modes, "Oviparous")
        if "sexual reproduction" in blob or blob == "sexual":
            add_mode(modes, "Sexual")
        if "reproduction" in blob and not modes:
            add_mode(modes, "Sexual")
    if modes:
        fill_reproduction(record, modes, source)


def scan_eol_traits(targets: set[str], records: dict[str, dict], inferred_traits: dict[str, set[str]]) -> None:
    trait_to_species = defaultdict(list)
    for species_name, trait_ids in inferred_traits.items():
        for trait_id in trait_ids:
            trait_to_species[trait_id].append(species_name)

    command = ["unzip", "-p", str(ROOT / "data_sources" / "eol_traits_all.zip"), "trait_bank/traits.csv"]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, text=True, encoding="utf-8", errors="replace")
    assert process.stdout is not None
    reader = csv.DictReader(process.stdout)
    for row in reader:
        direct_species = normalize_species_name(row.get("scientific_name", ""))
        resource_pk = normalize_csv_value(row.get("resource_pk"))
        matched_species = []
        if direct_species in targets:
            matched_species.append((direct_species, False))
        for species_name in trait_to_species.get(resource_pk, []):
            if species_name != direct_species:
                matched_species.append((species_name, True))
        if not matched_species:
            continue

        citation_text = normalize_csv_value(row.get("citation")) or normalize_csv_value(row.get("source"))
        for species_name, inferred in matched_species:
            source = {
                "dataset": "EOL TraitBank",
                "url": EOL_URL,
                "citation": citation_text or f"EOL TraitBank row for {species_name}",
            }
            if inferred:
                source = dict(source)
                source["citation"] = source["citation"] + " (clade-inferred in EOL TraitBank)"
            apply_eol_trait(records[species_name], row, source)

    process.stdout.close()
    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(f"EOL trait scan failed with exit code {return_code}")


def coverage(records: dict[str, dict]) -> dict[str, int]:
    counts = defaultdict(int)
    for record in records.values():
        life = record["life_history"]
        if record["common_names"]:
            counts["common_name"] += 1
        if all(record["taxonomy"].get(key) for key in ("phylum", "class", "order", "family", "genus")):
            counts["taxonomy"] += 1
        if life["lifespan_years"] is not None:
            counts["lifespan_years"] += 1
        if life["length_meters"] is not None or life["height_meters"] is not None:
            counts["length_or_height"] += 1
        if life["mass_kilograms"] is not None:
            counts["mass_kilograms"] += 1
        if life["reproduction_modes"]:
            counts["reproduction_modes"] += 1
        if life["lifespan_years"] is not None and (life["length_meters"] is not None or life["height_meters"] is not None) and life["mass_kilograms"] is not None and bool(life["reproduction_modes"]):
            counts["life_history_complete"] += 1
    return counts


def load_existing_records(path: Path | None) -> dict[str, dict]:
    if path is None or not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    species = payload.get("species", [])
    return {
        normalize_species_name(record.get("scientific_name", "")): record
        for record in species
        if normalize_species_name(record.get("scientific_name", ""))
    }


def merge_existing_record(record: dict, existing: dict) -> dict:
    merged = dict(record)

    merged["common_names"] = prefer_unique(existing.get("common_names", []) + record["common_names"])

    taxonomy = dict(record["taxonomy"])
    for field, value in existing.get("taxonomy", {}).items():
        if field in taxonomy and taxonomy[field] is None and value is not None:
            taxonomy[field] = value
    merged["taxonomy"] = taxonomy

    life_history = dict(record["life_history"])
    for field, value in existing.get("life_history", {}).items():
        if field == "reproduction_modes":
            continue
        if field in life_history and life_history[field] is None and value is not None:
            life_history[field] = value
    life_history["reproduction_modes"] = prefer_unique(
        existing.get("life_history", {}).get("reproduction_modes", []) + record["life_history"]["reproduction_modes"]
    )
    merged["life_history"] = life_history

    genome = dict(record["genome"])
    for field, value in existing.get("genome", {}).items():
        if field in genome and genome[field] is None and value is not None:
            genome[field] = value
    merged["genome"] = genome

    provenance = {}
    existing_provenance = existing.get("provenance", {})
    generated_provenance = record["provenance"]
    for field in set(existing_provenance) | set(generated_provenance):
        provenance[field] = merge_unique_sources(
            existing_provenance.get(field, []),
            generated_provenance.get(field, []),
        )
    merged["provenance"] = provenance

    for field, value in existing.items():
        if field in {"scientific_name", "common_names", "taxonomy", "life_history", "genome", "provenance"}:
            continue
        merged.setdefault(field, value)

    return merged


def print_report(records: dict[str, dict]) -> None:
    stats = coverage(records)
    total = len(records)
    print(f"species:               {total}")
    print(f"common names:          {stats['common_name']}/{total}")
    print(f"taxonomy:              {stats['taxonomy']}/{total}")
    print(f"lifespan:              {stats['lifespan_years']}/{total}")
    print(f"length or height:      {stats['length_or_height']}/{total}")
    print(f"mass:                  {stats['mass_kilograms']}/{total}")
    print(f"reproduction:          {stats['reproduction_modes']}/{total}")
    print(f"life-history complete: {stats['life_history_complete']}/{total}")
    print()
    for scientific_name, record in records.items():
        life = record["life_history"]
        missing = []
        if not record["common_names"]:
            missing.append("common-name")
        if not all(record["taxonomy"].get(key) for key in ("phylum", "class", "order", "family", "genus")):
            missing.append("taxonomy")
        if life["lifespan_years"] is None:
            missing.append("lifespan")
        if life["length_meters"] is None and life["height_meters"] is None:
            missing.append("length")
        if life["mass_kilograms"] is None:
            missing.append("mass")
        if not life["reproduction_modes"]:
            missing.append("reproduction")
        if missing:
            print(f"{scientific_name}: {', '.join(missing)}")


def build_records(species_names: list[str]) -> dict[str, dict]:
    records = {name: make_record(name) for name in species_names}
    targets = set(records)
    load_anage(targets, records)
    load_amniote(targets, records)
    load_amphibio(targets, records)
    page_ids = load_eol_page_ids(targets)
    inferred_traits = load_eol_inferred_traits(page_ids)
    scan_eol_traits(targets, records, inferred_traits)
    for record in records.values():
        record["common_names"] = prefer_unique(record["common_names"])
        record["life_history"]["reproduction_modes"] = prefer_unique(record["life_history"]["reproduction_modes"])
    return records


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--species-file", type=Path, help="Plain text file of scientific names, one per line. Defaults to src/curated_animals.rs")
    parser.add_argument("--output", type=Path, help="Write supplement JSON to this path")
    parser.add_argument(
        "--existing",
        type=Path,
        help="Existing supplement JSON to merge and preserve manual sourced fields from. Defaults to assets/curated_animal_sources.json when present.",
    )
    parser.add_argument("--report", action="store_true", help="Print coverage and missing fields")
    args = parser.parse_args()

    if args.species_file:
        species_names = [normalize_whitespace(line) for line in args.species_file.read_text(encoding="utf-8").splitlines() if normalize_whitespace(line)]
    else:
        species_names = parse_roster(ROOT / "src" / "curated_animals.rs")

    records = build_records(species_names)
    existing_path = args.existing
    if existing_path is None:
        default_existing = ROOT / "assets" / "curated_animal_sources.json"
        if default_existing.exists():
            existing_path = default_existing
    existing_records = load_existing_records(existing_path)
    payload = {
        "species": [
            merge_existing_record(records[name], existing_records[name]) if name in existing_records else records[name]
            for name in species_names
        ]
    }

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    if args.report or not args.output:
        print_report(records)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
