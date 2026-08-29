#!/usr/bin/env python3

"""Build BioDex's compact Tree of Sex supplement from the archived CSV files.

Run with uv so the repository keeps a single Python workflow:

    uv run scripts/build_tree_of_sex_supplement.py \
      --vertebrates /path/to/vert.data-may19.csv \
      --invertebrates /path/to/invert.data-may19.csv

The output intentionally contains only taxa in CURATED_ANIMAL_SPECIES. The
original dataset is much broader, but shipping its literature columns would
inflate a small offline field guide for records its starter index cannot open.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ROSTER_PATH = ROOT / "src" / "curated_animals.rs"
OUTPUT_PATH = ROOT / "assets" / "tree_of_sex_supplement.json"

DATASET = "Tree of Sex"
VERSION = "2014-05-19"
DOI_URL = "https://doi.org/10.5061/dryad.v1908"
DATASET_CITATION = (
    "The Tree of Sex Consortium (2014). Tree of Sex: a database of sexual "
    "systems. Scientific Data 1:140015. doi:10.1038/sdata.2014.15"
)


def normalize(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_name(value: str) -> str:
    return normalize(re.sub(r"\([^)]*\)", "", value)).casefold()


def roster() -> dict[str, str]:
    source = ROSTER_PATH.read_text(encoding="utf-8")
    match = re.search(
        r"CURATED_ANIMAL_SPECIES:\s*&\[&str\]\s*=\s*&\[(.*?)\];", source, re.S
    )
    if not match:
        raise RuntimeError(f"could not locate curated roster in {ROSTER_PATH}")
    names = re.findall(r'"([^\"]+)"', match.group(1))
    return {normalize_name(name): name for name in names}


def find_column(row: dict[str, str], prefix: str, *, source: bool = False) -> str:
    expected = f"source: {prefix}" if source else prefix
    for key, value in row.items():
        if key.casefold().startswith(expected.casefold()):
            return normalize(value)
    return ""


def compact_row(row: dict[str, str], group: str, row_number: int, name: str) -> dict:
    fields: dict[str, str] = {}
    citations: dict[str, str] = {}
    for output_name, column_prefix in (
        ("sexual_system", "Sexual System"),
        ("karyotype", "Karyotype"),
        ("genotypic", "Genotypic"),
        ("haplodiploidy", "Haplodiploidy"),
        ("environmental", "Environmental"),
        ("polyfactorial", "Polyfactorial"),
    ):
        value = find_column(row, column_prefix)
        if not value:
            continue
        fields[output_name] = value
        citations[output_name] = find_column(row, column_prefix, source=True) or DATASET_CITATION

    return {
        "scientific_name": name,
        "record_id": f"{group}:{row_number}",
        "fields": fields,
        "citations": citations,
        "notes": normalize(row.get("notes, comments")),
    }


def read_rows(path: Path, group: str, wanted: dict[str, str]) -> list[dict]:
    records = []
    with path.open(encoding="utf-8-sig", errors="replace", newline="") as handle:
        for row_number, row in enumerate(csv.DictReader(handle), start=2):
            source_name = f"{normalize(row.get('Genus'))} {normalize(row.get('species'))}"
            canonical_name = wanted.get(normalize_name(source_name))
            if canonical_name is None:
                continue
            record = compact_row(row, group, row_number, canonical_name)
            if record["fields"]:
                records.append(record)
    return records


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--vertebrates", type=Path, required=True)
    parser.add_argument("--invertebrates", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args()

    wanted = roster()
    records = [
        *read_rows(args.vertebrates, "vertebrate", wanted),
        *read_rows(args.invertebrates, "invertebrate", wanted),
    ]
    records.sort(key=lambda record: (record["scientific_name"].casefold(), record["record_id"]))

    payload = {
        "dataset": {
            "name": DATASET,
            "version": VERSION,
            "url": DOI_URL,
            "citation": DATASET_CITATION,
        },
        "records": records,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    covered = len({record["scientific_name"] for record in records})
    print(f"wrote {len(records)} evidence rows for {covered} starter taxa to {args.output}")


if __name__ == "__main__":
    main()
