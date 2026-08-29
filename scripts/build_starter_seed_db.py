#!/usr/bin/env python3

import argparse
import pathlib
import sqlite3

def parse_curated_species(repo_root: pathlib.Path) -> list[str]:
    names: list[str] = []
    in_block = False
    for line in (repo_root / "src" / "curated_animals.rs").read_text(
        encoding="utf-8"
    ).splitlines():
        if not in_block:
            if "pub const CURATED_ANIMAL_SPECIES" in line:
                in_block = True
            continue
        if line.strip() == "];":
            break
        if '"' not in line:
            continue
        names.append(line.split('"')[1].lower())

    if not names:
        raise RuntimeError("failed to parse curated species list")
    return names


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the bundled BioDex starter cache database."
    )
    parser.add_argument("source_db", type=pathlib.Path)
    parser.add_argument("output_db", type=pathlib.Path)
    args = parser.parse_args()

    repo_root = pathlib.Path(__file__).resolve().parent.parent
    curated_species = parse_curated_species(repo_root)
    placeholders = ",".join("?" for _ in curated_species)

    args.output_db.parent.mkdir(parents=True, exist_ok=True)
    if args.output_db.exists():
        args.output_db.unlink()

    source = sqlite3.connect(f"file:{args.source_db}?mode=ro", uri=True)
    dest = sqlite3.connect(args.output_db)

    try:
        dest.executescript(
            """
            PRAGMA journal_mode = DELETE;
            PRAGMA synchronous = FULL;

            CREATE TABLE species (
                scientific_name TEXT PRIMARY KEY,
                data_json TEXT NOT NULL,
                cached_at INTEGER NOT NULL,
                last_accessed INTEGER NOT NULL
            );

            CREATE TABLE rich_species (
                scientific_name TEXT PRIMARY KEY,
                data_json TEXT NOT NULL,
                enriched_at INTEGER NOT NULL
            );

            CREATE TABLE taxon_names (
                gbif_key INTEGER PRIMARY KEY,
                scientific_name TEXT NOT NULL,
                canonical_name TEXT,
                rank TEXT NOT NULL,
                kingdom TEXT,
                phylum TEXT,
                class TEXT,
                order_name TEXT,
                family TEXT,
                genus TEXT
            );

            CREATE TABLE user_stats (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )

        species_rows = source.execute(
            f"""
            SELECT scientific_name, data_json, cached_at, last_accessed
            FROM species
            WHERE scientific_name IN ({placeholders})
            """,
            curated_species,
        ).fetchall()
        rich_rows = source.execute(
            f"""
            SELECT scientific_name, data_json, enriched_at
            FROM rich_species
            WHERE scientific_name IN ({placeholders})
            """,
            curated_species,
        ).fetchall()
        taxon_rows = source.execute(
            f"""
            SELECT gbif_key, scientific_name, canonical_name, rank,
                   kingdom, phylum, class, order_name, family, genus
            FROM taxon_names
            WHERE lower(scientific_name) IN ({placeholders})
               OR lower(coalesce(canonical_name, '')) IN ({placeholders})
            """,
            curated_species + curated_species,
        ).fetchall()

        if len(species_rows) != len(curated_species):
            raise RuntimeError(
                f"expected {len(curated_species)} species rows, found {len(species_rows)}"
            )
        if len(rich_rows) != len(curated_species):
            raise RuntimeError(
                f"expected {len(curated_species)} rich species rows, found {len(rich_rows)}"
            )

        dest.executemany(
            "INSERT INTO species (scientific_name, data_json, cached_at, last_accessed) VALUES (?, ?, ?, ?)",
            species_rows,
        )
        dest.executemany(
            "INSERT INTO rich_species (scientific_name, data_json, enriched_at) VALUES (?, ?, ?)",
            rich_rows,
        )
        dest.executemany(
            """
            INSERT INTO taxon_names (
                gbif_key, scientific_name, canonical_name, rank,
                kingdom, phylum, class, order_name, family, genus
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            taxon_rows,
        )
        dest.commit()
        dest.execute("VACUUM")
    finally:
        dest.close()
        source.close()


if __name__ == "__main__":
    main()
