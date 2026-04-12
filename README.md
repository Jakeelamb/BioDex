# BioDex

BioDex is a terminal-native species atlas with real portraits, range maps, cached taxonomy browsing, and fast offline navigation.

## Demo

A short terminal demo clip belongs here.

## What It Does

- Curated 100-species browsing pack with cached local navigation
- Alphabetical species browser plus taxonomy browser mode
- Portraits, range maps, and compact stats inside the terminal
- Offline taxonomy search once the backbone import is present
- SQLite-backed local cache for repeated browsing without re-fetching

## Requirements

- Rust toolchain
- An image-capable terminal if you want in-terminal portraits and range maps; otherwise BioDex falls back to text placeholders

## Quick Start

Build and run the optimized binary:

```bash
cargo build --release
./target/release/biodex
```

Open a specific species directly:

```bash
./target/release/biodex "Homo sapiens"
```

Run from source without building the release binary first:

```bash
cargo run --release -- "Panthera leo"
```

Running `biodex` with no arguments opens the TUI at `Animalia`.

## Common Commands

- `biodex`: open the TUI at `Animalia`
- `biodex --text "Homo sapiens"`: print species data without launching the TUI
- `biodex --prefetch`: materialize the default 100-species hot cache
- `biodex --prefetch-animals`: refresh the curated Animalia candidate set and cache media
- `biodex --import-backbone`: import the GBIF backbone for offline taxonomy search
- `biodex --cache-all-rich`: long-running resumable sweep for richer cached species rows
- `biodex --stats`: show local cache statistics

## Controls

- `↑/↓` or `j/k`: move through the active navigator
- `t`: swap between the A-Z species list and taxonomy mode
- `Enter` / `l` / `→`: open the selected entry
- `h` / `←`: move up a taxonomy level
- `/`: search
- `r`: refresh live data
- `f`: toggle saved status
- `?`: help

## Data Sources

BioDex pulls from a few sources and caches the merged result locally:

| Source | Used for |
| --- | --- |
| GBIF | taxonomy matching, offline backbone import, occurrence counts, continent/range data, raster map overlays |
| NCBI | taxonomy IDs, lineage, genome metadata when available |
| iNaturalist | preferred species portraits when available |
| Wikipedia | summaries and article text used for descriptions and fallback life-history extraction |
| Wikidata | conservation status, aliases, rank hints, and structured life-history fields |
| Ensembl | supplementary genome statistics when available |
| Ollama | optional local pass used to fill missing life-history fields from cached article text |
| Local curated pack | bundled supplement for the 100-species starter set |

## Caching

- BioDex stores its local database and cache under `biodex` app directories.
- Species rows, portraits, and range maps are cached locally after fetch.
- The default 100-species browser is designed to feel instant once the hot cache is seeded.
- Offline taxonomy search depends on the GBIF backbone import.

## License

BioDex source code is licensed under the MIT License. See [LICENSE](LICENSE).

Species data, images, range maps, and other third-party content fetched or cached by BioDex are not relicensed by this repository and remain subject to the terms of their original sources.
