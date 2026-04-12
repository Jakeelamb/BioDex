# BioDex

BioDex is a terminal-native species atlas with real portraits, range maps, cached taxonomy browsing, and fast offline navigation.

## Features

- Curated 100-species browsing pack with instant cached navigation
- Auto-loading alphabetical species browser plus taxonomy browser mode
- Portraits, raster range maps, and compact stat panels in the terminal
- Offline taxonomy search and cached rich species profiles
- SQLite-backed local cache for fast repeated browsing

## Build

Run in debug:

```bash
cargo run -- "Homo sapiens"
```

Build and run the optimized release binary:

```bash
cargo build --release
./target/release/biodex
```

## Useful Commands

```bash
biodex --prefetch
biodex --import-backbone
biodex --cache-all-rich
```

## Controls

- `↑/↓` or `j/k`: move through the active navigator
- `t`: swap between the A-Z species list and taxonomy mode
- `Enter` / `l` / `→`: open the selected entry
- `h` / `←`: move up a taxonomy level
- `/`: search
- `r`: refresh live data
- `f`: toggle saved status
- `?`: help

## Notes

- BioDex stores its local database and cache under `biodex` app directories.
- The optimized binary path is `target/release/biodex`.
