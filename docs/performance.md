# Performance campaign

Profile date: 2026-08-28. Workload: the release build opening the bundled
100-species audit (`biodex --audit-animals`) and a cached offline Mallard text
record. The audit exercises application startup, SQLite reads, JSON decoding,
local supplements, lineage validation, media lookup, and completeness checks.
Output was compared byte for byte after every round.

## Results

| Revision | Audit mean | Cached text mean | Audit max RSS | Heap allocations | Tracked peak heap |
| --- | ---: | ---: | ---: | ---: | ---: |
| Baseline | 73.2 ms | 65.6 ms | 22,896 KiB | 426,762 | 7.09 MiB |
| Shared HTTP pool | 28.8 ms | 22.5 ms | 19,612 KiB | 95,447 | 4.08 MiB |
| Hot-path allocation cleanup | 28.5 ms | 17.2 ms | 19,012 KiB | 93,103 | 3.81 MiB |
| Lazy networking | 17.2 ms | 7.6 ms | 13,964 KiB | 14,689 | 2.71 MiB |

Final change from baseline: the audit is about 76% faster, cached text lookup
is about 88% faster, max RSS is about 39% lower, and the allocation profiler
records about 97% fewer allocation calls. Heaptrack changes runtime and RSS, so
its allocation and tracked-heap columns are used comparatively rather than as
un-instrumented wall-clock measurements.

## Profile-led changes

1. CPU and heap profiles showed six independent `reqwest` clients repeatedly
   loading the OpenSSL certificate store. All adapters now share one connection
   pool and TLS context.
2. The allocation profile then showed canonical lineage reconstruction,
   display-string construction inside a boolean audit check, and incremental
   audit-vector growth. Canonical cached lineages are now validated in place,
   completeness uses a non-allocating predicate, and gap vectors are sized once.
3. The next CPU profile still showed TLS setup on commands that never use the
   network. API adapters are now inert handles; the shared HTTP client is
   initialized only by the first real request.

## Assembly decision

No assembly-level optimization is justified. After the Rust-level changes, the
largest self-time entries are SQLite statement parsing and execution, Serde JSON
decoding, runtime/channel startup, and small independent BioDex functions. There
is no stable, arithmetic-heavy kernel large enough to repay handwritten SIMD,
unsafe code, or target-specific assembly. The remaining work is architectural
or data-format work, not instruction selection.

## Sustained TUI render campaign

The production renderer also has an ignored, deterministic profiling workload.
It drives 5,000 full 180x50 field-record frames through Ratatui's `TestBackend`
while moving the selected species across a 100-entry navigator. This isolates
BioDex layout, record rendering, atlas fallback, and navigation work from PTY
capture latency. A normal companion test renders one frame and checks the
important record surfaces.

| Round | Time per frame | Allocations | Temporary allocations |
| --- | ---: | ---: | ---: |
| Baseline | 221.6 us | 3,276,178 | 1,260,406 |
| Cached atlas + borrowed labels | 204.9 us | 2,236,217 | 1,030,406 |
| Allocation-free rank matching | 209.1 us | 1,831,197 | 635,406 |
| Direct navigator rows | 197.1 us | 1,631,230 | 615,406 |

The final frame figure is the mean of 15 warmed runs; intermediate timing is a
single direct run and is included to show measurement variance, not a monotonic
claim. Allocation counts come from identical Heaptrack runs. From baseline to
the final round, allocation calls fell 50.2%, temporary allocations fell 51.2%,
and observed frame time improved by about 11%. Max RSS remained effectively
flat at roughly 25 MiB because the removed allocations were short-lived churn,
not retained working state.

The allocation profile led to three changes:

1. The ASCII range atlas is cached by species, bounds, continents, and panel
   size, then written directly into the frame buffer instead of regenerating
   styled strings on every keypress.
2. Clean labels borrow their source strings, and rank/color decisions use
   case-insensitive comparisons without allocating normalized copies.
3. Navigator rows render directly as spans instead of building a heap-backed
   `Vec<Line>` and `Vec<Span>` for each visible species.

The final CPU profile is dominated by Ratatui buffer diffing, Unicode width and
grapheme traversal, symbol writes, and cell styling. No BioDex arithmetic kernel
remains dominant, so assembly or unsafe SIMD would add portability and
maintenance cost without addressing the measured bottleneck.

## Reproduction

```bash
cargo build --release
hyperfine --warmup 5 --runs 50 \
  './target/release/biodex --audit-animals >/dev/null'
hyperfine --warmup 5 --runs 50 \
  './target/release/biodex --text --offline "Anas platyrhynchos" >/dev/null'
perf record -F 1999 -g --call-graph dwarf -- \
  bash -c 'for i in $(seq 1 300); do ./target/release/biodex --audit-animals >/dev/null; done'
heaptrack --record-only ./target/release/biodex --audit-animals

# Deterministic production-render workload (printed duration excludes Cargo startup)
cargo test --release profile_sustained_render -- --ignored --nocapture

# For profiler runs, use the test executable printed by this command
cargo test --release --no-run
perf record -F 999 -g --call-graph dwarf -- \
  ./target/release/deps/biodex-<test-hash> \
  profile_sustained_render --ignored --nocapture
heaptrack --record-only ./target/release/deps/biodex-<test-hash> \
  profile_sustained_render --ignored --nocapture
```
