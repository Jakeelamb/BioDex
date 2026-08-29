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
```
