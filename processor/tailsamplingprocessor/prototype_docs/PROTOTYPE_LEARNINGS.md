# Prototype Learnings: Incremental Tail Sampling & Compact Payload Arenas

**Recommendation**: Upstream `sampling_strategy: "incremental"` paired with **`ivm-lz4-heap` (Recommended)** as the default compact payload backend (pure portable Go `noscan` slabs, separate Resource/Scope intern tables, and zero-dependency LZ4 block compression). Keep uncompressed `ivm-interned-heap` as an optional CPU-focused mode, and do **not** ship off-heap `mmap` (`ivm-lz4-mmap`) by default.

At `200k spans/sec` (`2M` concurrent buffered spans, `10s` decision window, `GOGC=100`, `num_shards=4`), `ivm-lz4-heap` cuts steady OS `VmRSS` by **78.0% to 82.8%** (`4.02-4.08 GB` down to `701-884 MB`) and peak Go `HeapAlloc` by **80.3% to 85.0%**. It shrinks scannable heap by **94.5% to 95.5%** and retained `HeapObjects` by **99.35% to 99.45%** (`32.7M-32.9M` down to `181k-214k`). That lowers GC CPU by **97.9% to 99.2%** (`20.1-21.6s` down to `0.18-0.43s`) and total active CPU (`User + GC`) by **71.8% to 76.0%**, while matching `trace-complete` decisions **100.0%** with **0% regression** on existing benchmarks (`benchstat`).

---

## 1. Candidate Bakeoff Summary (`200k spans/sec` W1 & W2, `100k spans/sec` W3 Worst Case)

**Workload & Shape Legend** (`W4-*` denotes sustained `10s` warmup + `10s` measure bursts at `50k-200k spans/sec` across 3 out-of-order batches/trace):
- **W1 (Production Short-Circuit)**: `status_code=ERROR` (`1%` or `5%`), cross-batch `latency > 500ms` (`3%`), `probabilistic(1%)`.
- **W2 (Early Drop + Cross-Batch `AND`)**: `drop(http.route="/healthz")` (`35%`), `status_code=ERROR` (`2%`), cross-batch `AND(service-1, status=500)` (`~3%`), `latency > 500ms` (`~3%`).
- **W3 (Zero Short-Circuit Worst Case)**: `status_code=ERROR` with `0%` errors across 5 batch shapes (`mixed` 4 shared resources, `sdk` 1 shared resource, `few-traces-per-resource`, `resource-per-trace`, and `ephemeral-unique-res` with a unique `k8s.pod.uid` per trace).

*All memory values use decimal megabytes (`1 MB = 10^6 bytes`), `num_shards=4`, `GOGC=100`, isolated serial OS subprocesses (`milestone_3_evidence.md`).*

| Candidate | W1 200k Steady RSS | W2 200k Steady RSS | W3 100k `sdk` RSS | W1 200k GC CPU | W1 200k Active CPU | Unpaced W1 / W2 Max | Verdict |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `trace-complete` (Baseline A) | 4,019 MB | 4,085 MB | 1,478 MB | 21.60 s | 46.89 s | 467k / 426k sps | Baseline oracle |
| `ivm-interned-heap` (C2) | 1,602 MB (-60%) | 1,198 MB (-71%) | 810 MB (-45%) | 0.11 s (-99.5%) | 8.60 s (-82%) | 851k / 949k sps | Max-throughput option |
| **`ivm-lz4-heap` (C4a)** | **884 MB (-78%)** | **701 MB (-83%)** | **255 MB (-83%)** | **0.18 s (-99.2%)** | **11.28 s (-76%)** | **598k / 662k sps** | **(Recommended)** |
| `ivm-lz4-mmap` (C4b) | 575 MB (-86%) | 452 MB (-89%) | 187 MB (-87%) | 0.71 s (-96.7%) | 14.23 s (-70%) | 510k / 602k sps | Not recommended (GC trap) |

---

## 2. What Worked vs. What Surprised Us

### What Worked
- **5-State Incremental Circuit Fixes All 4 Upstream `span-ingest` Parity Bugs**: Upgrading from naive single-batch checks to a 5-state lattice (`DefTrue`, `DefFalse`, `DefError`, `MayTrue`, `MayFalse`) stored in a pointer-free `noscan` trace table passed **13,800 randomized multi-batch traces** plus targeted error-propagation, stateful-hybrid, multi-shard, and W1/W2 benchmark-stream suites (`milestone_1_evidence.md`, `milestone_3_evidence.md`) with **0 mismatches** across all 17 built-in policy types. On live W4-W2 (`100k spans/sec`), upstream `span-ingest` silently dropped **100% of non-dropped traces** (`0` sampled spans vs `53,980` expected) and emitted **`29,260` false `trace_dropped_too_early`** errors; `ivm-lz4-heap` matched `trace-complete` exactly (`53,980` sampled spans, `0` `dropped_too_early`).
- **Zero-Unmarshal Pending Expiry**: Every IVM arena candidate achieved **100.0% zero-unmarshal drop ratio** across all **64 IVM runs** (out of 81 total), including **0 arena unmarshals** on 100% of expired W3 traces (`1M` live spans). On `ivm-lz4-heap`, tick p99 latency dropped from `144 ms` to `26 ms` (`-81.9%`) on `200k` W1 and from `262 ms` to `68 ms` (`-74.0%`) on `200k` W2.
- **Direct Stdlib OTLP Wire Writer + Separate Intern Slabs**: Writing OTLP proto3 directly from `pdata` into reusable per-shard scratch buffers (grouped per `(traceID, incoming ResourceSpans)`) preserved `TracesSize`, all `pcommon.Value` variants, `Resource.EntityRefs`, and unknown wire fields without touching `go.mod` or `go.sum`. Keeping resource and scope intern tables (`resIntern` and `scopeIntern`) on separate tick-sealed, `<25%`-compacted slabs kept steady slab fragmentation at `1.03x-1.23x` even under 100% unique ephemeral `k8s.pod.uid` churn (`-74.9%` steady RSS on W4-W3 `ephemeral-unique-res`).
- **Zero Regression on Existing `trace-complete`**: Interleaved 6-pass `benchstat` against `upstream/main` (`db80cc0daf0`) measured **-0.00%** on `retained-B/span` (`p >= 0.472`) and `retained-B/trace` (`p >= 0.545`), and **0 statistically significant regressions** across all CPU/throughput benchmarks (`p >= 0.121`).

### Five Genuine Surprises from the Data

1. **Off-Heap `mmap` Hits a `GOGC=100` GC-Frequency Trap**:
   We expected off-heap `mmap` (`ivm-lz4-mmap`) to be the GC winner. In reality, hiding payload bytes from Go's GC shrinks retained live Go heap to `30.0-113.1 MB` (`next_gc` around `60-225 MB`). Because incoming OTLP span batches still allocate transient Go heap (`~630-890 MB` per 10s at `100k spans/sec` in the load generator, rising to `862-1,089 MB` total `Window TotalAlloc` at `shards=4` with sharding and sampled-trace unmarshaling, or `713 MB` at `shards=1`), `GOGC=100` triggers **14-25 GC cycles per 10s paced** (up to **44 in 5s unpaced**) vs `4-10` on `ivm-lz4-heap` (`2.22x-4.00x` more cycles paced). That drove **`2.23x-4.48x` higher paced GC CPU** than `ivm-lz4-heap` (`0.160-0.311s` pause CPU vs `0.029-0.111s`) and STW p99 spikes up to `4.19 ms` paced / `5.24 ms` unpaced. Raising `GOGC=400` on `ivm-lz4-mmap` (`W4-W1 100k`) cut GC cycles (`19 -> 5`) and GC CPU (`0.49s -> 0.15s`), confirming the cause, but inflated its steady `VmRSS` to `463.8 MB` (slightly worse than default-`GOGC=100` `ivm-lz4-heap` at `461.4 MB` and `0.14s` GC CPU). Pure-Go `noscan` slabs give the best of both worlds: Go's pacer sees the slab bytes (keeping GC frequency low) while Go's marker skips scanning them (`-94.5%` scan heap on W1 200k).

2. **Zero-Dependency LZ4 Compression Pays for Itself in Total Active CPU**:
   We expected LZ4 to trade higher CPU for lower RAM. Instead, on `200k spans/sec` W1, `ivm-lz4-heap` cut **Total Active CPU (`User + GC`) by 76.0%** (`46.89s -> 11.28s`), and by **71.8%** on W2 (`43.00s -> 12.11s`), because eliminating `splitResourceSpansByTrace`, pointer-heavy GC scanning, and tick-time policy re-evaluation saved far more CPU than LZ4 consumed. Uncompressed `ivm-interned-heap` misses the `50%` Peak Heap target (`-41.5%` to `-47.9%`) and stays below `50%` steady RSS reduction on `sdk` (`-45.2%`), `few-traces-per-resource` (`-44.9%`), `resource-per-trace` (`-42.8%`), and `ephemeral-unique-res` (`-34.9%`) under W3 worst case. By contrast, `ivm-lz4-heap` achieves **-74.9% to -82.7% steady RSS** (**-78.9% to -85.6% Peak Heap**) across all 5 shapes even at `0%` short-circuit.

3. **In Go 1.27, Multi-GB Pointer Heaps Hurt Concurrent GC CPU Far More Than STW Pause Duration**:
   On `trace-complete` at `200k spans/sec`, Go 1.27 kept STW pauses sub-millisecond (`0.33-0.46 ms` across `2` GC cycles/10s), while concurrent mark-scan burned **`20.1-21.6 CPU-seconds` per 10s** (`>2` full cores scanning `32.7M-32.9M` retained live objects). Because `trace-complete` ran only `1-2` GC cycles per 10s (`2-4` pauses) while `ivm-lz4-heap` ran `4-10` cycles (`8-20` pauses, where `p99` equals the single worst pause), literal `p99 <= baseline` held on `6/12` paced cells for `ivm-lz4-heap` (`7/12` for `ivm-interned-heap`, `3/12` for `ivm-lz4-mmap`). `ivm-lz4-heap` stayed sub-millisecond (`0.39-0.92 ms`) on `10/12` paced cells (with outliers at `3.15 ms` on `50k` and `1.84 ms` on `shards=1`) and halved STW p99 under unpaced W2 burst (`2.62 -> 1.31 ms`).

4. **`splitResourceSpansByTrace` Bypass Ablation Separates Memory Wins from Latency/Throughput Wins**:
   Running `ivm-lz4-heap` and `ivm-interned-heap` with `bypass_split=false` vs `true` proved that **~99% of the RSS and GC-scan reductions come from IVM + compact arenas** (`465.4 MB` vs `461.4 MB` RSS at `shards=4`, `-76.0%` vs `-76.2%`). Meanwhile, bypassing `splitResourceSpansByTrace` independently cut ingest p99 latency by **2.8x-4.0x** at `shards=4` (`1,965 -> 692 us` on `ivm-lz4-heap`) and **998x** at `shards=1` (`33.3 ms -> 33 us` on `ivm-interned-heap`), and boosted unpaced `shards=4` throughput by **+42.8% to +49.3%** (`419k -> 598k sps` on `ivm-lz4-heap`; `570k -> 851k sps` on `ivm-interned-heap`). At `num_shards=1`, unpaced `ivm-lz4-heap` (`175k sps`) trails `trace-complete` (`200k sps`) because LZ4 runs on one shard goroutine while `trace-complete` spreads splitting across 4 producers; at `num_shards=4`, `ivm-lz4-heap` leads by `1.28x-1.55x`.

5. **Three Upstream Collector Bugs Discovered During Parity & Wire Verification**:
   - **`pdata@v1.71.1/internal/proto/unmarshal.go:35-62` (`ConsumeUnknown`)**: Returns immediately on the first inner field inside a non-empty proto2 group (`WireTypeStartGroup`), causing `ErrUnexpectedEndOfGroup` on valid wire groups.
   - **`processor/tailsamplingprocessor/internal/sampling/latency.go:44-60`**: When `upper_threshold_ms > 0`, evaluates prefix latency in arrival order (returning `Sampled` early even when later spans push duration above the upper bound), overwrites valid `StartTimestamp == 0`, and underflows `uint64` on clock skew (`maxEnd < minStart`).
   - **Upstream `span-ingest` (`processor.go:510, 751-778, 995-1045, 1412-1428` at `db80cc0daf0`)**: 4 parity bugs (`drop` + positive = 0% sampled, cross-batch `and`/`latency` missed, `not`/`invert_match` premature positives), cached `trace_dropped_too_early` inflation, `NopCache` slot leak / `block_on_overflow` deadlock, and per-trace `0 ms` writes flooding `sampling_decision_timer_latency`.

---

## 3. Discrepancies vs. Original Prototype Design Sketch

| Initial Prototype Assumption | Ground-Truth Finding | Recommended Design Update |
| :--- | :--- | :--- |
| Off-heap arena beats Go heap | `mmap` burns `2.23x-4.48x` more GC CPU under `GOGC=100` | Default to pure-Go `noscan` slabs + LZ4 (`ivm-lz4-heap`) |
| Latch `KEEP` on first positive match | Unsafe with pending `drop`, stateful limiters, or winner order | Use 5-state lattice + compile-time side-effect and winner guards |
| Focus on GC p99 STW pause time | Go 1.27 STW is `<1 ms`; concurrent mark-scan burns `20+s` CPU | Gate on GC CPU, scannable heap, `VmRSS`, and STW pauses |
| Zero-copy raw byte buffering | `pdata` arrives decoded; wire encode copies once into scratch | Keep direct stdlib wire writer (0 copy on early drops/keeps) |

---

## 4. Productionization Gap Ledger (Before Upstreaming to `opentelemetry-collector-contrib`)

1. **Public Config, Feature Gate, & Docs**: Add a feature gate or opt-in config field for `sampling_strategy: "incremental"` and optional payload compression toggle (`lz4` vs `uncompressed`), regenerate `config.schema.yaml`, add `.chloggen` entry, and document `incremental` vs `trace-complete` vs `span-ingest` in `README.md`.
2. **Code Decomposition & Upstream PR Split**: Split the prototype cleanly into independent, reviewable PRs:
   - PR 1 (Bug fixes in `opentelemetry-collector-contrib`): `latency.go` clock-skew/zero-timestamp/upper-bound fixes + `span-ingest` `dropped_too_early` fix (plus a separate upstream PR in `open-telemetry/opentelemetry-collector` for the `pdata` `ConsumeUnknown` group bug).
   - PR 2 (IVM Engine): 5-state incremental decision circuit (`ivm_circuit.go`, `ivm_processor.go`, `ivm_parity_test.go`) usable with both `inMemoryTailStorage` and external `tail_storage` extensions (unlocking zero-unmarshal `Delete` on `pebbletailstorageextension`).
   - PR 3 (Compact `noscan` Arena): `wire_codec.go`, `lz4_codec.go`, and heap-only `arena_storage.go` (dropping `arena_mmap_*.go` / `arena_madvise_*.go` simplifies cross-OS maintenance and avoids the `mmap` GC trap).
3. **Fuzzing & Malformed Input Hardening**: Add native Go fuzz targets (`FuzzWireCodecRoundTrip`, `FuzzLZ4BlockDecode`) alongside existing `fuzz_test.go` to hammer decoder bounds and recursion limits continuously in OSS-Fuzz.
4. **Telemetry & Observability Parity**: Document that per-policy `sampling_policy_execution_count` is lower under `incremental` when short-circuiting skips redundant policies; add optional arena metrics (slab utilization, compaction count, compression ratio, zero-unmarshal drop count).
5. **Future OTLP Proto Fields**: `wire_codec.go` handles `Resource.EntityRefs` via `ptrace.ProtoMarshaler.ResourceSpansSize` delta detection; if upstream OTLP adds new fields on `Span` or `Scope`, `wire_codec.go` should either share generated marshaling helpers from `pdata` or keep a test guard comparing `wire_codec` size against `ptrace.ProtoMarshaler.ResourceSpansSize` on a populated fixture (already covered by `TestWireCodec_LosslessPdataAndExactProtoSize`).
6. **External `TailStorage` Compatibility**: `incremental` already falls back cleanly to configured `tail_storage` extensions (`Append`/`Take`/`Delete`); add an integration test pairing `sampling_strategy: "incremental"` with a mock `BatchTailStorage` to lock in zero-unmarshal `Delete` behavior.
