# Demo & Walkthrough Plan: Incremental Tail Sampling Prototype

This plan gives reviewers two ways to inspect the prototype once committed and pushed to a prototype fork branch (`tailsampling-ivm-prototype` based on `open-telemetry/opentelemetry-collector-contrib` commit `db80cc0daf0`): a reproducible terminal walkthrough (Option A) and a 6-scene AI-narrated walkthrough video storyboard and script (Option B).

---

## Option A: Interactive Terminal Walkthrough (~3 Minutes for Steps 1-4)

After checking out branch `tailsampling-ivm-prototype` and changing into `processor/tailsamplingprocessor`, run these 4 verification steps in order (plus optional Step 5 for full `benchstat` regression checking).

### Step 1: Show Why Existing `span-ingest` Cannot Solve This (~4s)
```bash
go test -v -run '^TestUpstreamSpanIngestKnownParityFailures$' .
```
**What to point out**: All 4 subtests pass by proving upstream `span-ingest` fails parity against `trace-complete` (100% trace loss when `drop` is paired with any positive policy, missed cross-batch `and`, missed cross-batch `latency`, and premature false positives on `not`/`invert_match`).

### Step 2: Verify 100% Parity on Randomized & Benchmark Streams (~28s)
```bash
go test -race -count=1 -v -run '^(TestIVMParity|TestIVMBakeoffBenchmarkStreamParity$)' .
```
**What to point out**: `TestIVMParity` verifies **0 mismatches across 13,800 randomized multi-batch traces**, and the sibling suites (`TestIVMParity_OTTLErrorLatticeAndStatefulHybrid`, `TestIVMParity_MultiShardAndOver32Nodes`, `TestIVMParityLatencyOracleB`) cover `ottl_condition` error propagation, stateful hybrids (`rate_limiting`, `bytes_limiting`, `composite`), multi-shard runs, and bounded latency, together covering all 17 built-in policy types. `TestIVMBakeoffBenchmarkStreamParity` proves 100% TraceID and span-count parity on the exact W1 (errors + cross-batch latency + probabilistic) and W2 (`35%` `/healthz` drop + errors + cross-batch `and` + latency) benchmark streams under in-order and out-of-order arrivals (`shards=1` and `shards=4`, including late `/healthz` `drop > sample` freeing buffered arena chunks).

### Step 3: Verify Lossless Compact Arenas & W3 Worst-Case Memory Drop (~8s)
```bash
go test -count=1 -v -run '^(TestWireCodec_LosslessPdataAndExactProtoSize|TestArena_LosslessRoundTripAll5Modes|TestArena_W3WorstCaseMemoryReduction)$' .
```
**What to point out**: Even on W3 (`0%` short-circuit worst case, `49,860` buffered spans across 5 traffic shapes including ephemeral unique resources), `lz4Heap` cuts retained Go heap by **85.0% to 90.3%** and live heap objects by **99.9%**, while tick-expiring all 3,840 traces with **0 unmarshals** and **0 leaked slabs**.

### Step 4: Run a Live Head-to-Head 100k Spans/Sec Subprocess Comparison (~45s)
```bash
go test -c -o /tmp/ts_demo.test .
GOGC=100 IVM_BAKEOFF_RUN=1 IVM_CANDIDATE=trace-complete IVM_WORKLOAD=w2 IVM_SHAPE=mixed IVM_SHARDS=4 IVM_RATE=100000 /tmp/ts_demo.test -test.run='^TestIVMBakeoffSubprocess$' -test.v | grep BAKEOFF_JSON
GOGC=100 IVM_BAKEOFF_RUN=1 IVM_CANDIDATE=ivm-lz4-heap   IVM_WORKLOAD=w2 IVM_SHAPE=mixed IVM_SHARDS=4 IVM_RATE=100000 /tmp/ts_demo.test -test.run='^TestIVMBakeoffSubprocess$' -test.v | grep BAKEOFF_JSON
rm -f /tmp/ts_demo.test
```
**What to point out**: On W2 (`35%` `/healthz` drop + errors + cross-batch `and` + latency), watch `steady_vm_rss_bytes` drop from `~1,938 MB` to `~353 MB` (`-81.8%`), `steady_gc_scan_heap_bytes` drop from `~1,206 MB` to `~56 MB` (`-95.3%`), `window_gc_cpu_seconds` drop from `~9.1s` to `~0.24s` (`-97.4%`), `ingest_p99_us` drop from `~11,908 us` to `~746 us`, `zero_unmarshal_drop_ratio` hit `1.0` (`100%`), and `sampled_spans_out` match exactly (`53,980` vs `53,980`).

### Step 5 (Optional): Confirm Zero Regression on Upstream `trace-complete` (~6 Minutes)
```bash
# Build base test binary from upstream commit db80cc0daf0 in a temporary worktree, then compare against current tree:
git worktree add --detach /tmp/ts_base_wt db80cc0daf0
(cd /tmp/ts_base_wt/processor/tailsamplingprocessor && go test -c -o /tmp/ts_base.test .)
git worktree remove --force /tmp/ts_base_wt
go test -c -o /tmp/ts_head.test .

for i in 1 2 3 4 5 6; do
  /tmp/ts_base.test -test.run='^$' -test.bench='^(BenchmarkSampling|BenchmarkProcessorThroughput|BenchmarkSplitResourceSpansByTrace)$' -test.benchtime=500ms -test.count=1 >> /tmp/cpu_base.txt
  /tmp/ts_head.test -test.run='^$' -test.bench='^(BenchmarkSampling|BenchmarkProcessorThroughput|BenchmarkSplitResourceSpansByTrace)$' -test.benchtime=500ms -test.count=1 >> /tmp/cpu_head.txt
  /tmp/ts_base.test -test.run='^$' -test.bench='^BenchmarkProcessorMemory$' -test.benchtime=1x -test.count=1 >> /tmp/mem_base.txt
  /tmp/ts_head.test -test.run='^$' -test.bench='^BenchmarkProcessorMemory$' -test.benchtime=1x -test.count=1 >> /tmp/mem_head.txt
done
benchstat /tmp/cpu_base.txt /tmp/cpu_head.txt
benchstat /tmp/mem_base.txt /tmp/mem_head.txt
rm -f /tmp/ts_base.test /tmp/ts_head.test /tmp/cpu_base.txt /tmp/cpu_head.txt /tmp/mem_base.txt /tmp/mem_head.txt
```
**Expected output**: `retained-B/span` and `retained-B/trace` show `-0.00%` delta (`p >= 0.472`), and all CPU/throughput metrics show `0` statistically significant regressions (`p >= 0.121`).

---

## Option B: AI-Narrated Walkthrough Video Storyboard & Script (~3 Minutes 25 Seconds)

Ready for Phase 4 1080p MP4 rendering if requested. All spoken narration avoids raw code symbols, backticks, and URLs, and targets ~140-150 words per minute.

### Scene 1: The Problem & Why Existing Fast Paths Fail (0:00 - 0:38)
- **Visual**: Side-by-side diagram of `trace-complete` buffering `2M` spans on W1 (`4.0 GB` RSS, `32.9M` retained heap objects, `21.6s` GC CPU in 10s) vs. upstream `span-ingest` silently dropping 100% of traces when a drop policy is present.
- **Narration**:
  "Tail sampling in the OpenTelemetry Collector is expensive because it keeps full decoded trace trees on the Go heap for the entire decision window. At two hundred thousand spans per second over ten seconds, standard tail sampling uses over four gigabytes of memory and burns more than two full CPU cores scanning nearly thirty three million live Go objects. The existing single batch fast path tries to evaluate spans on arrival, but it breaks parity on four common patterns, including dropping every trace whenever a drop policy is combined with a positive policy."

### Scene 2: The 5-State Incremental Decision Circuit (0:38 - 1:18)
- **Visual**: Animated state diagram showing the 5-state lattice (`DefTrue`, `DefFalse`, `DefError`, `MayTrue`, `MayFalse`) stored in a pointer-free flat slot array, with compile-time safety gates for drop precedence, stateful rate limiters, and winner ordering.
- **Narration**:
  "Our prototype introduces an incremental sampling strategy built on two ideas. First, we compile sampling policies into a five state incremental decision circuit stored in a pointer free trace table. Every incoming span updates lightweight per trace state. Safe positive matches short circuit immediately and stream straight through. Deterministic drops eject right away and free any buffered spans. Pending non sampled traces expire at window close without ever unmarshaling their payloads. Across thirteen thousand eight hundred randomized multi batch traces plus targeted error propagation, stateful hybrid, and multi shard suites covering all seventeen built in policy types, it achieves one hundred percent parity with standard tail sampling."

### Scene 3: Decoupled Compact Arenas & Direct Wire Encoding (1:18 - 1:50)
- **Visual**: Data flow diagram: incoming `ptrace.Traces` -> direct OTLP proto3 encode into reusable scratch buffer -> separate Resource & Scope intern slabs -> per-chunk pure-Go LZ4 compression -> pointer-free `noscan` slabs with `<25%` generational compaction.
- **Narration**:
  "Second, we decouple metadata from payloads. For pending traces that must wait for the decision window, we encode spans directly into compact protocol buffer wire chunks using a reusable scratch buffer, intern repeated resource and scope headers onto separate compacted slabs, and compress chunks in pure Go using a lightweight LZ4 codec with zero new dependencies. Because all payload bytes live in pointer free byte slabs, the Go garbage collector skips scanning them entirely."

### Scene 4: Bakeoff Results & The Surprising `mmap` Discovery (1:50 - 2:42)
- **Visual**: Bar charts from `milestone_3_evidence.md` comparing `trace-complete`, `ivm-interned-heap`, `ivm-lz4-heap`, and `ivm-lz4-mmap` across 200k W1 (errors + latency + probabilistic), 200k W2 (`35%` `/healthz` drop + errors + cross-batch `and`), and 100k W3 (`0%` short-circuit across all 5 shapes including ephemeral unique resources), plus a callout showing `mmap` GC cycles (`19` vs `5`) and `GOGC=400` RSS parity (`464 MB` vs `461 MB`).
- **Narration**:
  "We benchmarked every candidate serially in isolated processes from fifty thousand to two hundred thousand spans per second across eighty one runs. The biggest surprise was off heap memory mapping. Hiding payload bytes from Go shrinks the visible live heap so much that default Go garbage collection runs two to four times more often, burning two to four and a half times more garbage collection CPU than pure Go slabs. By contrast, compressed pure Go byte slabs cut steady memory by seventy eight to eighty three percent at two hundred thousand spans per second, slash garbage collection CPU by up to ninety nine percent, cut total active CPU by over seventy percent, and still save seventy five to eighty three percent memory in the zero short circuit worst case."

### Scene 5: Ablation, Throughput, & Zero Baseline Regression (2:42 - 3:08)
- **Visual**: Split-bypass ablation chart (`465 MB` vs `461 MB` RSS; `1,965 us` vs `692 us` ingest p99; `419k` vs `598k sps`), unpaced 4-shard throughput (W1 `467k` -> `598k` LZ4 / `851k` uncompressed; W2 `426k` -> `662k` / `949k`), and `benchstat` green badge (`-0.00%` retained bytes).
- **Narration**:
  "An ablation study confirmed that ninety nine percent of the memory and garbage collection savings come from the incremental circuit and compact arenas rather than bypassing batch splitting, while the split bypass cuts ingest tail latency by nearly three to four times and lifts four shard throughput by over forty percent. Meanwhile, full package benchmarks confirm zero regression on existing standard tail sampling."

### Scene 6: Decision & Next Steps (3:08 - 3:25)
- **Visual**: 3-step upstream PR roadmap (PR 1: upstream bug fixes; PR 2: IVM engine; PR 3: pure-Go `ivm-lz4-heap` arena).
- **Narration**:
  "Our recommendation is to upstream the incremental strategy with compressed pure Go byte slabs as the default storage backend, avoiding memory mapping complexity altogether. We can split the work into three clean pull requests: standalone fixes for the upstream bugs we uncovered, the incremental decision circuit, and the pure Go compressed slab store."
