# Prototype Charter & Plan: `tail_sampling` Incremental Predicate Short-Circuiting & Metadata/Payload Decoupling

## 1. Ground-Truth Audit of Upstream `tailsamplingprocessor` (Baseline `HEAD`)

Before proposing new code, we audited `open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor` at `upstream/main` (`v0.165.1+`, `HEAD` commit `db80cc0daf0`, last `tailsamplingprocessor` commit `f63d60bada0`). Upstream already includes `sampling_strategy: "span-ingest"`, `num_shards`, and `tailstorageextension` (`pebbletailstorageextension`). However, our code audit and baseline memory benchmark uncovered **two structural problems**:

### 1.1 Retained Go Heap & GC Pressure in `inMemoryTailStorage`
Running upstream's own `BenchmarkProcessorMemory` (`30` ticks × `128` traces/batch, `1,662` spans/batch = `12.984` spans/trace avg, `14` resource attributes + `7` base span attributes per span):
- `shape=mixed` (`4` resources per batch, spans spread across all `4`): **`892.7 retained-B/span`**, **`11,592 retained-B/trace`**.
- `shape=resource-per-trace`: **`723.6 retained-B/span`**, **`9,395 retained-B/trace`**.
- `shape=few-traces-per-resource`: **`725.2 retained-B/span`**, **`9,416 retained-B/trace`**.
- `shape=sdk` (`1` resource, `128` traces): **`725.4 retained-B/span`**, **`9,419 retained-B/trace`**.

Why it stays so large and GC-heavy:
1. `inMemoryTailStorage` holds live Go `ptrace.Traces` object trees (`map[pcommon.TraceID]ptrace.Traces`) on the Go heap for the full `decision_wait` (`30s` default). Every span creates dozens of Go heap pointers (`KeyValue` slices, string headers, `anyValue` structs), forcing Go's concurrent tri-color GC to scan tens of millions of pointers at high scale (`200k spans/sec × 30s = 6M buffered spans ≈ 5.36 GB` decimal / `4.99 GiB` live heap for `mixed` shape). Note: in Go 1.26, STW pause duration (`/gc/pauses:seconds`) is often sub-millisecond even on multi-GB heaps; pointer-heavy heap cost manifests strongly in **concurrent GC mark-scan CPU** and **mutator mark-assist** (`/cpu/classes/gc/total:cpu-seconds`, `/cpu/classes/gc/mark/assist:cpu-seconds`, `/gc/scan/heap:bytes`).
2. `splitResourceSpansByTrace` (`processor.go:1051-1142`) copies `Resource` (`14` attributes, `~531 B` protobuf) into every trace that shares a `ResourceSpans` (`resourceOwner.rs.Resource().CopyTo`), multiplying resource memory by the number of traces per batch (up to `4×` per trace in `mixed`).
3. At `decision_wait` expiry (`processor.go:787-863`), even with an external `TailStorage` like `pebbletailstorageextension`, the processor must call `TakeMany` / `UnmarshalTraces` on **100%** of pending traces to run policy evaluation, then immediately discards the **95%–99%** that resolve to `NotSampled`.

### 1.2 Live Issues in Upstream `sampling_strategy: "span-ingest"` & `latency.go`
Upstream's `span-ingest` (`processor.go:995-1045`, `763-778`, `1249-1293`) evaluates only the **single incoming batch** (`spanIngestTraceData`) without maintaining incremental trace state across batches. Auditing the source reveals **4 decision-parity bugs**, **1 telemetry/allocation/`Composite.IsStateful()` gap**, and **1 upstream `latency.go` arrival-order quirk**:

**4 Decision-Parity Bugs in `span-ingest` vs. `trace-complete`:**
1. **`drop` + any positive policy drops 100% of non-dropped traces**: In `makeDecisionOnSpanIngest` (`processor.go:1001-1030`), whenever a `drop` policy returns a non-`Dropped`, non-error result on the incoming batch, `dropPolicyPending = true` causes the loop to `break` before evaluating any positive policy (exception: if a `drop` policy returns `err != nil`, `processor.go:1013-1017` `continue`s without setting `dropPolicyPending`). On tick (`processor.go:763-778`), `samplingPolicyOnTick` finalizes every pending trace as `NotSampled` **without re-evaluating policies**. Result: **0% of non-erroring traces are sampled whenever a `drop` policy exists**.
2. **Cross-batch `and` and `drop` multi-subpolicy fail silently**: If `AND(service.name == "service-1", http.response.status_code == 500)` receives `service.name == "service-1"` in batch 1 and `http.response.status_code == 500` in batch 2, both single-batch evaluations return `NotSampled`, and tick cleanup drops the trace. The same gap hits multi-subpolicy `drop` policies.
3. **Cross-batch `latency` fails silently**: `latency.Evaluate` (`internal/sampling/latency.go:36-61`) reports `IsStateful() == false` when `upper_threshold_ms == 0`, so `span-ingest` accepts it. However, `latency.Evaluate` computes `maxEnd - minStart` using only spans in the single incoming batch, missing traces whose total duration spans across batches.
4. **`not` and `invert_match` emit premature false positives**: `not.IsStateful()` delegates to `subPolicyEvaluator.IsStateful()` (`internal/sampling/not.go:51-53`), and `string_attribute`/`numeric_attribute`/`boolean_attribute` all hardcode `IsStateful() == false` even when `invert_match: true`. Thus `not(status_code == ERROR)` samples immediately on an `OK` batch even if a subsequent batch has `ERROR`.

**1 Operational / Telemetry / Allocation / Stateful-Check Gap:**
5. **Cached vs. `NopCache` lifecycle bugs, per-batch map allocations, & `Composite.IsStateful()`**:
   - When `decision_cache` **is** configured (`SampledCacheSize > 0` or `NonSampledCacheSize > 0`), `releaseSampledTrace`/`releaseNotSampledTrace` (`processor.go:1412-1428`) call `dropTrace(id)` without calling `decisionBatcher.RemoveFromBatch(id, batchID)`, causing `samplingPolicyOnTick` (`processor.go:751-754`) to falsely increment `trace_dropped_too_early` on already-decided traces.
   - When `decision_cache` is **NOT** configured (`NopCache`), `releaseSampledTrace`/`releaseNotSampledTrace` do NOT call `dropTrace(id)`, so early-decided traces remain in `idToTrace` past tick expiry (`processor.go:759-761` skips them on tick, leaking until `NumTraces` eviction or deadlocking `block_on_overflow: true`). Furthermore, late spans hitting `finalDecision == NotSampled` re-fire `nonSampledHooks` (`processor.go:1313-1315`), and late spans hitting `finalDecision == Dropped` log `"Unexpected sampling decision"` warnings (`processor.go:1316-1317`).
   - `newPolicyEvaluationMetrics` (`processor.go:1250`) allocates a `[]map[Decision]policyDecisionMetrics` on every single incoming span batch.
   - `Composite.IsStateful()` (`composite.go:138-145`) returns `false` when subpolicies are stateless despite stateful per-second `sampledSPS` counters.

**1 Upstream `latency.go` Quirk (`trace-complete` itself):**
6. **Prefix short-circuit & zero-timestamp quirk in `internal/sampling/latency.go:44-60`**: When `upper_threshold_ms > 0`, `latency.Evaluate` checks `thresholdMs < ms && ms <= upperThresholdMs` inside `hasSpanWithCondition` on each span prefix in arrival order, returning `Sampled` early on a prefix `[0, 6s]` even if a later span widens the trace to `[0, 15s]` exceeding `upper_threshold_ms = 10s`. Additionally, `if minTime == 0` overwrites a real `0` start timestamp if a subsequent span arrives, and `int64(maxTime - minTime)` wraps negative on unsigned underflow if `maxTime < minTime` (clock skew).

---

## 2. Core Hypothesis & Objectives

**Hypothesis**: By combining (a) a **5-state incremental decision circuit** (IVM/DBSP-style metadata delta engine supporting error propagation, recursive stateful-side-effect guards, and exact threshold/winner ordering) with (b) a **decoupled, pointer-free, Resource/Scope-interned OTLP wire-format payload arena** written directly from `pdata` into reusable scratch buffers (plus optional per-batch-chunk zero-dependency LZ4 compression), we can simultaneously achieve:
1. **100% decision parity** with `trace-complete` across in-order and out-of-order multi-batch traces (fixing all 4 parity bugs of `span-ingest`), with explicit oracle separation only for the documented `latency.go` arrival-order prefix bug and mid-window `SetSamplingPolicy` reloads on already-short-circuited traces.
2. **`>= 50%` reduction in Go `HeapAlloc`** (across **all 4 shapes** including `sdk` and `resource-per-trace` via interning/LZ4 or `mmap`), **`>= 85%` reduction in live `HeapObjects`**, AND **`>= 30%–40%+` real OS `VmRSS` reduction** under sustained multi-GB load (proving physical RAM savings, not just moving heap to `mmap`).
3. **`>= 50%` reduction in Go GC mark-scan CPU & scanned heap bytes** (`/cpu/classes/gc/total:cpu-seconds`, `/gc/scan/heap:bytes`) with STW pause p99 `<= baseline` under sustained **50k–200k spans/sec** bursts, and **zero unmarshaling** of pending traces that resolve to `NotSampled` at window close.
4. **Zero regression** on existing `trace-complete` benchmarks (`benchstat` across the full `tailsamplingprocessor` package).

---

## 3. Scope, Exact Semantics, & Intentional PoC Simplifications

### In Scope
- Opt-in **`sampling_strategy: "incremental"`** integrated inside `processor/tailsamplingprocessor` (**zero changes to `go.mod` / `go.sum`**; strictly stdlib `encoding/binary`, `hash/fnv` or `hash/maphash`, and `syscall.Mmap` with `syscall.MAP_ANON` under `//go:build linux || darwin`, isolating Linux-only `syscall.Madvise` into `//go:build linux`). Zero breaking changes to default `trace-complete` or `span-ingest`. Existing `tsp.idToTrace`, `tsp.deleteTraceQueue`, and `tsp.workChan` remain completely untouched for `trace-complete` and `span-ingest` so all upstream tests compile and pass unchanged.
- **Full built-in policy coverage** across all 17 `PolicyType` values in `config.go:20-59`: `always_sample`, `status_code`, `string_attribute` (exact, regex+LRU, `invert_match`), `numeric_attribute` (`invert_match`), `boolean_attribute` (`invert_match`), `latency` (lower + upper bound), `span_count` (`min_spans`, `max_spans`), `probabilistic` (legacy FNV + `usetracestate` `rv`/`th`, compiled using the global feature gate `metadata.ProcessorTailsamplingprocessorUsetracestateFeatureGate.IsEnabled()` matching `probabilistic.go:66`), `trace_state`, `trace_flags`, `ottl_condition` (span + spanevent), `and`, `not`, `drop`, plus graceful hybrid window-close evaluation for `rate_limiting`, `bytes_limiting`, `composite`, and custom `samplingpolicy.Extension`.

### Exact 5-State Incremental Lattice & Strictly `noscan` Per-Trace Table
Every shard running `sampling_strategy: "incremental"` uses a dedicated, strictly **`noscan`** trace table separate from `tsp.idToTrace` / `tsp.deleteTraceQueue`:
- `traceIndex map[pcommon.TraceID]uint32` (key = `[16]byte`, value = `uint32` slot index — **0 Go pointers**!).
- Flat `slots []ivmTraceSlot` containing **strictly pointer-free scalar fields**: `traceID [16]byte`, `arrivalNano int64`, `decisionNano int64` (never `time.Time`, which carries `*time.Location`!), `spanCount int64`, `sizeBytes uint64`, `batchID uint64`, `minStart uint64`, `maxEnd uint64`, `hasTimestamps uint8`, `finalDecision int32`, `policyIdx int32`, `finalThreshold uint64`, `circuitVer uint32`, `tsFlags uint8` (`hasSamplingInfo`/`hasRandomness`), `tsRandomness uint64`, `errTableIdx uint32` (index into a tiny out-of-band error slice, never a `string` inside `ivmTraceSlot`!), intrusive FIFO `prevSlot uint32` / `nextSlot uint32` (0 pointers, replacing `*list.List` for `incremental`!), and `chunkHead uint32` / `chunkTail uint32`.
- Flat `nodeStates []uint8` stride slice (`slotIdx * numNodes`, supporting **arbitrary `numNodes`** including `> 32` circuit nodes without fixed-array overflow or pointers!).
- Channel handoff (`rawWorkChan`): sized dynamically to mirror `cap(tsp.workChan)` at `Start()` (`0` when `withTestController` replaces `workChan` with an unbuffered channel, preserving deterministic test synchronization), and `Shutdown()` closes the active channel cleanly so zero queued batches are dropped.

Every circuit node tracks a **5-state value** (`uint8`):
- `StateDefTrue` (1): Permanently `true` for all future extensions $T' \supseteq T$.
- `StateDefFalse` (2): Permanently `false` for all future extensions $T' \supseteq T$.
- `StateDefError` (3): Evaluation errored on a span (`ottl_condition` with `error_mode: propagate` or erroring evaluator).
- `StateMayTrue` (4): Currently `true` if the window closed now; future spans may still change it.
- `StateMayFalse` (5): Currently `false` if the window closed now; future spans may still change it.

**Recursive Stateful / Side-Effect Tagging:**
At compile time, recursively walk the policy tree (`and`, `not`, `drop`, `composite`) and mark every node whose subtree contains **`rate_limiting`**, **`bytes_limiting`**, **`composite`**, or a custom **`samplingpolicy.Extension`** as `hasSideEffect = true` (opaque/stateful). Also compute each positive policy's static **`minPossibleThreshold`** (`probabilistic` = `tracestateThreshold`, `and` = `max(subTh)`, all other built-ins = `AlwaysSampleThreshold`).

**Exact Combinator Truth Tables (Side-Effect Safe):**
- **`NOT(X)`** (`not.go:33-48` bubbles `err` directly without inverting):
  - `DefTrue -> DefFalse`, `DefFalse -> DefTrue`, `MayTrue -> MayFalse`, `MayFalse -> MayTrue`, **`DefError -> DefError`**.
- **`AND(X_1, ..., X_m)`** and **`DROP(X_1, ..., X_m)`** (ordered evaluation matching `and.go:62-75` and `drop.go:36-46`):
  - Empty subpolicy list (`m == 0`): vacuously **`DefTrue`** (`Sampled` for `AND`, `Dropped` for `DROP`).
  - Walk `i = 0 .. m-1` in slice order:
    - If `X_i` has `hasSideEffect == true`: **STOP incremental short-circuiting here** — defer full left-to-right evaluator execution of this combinator (and never short-circuit `AND`/`DROP` to `DefFalse` based on a later `X_k == DefFalse` when `k > i`, because upstream `and.go`/`drop.go` run `X_i` first and consume limiter tokens!).
    - If `X_i == DefFalse` (and NO earlier `X_j`, `j < i`, has `hasSideEffect == true`): short-circuit and return **`DefFalse`** (an earlier `NotSampled` suppresses any later `err`, matching `and.go:68-70` & `drop.go:42-44`).
    - If `X_i == DefError`: if all prior `X_0 .. X_{i-1}` are `DefTrue`, return **`DefError`** (an earlier `err` propagates, preventing `drop` at `processor.go:662-666`); if any prior `X_j` is `MayFalse`/`MayTrue`, return `MayFalse` until tick finalization, where slice-order evaluation resolves `false` vs `err` identically to `and.go`/`drop.go`.
  - If all `X_i == DefTrue` -> **`DefTrue`**.
  - Otherwise, if no `X_i` is `DefFalse`, no earlier-than-first-`DefError` `X_j` is `MayFalse`, and all `X_i ∈ {DefTrue, MayTrue}` -> **`MayTrue`**; else **`MayFalse`**.
- *Note on arrival-order edge case for `ottl_condition` when a trace has BOTH a matching span AND an erroring span*: `ottl.go:88-115` returns on whichever span it encounters first (`Sampled` if match precedes error, `Error` if error precedes match). Our circuit latches whichever occurs first on ingest (`DefTrue` or `DefError`). Parity tests verify error-only, match-only, and in-order mixed traces.

**Exact Top-Level Short-Circuit Safety Gates (`drop > sample` parity):**
1. **Immediate Early Ejection (`Dropped`)**: Triggered on `drop_i == DefTrue` ONLY when:
   - `drop_i` AND all earlier `drop_j` (`j < i`) have `hasSideEffect == false` (so early ejection never skips `limiter.AllowN` token consumption on an earlier stateful `drop` subpolicy!), **AND**
   - If `len(nonSampledHooks) > 0` (which receives `td.PolicyName`), no earlier `drop_j` (`j < i`) is still pending (`MayFalse`/`MayTrue`).
   - *(Note: once all earlier `drop` policies are pure and non-matching, early `drop` ejection IS safe even when later positive policies contain `BatchEvaluator`, `rate_limiting`, `bytes_limiting`, or `composite`, because `processor.go:824-838` excludes dropped traces before `runBatchPolicies` and positive evaluation!)*
2. **Immediate Early Ejection (`NotSampled`)**: Triggered when:
   - ALL `drop` policies are `DefFalse` or `DefError` (and have `hasSideEffect == false`), **AND**
   - ALL positive policies are `DefFalse` or `DefError` (and have `hasSideEffect == false`; note: under `usetracestate` with `0 < pct < 100`, `probabilistic` holds in `MayTrue`/`MayFalse` until explicit `rv` arrives, so this early ejection fires for `probabilistic` ONLY when `usetracestate` is disabled OR an explicit `rv` has arrived!).
3. **Immediate Positive Short-Circuit (`Sampled`)**: Triggered on positive policy `i` (`DefTrue`) ONLY when **ALL** of the following hold:
   - ALL `drop` policies have reached `DefFalse` or `DefError` and have `hasSideEffect == false`.
   - **Recursive Stateful / Batch / Extension Guard**:
     - Config contains NO `BatchEvaluator` (`useTracestate` + `rate_limiting`/`bytes_limiting`) anywhere, AND NO custom `samplingpolicy.Extension` anywhere (including recursively inside `and`/`not`/`drop`/`composite`).
     - If `sample_on_first_match == false`: NO positive policy anywhere in the config recursively has `hasSideEffect == true` (since `trace-complete` evaluates all policies and consumes tokens even after an earlier `Sampled`).
     - If `sample_on_first_match == true`: NO positive policy at index `j <= i` recursively has `hasSideEffect == true`.
   - **Unified Threshold & `PolicyName` Winner Guard** (`processor.go:935-941` runs `ThresholdLessThan(th, threshold)` unconditionally regardless of `tsp.useTracestate`, and `probabilistic.go:104-108` always reports `s.tracestateThreshold`!):
     - Let `th_i` be policy `i`'s resolved threshold.
     - If **`sample_on_first_match == true`**:
       - If `useTracestate || recordPolicy || len(sampledHooks) > 0`: require ALL earlier positive policies (`j < i`) to be `DefFalse` or `DefError` (so the advertised `th` / `PolicyName` matches config order).
       - Otherwise (none of those enabled): latch immediately on `DefTrue`!
     - If **`sample_on_first_match == false`**:
       - If `recordPolicy || len(sampledHooks) > 0` (regardless of `useTracestate`!): wait if ANY earlier (`j < i`) pending policy has `minPossibleThreshold <= th_i` OR ANY later (`j > i`) pending policy has `minPossibleThreshold < th_i`.
       - Else if `useTracestate == true`: wait if ANY other (`j != i`) pending policy has `minPossibleThreshold < th_i`.
       - Otherwise (none of those enabled): latch immediately on `DefTrue`!
   - *(Note on `NopCache` vs cached post-latch `Sampled` attributes: on `incremental`'s `NopCache` latched-`Sampled` fast path, stamp `tailsampling.policy` via `sampling.SetAttrOnScopeSpans` whenever `tsp.recordPolicy && policyName != ""` so in-window spans match `trace-complete`. On cached mode (`SampledCacheSize > 0`), `processCachedTrace` stamps `tailsampling.cached_decision = true` on post-latch spans, which is explicitly documented and normalized in `TestIVMParity`.)*
4. **Deferred Positive Latch Under Pending `drop`**: When positive policy `i` reaches `DefTrue` while a pure `drop` policy is still pending (and no stateful/winner guard requires further positive eval), the circuit stops evaluating further positive policies, evaluates ONLY the still-pending `drop` policies on future spans, and resolves in O(1) at tick expiry without re-scanning buffered payloads.

### Direct Zero-Alloc `pdata -> OTLP` Wire Encoder & Compact Payload Arenas (`wire_codec.go` + `arena_storage.go`)
- **Direct Stdlib OTLP Proto3 Wire Writer (`wire_codec.go`) — True 1-Copy, 0 Per-Batch Heap Alloc**:
  - Instead of calling `ptrace.ProtoMarshaler.MarshalTraces(td)` (which allocates a brand-new `~570 KiB` heap `[]byte` on every batch, marshals even early-dropped spans, and emits fields in reverse order), we implement a compact stdlib `encoding/binary` direct OTLP wire writer (`appendResourceHeader`, `appendScopeHeader`, `appendSpan`) writing directly into a **reusable per-shard `[]byte` scratch buffer**!
  - Covers every public field of `pcommon.Resource` (`attributes`, `dropped_attributes_count`), `pcommon.InstrumentationScope` (`name`, `version`, `attributes`, `dropped_attributes_count`), `ptrace.ResourceSpans.SchemaUrl`, `ptrace.ScopeSpans.SchemaUrl`, `ptrace.Span` (all 16 fields including `events`, `links`, `status`, `flags`, `tracestate`, dropped counts), and `pcommon.AnyValue` (all 7 variants: string, bool, int, double, array, kvlist, bytes).
  - Early-dropped and already-latched `Sampled` spans perform **0 serialization** (`0` copies into scratch or slab!).
  - Additionally includes a stdlib wire-level frame splitter/reassembler (`splitRawWire` / `reassembleRawWire`) handling wire types `0–5` (with bounded group recursion depth `<= 64`), `schema_url`, omitted proto3 zero `TraceID`, reverse field order (`Resource` field 1 emitted after `ScopeSpans` field 2), and arbitrary unknown fields — verified via wire-level round-trip (`rawBytes -> extract/intern -> reassemble -> rawBytes`), since `pdata`'s `ProtoUnmarshaler` itself discards unknown fields (`ConsumeUnknown`).
- **Per-`(traceID, incoming ResourceSpans)` Grouping (Preserves `TracesSize` & Multi-Scope Fidelity)**:
  - Each slab chunk groups **all spans of `traceID` across all matching `ScopeSpans` within ONE incoming `ResourceSpans`**, keeping `ResourceSpans` grouping identical to `inMemoryTailStorage` (`processor.go:1089-1129`, `inmemory.go:32-34`). This prevents `Resource` duplication on multi-scope traces (`net/http` + `database/sql`), preserves exact `TracesSize` for `bytes_limiting` (`bytes_limiting.go:71-75`), AND doubles LZ4 chunk size on multi-scope traffic!
  - *(Specifically: each chunk encodes one `resID uint32` plus `k` scope runs `(scopeID uint32, spanCount uint32, spanWireBytes...)` — no cross-batch merging of separate `ResourceSpans`!)*
- **Intrusive `noscan` Refcounted Intern Tables (Separate Resource & Scope Tables, Slab-Compacted)**:
  - **Separate** `resourceIntern` and `scopeIntern` tables so constant long-lived `Scope` headers (`net/http`, `database/sql`) NEVER pin slabs holding churning ephemeral `Resource` (`k8s.pod.uid`) headers!
  - Content hash via stdlib `hash/fnv` (64-bit) using an **intrusive `noscan` hash map** **`buckets map[uint64]uint32`** chaining collisions via `entries[id].nextInBucket uint32` (`0` slice pointers -> 100% `noscan`!) + exact `bytes.Equal` verification.
  - Explicit `refCount uint64` incremented per chunk referencing the entry and decremented on `Take`/`Delete`/eviction; deleted immediately when `refCount == 0`.
  - **Map compaction with `peakEntries` reset**: When `liveEntries == 0`, reset `buckets` and set `peakEntries = 0`. When `liveEntries < peakEntries/4 && peakEntries >= 1024`, reallocate `buckets` to exact `liveEntries` AND **reset `peakEntries = liveEntries`** (eliminating `O(N^2)` rebuild thrashing!).
  - **Intern slab storage & compaction**: Both `resourceIntern` and `scopeIntern` store payload bytes in **tick-sealed slabs** (`noscan` `[]byte` for heap arenas, **`mmap` slabs** for `mmap` arenas!) with the same **`< 25%` live-byte compaction** (relocating surviving intern entries and updating `entries[id].slabIdx/offset` on tick rotation) so even a single pinned long-lived `Resource` sharing a slab with ephemeral pods never leaks RSS monotonically!
- **Utilization-Aware Slab Rotation, Generational Compaction, & Cross-Platform `mmap` Safety**:
  - Active chunk slab defaults to `256 KiB` (scaling smoothly without a `16 MiB` minimum floor across 4 shards). On tick (`OnTick()`), utilization-aware `sealActive()` releases empty slabs immediately and seals active slabs only when `>= 75%` full or fragmented (`< 50%` live) after aging past `decisionWaitTicks` (preventing low-volume ticks from wasting 70%–80% tail slack while still rotating strictly per-tick at high volume!).
  - Slabs pinned past `decision_wait` with `< 25%` live byte utilization automatically compact their surviving chunks/entries into a dedicated generational `compactSlabID` (so aged surviving items never re-mix with churning ephemeral ingest), and the old slab recycles immediately.
  - **Darwin-safe build tags** (`0` `go.mod` edits):
    - `arena_mmap_unix.go` (`//go:build linux || darwin`): uses `syscall.MAP_ANON` (NOT `syscall.MAP_ANONYMOUS` which is missing on Darwin!).
    - `arena_madvise_linux.go` (`//go:build linux`): calls `syscall.Madvise(b, syscall.MADV_DONTNEED)` when reclaiming idle slabs.
    - `arena_madvise_other.go` (`//go:build !linux`): no-op / `Munmap` reclamation stub (so `GOOS=darwin GOARCH=arm64 go build ./...` compiles cleanly!).
  - **Lazy allocation & leak-proof shutdown**: Allocate `mmap` slabs lazily on first `Append` (never leaking mappings if `Start()` fails before `go tsp.loop()`), and run **`defer tsp.closeTailStorage()`** (`syscall.Munmap`) on `loop()` exit covering normal `Shutdown`, `DropPendingTracesOnShutdown: true`, AND `ctx.Done()`.
- **Optional Zero-Dependency Pure-Go LZ4 Block Compression (`lz4_codec.go`)**:
  - Exact wire math (`~344.5 B/span` uncompressed vs `723.6–725.4 B/span` on `sdk`/`resource-per-trace`/`few-traces-per-resource` -> only `~17–18 B/span` headroom for `50%`!) motivates adding a ~150-line pure-Go LZ4 block encoder/decoder (zero external deps!) compressing per-`(trace, rs)` chunks when `len >= 128` and compressed size `< 90%` of raw size (else stored raw with 1-bit flag). Single-trace `Take()` remains O(trace size) with zero full-slab decompression, compressing realistic multi-span chunks by `~2.5×–5×` so **even worst-case W3 (`0%` short-circuit) clears `>= 50%` on `HeapAlloc` AND `VmRSS` across ALL 4 shapes!**

### Exact Parity Handling for All Processor Lifecycle Contracts
- **`block_on_overflow: true` + `NopCache`**: On tick expiry, already-latched `NopCache` traces (`FinalDecision != Unspecified`) MUST be evicted from the trace table (preventing the upstream `block_on_overflow` deadlock at `processor.go:634-637, 759-761`).
- **Late `Dropped` / `NotSampled` spans under `NopCache`**: Silently discarded with 0 allocations, 0 `nonSampledHooks` re-fires, and 0 `"Unexpected sampling decision"` warnings.
- **`DecisionWaitAfterRootReceived > 0`**: Skips `MoveToEarlierBatch` once a trace is already latched (`FinalDecision != Unspecified`) so the latch window is not prematurely shortened.
- **`maximum_trace_size_bytes`**: Sums equivalent per-trace `SizeBytes` (including `Resource`/`Scope` overhead matching `m.ResourceSpansSize`). Under **cached mode** (`nonSampledIDCache.Get(id)` hit after `Put`), calls `decisionBatcher.RemoveFromBatch` + evicts trace immediately. Under **`NopCache`**, immediately deletes buffered payload (`tailStorage.Delete(id)`), retains the tiny latched `FinalDecision = NotSampled` slot in the trace table, and **does NOT call `RemoveFromBatch`** so the scheduled tick cleans it up at `decision_wait` expiry (preventing both late-span trace rebirth AND `block_on_overflow` deadlock!).
- **Empty `ScopeSpans`**: Skipped identically to `splitResourceSpansByTrace` (`processor.go:1092, 1156`).
- **`withPolicies(...)` test injection**: When `tsp.policies` is injected directly with empty `cfg.PolicyCfgs`, cleanly falls back to standard evaluator-backed execution on the incoming batch.
- **`SetSamplingPolicy` hot-reload**: Recompiles the IVM circuit with incremented `circuitVer`; traces already buffered under an older `circuitVer` fall back to full evaluator execution at tick expiry. (Traces already short-circuited under the *old* policies before reload cannot be retroactively un-emitted; parity applies to traces still pending at reload time.)
- **Upstream `latency.go` edge cases**: Tracks explicit `hasTimestamps`, `minStart`, `maxEnd`. Handles `StartTimestamp == 0` explicitly (distinct from unset) and guards against unsigned underflow (`if maxEnd < minStart { ms = 0 }`). For `upper_threshold_ms > 0`, evaluates true trace-wide `(minStart, maxEnd)` rather than replicating `latency.go:44-60`'s arrival-order prefix bug (tested via separate oracle in M1).

### Explicitly Out of Scope (Intentional PoC Simplifications)
- Disk spillover / crash persistence (`pebbletailstorageextension` remains separate; `incremental` works with any `TailStorage` fallback if `tail_storage` is set).
- `config.schema.yaml` regeneration / `.chloggen` entries until user selects which candidate(s) to upstream.
- Windows `mmap` (falls back transparently to `noscan` heap slabs via `arena_mmap_fallback.go`).
- Exact per-policy metric execution-count parity when short-circuiting skips subsequent policies (documented transparently in `PROTOTYPE_LEARNINGS.md`).

---

## 4. Candidate Approaches, `shardedProcessor` Bypass Disclosure, & Symmetric Evaluation Rubric

**Fairness & Bypass Disclosure**: In `trace-complete` and `span-ingest`, `shardedProcessor.ConsumeTraces` calls `splitResourceSpansByTrace` (`sharded_processor.go:195`), which clones `Resource` once per trace in a shared `ResourceSpans` (`~280 µs`, `~850 KiB`, `~10.7k allocs/batch` on `mixed`). In `incremental` mode:
- We benchmark **BOTH `num_shards=1` AND `num_shards=4`** across all candidates.
- We explicitly run an **ablation (`ivm-with-split` vs `ivm-bypass-split`)** isolating the effect of bypassing `splitResourceSpansByTrace` (`num_shards=1`: direct batch handoff; `num_shards > 1`: splitting into at most `num_shards` buckets instead of `128` trace buckets!) from the effects of IVM short-circuiting, wire-format slabs, Resource/Scope interning, `mmap`, and LZ4 compression.

| Candidate | Strategy | Payload Storage | Purpose in Bakeoff |
| :--- | :--- | :--- | :--- |
| **Baseline A** | `trace-complete` | `inMemoryTailStorage` (`ptrace.Traces` Go heap) | Ground-truth correctness oracle & baseline RSS/Heap/GC/throughput. |
| **Baseline B** | `span-ingest` | `inMemoryTailStorage` (`ptrace.Traces` Go heap) | Upstream fast-path baseline; exposes 4 live parity bugs. |
| **Candidate 1 (`ivm-raw-slab`)** | `incremental` | `rawProtoSlabArena` (in-heap `noscan` `[]byte` slabs, NO interning) | Isolates wire-format decoupling + IVM circuit without interning. |
| **Candidate 2 (`ivm-interned-heap`)** | `incremental` | `internedHeapArena` (in-heap `noscan` `[]byte` slabs + Resource/Scope interning) | Pure portable Go (no `unsafe`, no `mmap`, no compression). |
| **Candidate 3 (`ivm-interned-mmap`)** | `incremental` | `internedMmapArena` (off-heap `syscall.Mmap` slabs + `mmap`-backed intern slabs) | Marginal delta of off-heap `mmap` vs `noscan` Go slabs (zero Go GC visibility). |
| **Candidate 4 (`ivm-lz4-heap` & `ivm-lz4-mmap`)** | `incremental` | Same interned arena + zero-dep pure-Go LZ4 per-chunk compression | Achieves `>= 50%` reduction on `HeapAlloc` AND `VmRSS` across ALL 4 shapes including worst-case W3 (`0%` short-circuit) and `sdk`/`resource-per-trace`. |

*(Every memory/GC benchmark runs **strictly serially in isolated OS subprocesses** with `GOGC=100` so OS `VmRSS` / `VmHWM` and GC metrics never contaminate adjacent runs.)*

### Pinned Workload Matrix (Shared Across All Candidates)
Common parameters unless stated otherwise: `GOGC=100`, `DecisionCache: {SampledCacheSize: 100_000, NonSampledCacheSize: 200_000}`, `NumTraces: 250_000`, `NumShards: 1` and `4`, `benchTraceSizes = [1, 2, 4, 8, 16, 48]` (`~13` spans/trace avg), `14` resource attrs + `7` base span attrs, spans split across **3 out-of-order incoming batches per trace** (shuffled).
1. **W1 — Production Short-Circuit (No Drop)**: `status_code=ERROR` (**`1%` and `5%`** error rate, error on random span), `latency > 500ms` (`3%` match, spanning across batches), `probabilistic(1%)`.
2. **W2 — Early Eject (`drop`) + Cross-Batch `AND` + Positive**:
   - `drop(http.route == "/healthz")` (`35%` of traces; `/healthz` set on root/early span),
   - `status_code=ERROR` (`2%` of non-dropped traces),
   - `AND(service.name == "service-1", http.response.status_code == 500)` (**explicitly setting `http.response.status_code == 500` with `StatusCodeUnset` in batch 2, while `service.name == "service-1"` is in batch 1**, so `status_code=ERROR` CANNOT subsume cross-batch `AND`!),
   - `latency > 500ms` (`3%`, spanning across batches).
3. **W3 — All-Pending Worst Case (`0%` Short-Circuit, `0%` Early Eject)**: `status_code=ERROR` with `0%` error traces across **all 4 shapes** (`mixed`, `resource-per-trace`, `few-traces-per-resource`, `sdk`) + **ephemeral unique resources** variant (`k8s.pod.uid` unique per trace). Pure worst-case storage & zero-unmarshal tick test.
4. **W4 — High-Volume Sustained Multi-Second Burst (`50k`, `100k`, and `200k` spans/sec)**:
   - Sustained paced bursts across **ALL THREE workloads (`W1`, `W2`, and `W3`)**:
     - **W4-W1**: `100k` & `200k spans/sec` × `10s` window (`1M` & `2M` live buffered spans) with **`1%` and `5%`** error rates (`mixed` shape).
     - **W4-W2**: `100k` & `200k spans/sec` × `10s` window (`1M` & `2M` live spans pre-drop) with `35%` `/healthz` early drop + `2%` error + cross-batch `AND` (`mixed` shape).
     - **W4-W3**: `100k spans/sec` × `10s` window (**`1M` concurrent live pending spans**, `0%` short-circuit) across `mixed`, `sdk`, `resource-per-trace`, and **ephemeral unique resources** (`k8s.pod.uid`).
     - Also `50k spans/sec` baseline point to measure scaling curve (`50k -> 100k -> 200k spans/sec`).
   - Protocol: `1` full warmup window + `1` full steady-state measurement window at live `GOGC=100` **WITHOUT** forcing `runtime.GC()` during the measurement window, plus end-of-window post-`runtime.GC()` retained snapshot.
   - Metrics recorded per isolated subprocess:
     - Peak & steady-state **Go `HeapAlloc`**, **`HeapInuse`**, **`HeapObjects`**, **`TotalAlloc`**
     - Peak & steady-state **OS `VmRSS`** & **`VmHWM`** (via `/proc/self/status`)
     - Go **`runtime/metrics` GC profile**: `/gc/pauses:seconds` (`p50`, `p95`, `p99`, `max`), `/cpu/classes/gc/total:cpu-seconds`, `/cpu/classes/gc/mark/assist:cpu-seconds`, `/gc/scan/heap:bytes`, `NumGC`
     - **Sustained max throughput** (`spans/sec`), **ingest p99 latency**, **tick p99 latency** (`ms`)
     - **Zero-unmarshal drop ratio** (`droppedWithoutUnmarshal / totalDecidedNotSampled`) and **slab fragmentation ratio** (`allocatedSlabBytes / livePayloadBytes`).

---

## 5. Milestones & Falsifiable Exit Gates

### Milestone 1: 5-State IVM Decision Circuit & Parity Oracle
- **Deliverable**: `ivm_circuit.go` (5-state node compiler + recursive side-effect tagging + strictly `noscan` flat `ivmTraceSlot` table + resource-once batch pre-eval), integration into `config.go`, `processor.go`, `sharded_processor.go` under `sampling_strategy: "incremental"` (leaving `tsp.idToTrace`, `tsp.deleteTraceQueue`, `tsp.workChan` completely untouched for `trace-complete` and `span-ingest`), and `ivm_parity_test.go`.
- **Verification Commands**:
  - `go test -race -count=1 ./...`
  - `go test -v -run 'TestIVMParity|TestUpstreamSpanIngestKnownParityFailures|TestIVMLifecycle' .`
- **Falsifiable Exit Gates & Baseline-vs-Change Proof**:
  1. `TestUpstreamSpanIngestKnownParityFailures` **PASSES by proving Baseline B (`span-ingest`) FAILS parity** against `trace-complete` on all 4 scenarios (`drop`+positive = 0% sampled, cross-batch `and` missed, cross-batch `latency` missed, `not`/`invert_match` premature false sample).
  2. `TestIVMParity` achieves **100.0% decision parity (`0` mismatches)** across **>= 5,000 randomized multi-batch traces** (in-order, reversed, shuffled out-of-order):
     - **Oracle A (`trace-complete`)**: All built-in policy types, nested `and`/`not`/`drop`, `invert_match`, `probabilistic` (with & without `usetracestate`, consistent `rv` per trace), `latency` (`upper_threshold_ms == 0` non-zero timestamps; non-prefix-divergent bounded `latency`), `span_count` (`min` & `max`, including `max < min`, `max < 0`, `min==0 && max==0`), `ottl_condition` (span + spanevent, both `error_mode: ignore` and `propagate`), `rate_limiting`/`bytes_limiting`/`composite` hybrid configs (including `BatchEvaluator` and stateful subpolicies nested inside `and`/`drop`/`not`!), `num_shards=1,4`, `decision_cache` on/off (normalizing `tailsampling.cached_decision = true` delta on cached post-latch spans), `sample_on_first_match` on/off, `recordPolicy` on/off (verifying `PolicyName` & `FinalThreshold` match even when `probabilistic` precedes `status_code` with `useTracestate == false`!), `WithSampledHooks`/`WithNonSampledHooks`, all subtle upstream quirks (Sec 6 item 5), and `SetSamplingPolicy` reload for still-pending traces.
     - **Oracle B (order-independent trace-wide `latency` oracle)**: Dedicated tests for prefix-divergent bounded `latency` (`[0, 6s]` then `[0, 15s]` with `5s..10s` bounds) and `StartTimestamp == 0` / clock-skew (`maxEnd < minStart`) verifying `incremental` is order-independent while documenting `trace-complete`'s prefix bug.
  3. **Early short-circuit & lifecycle proof (`TestIVMLifecycle`)**:
     - W1 (no drop): traces matching `ERROR` on span 1 emit BEFORE any tick fires (`0` ticks) and subsequent spans bypass buffering (`buffered_spans == 0`).
     - W2 (`drop`):
       - **Cached (`NonSampledCacheSize > 0`)**: `/healthz` traces evict immediately from `traceIndex` AND `decisionBatcher.RemoveFromBatch` (`dropped_too_early == 0`).
       - **`NopCache` (`NonSampledCacheSize == 0`)**: payload freed immediately (`bufferedBytes == 0`), tiny latched decision kept in `traceIndex` until tick expiry, late spans dropped silently with 0 warnings / 0 hook re-fires, tick removes from `traceIndex` without deadlocking `block_on_overflow: true`.
     - Explicit unit tests for `block_on_overflow: true` (including `maximum_trace_size_bytes` under `NopCache`!), `DecisionWaitAfterRootReceived > 0`, `maximum_trace_size_bytes` (cached & `NopCache`, proving zero late-span rebirth AND zero deadlock!), empty `ScopeSpans`, `withPolicies(...)` fallback, and `tailsampling.policy` stamped on `NopCache` post-latch `Sampled` spans.

### Milestone 2: Decoupled Zero-Pointer Compact Payload Arenas (`wire_codec.go` + `lz4_codec.go` + Arena Backends)
- **Deliverable**:
  - `wire_codec.go`: direct zero-alloc stdlib `pdata -> OTLP proto3` wire writer (`appendResourceHeader`, `appendScopeHeader`, `appendSpan` into reusable scratch buffer) + wire-level raw frame splitter/reassembler (wire types `0–5` with bounded depth `64`, reverse field order, `schema_url` + unknown fields, proto3 omitted zero `TraceID`), grouped per **`(traceID, incoming ResourceSpans)`**.
  - `lz4_codec.go`: zero-dep pure-Go LZ4 block encoder/decoder for chunks `>= 128B` when savings `>= 10%`.
  - `arena_storage.go`, `arena_mmap_unix.go` (`//go:build linux || darwin`, `syscall.MAP_ANON`), `arena_madvise_linux.go` (`//go:build linux`), `arena_madvise_other.go` (`//go:build !linux`), `arena_mmap_fallback.go` (`//go:build !linux && !darwin`) implementing `rawProtoSlabArena`, `internedHeapArena`, `internedMmapArena`, `lz4HeapArena`, `lz4MmapArena` with utilization-aware `sealActive()`, generational `compactSlabID` `<25%` compaction on BOTH chunk and intern slabs, intrusive `noscan` `map[uint64]uint32` refcounted intern tables (separate resource & scope tables) + `peakEntries` reset, lazy `mmap` allocation, and `defer closeTailStorage()`.
  - `arena_storage_test.go` with canonical local `ptrace.Traces` deep-comparison helper (zero `go.mod` change!).
- **Verification Commands**:
  - `go test -race -count=1 ./...` (including `goleak` package tests)
  - `GOOS=darwin GOARCH=arm64 go build ./...` (cross-compile check verifying Darwin `syscall` compatibility!)
  - `go test -v -run 'TestWireCodec|TestLZ4|TestArena' .`
- **Falsifiable Exit Gates & Baseline-vs-Change Proof**:
  1. **100% lossless round-trip**:
     - `pdata` round-trip via local canonical comparison helper across rich traces (all `pcommon.Value` types: str/int/double/bool/bytes/slice/map/empty, events, links, tracestate, flags, `schema_url`, dropped counts, multi-scope `ResourceSpans` preserving exact `ResourceSpans` count & `TracesSize`, omitted zero `TraceID`).
     - Wire-level round-trip (`rawBytes -> extract/intern -> reassemble -> rawBytes`) verifying synthetic unknown protobuf fields across wire types `0–5` and reverse field order.
  2. **Zero leaks & deterministic cleanup**: After 50,000 traces across tick expiry, early eject, early keep, `NumTraces` overflow, high-cardinality ephemeral `k8s.pod.uid` burst (`10,000` unique resources), `Start()` error path, `DropPendingTracesOnShutdown: true`, and `ctx.Done()`: `liveSlabs == 0`, `liveChunks == 0`, `liveInternedEntries == 0`, `mmapActiveBytes == 0`, intern maps compacted (`peakEntries == 0`), and `goleak` reports 0 leaked goroutines.
  3. **Steady-state pinning & compaction proof**:
     - Chunk pinning: pinning 1% of traces across ticks triggers `<25%` compaction and keeps `allocatedSlabBytes / livePayloadBytes <= 2.0` at steady state.
     - **Intern table pinning**: keeping `1` pinned long-lived `Resource` + shared `Scope` headers (`net/http`, `database/sql`) while churning `10,000` ephemeral unique `Resource`s across ticks proves intern slabs compact cleanly (`internAllocatedBytes` stays bounded, no monotonic RSS leak!).
  4. **Non-tautological memory proof on W3 (worst case, `0%` short-circuit)**:
     - On **`mixed`**: `internedHeapArena` achieves **`>= 50%` `HeapAlloc` reduction** (and `>= 85%` `HeapObjects` reduction) vs `inMemoryTailStorage`.
     - Across **ALL 4 shapes (`mixed`, `resource-per-trace`, `few-traces-per-resource`, `sdk`)**: `lz4HeapArena` achieves **`>= 50%` `HeapAlloc` reduction** and `internedMmapArena`/`lz4MmapArena` achieve **`>= 90%` `HeapAlloc` reduction** vs `inMemoryTailStorage` even under ephemeral unique resources!
  5. **Zero-unmarshal proof**: On W3 (`0%` sampled), `unmarshalCount == 0` across 100% of expired traces.

### Milestone 3: High-Volume Sustained Bakeoff (50k–200k spans/sec), GC Profiling, & Full-Package `benchstat`
- **Deliverable**:
  - Isolated subprocess bakeoff harness (`ivm_bench_test.go`) running Baseline A, Baseline B, Candidates 1–4 (+ `splitResourceSpansByTrace` bypass ablation) across W1–W4 (`num_shards=1` and `num_shards=4`), recording JSON/CSV metrics into `milestone_3_evidence.md`.
  - Full-package `benchstat` comparing compiled test binaries (`upstream/main` vs `HEAD`):
    - CPU/throughput benchmarks (`BenchmarkSampling`, `BenchmarkProcessorThroughput`, `BenchmarkSplitResourceSpansByTrace`): `-count=6 -benchtime=500ms` (strictly serial).
    - `BenchmarkProcessorMemory`: `-count=6 -benchtime=1x` gated strictly on **`retained-B/span` and `retained-B/trace`** (excluding noisy `sec/op` which includes `2 × runtime.GC()`).
- **Falsifiable Exit Gates**:
  1. **Memory Gate (#1)**: On sustained W4 (`1M–2M` live buffered spans, `GOGC=100`, isolated subprocesses), best candidate achieves **`>= 50%` reduction in peak `HeapAlloc`** AND **`>= 40%` reduction in steady-state OS `VmRSS`** vs Baseline A (`trace-complete`) on BOTH **W4-W1** (`1%` & `5%` error) and **W4-W2** (`35%` drop + `2%` error), AND **`>= 30%` reduction in steady-state OS `VmRSS`** on **W4-W3** (`0%` short-circuit, `1M` live spans across `mixed`, `sdk`, `resource-per-trace`, and ephemeral unique resources)!
  2. **GC Gate (#2)**: On sustained W4 (`1M–2M` live spans), achieves **`>= 50%` reduction** in `/gc/scan/heap:bytes` AND `/cpu/classes/gc/total:cpu-seconds` vs Baseline A, with Go GC STW pause p99 (`/gc/pauses:seconds`) **`<= Baseline A`** (no pause regressions).
  3. **Correctness Gate (#3)**: 100% sampled-TraceID set parity against Baseline A verified directly on the W1 and W2 benchmark streams under both in-order and out-of-order arrivals (`0` false positives, `0` false negatives).
  4. **Full-Package `benchstat` Gate**: No statistically significant regression (`> 5%`, `p < 0.05`) on existing `trace-complete` benchmarks (`BenchmarkSampling`, `BenchmarkProcessorThroughput`, `BenchmarkSplitResourceSpansByTrace`, and `retained-B/*` of `BenchmarkProcessorMemory`).

---

## 6. Key Technical Risks & Upstream Parity Quirks Audited Upfront

1. **Intrusive `noscan` refcounted intern tables**: Separate `resourceIntern` and `scopeIntern` tables; `map[uint64]uint32` collision-chained via `nextInBucket uint32` + exact `bytes.Equal` + explicit `refCount uint64` + map compaction with `peakEntries = liveEntries` reset. Intern bytes live in tick-sealed, `<25%`-compacted slabs (`mmap` slabs for `mmap` arenas).
2. **Thread safety / data races (`num_shards > 1`)**: Each shard owns its own compiled IVM circuit, regex LRU caches, arena, and intern tables. When `num_shards == 1`, `incremental` hands `ptrace.Traces` directly to shard 0 without cloning `Resource`; when `num_shards > 1`, `ConsumeTraces` partitions spans into at most `num_shards` buckets so zero mutable objects are shared across goroutines (verified under `go test -race` with parallel producers).
3. **Stateful, `BatchEvaluator`, & Winner/Threshold guards**: Early `Sampled` and `Dropped` short-circuits recursively inspect `and`/`not`/`drop`/`composite` for stateful side-effecting subpolicies (`rate_limiting`, `bytes_limiting`, `composite`, custom `Extension`). `PolicyName` and `FinalThreshold` checks honor `processor.go:935-941` unconditionally (even when `useTracestate == false`).
4. **Utilization-aware slab rotation & cross-platform `mmap` cleanup**: `sealActive()` runs on every tick for both chunk and intern slabs, releasing empty slabs immediately and sealing active slabs when `>= 75%` full or fragmented (`< 50%` live after aging) so low-volume ticks never waste tail slack; slabs pinned past `decision_wait` with `< 25%` live utilization compact into a dedicated generational `compactSlabID`; `syscall.MAP_ANON` works on both Linux and Darwin; `syscall.Madvise` is isolated under `//go:build linux`; `mmap` slabs allocate lazily and unmap via `defer closeTailStorage()` on `loop()` exit (`Shutdown`, `DropPendingTracesOnShutdown: true`, or `ctx.Done()`).
5. **All 8 Subtle Upstream Policy Parity Quirks** (all explicitly tested in `TestIVMParity`):
   - `string_attribute` (`string_tag_filter.go:81, 109-125, 133-148`): exact mode drops `""` from `valuesMap`; regex mode evaluates `v.Str() == ""` on non-string resource attributes while skipping `v.Str() == ""` on span attributes.
   - `boolean_attribute` (`boolean_tag_filter.go:46, 64`): non-bool attributes return `v.Bool() == false`, matching `value: false`.
   - `numeric_attribute` (`processor.go:390-395`, `numeric_tag_filter.go:32-35, 64, 73`): `MinValue == 0` / `MaxValue == 0` map to `nil` (unbounded); when BOTH are `0`, `NewNumericAttributeFilter` returns `nil` (guard cleanly); `v.Int()` returns `0` for non-int values when key exists.
   - `and` & `drop` (`and.go:60-76`, `drop.go:32-47`): empty subpolicy slice vacuously returns `Sampled` / `Dropped`.
   - `span_count` (`span_count_sampler.go:34-46`): `max_spans > 0 && max_spans < min_spans` or `max_spans < 0` is always `DefFalse`; `min_spans == 0 && max_spans == 0` is `DefTrue` on span 1.
   - `trace_state` (`trace_state_filter.go:33`): ignores `value == ""` and entries with `len(key)+len(value) >= 256`.
   - `probabilistic` under `usetracestate` (`probabilistic.go:66, 91-110, 158-187`): reads global feature gate directly; first explicit `rv` wins; if none arrives, `hasSamplingInfo` switches from FNV to `TraceIDToRandomness`.
   - `ottl_condition` (`ottl.go:88-115`): returns `(Error, err)` on first erroring span/event under `error_mode: propagate`.
