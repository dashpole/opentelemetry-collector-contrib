# Prototype Learnings: Retraction-Safe Reset & Out-of-Order Handling in `cumulative_to_delta`

**Target Component**: `open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor`
**Worktree / Branch**: `cumulativetodelta-retraction-prototype` (based on `upstream/main`)

---

## 1. Executive Summary & Core Takeaway

Adding a bounded, timestamp-ordered history window (`16 B` pointer-free `sumObs` entries, `48 B` `retractionState` in a separate `retractionStates sync.Map`) eliminates **100%** of error drops in `zset` and `zset_compact` (and on distinct-timestamp traces in `non_negative`; in Scenario C, `non_negative` intentionally rejects `1.96%` net-negative same-timestamp revisions without mutating history), plus **100%** of false-reset tail-rewind spikes, prefix-oracle spikes, and inverted intervals across all 5%-15% out-of-order traces (`<= 10s` jitter inside a `30s` `reorder_window`). It adds **zero emission delay** on the happy path and **`+127.0` to `+127.9 B/series` (`+6.05` to `+6.09 MiB` for 50,000 series)** at `5s` scrape (`W=8`).

Two non-obvious architectural findings emerged that directly affect OpenTelemetry spec and Collector design:

1. **You do NOT need negative deltas to achieve exact global cumulative conservation under out-of-order arrivals and restarts.**
   We proved and fuzz-verified (3,000 `int64` `math/big.Int` trials + 3,000 `float64` trials) that for non-negative cumulative counters (`v >= 0`) arriving at distinct timestamps, the oracle total `C(H)` is **monotonically non-decreasing under set insertion** (the single exception being an `InitialValueAuto` head-prepend that flips `initEmitted` from `true` to `false` when `t_x <= StartTimestamp`). Thus `non_negative` mode recovers **100%** of late pre-reset and retroactively discovered hidden-reset counts with **zero negative deltas** while keeping **`is_monotonic = true`** (note: late pre-reset, hidden-reset, and upward-revision recoveries still emit overlapping `(start, end]` intervals, so it satisfies the non-negativity rule in `specification/metrics/data-model.md` rather than strict non-overlap). Across Scenarios A-E, average emitted/input ratio stays `0.79x-0.95x` (isolated hidden-reset inputs still emit 2 points per 1 input). The *only* capability requiring signed negative retractions (`zset` / `zset_compact`) is **redistributing coarse-window totals back to their exact sub-window timestamps** (`int64` `BucketMAE = 0` vs `3.76-12.21` in `non_negative`; `1.163e+06` on `1e8`-scale `float64` Scenario D) or handling **same-timestamp negative-net revisions**.
2. **Legacy `deltatocumulativeprocessor` drops late retractions on timestamps, NOT negative signs, yet still benefits massively (40.38x error reduction).**
   Because `deltatocumulativeprocessor/internal/delta/delta.go:46-58` runs `ErrOlderStart` (`start < state.Start`) and `ErrOutOfOrder` (`ts <= state.Timestamp`) before signed `Adder` (`state + dp`), it rejects all mid-window retractions (`394` dropped in `zset`, `231` in `zset_compact`, `3` in `non_negative`). Even so, feeding our output into `deltatocumulativeprocessor` cut cumulative error by **40.38x** (`+7,794` / `+17.52%` in legacy `off` down to `-193` / `-0.43%` across all 3 retraction modes), because preventing `prevPoint` rewind eliminates baseline's spurious double-counting. Under `InitialValueDrop`, head-prepends also trigger `ErrOlderStart` (`1` drop verified across all 3 modes in `InitialValueDrop_HeadPrependTriggersErrOlderStart`).

---

## 2. Mode Trade-Off Matrix (Measured Evidence)

Evidence from `milestone_2_evidence.md` (`ScenarioA_SharedID_5s_15pctOoO`, 3,600 inputs, `W=8`) and `milestone_3_evidence.md` (50,000 active series, binary `MiB`):

| Dimension | `off` (Legacy Baseline) | `zset` (Full DBSP) | `zset_compact` | `non_negative` `(Recommended for OTel/TSDB)` |
|---|---|---|---|---|
| Error Drops (`5s`, 15% OoO) | `15.25%` | **`0.00%`** | **`0.00%`** | **`0.00%`** (`1.96%` on Scenario C rev) |
| Tail-Rewind Spikes (`SpkT`) | `439` | **`0`** | **`0`** | **`0`** |
| Prefix-Oracle Excess (`SpkP`) | `18` | **`0`** | **`0`** | **`0`** |
| Inverted Intervals (`start > end`) | `78` | **`0`** | **`0`** | **`0`** |
| Global Conservation Error | `36,712` | **`0` (exact)** | **`0` (exact)** | **`0` (exact on `H_accepted`)** |
| Per-Bucket MAE (`5s`, 15% OoO) | `23.32` | **`0`** | **`0`** | `10.95` (coarse in-gen windows) |
| DBSP `(start, end]` Z-Set Exact | No (`2,164` MM) | **Yes (`0` MM)** | No (`858` MM) | No (`1,323` MM) |
| Output `IsMonotonic` | `true` | `false` (opt-in `true`) | `false` (opt-in `true`) | **`true` (always)** |
| Negative Deltas Emitted | `0` | `482` | `412` | **`0`** |
| Wire Amplification (`emitted/in`) | `0.85x` | `1.27x` | `1.10x` | **`0.87x`** |
| Legacy `deltatocumulative` Error | `+17.52%` (`+7,794`) | `-0.43%` (`-193`, `394` drop) | `-0.43%` (`-193`, `231` drop) | **`-0.43%` (`-193`, `3` drop)** |
| 50k Heap Delta (`W=8`, `5s`) | `+0.0 B` (`11.06 MiB`) | **`+127.0 B` (`17.11 MiB`)** | **`+127.5 B` (`17.13 MiB`)** | **`+127.9 B` (`17.15 MiB`)** |
| 50k Heap Delta (`W=17`, `2s`) | `+0.0 B` (`11.03 MiB`) | **`+383.9 B` (`29.34 MiB`)** | **`+384.6 B` (`29.37 MiB`)** | **`+383.7 B` (`29.33 MiB`)** |
| 50k Processor Latency (`15% OoO`)* | `3.569 us` (`224 B, 9a`) | `3.420 us` (`~`, `228 B, 8a`) | `3.543 us` (`~`, `205 B, 7a`) | `3.302 us` (`~`, `192 B, 7a`) |
| 50k Tracker Latency (`15% OoO`)* | `1.420 us` (`112 B, 3a`) | `1.375 us` (`~`, `61 B, 1a`) | `1.368 us` (`~`, `61 B, 1a`) | `1.432 us` (`~`, `61 B, 1a`) |

*\*Benchmark Fairness & Harness Footnote*:
- **`Load`-first asymmetry**: All 3 new modes use a `Load`-first cache hit path in `ConvertSum` (`-64 B/op, -2 allocs/op` vs unmodified `upstream/main` `off`, which calls `LoadOrStore(hashableID, &state{...})` on every hit). `off` was intentionally left untouched so the regression benchmark against `upstream/main` stays unconfounded.
- **Amortized slice growth & multi-point `pdata` expansion (`+13 B/op` tracker, `+32 B/op` processor)**: Verified via `pprof -alloc_space`, pre-warmed 50k series start at `cap = 8` (`128 B`); `15%` dense mid-window insertions push `len` past `8`, triggering one-time `cap 8 -> 16` (`256 B`) growth amortized over the `1s` run (`48 -> 61 B/op` in tracker, falling to `54 B/op` at `2M` iters). In the processor benchmark, `non_negative` (`160 -> 192 B/op`) includes both amortized `cap 8 -> 16` growth (`~21 B/op`) and `dps.AppendEmpty()` + `Attributes().CopyTo()` (`~11 B/op`) on occasional 2-point hidden-reset emissions; `zset_compact` (`205 B/op`) and `zset` (`228 B/op, 8 allocs/op`) reflect additional 2- and 3-point `pdata` expansions on reorders.
- **`b.StopTimer()` / `b.StartTimer()` in processor `OutOfOrder15Pct`**: Input fixture rebuilds when `DataPoints().Len() != 1` are excluded from `B/op` and `allocs/op` via `b.StopTimer()` / `b.StartTimer()`, keeping `off` at exact `224 B/op, 9 allocs/op` while adding `~1 us/op` timer pause/resume overhead symmetrically across all 4 modes. See the pure tracker row (`1.37-1.43 us`, zero `StopTimer` calls) for unbuffered conversion latency.

**Recommendation**:
- For **standard OpenTelemetry / Cloud Monitoring / Prometheus / Datadog pipelines**: **`non_negative`** `(Recommended)` gives exact global conservation, `0` negative deltas, `is_monotonic = true`, low wire volume (`0.87x` avg on A3, `0.79x-0.95x` across A-E), and only `3` downstream drops in legacy `deltatocumulativeprocessor`.
- For **DBSP / Feldera / OTel-Arrow / ClickHouse Z-set pipelines** requiring exact sub-window rate accuracy and bidirectional revision support: **`zset`** (`0` interval-key mismatches, `0` bucket MAE, `1.27x` wire volume at 15% OoO), or **`zset_compact`** if the backend aggregates strictly by end `Timestamp` (`1.10x` wire volume, `13.52%` fewer total points / `63.65%` fewer extra points on A3).

---

## 3. Surprises & Design Discrepancies Discovered

1. **OTLP `Histogram` & `ExponentialHistogram` cannot encode negative Z-set retractions on the wire**:
   Both `HistogramDataPoint.count` / `bucket_counts` and `ExponentialHistogramDataPoint.count` / `zero_count` / `bucket_counts` are unsigned **`uint64`** in `opentelemetry/proto/metrics/v1/metrics.proto`. Representing signed Z-set retractions for histograms requires either a protocol-level signed weight column (e.g., an explicit `weight: int64` column in OTAP / Arrow Dataflow) or sticking to `non_negative` reset recovery for histograms.
2. **Naive floating-point `net = (d_1 + d_2) - d_old > 0` fails on 1-ULP rounding errors**:
   Gate 1 review caught that 1-ULP IEEE-754 rounding in `(d_1 + d_2) - d_old` misclassifies normal monotonic triples like `(0.1, 0.2, 2.0)` and `(5000.0, 1057.562965768663, 3672.887320536073)` as resets, causing `non_negative` to emit the entire `d_1` term (`+28.8%` on the single post-reset interval `d_old = 3672.89`, or `+12.2%` of the two-generation total `8672.89`, approaching `+100%` of an interval when `x` is near `b`). Replacing numerical subtraction with our exact structural 5-case partition plus Sterbenz's Lemma (`cmpDiffFloat` using `big*0.5 > base` to avoid `2*base` overflow near `MaxFloat64`) eliminated 100% of sign errors and `Inf`/`NaN` overflows across `int64` and `float64` (verified against `104,976` Python `fractions.Fraction` tuples during review).
3. **Upward same-timestamp revisions can have NEGATIVE net impact (`net < 0`), and `InitialValueAuto` head-prepend flips can drop `C(H)`**:
   Counterintuitively, increasing a value at an existing timestamp can *remove* a previously assumed counter reset and shrink the total cumulative sum (e.g., `[(10,100), (20,50)] -> (20,120)` turns delta `+50` into `+20`, `net = -30`). Similarly, under `InitialValueAuto`, prepending a point at `t_x <= StartTimestamp` flips `initEmitted` from `true` to `false` (subtracting `v_x`). Both require exact structural classification and rejecting without mutating `H` in `non_negative` mode.
4. **Pre-existing upstream issues discovered in `cumulativetodeltaprocessor`**:
   - `internal/tracking/tracker.go:153-157` (`upstream/main`; `tracker.go:165-169` on this branch): `t.states.LoadOrStore(hashableID, &state{...})` allocates `64 B` (`2` extra allocs: `&state{}` + interface boxing) on **every steady-state cache hit** (`112 B/op, 3 allocs/op` vs `48 B/op, 1 alloc/op` with `Load`-first). This is an independent `-64 B/op` speedup that should be split into its own PR if upstreamed.
   - `internal/tracking/tracker.go:153-157` + `356-357` (`upstream/main`; `tracker.go:165-169` + `368-369` on this branch): `t.streams.Add(1)` runs *after* `LoadOrStore` without locking the new entry, so a concurrent `removeStale` sweep can delete a newly stored entry and decrement `t.streams` first, transiently exposing `Streams() == -1`. Our `retractionStates` path prevents this by locking the unshared `newS.mu` across `LoadOrStore` and calling `CompareAndDelete` under `s.mu`.
   - `internal/tracking/tracker.go:172` (`upstream/main`; `tracker.go:184` on this branch): `InitialValueAuto` checks `ObservedTimestamp == metricID.StartTimestamp` instead of `<=`, allowing clock-skewed points with `ts < StartTimestamp` to emit inverted intervals `(StartTimestamp, ts]`.
   - `processor.go:232` (`upstream/main`; fixed at `processor.go:280` on this branch): `droppedReasonAttrs[id.MetricType][reason]` performs an unchecked map lookup; any unmapped drop reason passes a `nil` `metric.MeasurementOption` to `Int64Counter.Add`, which panics under a real `sdk/metric` `MeterProvider` (`NewNopSettings` masks it).
5. **Memory compaction details matter at scale (all units binary `MiB`)**:
   - Dropping the originally planned `inline [4]sumObs` saved `+64 B/series` (`3.05 MiB` at 50k series) because `5s` and `2s` scrape windows hold `8` and `17` points (`> 4`), spilling 100% of the time.
   - Computing eviction over the logical `(n+1)`-element view and shifting in-place before growing pinned `cap == 8` at `W=8` (`+127.0` to `+127.9 B/series`, `6.05-6.09 MiB` total delta, matching `8 * 16 B = 128 B`).
   - Switching slice growth past `16` from Go's `2x` doubling (`16 -> 32`, analytical `+512 B/series`, `35.44 MiB` total) to `+8` (`16 -> 24`, measured `+383.7` to `+384.6 B/series`, `29.33-29.37 MiB` total) saves **`128 B/series` (`6.10 MiB` analytical savings at 50k series, `-25.0%` incremental delta / `-17.2%` total heap)**.
6. **Rotating `StartTimestamp` multiplies state entries identically across all modes**:
   Because `MetricIdentity.Write()` includes `StartTimestamp`, `3` generations (`2` restarts, `R=2`, `R+1=3`) per `max_staleness` window create `150,000` entries for `50,000` series (`34.13 MiB` in `off`, `52.41 MiB` in `zset`). Both `off` and retraction modes pay `R+1` linear scaling until `max_staleness` expires.

---

## 4. Productionization Gap Ledger

Before shipping beyond prototype or proposing upstream in `open-telemetry/opentelemetry-collector-contrib`:

1. **`mdatagen` & Schema Sync**: Add new `reason` enum values (`window_exceeded`, `duplicate`, `invalid_value`, `rejected_revision`) to `metadata.yaml` and regenerate `internal/metadata`, `documentation.md`, and `config.schema.yaml` via `make generate`.
2. **Standalone `Load`-first & `Streams()` Race PR**: Extract the independent `Load`-first optimization (`-2 allocs/op, -64 B/op`) and `Streams() >= 0` `CompareAndDelete` race fix in `tracker.go` into a separate upstream PR (do not bundle into a feature PR).
3. **Histogram / ExponentialHistogram Strategy**: Decide whether to extend `non_negative` ordered-history protection to histograms (preventing false-reset spikes without emitting negative `uint64` counts) or target OTAP / Arrow Dataflow with an explicit signed `weight: int64` column for full DBSP Z-set histograms.
4. **OTel Specification Alignment**: Decide upstream (OTEP / Metrics SIG) whether `zset` retraction streams should: (a) unconditionally set `IsMonotonic = false` (current default, strict spec compliance but may downgrade counters to gauges in Prometheus exporters), or (b) define an explicit OTLP/OTAP retraction signal / `DataPointFlags` bit so backends retain counter semantics alongside signed weights.
5. **Test Runtime Under `-race`**: `TestMemoryFootprint50kSeries` runs in `13.64s` normally and `210.9s` under `-race` (`Makefile.Common:21` sets `GOTEST_TIMEOUT?= 900s` (15m) in contrib CI, and `-short` skips it). Consider adding a `//go:build !race` scale factor (e.g., `5_000` under `-race`, `50_000` without `-race`) before merging into upstream CI so PR checks stay fast.
6. **Stale-Series Sweeping Under High Rotating-`StartTimestamp` Churn**: Consider a shorter per-generation eviction heuristic or retiring old `StartTimestamp` entries once a newer `StartTimestamp` arrives for the same base series key after `reorder_window` elapses.
7. **Symmetric Upstream PR Benchmark**: Because these prototype benchmarks compare unmodified `upstream/main` `off` (`LoadOrStore` hit allocs) against `ConvertSum` (`Load`-first hit path), any upstream feature PR should benchmark against a `Load`-first baseline to isolate pure history-maintenance overhead.
