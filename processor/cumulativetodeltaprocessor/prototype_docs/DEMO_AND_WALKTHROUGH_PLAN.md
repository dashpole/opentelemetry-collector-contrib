# Demo & Walkthrough Plan: Retraction-Safe `cumulative_to_delta` Prototype

**Repository / Branch**: `open-telemetry/opentelemetry-collector-contrib` (`cumulativetodelta-retraction-prototype`, local branch ahead of `upstream/main`)

---

## Option A: Interactive Terminal Walkthrough (Reproducible Commands)

All commands run from the `processor/cumulativetodeltaprocessor` directory of the `cumulativetodelta-retraction-prototype` branch (zero external services or credentials required).

### Step 1: See the Baseline Failure vs. Retraction Fix (2 seconds)
Replays the exact 5-point cross-restart out-of-order scenario:
- Gen 1: `t=10: 100`, `t=20: 200`, `t=24: 240`; pod restart at `t=25`; Gen 2: `t=30: 50`, `t=40: 120` (true total = `360`).
- Arrival order: `(10, 100) -> (30, 50) -> (24, 240) -> (20, 200) -> (40, 120)`.
- Shows `off` outputting `290` (`-70` error, 1 inverted interval `(30, 24]: +190`), while `zset`, `zset_compact`, and `non_negative` all output exact `360` with `0` inverted intervals.

```bash
go test -v -count=1 -run 'TestBaselineVsRetractionProof|TestRetractionExemplarClearingAndCausalOrder|TestRetractionTelemetryRealSDKNoPanic' .
```

### Step 2: Run the 5%-15% Out-of-Order Synthetic Trace Evaluation & Downstream Probes (~6 seconds)
Prints side-by-side tables across all 4 modes for `5%`/`10%`/`15%` OoO (`<= 10s` jitter), `5s` & `2s` scrapes, shared & rotating `StartTimestamp`, duplicates & revisions, `1e8`-scale `float64`, `InitialValueDrop`, and the 3 downstream integrators (DBSP Z-Set, Additive TSDB, and legacy `deltatocumulativeprocessor`).

```bash
go test -v -count=1 -run 'TestEvaluationSyntheticTraces|TestDownstreamCompatibility' .
```

### Step 3: Verify Correctness & Concurrency (~5s normal, ~22s `-race`)
Runs 20,000 in-place eviction trials against a naive oracle, 3,000 `int64` `math/big.Int` randomized permutation trials, 3,000 `float64` trials, IEEE-754 Sterbenz/ULP adversarial counterexamples, and concurrent mixed Sum + Histogram `removeStale` stress tests.

```bash
go test -race -count=1 ./internal/tracking/...
```

### Step 4: Measure 50,000-Series Memory Footprint & `benchstat` Latency (~14s memory + serial benchmarks)

```bash
# Memory footprint at 50k active series (W=8 @ 5s scrape, W=17 @ 2s scrape, 150k rotating StartTS):
go test -v -count=1 -run '^TestMemoryFootprint50kSeries$' .

# Serial 50k-series processor and tracker latency benchmarks:
go test -run '^$' -bench '^Benchmark50k' -benchmem -count=6 -benchtime=1s .
go test -run '^$' -bench '^BenchmarkTracker50k' -benchmem -count=6 -benchtime=1s ./internal/tracking
```

---

## Option B: AI-Narrated Walkthrough Video Storyboard & Script (Optional Phase 4)

Total runtime: ~2 minutes 45 seconds (5 scenes, 1080p dark terminal + diagrams). Spoken narration omits raw code symbols, backticks, and URLs.

| Scene | Duration | Visual Summary |
|---|---:|---|
| 1. The Out-of-Order Bug | 30s | Delayed `200` double-count + cross-restart `(30, 50) -> (24, 240)` backwards interval |
| 2. DBSP Retractions & Modes | 35s | 8-point sorted window diagram; full, compact, and non-negative emissions |
| 3. The Non-Negative Surprise | 35s | 6 value-ordering proofs + floating-point rounding trap caught by Sterbenz math |
| 4. Trace Results & Downstream | 35s | Milestone 2 tables (`15%` OoO, `439` spikes -> `0`) + 40x error drop matrix |
| 5. Scale, Memory & Next Steps | 30s | 50k-series heap chart (`11.1` vs `17.1` MiB) + `benchstat` off-mode parity |

#### Scene 1: The Out-of-Order Bug (30s)
- **Visual**: Two stacked timelines: (a) monotonic counter `100 -> 200 -> 300 -> 400` where `200` arrives behind `300`, rewinding legacy baseline state and causing a double-count spike to `500` (all forward intervals); (b) pod restart sequence `(30, 50) -> (24, 240)` where the late pre-restart sample has a higher value, emitting the backwards interval `(30, 24]: +190`.
- **Narration**: Today, the OpenTelemetry cumulative to delta processor keeps only one previous point per stream and ignores timestamps. When network jitter delivers an older, smaller value late, it mistakes that drop for a counter reset, drops the point, and rewinds its state backwards so the next sample double counts the gap. And when a late pre-restart sample arrives with a higher value after a restart, it emits a backwards time interval.

#### Scene 2: DBSP Retractions & The Three Modes (35s)
- **Visual**: Side-by-side algebraic diagram showing a bounded sorted window (8 points, 16 bytes each) and the emissions of three modes: full retraction triples, compact end-timestamp adjustments, and strictly non-negative recovery.
- **Narration**: Drawing on differential stream processing, we replace the single previous point with a small timestamp-ordered window per stream. When a late point arrives between two observations, full retraction mode cancels the old interval with a negative weight and inserts the two exact sub-intervals. Compact mode merges the retraction and second edge at the same timestamp, cutting extra datapoints by over sixty percent, or about fourteen percent fewer total datapoints.

#### Scene 3: The Non-Negative Surprise (35s)
- **Visual**: Visual proof table of all six value orderings showing total cumulative work never shrinks when distinct-timestamp points arrive late, followed by the floating-point rounding catch fixed with structural branching.
- **Narration**: While prototyping, we discovered a surprising property: for non-negative counters arriving at distinct timestamps, total work across resets never shrinks when a late point fills a gap. That means our non-negative mode recovers one hundred percent of late pre-reset counts and retroactively discovered resets without ever emitting a negative number. We also caught a subtle floating-point rounding trap that overcounted a single interval by nearly twenty-nine percent in our test cases, and fixed it with exact value-ordering rules and Sterbenz arithmetic.

#### Scene 4: Real Trace Results & Downstream Compatibility (35s)
- **Visual**: Terminal capture of Milestone 2 tables: baseline drops `15%` with `439` spikes vs `0%` drops and `0` spikes; downstream compatibility matrix highlighting the `40x` error drop even inside legacy `delta_to_cumulative`.
- **Narration**: Across synthetic Kubernetes traces with fifteen percent out-of-order delivery and frequent pod restarts, the baseline loses over fifteen percent of datapoints, fires over four hundred tail-rewind spikes, and drifts by over thirty-six thousand seven hundred counts. All three new modes hit zero error drops, zero spikes, and zero global conservation error. Even when feeding into a legacy downstream converter that rejects late timestamps, error falls forty-fold because the processor stops rewinding its state.

#### Scene 5: Scale, Memory & Next Steps (30s)
- **Visual**: 50,000-series memory chart (`11.06 MiB` baseline vs `17.11 MiB` at 5-second scrapes, `+127` bytes per series; `29.34 MiB` at 2-second scrapes) and `benchstat` output showing zero overhead in default off mode.
- **Narration**: At fifty thousand active series with five-second scrapes, the ordered window adds just one hundred twenty-seven bytes per series, or six mebibytes total, with pointer-free history slices that the garbage collector does not need to scan and zero regression when disabled. Because standard histograms use unsigned counts on the wire, negative retractions for histograms require a signed weight field in future Arrow formats. For standard pipelines today, non-negative mode is ready to consider upstream, while full retraction mode unlocks exact sub-window accuracy for differential engines.
