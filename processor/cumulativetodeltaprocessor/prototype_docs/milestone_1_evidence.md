# Milestone 1 Evidence: Core Engine, Processor Integration & Correctness Suite

**Date**: 2026-11-18
**Files Added/Modified**:
- `config.go` (`RetractionMode`, `ReorderWindow`, `MaxHistoryPoints`, `PreserveMonotonic` with Go zero defaults + `Validate()`)
- `processor.go` (retraction config validation inside `newCumulativeToDeltaProcessor`, `convertNumberDataPointsRetraction`, causal multi-point batch ordering, exemplar clearing on synthetic points, nil-safe `recordDropped` with precomputed entries + lowercase snake_case fallback at `processor.go:280`, unconditional `ms.SetIsMonotonic(false)` after `baseIdentity` capture when `!preserve_monotonic`)
- `internal/tracking/tracker.go` (separate `retractionStates sync.Map`, `NewMetricTrackerWithRetraction`, `removeStale` dispatch)
- `internal/tracking/retraction.go` (16 B `sumObs` `noscan`, 48 B `retractionState`, `Load`-first fast path, `CompareAndDelete` under `s.mu`, `ConvertSum` entry guard rejecting invalid/`off` modes before mutating `retractionStates`, `cmpDiffInt` / Sterbenz-exact `cmpDiffFloat` with `big*0.5 > base`, logical `(n+1)` in-place insert-and-compact with `+8` step growth past 16, all 3 modes `zset` / `zset_compact` / `non_negative`)
- `internal/tracking/retraction_test.go` (struct sizes, all 6 value permutations, IEEE-754 rounding triples, Sterbenz cross-regime & `1.5e308` / `1e16` / subnormals, `auto` head-prepend flips & clock skew, zero-delta `OutcomeAbsorbed`, invalid/`off` entry guard test, 20,000 eviction trials, steady-state `cap=8` pin, 3,000 `int64` `math/big.Int` randomized trials, 3,000 `float64` randomized trials, concurrent `-race` stress tests synced with `workerWG.Wait()`, pure tracker benchmarks `BenchmarkTracker50kInOrder` and `BenchmarkTracker50kOutOfOrder15Pct`)
- `retraction_processor_test.go` (baseline-vs-change proof, config validation + `CreateMetrics` rejection + `confmap.NewFromStringMap` YAML unmarshal, exemplar preservation/clearing on both `n==1` and `n>1` batches, `NaN` passthrough, edge cases including `n==1` 4-delta interior rev / single `NaN` / `±Inf` / `NaN` at `i==0` before rebuild / all-dropped `n>1` batch, real `componenttest.NewTelemetry()` SDK meter test proving zero nil-option panic across all 5 drop reasons)

## Baseline-vs-Change Proof (`TestBaselineVsRetractionProof`)

Input sequence (Gen 1: `t=10: 100`, `t=20: 200`, `t=24: 240`; pod restart at `t=25`; Gen 2: `t=30: 50`, `t=40: 120`; true total across both generations = **360**):
Arrival order with jitter: `(10, 100)`, `(30, 50)`, `(24, 240)`, `(20, 200)`, `(40, 120)`.

| Mode | Output `IsMonotonic` | Sum of Emitted Deltas | Conservation Error vs Oracle (360) | Inverted Intervals (`start > end`) | Negative Deltas |
|---|---|---|---|---|---|
| `off` (legacy baseline) | `true` | `290` (`100 + 190`) | **`-70` (FAIL)** | **`1`** (`(30, 24]: +190`) | `0` |
| `zset` (`preserve_monotonic: false`) | `false` | **`360`** | **`0` (exact)** | **`0`** | `> 0` (retractions) |
| `zset` (`preserve_monotonic: true`) | `true` | **`360`** | **`0` (exact)** | **`0`** | `> 0` (retractions) |
| `zset_compact` | `false` | **`360`** | **`0` (exact)** | **`0`** | fewer pts than `zset` |
| `non_negative` | `true` | **`360`** | **`0` (exact)** | **`0`** | **`0`** |

## Key Invariants Verified

- `go vet ./...` and `gofmt -l .`: clean (zero issues).
- `go test -race -short -count=1 ./...`: PASS (`cumulativetodeltaprocessor` `27.798s`, `internal/tracking` `21.548s`, `internal/metadata` `1.077s`, `internal/metadatatest` `1.110s`).
- `unsafe.Sizeof(sumObs{}) == 16`, `unsafe.Sizeof(retractionState{}) == 48` (64-bit amd64; `points` at field 0 -> `ptrdata = 8 B`).
- `TestInsertAndCompactMatchesReference20k`: 20,000 / 20,000 random sequences match reference naive insert-then-compact (`cap(s.points) <= maxPts` strictly).
- `TestSteadyState5sScrapeNoSliceDoubling`: after 200 scrapes at 5s interval / 30s window, `len == 8`, `cap == 8` (never doubles to 16), `hasEvicted == true`, late `< anchor` and revised `== anchor` drop with `window_exceeded`, exact anchor duplicate drops with `duplicate`.
- `TestRandomizedPropertyInt64`: 1,000 trials x 3 modes = 3,000 trials (`math/big.Int` exact `0` conservation error, `0` inverted intervals, `0` Z-set mismatches for `zset`, `0` bucket mismatches for `zset` and `zset_compact`, `0` negative deltas for `non_negative`).
- `TestRandomizedPropertyFloat64`: 1,000 trials x 3 modes = 3,000 trials (relative tolerance `<= 1e-9 * max(1, maxVal)`, zero `NaN`/`Inf`/`-0.0` leakage).
- `TestRetractionTelemetryRealSDKNoPanic`: real `sdk/metric` `MeterProvider` records `initial`, `duplicate`, `invalid_value`, `rejected_revision`, and `window_exceeded` without panicking; `OutcomeAbsorbed` does not increment `datapoints_dropped`.
- `TestConfigUnmarshalRetractionConfmap` & `TestConfigValidateRetraction`: `confmap.NewFromStringMap` YAML unmarshaling verified for all 4 new config fields, invalid `retraction_mode` strings rejected, and `CreateMetrics` rejects unvalidated bad retraction configs while leaving `factory_test.go:46` (`missing_name`) intact.
- Existing test suite: all 22 `config_test.go` cases (including 12 literal `&Config{...}` assertions across `TestLoadConfig` and `TestLoadDeprecatedConfig`), `generated_component_test.go`, `processor_test.go`, `processor_telemetry_test.go`, `tracker_test.go`, and `goleak` pass unmodified.

## Pre-Existing Upstream Race Discovered During Testing

Running `TestMixedSumAndHistogramRemoveStaleConcurrent` caught a **pre-existing** upstream race in `internal/tracking/tracker.go` (`tracker.go:153-157` on `upstream/main`; `tracker.go:165-169` on this branch): the legacy `t.states` path (`Histogram` / `ExponentialHistogram` / `off` mode) calls `t.streams.Add(1)` *after* `t.states.LoadOrStore(...)` without locking the entry, so a concurrent `removeStale` sweep (`tracker.go:356-357` on `upstream/main`; `tracker.go:368-369` on this branch) can delete the brand-new entry and decrement `t.streams` first, transiently exposing `Streams() == -1`. Our new `retractionStates` path prevents this by locking the unshared `newS.mu` across `LoadOrStore` until `t.streams.Add(1)` runs and executing `CompareAndDelete` under `s.mu` (strictly verified in `TestRetractionRemoveStaleConcurrentStreamsNonNegative`).
