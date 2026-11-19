# Prototype Plan: Retraction-Safe Reset and Out-of-Order Handling in `cumulative_to_delta` (Rev 3)

**Target Component**: `open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor`
**Branch**: `cumulativetodelta-retraction-prototype` (branched from `upstream/main`)

---

## 1. Core Question & Hypothesis

Today's `cumulativetodeltaprocessor` stores only a single `prevPoint` per stream and never checks timestamps when computing deltas (`internal/tracking/tracker.go`). Under out-of-order delivery (5%-15% late points with up to 10s jitter) or counter resets without changing `StartTimestamp` (for example `StartTimestamp == 0` from Prometheus scrapes), two severe bugs happen:

1. **Out-of-order false reset & overcounting spike**: When `t=20 (v=200)` arrives after `t=30 (v=300)`, baseline sees `200 < 300`, misclassifies it as a reset, drops `t=20`, and **rewinds `prevPoint` to `(t=20, 200)`**. When `t=40 (v=400)` arrives, baseline emits `400 - 200 = +200` on `(20, 40]` instead of `+100` on `(30, 40]`. While duration-normalized rate on `(20, 40]` is still `200 / 20s = 10/s`, this causes a **2x per-step delta spike** (a 2x rate spike in backends that ignore `StartTimestamp` and divide by the fixed 10s scrape interval) and a **+100 permanent overcount** (`200 + 200 = 400` vs true growth `300`). If `t=20` has the same value as `t=30`, baseline emits an inverted interval (`StartTimestamp=30 > Timestamp=20`).
2. **True reset data loss**: When a pod restarts with `StartTimestamp == 0` (or unchanged `StartTimestamp`) and sends `t=30 (v=50)` after `t=20 (v=200)`, baseline drops `50` with `reason="reset"`. Those 50 post-restart events are permanently lost. If a late pre-restart point `t=24 (v=240)` arrives afterward, baseline sees `240 >= 50` and emits a spurious `+190` on an inverted interval `(30, 24]`.

**Hypothesis**: Keeping a compact, bounded, timestamp-sorted observation history per Sum stream (`16 B/observation`, pointer-free `noscan` slice) and emitting algebraic Z-set differential updates lets `cumulativetodeltaprocessor`:
- Eliminate false resets, spurious spikes, and error-dropped points inside `reorder_window` (`0%` error drops for jitter within `reorder_window`), with **zero emission delay** on the happy path.
- Achieve exact cumulative conservation (`sum(emitted deltas) == C_oracle(H_accepted)`) across arbitrary arrival permutations and pod restart cycles (bit-exact `math/big.Int` match for `int64`; bounded IEEE-754 ULP error for `float64`).
- Keep steady-state memory bounded and modest at 50k active series (`O(W)` observations per series, capped by `max_history_points`).

**Key Spec & Downstream Compatibility Reality**:
- OTel Metrics Data Model (`specification/metrics/data-model.md`) states that readers of **monotonic delta sums** (`is_monotonic = true`) *"SHOULD expect non-negative values"* and *"expect to have no overlap in time windows."*
- `deltatocumulativeprocessor` (`internal/delta/delta.go:46-58`) accepts signed values and both `IsMonotonic=true`/`false` sums (`Adder` performs unchecked `state + dp`), **but rejects datapoints violating start-time or timestamp monotonicity**: `dp.StartTimestamp() < state.StartTimestamp()` fails with `ErrOlderStart` (checked first, rejecting head-prepends), and `dp.Timestamp() <= state.Timestamp()` fails with `ErrOutOfOrder` (rejecting mid-window retractions and splits).
- OTLP `Histogram` and `ExponentialHistogram` counts and bucket counts are unsigned `uint64`, so negative retraction counts cannot be encoded in standard OTLP histograms.

---

## 2. Scope, Configuration, & Intentional Simplifications

**In Scope**:
- Monotonic cumulative `Sum` metrics (`int64` and `float64`).
- Preserving `StartTimestamp` in `MetricIdentity` (`identity.go:70`). Streams with rotating `StartTimestamp` on restart naturally isolate into separate entries in the state map (preventing cross-generation interference under clock skew), while streams with `StartTimestamp == 0` or unchanged `StartTimestamp` share one state entry and rely on ordered delta calculus for reset detection. Note: rotating `StartTimestamp` entries remain in memory until `max_staleness` expires (`1h` default), multiplying entry count by `R+1` for `R` restarts per staleness window across **all** modes (measured separately in Milestone 3).
- Opt-in configuration fields on `Config` (**Go zero values in `createDefaultConfig()`** so all 12 existing literal `&Config{...}` assertions in `config_test.go` remain unchanged):
  - `retraction_mode` (`tracking.RetractionMode`, underlying type **`int`** mirroring `tracking.InitialValue` in `tracker.go:37`):
    - `RetractionModeOff` (`iota = 0`, Go zero value; `UnmarshalText` accepts both `""` and `"off"`).
    - `RetractionModeZSet` (`"zset"`).
    - `RetractionModeZSetCompact` (`"zset_compact"`).
    - `RetractionModeNonNegative` (`"non_negative"`).
  - `reorder_window` (`time.Duration`, default `0`): when `retraction_mode != off` and `reorder_window == 0`, effective runtime default is `30s`.
  - `max_history_points` (`int`, default `0`): when `retraction_mode != off` and `max_history_points == 0`, effective runtime default is `64` (accommodating Milestone 2's 17-point steady state at `2s` scrape / `30s` window without capacity truncation).
  - `preserve_monotonic` (`bool`, default `false`): only meaningful for `zset`/`zset_compact`. When `false`, output converted `Sum` metrics unconditionally set `ms.SetIsMonotonic(false)` **after** capturing `baseIdentity` (so downstream schema never flips per-batch between monotonic and non-monotonic). When `true`, output keeps `IsMonotonic = true` for DBSP/OTAP/ClickHouse consumers expecting counter metadata with signed weights.
- **`Config.Validate()` rules**:
  - `retraction_mode`: must be one of `RetractionModeOff`, `RetractionModeZSet`, `RetractionModeZSetCompact`, `RetractionModeNonNegative` (`0` = `RetractionModeOff` passes `Validate()` seamlessly for all 12 existing literal `&Config{...}` cases!). Invalid text errors in `UnmarshalText`.
  - `reorder_window < 0`: returns validation error.
  - `max_history_points < 0 || max_history_points == 1`: returns validation error (`>= 2` required so an anchor plus at least one active point can be retained; `0` means "use runtime default `64`").
  - `preserve_monotonic == true` when `retraction_mode == off` or `non_negative`: allowed as a no-op, since both already preserve `is_monotonic = true`.
- **Telemetry safety & `metadata.yaml` gap**:
  - `metadata.yaml` currently lists `reason` enum `[reset, initial, bucket_mismatch]`. We keep `metadata.yaml` unregenerated (`mdatagen` not run during prototype; noted as a productionization gap before any upstream PR).
  - Because `processor.go:232` (on `upstream/main`; fixed at `processor.go:280` on this branch) does an unchecked map lookup `droppedReasonAttrs[id.MetricType][reason]`, passing an unmapped reason returns `nil` `metric.MeasurementOption` and panics inside `go.opentelemetry.io/otel/metric.NewAddConfig` under a real `sdk/metric` `MeterProvider`. We fix `recordDropped` with precomputed entries for all drop reasons AND an `opt, ok` fallback guard using lowercase snake_case metric types (`"sum"`, `"histogram"`, `"exponential_histogram"`, never `id.MetricType.String()`) so `nil` can never reach `Add()`.
  - Defined drop reasons on `Sum` in retraction mode:
    - `initial`: first point dropped by `initial_value: drop` or `auto` (`StartTimestamp < startTime || ts <= StartTimestamp`).
    - `window_exceeded`: point strictly older than compacted anchor (`t < H[0].ts && hasEvicted`), or revision of compacted anchor (`t == H[0].ts && v != H[0].v && hasEvicted`).
    - `duplicate`: exact duplicate (`t == H[i].ts && v == H[i].v`).
    - `invalid_value`: `±Inf` float64 or negative cumulative value (`v < 0`) on a monotonic Sum.
    - `rejected_revision`: `non_negative` mode only: same-timestamp revision or `InitialValueAuto` head-prepend flip whose exact net change is `< 0` (dropped **without mutating `H`**).
  - `CumulativetodeltaDatapoints` counting semantics: counts **emitted delta datapoints** (`Add(ctx, int64(len(deltas)), attrs)` when `len(deltas) > 0`), matching `off` mode (`1` valid conversion -> `+1`).
  - **`OutcomeAbsorbed` (Generalized Across ALL 3 Modes)**: Any **accepted** transition that mutates `H` while emitting `0` delta datapoints returns `OutcomeAbsorbed` (`len(deltas) == 0, reason == ""`) across **all 3 retraction modes** (`zset`, `zset_compact`, `non_negative`):
    - e.g., `non_negative` mid-window `net == 0` reorder or head-prepend with `v_x <= v_0`;
    - `zset_compact` mid-window Case 1/2 with `d_1 == 0` or head-prepend with `(!newInitEmit || v_x == 0) && adj == 0`;
    - `zset` / `zset_compact` head rev with `!initEmitted && (len == 1 || oldR == newR)`, tail rev with `oldL == newL` (e.g., `prev=100, old=50 -> new=150`), or interior rev with `oldL == newL && oldR == newR`.
    - `convertNumberDataPoints` **never** calls `recordDropped` or indexes `deltas[0]` when `len(deltas) == 0` (it simply removes the incoming `dp`).

**Out of Scope / Intentional Simplifications**:
- Signed retractions for `Histogram` and `ExponentialHistogram` (OTLP `count`/`bucket_counts` are unsigned `uint64`; histograms remain on the existing `*state` path, documented in `PROTOTYPE_LEARNINGS.md`).
- Running `mdatagen` to regenerate `internal/metadata` / `config.schema.yaml` (deferred until upstream PR promotion).
- Cross-collector distributed sharding or durable disk state.

---

## 3. Candidate Approaches & Exact Transition Semantics

Let `H = [(t_0, v_0), ..., (t_{k-1}, v_{k-1})]` be the timestamp-sorted non-negative observations (`t_0 < ... < t_{k-1}`, `v_i >= 0`, finite `float64` or `int64`) stored for a stream.

Define the edge delta function for consecutive timestamps `prev.t < curr.t`:
```
intervalDelta(prev, curr) = curr.v - prev.v   if curr.v >= prev.v
                          = curr.v            if curr.v < prev.v   (true reset)
```
Because both `prev.v >= 0` and `curr.v >= 0`, `intervalDelta` is ALWAYS in `[0, MaxInt64]` for `int64` (cannot overflow!) and `>= 0` finite for `float64`. Negating `-d` for retraction is in `[-MaxInt64, 0]` (cannot overflow `int64`!). Normalize `-0.0` to `+0.0` (`if v == 0 { v = 0 }`) on all emitted `float64` values.

### Initial-Point Emission Rule (`shouldEmitInitial`)
Fixing `tracker.go:172` (`upstream/main`; `tracker.go:184` on this branch), which checks `ObservedTimestamp == metricID.StartTimestamp` instead of `<=` and allows `ts < StartTimestamp` clock skew to emit inverted intervals:
- `InitialValueKeep`: `emit = true`; `initStartTS = metricID.StartTimestamp` if `metricID.StartTimestamp <= obsTS`, else `0`.
- `InitialValueAuto`: `emit = (metricID.StartTimestamp >= trackerStartTime && obsTS > metricID.StartTimestamp)`; `initStartTS = metricID.StartTimestamp` if `emit`, else `0`.
- `InitialValueDrop`: `emit = false`; `initStartTS = 0`.

We store `initStartTS` and `initEmitted` on `retractionState` and update BOTH on **every accepted head-prepend across all 3 modes** (`zset`, `zset_compact`, `non_negative`), never on rejected head-prepends!

Define the exact oracle cumulative total over accepted history `H` (`math/big.Int` for `int64` to prevent cross-reset accumulator overflow):
`C(H) = (initEmitted ? v_0 : 0) + sum_{i=1..k-1} intervalDelta(h_{i-1}, h_i)`

### Core Exact Structural Formulas (Zero Float64 ULP Sign Error, Zero `Inf`/`NaN`, Zero `int64` Overflow)

1. **Helper: Overflow-Free / Sterbenz-Exact Cross-Regime Comparison `cmpDiff(big, base, other)` (`big >= base >= 0, other >= 0`)**:
   Determines exact sign & value of `(big - base) - other`:
   - For **`int64`**: `d := big - base` (in `[0, MaxInt64]`, zero overflow!).
     - `d > other` -> `(+1, d - other)`
     - `d == other` -> `(0, 0)`
     - `d < other` -> `(-1, other - d)`
   - For **`float64`**:
     - If `big*0.5 > base` (`big > 2*base` without overflowing `2*base` to `+Inf`!): then `big - base > base`; if we also know `other < base`, `big - base > other` is **guaranteed strictly `> 0`**, returning `(+1, (big - base) - other)`! (If `other >= base` isn't guaranteed, compare `d := big - base` only when `big <= 2*base`; note: in ALL our cross-regime callers below, `other < base <= big` ALWAYS holds!)
     - Else (`base <= big <= 2*base`): by **Sterbenz's Lemma**, `d := big - base` is **BIT-EXACT** in IEEE-754 binary64! Compare `d` vs `other` directly: `d > other` -> `(+1, d - other)`, `d == other` -> `(0, 0)`, `d < other` -> `(-1, other - d)`.

2. **Mid-Window Insert `x` between `a` and `b` (`t_a < t_x < t_b`)**:
   Strictly partitioned by value ordering (NO `d_1 + d_2 - d_old` arithmetic!):
   - Case 1: `a <= x && x <= b` (in-gen monotonic reorder): `net = 0`, `d_1 = x - a`, `d_2 = b - x`, `adj(t_b) = -d_1`.
   - Case 2: `b < a && x <= b` (post-reset intra-gen reorder): `net = 0`, `d_1 = x`, `d_2 = b - x`, `adj(t_b) = -d_1` (`= -x`).
   - Case 3: `b < a && x > b` (late pre-reset `x >= a` or double-reset `b < x < a`): `d_1 = intervalDelta(a, x)`, `d_2 = b = d_old`, **`adj(t_b) = 0`**, `net = d_1 >= 0`.
   - Case 4: `a <= b && x > b` (hidden reset between `x` and `b`): `d_1 = x - a`, `d_2 = b`, **`adj(t_b) = +a >= 0`**, `net = x >= 0`.
   - Case 5: `a <= b && x < a` (hidden reset between `a` and `x`): `d_1 = x`, `d_2 = b - x`, **`adj(t_b) = +(a - x) > 0`**, `net = a >= 0`.

3. **Interior Revision `old -> new` (`0 < i < len-1`, `a = H[i-1], b = H[i+1]`, `non_negative` mode)**:
   Exact difference `net = resetPenalty(a, new, b) - resetPenalty(a, old, b)`, where `oA = old < a`, `oB = b < old`, `nA = new < a`, `nB = b < new`:
   - If **`oA == nA`** (common `a` term cancels identically, covering ALL 3 no-change/same-side cases including `a <= b && old < a && new < a`!):
     - `!nB && !oB`: `sign = 0` (`net = 0`)
     - `nB && !oB`: `sign = +1, mag = new` (since `new > b >= 0`)
     - `!nB && oB`: `sign = -1` (since `old > b >= 0`, reject!)
     - `nB && oB`: compare `new` vs `old` directly (`new > old` -> `+1, new - old`; `new < old` -> `-1`, reject!)
   - If **`nA && !oA`** (`new < a <= old`):
     - `!nB && !oB`: `sign = +1, mag = a` (`a > new >= 0`)
     - `!nB && oB`: `old >= a`, so `old == a` -> `sign = 0`; `old > a` -> `sign = -1` (reject!)
     - `nB && oB` (`b < new < a <= old`): call `cmpDiff(old, a, new)`! (Guaranteed `new < a <= old` -> Sterbenz-exact / zero overflow!)
       - If `old - a < new` -> `sign = +1, mag = new - (old - a)`
       - If `old - a == new` -> `sign = 0`
       - If `old - a > new` -> `sign = -1` (reject!)
     - (`nB && !oB` is impossible because `b < new < a <= old` implies `b < old`.)
   - If **`!nA && oA`** (`old < a <= new`):
     - `!nB && !oB`: `sign = -1` (`a > old >= 0`, reject!)
     - `nB && !oB`: `new >= a`, so `new == a` -> `sign = 0`; `new > a` -> `sign = +1, mag = new - a`
     - `nB && oB` (`b < old < a <= new`): call `cmpDiff(new, a, old)`! (Guaranteed `old < a <= new` -> Sterbenz-exact / zero overflow!)
       - If `new - a > old` -> `sign = +1, mag = (new - a) - old`
       - If `new - a == old` -> `sign = 0`
       - If `new - a < old` -> `sign = -1` (reject!)
     - (`!nB && oB` is impossible because `b < old < a <= new` implies `b < new`.)

   When `sign == +1` in `non_negative` interior rev, drop naive `newL - oldL` / `newR - oldR` completely and emit via the 4-branch pure-structural partition directly from `(oA, oB, nA, nB)` and `cmpDiff`:
   - **(1) `oB && nB` (all cases where `dr == 0` always)**: emit `(t_a, t_i]: +mag`.
   - **(2) `oA == nA && !oB && nB`** (`mag = new`): emit `(t_a, t_i]: new - old` and (if `old > 0`) `(t_i, t_b]: old`. (Since `old <= b < new`, `new - old > 0` and `(new - old) + old = new`.)
   - **(3) `nA && !oA && !nB && !oB`** (`new < a <= old <= b`, `mag = a`): call `s, dmag := cmpDiff(old, a, new)`; if `s < 0` (`old - a < new`, both edges `> 0`) emit `(t_a, t_i]: dmag` and `(t_i, t_b]: old - new`; else (mixed-sign `dl <= 0, dr > 0`) emit `(t_i, t_b]: a`.
   - **(4) `!nA && oA && nB && !oB`** (`old < a <= new`, `old <= b < new`, `mag = new - a > 0`): call `s, dmag := cmpDiff(new, a, old)`; if `s > 0` (`new - a > old`, both edges `> 0`) emit `(t_a, t_i]: dmag` and (if `old > 0`) `(t_i, t_b]: old`; else (mixed-sign `dl <= 0, dr > 0`) emit `(t_i, t_b]: new - a`.

4. **Head Revision `old -> new` (`i == 0, len > 1, !hasEvicted, v1 = H[1]`, `non_negative` mode)**:
   - If **`initEmitted == true`**: `v1` cancels identically -> `net = ([v1 < new]*new) - ([v1 < old]*old)` (NO mixed-sign positive case exists!):
     - `old <= v1 && new <= v1`: `net = 0` (`OutcomeAbsorbed`).
     - `old <= v1 && new > v1`: `net = +new > 0` -> emit `(initStart, t_0]: new - old` (if `> 0`) and `(t_0, t_1]: old` (if `> 0`). Both exact & `>= 0`, summing to `new`!
     - `old > v1 && new <= v1`: `net = -old < 0` -> reject (`rejected_revision`, do not mutate `H`).
     - `old > v1 && new > v1`: if `new > old`, emit `(initStart, t_0]: new - old` (since `oldR == newR == v1`); else reject.
   - If **`initEmitted == false`**: `net = intervalDelta(new, v1) - intervalDelta(old, v1)`:
     - `old <= v1 && new <= v1`: if `old > new`, emit `(t_0, t_1]: old - new`; else reject (`new > old`).
     - `old <= v1 && new > v1`: if `old > 0`, emit `(t_0, t_1]: +old`; else `OutcomeAbsorbed`.
     - `old > v1 && new <= v1`: if `new > 0`, reject (`net = -new < 0`); else `OutcomeAbsorbed`.
     - `old > v1 && new > v1`: `net = 0` (`OutcomeAbsorbed`).

5. **Tail Revision `old -> new` (`i == len-1, len > 1, prev = H[len-2]`, `non_negative` mode)**:
   - If `(old >= prev) == (new >= prev)` (both normal or both reset): compare `new` vs `old` directly! If `new > old`, emit `(t_prev, t_i]: new - old`; else reject.
   - If `old < prev && new >= prev`: call `cmpDiff(new, prev, old)`! If `+1`, emit `(t_prev, t_i]: mag`; if `0`, `OutcomeAbsorbed`; if `-1`, reject.
   - If `old >= prev && new < prev`: call `cmpDiff(old, prev, new)`! If `-1` (`old - prev < new`), emit `(t_prev, t_i]: new - (old - prev)`; if `0`, `OutcomeAbsorbed`; if `+1`, reject.

*(Note: For `zset_compact` on head/interior/tail revisions, emitting `newL - oldL` and `newR - oldR` telescopes cleanly via Sterbenz with zero drift; guard only the exact-same-delta zero check `newL != oldL`.)*

### Comparison Matrix

| Mode | Output `IsMonotonic` | Global Conservation | Per-Interval `(start, end]` Exactness | Per-End-Timestamp `t` Bucket Exactness | Emits Negative Deltas? |
|---|---|---|---|---|---|
| `off` (Baseline) | `true` | Fails (overcounts OoO, loses resets) | Fails | Fails | No |
| `zset` (Full DBSP) | `false` (`true` if `preserve_monotonic`) | Exact (`sum == C(H_accepted)`) | Exact (with zero-weight key deletion) | Exact | Yes (on retractions) |
| `zset_compact` | `false` (`true` if `preserve_monotonic`) | Exact (`sum == C(H_accepted)`) | No (`start` differs on adjustments) | Exact | Yes (in-gen reorder, `auto` head-prepend flip, or negative-net upward/downward rev; `0` or `+` on mid-window reset recoveries) |
| `non_negative` | `true` (always) | Exact for `H_accepted` (drops revisions & `auto` head-prepend `t_x <= StartTimestamp` flips where `net < 0` without mutating `H`) | No | Coarse after unsplit in-gen reorder; exact on isolated reset recoveries | **Never** |

### Complete Transition Table (All 3 New Modes)

All emitted datapoints carry `(StartTimestamp, Timestamp, Value)` and **always** satisfy `StartTimestamp <= Timestamp`. When rewriting synthetic datapoints (any point other than the native single-point emission matching incoming `Timestamp`), `processor.go` sets **both** `SetStartTimestamp` and `SetTimestamp`, preserves `Attributes` & `Flags`, and clears `Exemplars` on all synthetic/re-timestamped datapoints.

| Event | `zset` Output | `zset_compact` Output | `non_negative` Output |
|---|---|---|---|
| **NaN float64** | Pass through unmodified (`processor.go:265` on `upstream/main`; `processor.go:320,378,457` on this branch), do not mutate `H` | Same | Same |
| **`±Inf` or `v < 0`** | Drop (`reason="invalid_value"`), do not mutate `H` | Same | Same |
| **First point `h_0`** | If `shouldEmitInitial`: emit `(initStartTS, t_0]: +v_0`; else drop (`reason="initial"`). Store `[h_0]`. | Same | Same |
| **Exact dup (`t == t_i && v == v_i`)** | Drop (`reason="duplicate"`), no-op. | Same | Same |
| **Too old (`hasEvicted && (t < t_0 \|\| (t == t_0 && v != v_0))`)** | Drop (`reason="window_exceeded"`), no-op. | Same | Same |
| **Tail append (`t > t_last`)** | Emit `(t_last, t]: +intervalDelta(last, curr)` (including `+0` heartbeat). | Same | Same |
| **Mid-window insert (`t_a < t_x < t_b`)** | If `d_old != 0`: emit `(t_a, t_b]: -d_old`. Emit `(t_a, t_x]: +d_1`, `(t_x, t_b]: +d_2`. (2-3 pts) | Case 1/2 (`net==0`): if `d_1 != 0`, emit `(t_a, t_x]: +d_1` and `(t_a, t_b]: -d_1`; else `OutcomeAbsorbed`. Case 3: emit `(t_a, t_x]: +d_1`. Case 4: emit `(t_a, t_x]: +(x-a)` & (if `a>0`) `(t_x, t_b]: +a`. Case 5: emit `(t_a, t_x]: +x` & `(t_x, t_b]: +(a-x)`. | Case 1/2 (`net==0`): update `H`, return `OutcomeAbsorbed`. Case 3/4/5: identical positive emissions to `zset_compact` (all `>= 0`). |
| **Head prepend (`!hasEvicted && t_x < t_0`)** | Update `initStartTS`, `initEmitted`. Retract `(oldStart, t_0]: -oldInit` (if `oldInitEmitted && v_0 != 0`), emit `(newStart, t_x]: +v_x` (if `newInitEmit`), emit `(t_x, t_0]: +d_1`. | Update `initStartTS`, `initEmitted`. If `newInitEmit && v_x != 0`: emit `(newStart, t_x]: +v_x`. Compute structural `adj = d_1 - oldInit`; if `adj != 0`: emit `(start, t_0]: adj` (`oldStart` if `<0`, else `t_x`). Else if nothing emitted: `OutcomeAbsorbed`. | Keyed strictly on `(oldInitEmitted, newInitEmit)` (3 reachable pairs; `(false, true)` impossible since `t_x < t_0`): **`(false, false)`** (covers `drop` AND `auto` when `StartTS < startTime` or `t_0 <= StartTS`): update `initStartTS`/`initEmitted`, emit `(t_x, t_0]: +d_1`. **`(true, true)`**: update `initStartTS`/`initEmitted`; if `v_x <= v_0` -> `OutcomeAbsorbed`; else emit `(newStart, t_x]: +v_x`. **`(true, false)`** (`auto` flip when **`t_x <= StartTimestamp`**): if `v_x <= v_0 && v_x > 0` -> **reject (`rejected_revision`) without mutating `H` or `initEmitted`**; else (`v_x == 0` or `v_x > v_0`) update `initStartTS=0, initEmitted=false` and return `OutcomeAbsorbed`. |
| **Tail revision (`t == t_last, len > 1`)** | If `oldL != newL`: retract `(t_prev, t]: -oldL` (if `!=0`), emit `(t_prev, t]: +newL`. Else `OutcomeAbsorbed`. | If `newL != oldL`: emit `(t_prev, t]: newL - oldL`. Else `OutcomeAbsorbed`. | Formula #5 above (`cmpDiff` Sterbenz-exact; reject `rejected_revision` without mutating `H` if `<0`; `OutcomeAbsorbed` if `==0`). |
| **Head revision (`!hasEvicted, t == t_0`)** | If `initEmitted && v_old != 0`: retract `(initStart, t_0]: -v_old`; if `initEmitted`: emit `(initStart, t_0]: +v_new`. If `len > 1` & `oldR != newR`: retract `(t_0, t_1]: -oldR` (if `!=0`), emit `(t_0, t_1]: +newR`. If nothing emitted: `OutcomeAbsorbed`. | If `initEmitted && v_new != v_old`: emit `(initStart, t_0]: v_new - v_old`. If `len > 1` & `newR != oldR`: emit `(t_0, t_1]: newR - oldR`. If nothing emitted: `OutcomeAbsorbed`. | If `len == 1`: if `!initEmitted` -> `OutcomeAbsorbed`; else if `v_new >= v_old` -> emit `v_new - v_old`; else reject. If `len > 1`: Formula #4 above (`initEmitted` vs `!initEmitted`, reject without mutating `H` if `<0`). |
| **Interior revision (`0 < i < len-1`)** | Left: if `oldL != newL`: retract `-oldL` (if `!=0`), emit `+newL`. Right: if `oldR != newR`: retract `-oldR` (if `!=0`), emit `+newR`. If both unchanged: `OutcomeAbsorbed`. | Left: if `newL != oldL`, emit `(t_a, t_i]: newL - oldL`. Right: if `newR != oldR`, emit `(t_i, t_b]: newR - oldR`. If both unchanged: `OutcomeAbsorbed`. | Formula #3 above (common-`a` cancel + Sterbenz `cmpDiff`; reject without mutating `H` if `<0`; `OutcomeAbsorbed` if `==0`; positive edge(s) if `>0`). |

### Memory Layout, Concurrency Safety, & Exact In-Place Eviction

1. **In-Memory Structs (Zero Impact on Legacy `off` Mode)**:
   - Existing `state` struct (`48 B`) is **untouched** for `off` mode and all `Histogram`/`ExponentialHistogram` streams.
   - Sum streams in retraction mode use `*retractionState` stored in a **separate `retractionStates sync.Map`** on `MetricTracker` (`t.states` only ever holds `*state`, eliminating any `value.(*state)` panic risk in `removeStale`). `t.Streams()` reflects both maps.
   - Struct layout (64-bit amd64):
     ```go
     type sumObs struct {
         ts   pcommon.Timestamp // 8 B
         bits uint64            // 8 B (uint64(int64) or math.Float64bits(float64))
     } // 16 B total, zero pointers -> Go GC marks []sumObs as noscan!

     type retractionState struct {
         points      []sumObs          // 24 B (field 0 -> GC ptrdata = 8 B only!)
         mu          sync.Mutex        // 8 B
         initStartTS pcommon.Timestamp // 8 B
         flags       uint8             // bit0: initEmitted, bit1: hasEvicted, bit2: deleted
     } // 48 B total (same size class as legacy state)
     ```
2. **Load-First Fast Path & Zero-Spin Safe Concurrent Deletion (`-race` Free)**:
   - On cache hit, `ConvertSum` executes `t.retractionStates.Load(hashableID)`: **zero `retractionState` allocation on steady-state hits!**
   - On miss: construct `newS` with `[initialObs]`, `initStartTS`, `initEmitted`, lock unshared `newS.mu.Lock()`, call `t.retractionStates.LoadOrStore(hashableID, newS)`. If `!loaded`, call `t.streams.Add(1)` and `newS.mu.Unlock()` so `removeStale` cannot delete and decrement `t.streams` before `t.streams.Add(1)` (preventing transient `Streams() == -1`).
   - Inside `removeStale(staleBefore)`: acquires `s.mu.Lock()`, checks `s.points[len(s.points)-1].ts < staleBefore` (monotonic max timestamp), sets `s.flags |= flagDeleted`, calls **`t.retractionStates.CompareAndDelete(key, s)` (and `if deleted { t.streams.Add(-1) }`) WHILE STILL HOLDING `s.mu`**, then `s.mu.Unlock()`. Lock order is strictly `s.mu -> sync.Map` (no deadlock because `ConvertSum` never holds a `sync.Map` lock while acquiring `s.mu`), eliminating any busy-spin/livelock retry window!
   - In `ConvertSum`: after acquiring `s.mu.Lock()`, if `s.flags & flagDeleted != 0`, unlock and retry (at most 1 retry per eviction).
3. **Exact In-Place Insert-and-Evict Algorithm (Logical `(n+1)` View)**:
   Let `n = len(s.points)` and `insertIdx` in `[0, n]` be the sorted insertion position for new point `x` (`t_new`).
   Define virtual accessor `at(k)` for logical `(n+1)`-element sorted array `H_new = [H[:insertIdx], x, H[insertIdx:]]`:
   - `k < insertIdx` -> `s.points[k]`
   - `k == insertIdx` -> `x`
   - `k > insertIdx` -> `s.points[k-1]`
   Compute `maxTS = at(n).ts` (`max` of existing tail and `x.ts`).
   - `dropTime`: if `reorder_window > 0 && maxTS > pcommon.Timestamp(reorder_window)`:
     `cutoff := maxTS - pcommon.Timestamp(reorder_window)`
     Binary search `H_new` for first index `firstGE` with `at(firstGE).ts >= cutoff`.
     If `firstGE >= 2`, `dropTime = firstGE - 1` (leaving `at(firstGE - 1)` as the single left anchor strictly `< cutoff`!).
   - `dropCap`: if `(n + 1) > max_history_points`: `dropCap = (n + 1) - max_history_points`.
   - `drop := max(dropTime, dropCap)`.
   - If `drop == 0`: normal insert (`s.points = append(s.points, sumObs{})`, `copy(s.points[insertIdx+1:], s.points[insertIdx:])`, `s.points[insertIdx] = x`).
   - If **`drop >= 1`**:
     - **ALWAYS set `s.flags |= flagHasEvicted`** (triggered by EITHER `reorder_window` OR `max_history_points`!).
     - `newLen := n + 1 - drop` (guaranteed `<= n <= cap(s.points)`, **ZERO allocation, NO slice doubling!**).
     - Case (a) **`insertIdx < drop`** (`x` itself falls into the evicted prefix):
       - `if drop > 1 { copy(s.points[:newLen], s.points[drop-1:]) }` (`drop == 1` leaves `s.points` completely unchanged!)
       - `s.points = s.points[:newLen]`
     - Case (b) **`insertIdx >= drop`**:
       - `p := insertIdx - drop`
       - `copy(s.points[:p], s.points[drop:insertIdx])`
       - `if drop > 1 { copy(s.points[p+1:newLen], s.points[insertIdx:]) }` (`drop == 1` leaves suffix `s.points[insertIdx:]` in place!)
       - `s.points[p] = x`
       - `s.points = s.points[:newLen]`

---

## 4. Milestones & Falsifiable Exit Gates

### Milestone 1: Core Engine, Processor Integration & Exhaustive Correctness Suite
- **Deliverable**:
  - `internal/tracking/retraction.go` (`RetractionMode`, `RetractionDelta`, `Outcome`, `retractionState`, `ConvertSum`, exact `cmpDiff` & transition formulas, exact `(n+1)` insert-and-evict, zero-spin `removeStale`).
  - `config.go`, `processor.go`, `internal/tracking/tracker.go` wiring with zero impact on default `off` fast path, causal multi-point emission in `convertNumberDataPoints`, `dp.SetTimestamp` + exemplar clearing on synthetic points, `recordDropped` nil-safe guard with lowercase snake_case metric types, unconditional `ms.SetIsMonotonic(false)` after reading `baseIdentity` when `!preserve_monotonic` in `zset`/`zset_compact`.
  - `internal/tracking/retraction_test.go` and `retraction_processor_test.go` covering:
    - All 6 value permutations of `(v_a, v_x, v_b)` for both `int64` (exact) and `float64` (including `(0.1, 0.2, 2.0)`, `(0.1, 100.3, 1000.0)`, `(5000.0, 1057.562965768663, 3672.887320536073)`!) across `zset`, `zset_compact`, `non_negative`.
    - `int64` near-`MaxInt64` overflow cases (`(100, MaxInt64, MaxInt64-1)`), `float64` near-`MaxFloat64` (`1.5e308` Sterbenz anti-overflow), double-reset scale mismatch (`a=1e16, b=1.0, old=2.0, new=1.5`), and near-boundary cross-regime `b < new < a <= old`.
    - All same-timestamp revision cases (head/interior/tail, upward/downward, mixed-sign `net > 0`, `net < 0` rejection without mutating `H`, and 0-delta `OutcomeAbsorbed` transitions across all 3 modes).
    - Head-prepend across `keep`, `drop`, `auto` (including `auto` `t_x == StartTimestamp && v_x > 0` flip, **`t_x < StartTimestamp` clock-skew flip**, `StartTimestamp == 0` under `auto`, `v_0 == 0`, and `initEmitted`/`initStartTS` state update chaining into subsequent head revision!).
    - In-place `(n+1)` eviction-and-insert verification against a reference naive insert-then-compact oracle across 20,000+ random sequences (covering `dropTime`, `dropCap`, `insertIdx < drop`, `drop == 1`, `drop > 1`, and asserting `cap(points)` never doubles past steady state!).
    - Randomized permutation property test (1,000+ seeded trials per mode with resets, duplicates, revisions, and window compaction): computes `C_oracle(H_accepted)` per mode using `math/big.Int` for `int64` (`0` exact diff) and relative tolerance (`<= 1e-9 * max(1, maxVal)`) for `float64`.
    - Real `componenttest.NewTelemetry()` test (NOT nop!) proving `recordDropped` never panics on `window_exceeded`, `duplicate`, `invalid_value`, or `rejected_revision`.
    - Mixed `Sum` (`*retractionState`) + `Histogram` (`*state`) concurrent `Convert` + `removeStale` stress test under `-race` (verifying `Streams() >= 0` always!), plus `goleak`.
- **Verification Command**: `cd processor/cumulativetodeltaprocessor && go test -race -count=1 ./...`
- **Baseline-vs-Change Proof**: Side-by-side unit test feeding identical out-of-order + reset trace through `off` vs. `zset`/`zset_compact`/`non_negative`.

### Milestone 2: Synthetic Trace Replay Evaluation & Downstream Compatibility Verification
- **Deliverable**:
  - `evaluation_test.go` running deterministic seeded traces anchored strictly after `tracker.startTime` (`now + 1h` base clock) modeling Kubernetes/ephemeral pods:
    - Out-of-order rates: `5%`, `10%`, `15%`; latency jitter up to `10s`; scrape intervals `5s` (`W=8`) and `2s` (`W=17`); `reorder_window = 30s`, `max_history_points = 64`.
    - Both reset modes: (a) frequent container restarts with `StartTimestamp == 0` (shared identity) and (b) frequent restarts with rotating `StartTimestamp` (`StartTimestamp > tracker.startTime`), including flat `+0` heartbeat intervals so baseline inverted-interval emission is exercised in both scenarios.
    - Sub-suite with same-timestamp duplicates and value revisions.
  - Reports exact comparative metrics table across `off`, `zset`, `zset_compact`, `non_negative`:
    1. **Error Drops (%)**: `(reset + window_exceeded + invalid_value + rejected_revision) / total_inputs`, tracked separately from **Expected Suppressions** (`initial`, `duplicate`, `absorbed`). Exit gate: `0%` error drops in `zset` / `zset_compact` for jitter `<= 10s` with `reorder_window = 30s` (excluding intentional `rejected_revision` in `non_negative` revision sub-suite).
    2. **Spurious Delta Spikes** (two exact checks):
       - **(a) Tail-Rewind Overlapping Spikes**: count emitted points (excluding `NaN` passthrough) where **`Timestamp > prevMaxEmittedTimestamp && StartTimestamp < prevMaxEmittedTimestamp`** (advancing the high-water mark while starting strictly behind it, catching `off`'s false-reset rewind double-count on **both** Scenario (a) and Scenario (b)!). Exit gate: `0` for `zset`/`zset_compact`/`non_negative` vs `> 0` for `off`.
       - **(b) Prefix-Oracle Excess**: count of emitted points where `delta_emitted > oracle_gap_sum_at_arrival(start, end] + slack` (`slack = 0` for `int64`, `1e-9 * max(1, maxVal)` for `float64`) evaluated against the **prefix of inputs seen at or before emission time** (catches inverted pre-restart spikes on Scenario (a) without false-positiving on later downward revisions!). Exit gate: `0` for all 3 new modes vs `> 0` for `off` on Scenario (a).
    3. **Inverted Intervals**: count of emitted points with `StartTimestamp > Timestamp`. Exit gate: `0` for all 3 new modes vs `> 0` for `off` (both Scenario (a) and Scenario (b) with flat `+0` heartbeats).
    4. **Global Conservation Error**: `|sum(emitted) - C_oracle(H_accepted)|` (`0` bit-exact `big.Int` for `int64`; `<= 1e-9` relative for `float64`).
    5. **Per-End-Timestamp Bucket MAE**: average `|bucket_emitted[t] - bucket_oracle[t]|` across all distinct timestamps. Exit gate: `0` exact for `int64`, and relative/scaled `<= 1e-9 * max(1, maxVal)` for `float64`, across BOTH `zset` and `zset_compact`.
    6. **Interval-Keyed `(start, end]` DBSP Z-Set Exactness**: exact map equality after deleting zero-weight entries (`if sum == 0 { delete(m, key) }`). Exit gate: `0` mismatched keys for `zset`.
    7. **Wire Amplification & Negative Delta Count**: `emitted / inputs` and `# negative deltas` (`0` for `non_negative`!).
  - **Downstream Compatibility Probes**:
    1. **DBSP Interval-Keyed Z-Set Integrator** (with zero-weight key deletion): passes `100%` for `zset`.
    2. **End-Timestamp Additive TSDB Accumulator**: passes `100%` for `zset` and `zset_compact`.
    3. **Standalone `deltatocumulativeprocessor` Compatibility Probe** (replicates exact `ErrOlderStart` (`StartTimestamp < state.StartTimestamp`) and `ErrOutOfOrder` (`Timestamp <= state.Timestamp`) guards from `processor/deltatocumulativeprocessor/internal/delta/delta.go:46-58`, plus signed `Adder` from `data/add.go:34-44`): separately quantifies `ErrOlderStart` vs `ErrOutOfOrder` drops across all 4 modes.
- **Verification Command**: `go test -v -run 'TestEvaluation|TestDownstreamCompatibility' ./...`
- **Baseline-vs-Change Proof**: Quantifies baseline `off` drops, spikes, inverted intervals, and conservation error vs. `0%` error drops, `0` spikes, `0` inverted intervals, and exact conservation in `zset`/`zset_compact`/`non_negative`.

### Milestone 3: 50,000-Series Memory Footprint & `benchstat` Latency Benchmarks
- **Deliverable**:
  - `benchmark_50k_test.go` measuring:
    - **Memory Footprint at 50,000 Active Series** (`TestMemoryFootprint50kSeries`): captures baseline `ReadMemStats` after **two** `runtime.GC()` cycles + `runtime.KeepAlive`, populates 50,000 active `Sum` series at steady state (`5s` scrape / `30s` window -> `8` points retained, AND `2s` scrape / `30s` window -> `17` points retained), runs two `runtime.GC()` cycles, and reports delta `HeapAlloc`, `HeapObjects`, `B/series`, and GC pause stats across `off`, `zset`, `zset_compact`, `non_negative`. Separate sub-measurement reports rotating-`StartTimestamp` proliferation (3 generations / 2 restarts per series = `150,000` map entries) showing the `R+1` shared identity growth factor.
    - **Numeric Exit Gates**:
      - `off` (default) vs compiled `upstream/main` baseline (`ctd_bench_base.test`) on `BenchmarkConsumeMetrics`: **0 extra `allocs/op`**, **0 extra `B/op`**, and `benchstat` `sec/op` regression **`<= 3%` or statistically insignificant (`p > 0.05`)**.
      - `zset` / `zset_compact` / `non_negative` steady-state 50k series incremental memory delta vs `off`: **`<= 256 B/series` extra (`<= 12.21 MiB` binary / `12.80 MB` decimal total delta)** at `W=8` (`5s` scrape / `30s` window).
      - Per-point steady-state conversion latency under 50k active series cycling 1 datapoint per `b.Loop()` iteration (`-count=6 -benchtime=1s`): in-order AND 15% out-of-order + 10s jitter + restarts, strictly serial execution.
  - Full-package `benchstat` run: compiled `ctd_bench_base.test` vs `ctd_bench_new.test` run strictly serially (`-count=6 -benchtime=100ms` for fast existing `BenchmarkConsumeMetrics`, `-benchtime=1s` for 50k benchmarks), analyzed via `go run golang.org/x/perf/cmd/benchstat@latest` with `B/s` stripped if present.
- **Verification Command**:
  - `go test -v -run TestMemoryFootprint50kSeries ./...`
  - Serial compiled binary `benchstat` run.
