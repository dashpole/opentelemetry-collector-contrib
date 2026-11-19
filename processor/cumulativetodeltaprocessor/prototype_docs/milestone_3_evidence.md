# Milestone 3 Evidence: 50,000-Series Memory Footprint & `benchstat` Latency Benchmarks

**Environment**: `linux/amd64`, `Intel(R) Xeon(R) CPU @ 2.20GHz` (24 logical CPUs), `go1.27.3`
**Execution Discipline**: Compiled baseline binary (`ctd_bench_base.test` built from unmodified `upstream/main`) vs prototype binaries (`ctd_bench_new.test`, `ctd_tracker_bench.test`), executed **strictly serially** (one process at a time). All raw logs archived at `results/mem_50k.txt`, `results/bench_base.txt`, `results/bench_new.txt`, `results/benchstat.txt`, `results/bench_50k.txt`, `results/benchstat_50k.txt`, `results/bench_tracker.txt`, `results/benchstat_tracker.txt`.

## 1. Memory Footprint at 50,000 Active Series (`TestMemoryFootprint50kSeries`)

All memory measurements use **binary `MiB` (`1024 * 1024` bytes)**. Baseline vs steady-state heap delta measured with dual `runtime.GC()` + `runtime.KeepAlive(p)`. Runtime: `13.64s` without `-race` (`-short` skips it during fast `-race` runs; `210.9s` under `-race` in full run).

### 5s scrape / 30s window (`W = 8` points retained, `cap = 8` pinned)
| Mode | Heap MiB | B/series | Delta vs `off` | Heap Objects | GC Pause Total |
|---|---:|---:|---:|---:|---:|
| `off` | 11.06 MiB | 231.8 | +0.0 B | 217,164 | 5.88 ms |
| `zset` | 17.11 MiB | 358.8 | **+127.0 B** | 267,146 | 3.26 ms |
| `zset_compact` | 17.13 MiB | 359.3 | **+127.5 B** | 267,311 | 3.14 ms |
| `non_negative` | 17.15 MiB | 359.8 | **+127.9 B** | 267,410 | 4.21 ms |

- **Exit Gate Check**: `<= 256 B/series` incremental delta (`<= 12.21 MiB` binary / `12.80 MB` decimal) -> **PASSED with 2x headroom** (`+127.0` to `+127.9 B/series`, `+6.05` to `+6.09 MiB` total delta for 50,000 series; matches exact `8 * 16 B = 128 B` `noscan` slice size).

### 2s scrape / 30s window (`W = 17` points retained, `+8` step growth -> `cap = 24` = 384 B)
| Mode | Heap MiB | B/series | Delta vs `off` | Heap Objects | GC Pause Total |
|---|---:|---:|---:|---:|---:|
| `off` | 11.03 MiB | 231.4 | +0.0 B | 217,322 | 7.04 ms |
| `zset` | 29.34 MiB | 615.3 | **+383.9 B** | 267,298 | 4.05 ms |
| `zset_compact` | 29.37 MiB | 616.0 | **+384.6 B** | 267,507 | 4.89 ms |
| `non_negative` | 29.33 MiB | 615.1 | **+383.7 B** | 267,221 | 5.12 ms |

*Optimization note (analytical comparison)*: Go's standard `2x` slice doubling grows `cap 16 -> 32` (`32 * 16 B = 512 B/series`, analytical total `11.03 + 50,000*512/1,048,576 = 35.44 MiB`). Stepping capacity by `+8` once `cap >= 16` (`16 -> 24`, `24 * 16 B = 384 B/series`) achieves measured `+383.7` to `+384.6 B/series` (`29.33-29.37 MiB`), saving **`128 B/series` (`6.10 MiB` at 50k series, `-25.0%` incremental delta / `-17.2%` total heap)** versus `2x` doubling.

### Rotating `StartTimestamp` Scaling (50,000 series x 3 generations / 2 restarts = 150,000 map entries)
| Mode | Heap MiB | B/entry | Heap Objects |
|---|---:|---:|---:|
| `off` | 34.13 MiB | 238.6 | 658,606 |
| `zset` | 52.41 MiB | 366.4 | 808,507 |

Both modes scale linearly by `R+1` entries during `max_staleness` (`R=2` restarts -> `R+1 = 3` generations -> `150,000` map entries; each generation sets `startTS <= ts`) because `MetricIdentity` keys on `StartTimestamp`.

## 2. Full-Package Regression Gate (`benchstat`: `upstream/main` vs prototype `off` default)

Command: `-count=6 -benchtime=100ms -benchmem` on unmodified `BenchmarkConsumeMetrics` (`results/benchstat.txt`).

```text
goos: linux
goarch: amd64
pkg: github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor
cpu: Intel(R) Xeon(R) CPU @ 2.20GHz
                  │ results/bench_base.txt │    results/bench_new.txt     │
                  │         sec/op          │    sec/op     vs base         │
ConsumeMetrics-24              2.574µ ± 12%   2.442µ ± 23%  ~ (p=0.818 n=6)

                  │ results/bench_base.txt │    results/bench_new.txt     │
                  │          B/op           │    B/op     vs base           │
ConsumeMetrics-24                224.0 ± 0%   224.0 ± 0%  ~ (p=1.000 n=6) ¹
¹ all samples are equal

                  │ results/bench_base.txt │    results/bench_new.txt     │
                  │        allocs/op        │ allocs/op   vs base           │
ConsumeMetrics-24                9.000 ± 0%   9.000 ± 0%  ~ (p=1.000 n=6) ¹
¹ all samples are equal
```

- **Exit Gate Check**: `0` extra `B/op`, `0` extra `allocs/op`, no statistically significant latency difference (`p=0.818`). **PASSED.**

## 3. 50,000-Series Processor Benchmarks (`benchstat`, `-count=6 -benchtime=1s`)

Verbatim output from `results/benchstat_50k.txt`:

```text
goos: linux
goarch: amd64
pkg: github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor
cpu: Intel(R) Xeon(R) CPU @ 2.20GHz
                            │     off      │                zset                 │            zset_compact             │            non_negative             │
                            │    sec/op    │    sec/op     vs base               │    sec/op     vs base               │    sec/op     vs base               │
50kSeriesInOrder-24           2.386µ ± 36%   1.888µ ± 16%  -20.91% (p=0.009 n=6)   1.920µ ± 10%  -19.55% (p=0.015 n=6)   1.917µ ±  7%  -19.69% (p=0.004 n=6)
50kSeriesOutOfOrder15Pct-24   3.569µ ±  9%   3.420µ ±  6%        ~ (p=0.240 n=6)   3.543µ ±  7%        ~ (p=0.937 n=6)   3.302µ ± 10%        ~ (p=0.065 n=6)
geomean                       2.918µ         2.541µ        -12.94%                 2.608µ        -10.63%                 2.515µ        -13.80%

                            │    off     │               zset                │           zset_compact            │           non_negative            │
                            │    B/op    │    B/op     vs base               │    B/op     vs base               │    B/op     vs base               │
50kSeriesInOrder-24           224.0 ± 0%   160.0 ± 0%  -28.57% (p=0.002 n=6)   160.0 ± 0%  -28.57% (p=0.002 n=6)   160.0 ± 0%  -28.57% (p=0.002 n=6)
50kSeriesOutOfOrder15Pct-24   224.0 ± 0%   228.0 ± 0%   +1.79% (p=0.002 n=6)   205.0 ± 0%   -8.48% (p=0.002 n=6)   192.0 ± 1%  -14.29% (p=0.002 n=6)
geomean                       224.0        191.0       -14.73%                 181.1       -19.15%                 175.3       -21.75%

                            │    off     │               zset                │           zset_compact            │           non_negative            │
                            │ allocs/op  │ allocs/op   vs base               │ allocs/op   vs base               │ allocs/op   vs base               │
50kSeriesInOrder-24           9.000 ± 0%   7.000 ± 0%  -22.22% (p=0.002 n=6)   7.000 ± 0%  -22.22% (p=0.002 n=6)   7.000 ± 0%  -22.22% (p=0.002 n=6)
50kSeriesOutOfOrder15Pct-24   9.000 ± 0%   8.000 ± 0%  -11.11% (p=0.002 n=6)   7.000 ± 0%  -22.22% (p=0.002 n=6)   7.000 ± 0%  -22.22% (p=0.002 n=6)
geomean                       9.000        7.483       -16.85%                 7.000       -22.22%                 7.000       -22.22%
```

## 4. 50,000-Series Pure Tracker Benchmarks (`internal/tracking`, `-count=6 -benchtime=1s`)

Verbatim output from `results/benchstat_tracker.txt`:

```text
goos: linux
goarch: amd64
pkg: github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor/internal/tracking
cpu: Intel(R) Xeon(R) CPU @ 2.20GHz
                             │     off      │               zset                │            zset_compact            │            non_negative            │
                             │    sec/op    │   sec/op     vs base              │    sec/op     vs base              │    sec/op     vs base              │
Tracker50kInOrder-24           1.265µ ± 12%   1.245µ ± 5%       ~ (p=0.818 n=6)   1.352µ ± 17%       ~ (p=0.258 n=6)   1.210µ ± 35%       ~ (p=0.699 n=6)
Tracker50kOutOfOrder15Pct-24   1.420µ ± 12%   1.375µ ± 8%       ~ (p=0.093 n=6)   1.368µ ±  4%       ~ (p=0.065 n=6)   1.432µ ±  9%       ~ (p=0.851 n=6)
geomean                        1.340µ         1.308µ       -2.38%                 1.360µ        +1.49%                 1.316µ        -1.77%

                             │     off     │               zset                │           zset_compact            │           non_negative            │
                             │    B/op     │    B/op     vs base               │    B/op     vs base               │    B/op     vs base               │
Tracker50kInOrder-24           112.00 ± 0%   48.00 ± 0%  -57.14% (p=0.002 n=6)   48.00 ± 0%  -57.14% (p=0.002 n=6)   48.00 ± 0%  -57.14% (p=0.002 n=6)
Tracker50kOutOfOrder15Pct-24   112.00 ± 0%   61.00 ± 3%  -45.54% (p=0.002 n=6)   61.00 ± 3%  -45.54% (p=0.002 n=6)   61.00 ± 3%  -45.54% (p=0.002 n=6)
geomean                         112.0        54.11       -51.69%                 54.11       -51.69%                 54.11       -51.69%

                             │    off     │               zset                │           zset_compact            │           non_negative            │
                             │ allocs/op  │ allocs/op   vs base               │ allocs/op   vs base               │ allocs/op   vs base               │
Tracker50kInOrder-24           3.000 ± 0%   1.000 ± 0%  -66.67% (p=0.002 n=6)   1.000 ± 0%  -66.67% (p=0.002 n=6)   1.000 ± 0%  -66.67% (p=0.002 n=6)
Tracker50kOutOfOrder15Pct-24   3.000 ± 0%   1.000 ± 0%  -66.67% (p=0.002 n=6)   1.000 ± 0%  -66.67% (p=0.002 n=6)   1.000 ± 0%  -66.67% (p=0.002 n=6)
geomean                        3.000        1.000       -66.67%                 1.000       -66.67%                 1.000       -66.67%
```

## 5. Fairness, Asymmetric-Optimization, and Harness Disclosures

1. **`Load`-first asymmetry (`-64 B/op, -2 allocs/op`)**: The `ConvertSum` path calls `t.retractionStates.Load(hashableID)` before allocating, whereas unmodified `upstream/main` (`tracker.go:153` on `upstream/main`; `tracker.go:165` on this branch) unconditionally calls `t.states.LoadOrStore(hashableID, &state{...})` on every steady-state hit (allocating `48 B` for `&state{}` + `16 B` interface boxing). The `off` baseline was kept untouched to avoid confounding the regression gate against `upstream/main`; porting `Load`-first to legacy `tracker.go` is an independent upstream optimization.
2. **Amortized `cap 8 -> 16` slice growth and multi-point `pdata` expansion inside `OutOfOrder15Pct`**: Both `OutOfOrder15Pct` benchmarks pre-warm all 50,000 series in-order at `5s` scrapes (`cap = 8`, `128 B/series`). Dense `15%` mid-window insertions push `len` past `8`, triggering one-time `[]sumObs` slice growth to `cap = 16` (`256 B/series`, `12.21 MiB` if all 50,000 series grow). Verified via `go tool pprof -alloc_space` (`2,000,000` tracker iters drops to `54 B/op`; `500,000` processor `non_negative` iters drops to `189 B/op`):
   - In `Tracker50kOutOfOrder15Pct` (`~747k-1M` iters, zero `pdata` expansion): adds `~13 B/op` (`48 -> 61 B/op`, range `60-63 B/op`) purely from one-time `cap 8 -> 16` growth.
   - In `50kSeriesOutOfOrder15Pct` (`~320k-407k` iters): `non_negative` (`192 B/op`, `+32 B/op` above `160 B/op`) reflects both amortized `cap 8 -> 16` slice growth (`~21 B/op`) and `dps.AppendEmpty()` + `Attributes().CopyTo()` (`~11 B/op`) on occasional 2-point hidden-reset emissions (when an out-of-order point follows a `3%` reset). Above `non_negative`, `zset_compact` adds `+13 B/op` (`205 B/op`) for frequent 2-point `pdata` expansions, and `zset` adds `+36 B/op, +1 alloc/op` (`228 B/op, 8 allocs/op`) for 3-point `pdata` expansions.
3. **`b.StopTimer()` / `b.StartTimer()` in `Benchmark50kSeriesOutOfOrder15Pct`**: Rebuilding the `pmetric.Metrics` input fixture when `ConsumeMetrics` empties or expands `DataPoints()` (`Len() != 1`) is bracketed with `b.StopTimer()` / `b.StartTimer()` so fixture allocations are excluded (`off` stays exact `224 B/op, 9 allocs/op`). Note that invoking `StopTimer`/`StartTimer` on `~15%-18%` of iterations adds `~1 us/op` wall-time overhead symmetrically across all 4 modes compared to `InOrder` (`~3.3-3.6 us` vs `~1.9-2.4 us`), which is why the pure tracker benchmark `Tracker50kOutOfOrder15Pct` (`1.37-1.43 us`, zero `StopTimer` calls) isolates the true per-point latency.
4. **Identical in-order hot path**: `zset`, `zset_compact`, and `non_negative` execute the exact same tail-append fast path (`160 B/op, 7 allocs/op` processor; `48 B/op, 1 alloc/op` tracker). The `-20%` processor latency gain over `off` on `50kSeriesInOrder` is driven primarily by the `-64 B/op, -2 allocs/op` `Load`-first difference and multi-tenant CPU variance (`±36%` CI on `off`).
