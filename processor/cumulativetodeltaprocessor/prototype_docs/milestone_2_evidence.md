# Milestone 2 Evidence: Synthetic Trace Replay & Downstream Compatibility

**Command**: `go test -v -count=1 -run 'TestEvaluationSyntheticTraces|TestDownstreamCompatibility' .`
**Result**: `PASS` (`6.243s`, 0 failures; archived verbatim in `results/eval_m2.txt`)

All synthetic traces anchor timestamps strictly after `tracker.startTime` (`now + 1h`), use `reorder_window = 30s`, `max_history_points = 64`, latency jitter up to `10s`, periodic container restarts (every 18-37 scrapes), and 12% flat `+0` heartbeat intervals.

Columns:
- `ErrDrop%`: `(reset + window_exceeded + invalid_value + rejected_revision) / total_inputs` (excludes expected suppressions `initial`, `duplicate`, `absorbed`).
- `SpkT`: Tail-Rewind Overlapping Spikes (`Timestamp > prevMaxEmittedTimestamp && StartTimestamp < prevMaxEmittedTimestamp`).
- `SpkP`: Prefix-Oracle Excess spikes (`delta_emitted > oracle_gap_sum_at_arrival(start, end] + slack`).
- `Inv`: Inverted intervals (`StartTimestamp > Timestamp`).
- `GlobalErr`: `|sum(emitted) - C_oracle(H_accepted)|` (exact `big.Int.String()` for integer traces; deterministic sorted-key `float64` for Scenario D).
- `BucketMAE`: Mean absolute error per end-timestamp bucket vs. in-order oracle.
- `ZSetMM`: Mismatched interval keys `(start, end]` after deleting zero-weight entries.
- `Amp`: Wire amplification (`emitted / inputs`).
- `Neg#`: Count of emitted negative delta datapoints.

## Scenario A: Shared Identity (`StartTimestamp == 0`) with Frequent Restarts (30 series x 120 samples = 3,600 inputs)

### A1: 5s scrape (`W=8`), 5% OoO (<= 10s jitter)
| Mode | Inputs | Emitted | ErrDrop% | SpkT | SpkP | Inv | GlobalErr | BucketMAE | ZSetMM | Amp | Neg# |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `off` | 3600 | 3324 | 7.67% | 168 | 7 | 34 | 12580 | 8.933 | 915 | 0.92 | 0 |
| `zset` | 3600 | 3969 | 0.00% | 0 | 0 | 0 | 0 | 0 | 0 | 1.10 | 183 |
| `zset_compact` | 3600 | 3741 | 0.00% | 0 | 0 | 0 | 0 | 0 | 318 | 1.04 | 160 |
| `non_negative` | 3600 | 3421 | 0.00% | 0 | 0 | 0 | 0 | 3.756 | 502 | 0.95 | 0 |

### A2: 5s scrape (`W=8`), 10% OoO (<= 10s jitter)
| Mode | Inputs | Emitted | ErrDrop% | SpkT | SpkP | Inv | GlobalErr | BucketMAE | ZSetMM | Amp | Neg# |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `off` | 3600 | 3166 | 12.06% | 327 | 3 | 47 | 16108 | 15.12 | 1658 | 0.88 | 0 |
| `zset` | 3600 | 4324 | 0.00% | 0 | 0 | 0 | 0 | 0 | 0 | 1.20 | 361 |
| `zset_compact` | 3600 | 3892 | 0.00% | 0 | 0 | 0 | 0 | 0 | 638 | 1.08 | 326 |
| `non_negative` | 3600 | 3240 | 0.00% | 0 | 0 | 0 | 0 | 8.731 | 1006 | 0.90 | 0 |

### A3: 5s scrape (`W=8`), 15% OoO (<= 10s jitter)
| Mode | Inputs | Emitted | ErrDrop% | SpkT | SpkP | Inv | GlobalErr | BucketMAE | ZSetMM | Amp | Neg# |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `off` | 3600 | 3051 | 15.25% | 439 | 18 | 78 | 36712 | 23.32 | 2164 | 0.85 | 0 |
| `zset` | 3600 | 4571 | 0.00% | 0 | 0 | 0 | 0 | 0 | 0 | 1.27 | 482 |
| `zset_compact` | 3600 | 3953 | 0.00% | 0 | 0 | 0 | 0 | 0 | 858 | 1.10 | 412 |
| `non_negative` | 3600 | 3129 | 0.00% | 0 | 0 | 0 | 0 | 10.95 | 1323 | 0.87 | 0 |

### A4: 2s scrape (`W=17`), 15% OoO (<= 10s jitter)
| Mode | Inputs | Emitted | ErrDrop% | SpkT | SpkP | Inv | GlobalErr | BucketMAE | ZSetMM | Amp | Neg# |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `off` | 3600 | 2970 | 17.50% | 424 | 46 | 67 | 91607 | 39.73 | 2145 | 0.82 | 0 |
| `zset` | 3600 | 4706 | 0.00% | 0 | 0 | 0 | 0 | 0 | 0 | 1.31 | 549 |
| `zset_compact` | 3600 | 4008 | 0.00% | 0 | 0 | 0 | 0 | 0 | 936 | 1.11 | 471 |
| `non_negative` | 3600 | 3066 | 0.00% | 0 | 0 | 0 | 0 | 12.21 | 1386 | 0.85 | 0 |

*Extra-datapoint compaction ratios (`zset` -> `zset_compact`)*: In A3 (`5s, 15% OoO`), total emitted datapoints drop `13.52%` (`4571 -> 3953`, `618` fewer points), and extra datapoints above the `3600` input count drop `63.65%` (`971 -> 353`). Across Scenarios A1-E, total emitted datapoints drop `5.74%-15.30%`, and extra datapoints drop `59.36%-93.85%`.

## Scenario B: Rotating `StartTimestamp` (`InitialValueAuto`), 5s scrape, 15% OoO (15 series x 120 = 1,800 inputs)
| Mode | Inputs | Emitted | ErrDrop% | SpkT | SpkP | Inv | GlobalErr | BucketMAE | ZSetMM | Amp | Neg# |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `off` | 1800 | 1608 | 10.67% | 202 | 0 | 17 | 9694 | 15.77 | 927 | 0.89 | 0 |
| `zset` | 1800 | 2215 | 0.00% | 0 | 0 | 0 | 0 | 0 | 0 | 1.23 | 206 |
| `zset_compact` | 1800 | 1967 | 0.00% | 0 | 0 | 0 | 0 | 0 | 384 | 1.09 | 188 |
| `non_negative` | 1800 | 1591 | 0.00% | 0 | 0 | 0 | 0 | 10.39 | 586 | 0.88 | 0 |

## Scenario C: Same-Timestamp Duplicates (8%) & Upward/Downward Revisions (5%), 15% OoO (919 inputs)
| Mode | Inputs | Emitted | ErrDrop% | SpkT | SpkP | Inv | GlobalErr | BucketMAE | ZSetMM | Amp | Neg# |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `off` | 919 | 779 | 15.23% | 86 | 28 | 14 | 5651 | 31.13 | 517 | 0.85 | 0 |
| `zset` | 919 | 1098 | 0.00% | 0 | 0 | 0 | 0 | 0 | 0 | 1.19 | 147 |
| `zset_compact` | 919 | 930 | 0.00% | 0 | 0 | 0 | 0 | 0 | 188 | 1.01 | 111 |
| `non_negative` | 919 | 726 | 1.96% | 0 | 0 | 0 | 0 | 10.96 | 286 | 0.79 | 0 |

*Note*: `1.96%` in `non_negative` is intentional `rejected_revision` (18 net-negative same-timestamp revisions rejected without mutating history so `H_accepted` stays conserved at `GlobalErr = 0`).

## Scenario D: Float64 High-Dynamic-Range (`~1e8` scale), 15% OoO (1,000 inputs)
| Mode | Inputs | Emitted | ErrDrop% | SpkT | SpkP | Inv | GlobalErr | BucketMAE | ZSetMM | Amp | Neg# |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `off` | 1000 | 861 | 13.90% | 116 | 3 | 21 | 8.093e+08 | 2.161e+06 | 561 | 0.86 | 0 |
| `zset` | 1000 | 1251 | 0.00% | 0 | 0 | 0 | 9.537e-07 | 0 | 0 | 1.25 | 124 |
| `zset_compact` | 1000 | 1102 | 0.00% | 0 | 0 | 0 | 9.537e-07 | 1.863e-12 | 218 | 1.10 | 113 |
| `non_negative` | 1000 | 876 | 0.00% | 0 | 0 | 0 | 1.907e-06 | 1.163e+06 | 346 | 0.88 | 0 |

*Note*: `evaluation_test.go` sorts `acceptedFlt` map keys by `(series, startTS)` before summing `oracleTotalFloat`, making Scenario D bit-deterministic across 3 consecutive runs (`9.537e-07`, `9.537e-07`, `1.907e-06` identical on all 3 runs). On the exact `oracleTotalFloat` of `4.374922e+09` (`2^32 <= x < 2^33`), a single binary64 ULP is `2^-20 = 9.537e-07` (`2.18e-16` relative): `zset` and `zset_compact` match to **1 ULP** (`2.18e-16` relative), and `non_negative` matches to **2 ULPs** (`4.36e-16` relative).

## Scenario E: `InitialValueDrop` Absorbed-Counter Verification (10 series x 80 = 800 inputs, 15% OoO)
| Mode | Inputs | Emitted | ErrDrop% | SpkT | SpkP | Inv | GlobalErr | BucketMAE | ZSetMM | Amp | Neg# |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `off` | 800 | 681 | 13.62% | 85 | 4 | 14 | 6842 | 19.74 | 425 | 0.85 | 0 |
| `zset` | 800 | 976 | 0.00% | 0 | 0 | 0 | 0 | 0 | 0 | 1.22 | 92 |
| `zset_compact` | 800 | 852 | 0.00% | 0 | 0 | 0 | 0 | 0 | 168 | 1.06 | 76 |
| `non_negative` | 800 | 700 | 0.00% | 0 | 0 | 0 | 0 | 8.613 | 252 | 0.88 | 0 |

*Note*: Confirms `res.absorbed > 0` stays accurate in `evaluation_test.go` even after all 10 series record `initial` drops under `InitialValueDrop` (`nnRes.benignInitial == 10`).

## Downstream Compatibility Matrix (`TestDownstreamCompatibility`)
Trace: 10 series x 100 pts, 15% OoO <= 10s jitter, shared identity (`StartTimestamp == 0`, `InitialValueKeep`), Oracle Total = `44485`.

| Mode | Output `IsMonotonic` | DBSP Z-Set Err | Additive TSDB Err | `deltatocumulative` Accepted | `ErrOlderStart` Drops | `ErrOutOfOrder` Drops | `deltatocumulative` Err |
|---|---:|---:|---:|---:|---:|---:|---:|
| `off` | `true` | +7794 | +7794 | 837 | 0 | 13 | **+7794 (+17.52%)** |
| `zset` | `false` | **0** | **0** | 868 | 0 | 394 | **-193 (-0.43%)** |
| `zset_compact` | `false` | **0** | **0** | 868 | 0 | 231 | **-193 (-0.43%)** |
| `non_negative` | `true` | **0** | **0** | 868 | 0 | **3** | **-193 (-0.43%)** |

Subcase `InitialValueDrop_HeadPrependTriggersErrOlderStart`: under `InitialValueDrop`, arrival `(t=20, 100)` (initial dropped), `(t=30, 200)` (emits `(20,30]:+100` -> `stateStart=20`), followed by late head-prepend `(t=10, 50)` (emits `(10,20]:+50` with `start=10 < stateStart=20`) asserts `accepted == 1`, `ErrOlderStart == 1`, and `ErrOutOfOrder == 0` across all 3 retraction modes (`zset`, `zset_compact`, `non_negative`), verifying `ErrOlderStart` executes before `ErrOutOfOrder`.

Key finding: Even when fed into legacy `deltatocumulativeprocessor` (which drops all points with `ts <= lastTS`), **all 3 retraction modes cut cumulative error by 40.38x** (`+7794` -> `-193`) because preventing baseline's `prevPoint` rewind stops the false double-count spikes. `non_negative` triggers only `3` `ErrOutOfOrder` drops (the 3 late pre-reset recoveries) vs `394` (`zset`) and `231` (`zset_compact`).
