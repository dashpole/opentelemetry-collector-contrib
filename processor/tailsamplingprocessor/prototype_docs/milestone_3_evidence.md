# Milestone 3 Verification Evidence (Gate 2 Converged)

- **Worktree**: `<repo-root>`
- **Base Commit**: `db80cc0daf0` (`upstream/main`)
- **Runtime & Machine**: `go1.27.3 linux/amd64`, 24-core Intel Xeon @ 2.20GHz, 94 GiB RAM, `GOGC=100` (unless noted)
- **Harness**: `processor/tailsamplingprocessor/ivm_bench_test.go` (`TestIVMBakeoffBenchmarkStreamParity` + `TestIVMBakeoffSubprocess`)
- **Raw Artifacts**: `m3_bakeoff_batch1.jsonl` (41 runs), `m3_bakeoff_batch2.jsonl` (40 runs), `benchstat_cpu.txt`, `benchstat_mem.txt`
- **Total Isolated Serial Subprocess Runs**: `81` (`0` post-shutdown mmap/slab leaks across all 81 runs; `0` `trace_dropped_too_early` on all 79 `trace-complete` and IVM runs; non-zero `trace_dropped_too_early` on both upstream `span-ingest` runs)
- **`go.mod` / `go.sum` Changes**: `0` files modified

## Protocol Summary & Unit Conventions
- Every sustained W4 run executes strictly serially in a fresh OS subprocess (`./tailsampling_head.test`) with `GOGC=100`, `DecisionWait=10s`, `NumTraces=250,000`, `DecisionCache={Sampled: 100,000, NonSampled: 200,000}`, 4 concurrent producer workers emitting 3 staggered out-of-order phase batches per trace (`t+0s`, `t+1s`, `t+2s`) with bijective `splitmix64` TraceIDs, `10s` warmup window + `10s` measurement window with **no forced `runtime.GC()`** during warmup or measurement.
- Live memory and GC metrics are sampled every 250ms using STW-free `runtime/metrics` (`/memory/classes/heap/objects:bytes`, `/gc/heap/objects:objects`, `/gc/scan/heap:bytes`, `/gc/pauses:seconds`, `/cpu/classes/gc/*`) and `/proc/self/status` (`VmRSS`, `VmHWM`), with a lock-free atomic histogram for `sampling_decision_timer_latency`. `runtime.ReadMemStats` is called only once *after* the measurement window closes for the post-GC retained snapshot.
- **Units & Harness Accounting**: Tables 1-5 report decimal megabytes (`1 MB = 10^6 bytes`), while Section 7 (`benchstat`) reports Go's binary units (`Ki`/`Mi`). `Total Active CPU` in Section 4 is `User CPU + GC CPU` (not `/cpu/classes/total:cpu-seconds`, which includes idle cores). Both `User CPU` and `Window TotalAlloc` include the in-process 4-worker load generator (`template.CopyTo`, allocating `~630 MB` per 10s at `100k spans/sec` on `mixed` and `~860-890 MB` on 100-resource `resource-per-trace`/`ephemeral-unique-res` to simulate OTLP receiver unmarshaling, identical across all candidates); on `shards=4`, `ivm-*-mmap` `Window TotalAlloc` (`861.7-1,089.3 MB` across 100k shapes/workloads vs `712.8 MB` at `shards=1` on `W4-W1`) additionally includes processor-side `partitionTracesByShard` and sampled-trace unmarshaling allocations. Together, this transient allocation stream drives frequent GC cycles on small-Go-heap `mmap` runs under `GOGC=100`.
- `Zero-Unm` reports exact post-`Shutdown` ratio `(total_dropped_without_unmarshal + total_early_dropped + total_early_not_sampled) / (total_decided_not_sampled + total_decided_dropped)` with zero mid-tick skew.
- `*` On `span-ingest`, upstream `processor.go:529` records per-trace immediate decision latency (`0 ms`) onto `sampling_decision_timer_latency` on every incoming trace batch (`487,565` records on W4-W1, `362,238` on W4-W2 vs `36-38` 1s ticks on `trace-complete` and `ivm-*`), drowning out tick-level samples so its `Tick p99 = 0 ms*` is not comparable to tick-only latency.

## 1. W4-W1 Sustained Production Short-Circuit (`mixed`, `num_shards=4`, `DecisionWait=10s`)

### 1a. `100k spans/sec` (`1M` live spans, `1%` error, `3%` cross-batch latency, `1%` probabilistic)
| Candidate | Achieved sps | Peak Heap (vs base) | Steady VmRSS (vs base) | VmHWM | Steady GC Scan (vs base) | GC CPU (vs base) | GC Cycles | GC Pause p50/p99 | Ingest p99 | Tick p99 | Frag | Zero-Unm |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `trace-complete` | 100,058 | 2166.0 MB | 1940.9 MB | 2357.2 MB | 1176.2 MB | 7.93 s | 2 | 0.33 / 0.79 ms | 10474 us | 30 ms | n/a | 0.0% |
| `span-ingest` | 100,065 | 2276.7 MB (+5.1%) | 2176.2 MB (+12.1%) | 2461.6 MB | 1114.6 MB (-5.2%) | 10.83 s (+36.6%) | 3 | 0.26 / 0.46 ms | 7827 us | 0 ms* | n/a | 0.0% |
| `ivm-raw-slab` | 100,059 | 1296.7 MB (-40.1%) | 1234.2 MB (-36.4%) | 1369.6 MB | 137.0 MB (-88.4%) | 0.06 s (-99.3%) | 2 | 0.23 / 0.33 ms | 746 us | 8 ms | 1.09x | 100.0% |
| `ivm-interned-heap` | 100,059 | 826.0 MB (-61.9%) | 826.0 MB (-57.4%) | 891.7 MB | 106.3 MB (-91.0%) | 0.08 s (-99.0%) | 3 | 0.33 / 0.66 ms | 781 us | 12 ms | 1.10x | 100.0% |
| `ivm-interned-mmap` | 100,063 | 119.2 MB (-94.5%) | 500.1 MB (-74.2%) | 524.9 MB | 26.6 MB (-97.7%) | 0.49 s (-93.8%) | 19 | 0.39 / 0.66 ms | 1056 us | 27 ms | 1.10x | 100.0% |
| `ivm-lz4-heap` | 100,061 | 434.5 MB (-79.9%) | 461.4 MB (-76.2%) | 488.2 MB | 63.7 MB (-94.6%) | 0.14 s (-98.2%) | 5 | 0.33 / 0.79 ms | 692 us | 14 ms | 1.15x | 100.0% |
| `ivm-lz4-mmap` | 100,064 | 115.1 MB (-94.7%) | 309.1 MB (-84.1%) | 327.6 MB | 24.9 MB (-97.9%) | 0.49 s (-93.8%) | 19 | 0.39 / 0.79 ms | 1138 us | 15 ms | 1.13x | 100.0% |
| `ivm-lz4-mmap (GOGC=400)` | 100,062 | 278.8 MB (-87.1%) | 463.8 MB (-76.1%) | 519.1 MB | 74.8 MB (-93.6%) | 0.15 s (-98.1%) | 5 | 0.39 / 0.92 ms | 1223 us | 18 ms | 1.13x | 100.0% |

### 1b. `200k spans/sec` (`2M` live spans, `1%` error)
| Candidate | Achieved sps | Peak Heap (vs base) | Steady VmRSS (vs base) | VmHWM | Steady GC Scan (vs base) | GC CPU (vs base) | GC Cycles | GC Pause p50/p99 | Ingest p99 | Tick p99 | Frag | Zero-Unm |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `trace-complete` | 200,672 | 4288.8 MB | 4019.1 MB | 4623.1 MB | 2401.4 MB | 21.60 s | 2 | 0.26 / 0.33 ms | 19107 us | 144 ms | n/a | 0.0% |
| `ivm-interned-heap` | 200,132 | 1604.3 MB (-62.6%) | 1602.2 MB (-60.1%) | 1736.9 MB | 202.1 MB (-91.6%) | 0.11 s (-99.5%) | 3 | 0.33 / 0.52 ms | 704 us | 26 ms | 1.09x | 100.0% |
| `ivm-interned-mmap` | 200,126 | 224.1 MB (-94.8%) | 956.8 MB (-76.2%) | 994.5 MB | 47.7 MB (-98.0%) | 0.70 s (-96.8%) | 19 | 0.33 / 3.15 ms | 655 us | 61 ms | 1.09x | 100.0% |
| `ivm-lz4-heap` | 200,114 | 845.2 MB (-80.3%) | 884.3 MB (-78.0%) | 919.9 MB | 133.2 MB (-94.5%) | 0.18 s (-99.2%) | 5 | 0.33 / 0.79 ms | 670 us | 26 ms | 1.13x | 100.0% |
| `ivm-lz4-mmap` | 200,125 | 214.6 MB (-95.0%) | 574.6 MB (-85.7%) | 609.1 MB | 47.4 MB (-98.0%) | 0.71 s (-96.7%) | 20 | 0.33 / 1.57 ms | 814 us | 36 ms | 1.12x | 100.0% |

### 1c. `100k spans/sec` (`1M` live spans, `5%` error)
| Candidate | Achieved sps | Peak Heap (vs base) | Steady VmRSS (vs base) | VmHWM | Steady GC Scan (vs base) | GC CPU (vs base) | GC Cycles | GC Pause p50/p99 | Ingest p99 | Tick p99 | Frag | Zero-Unm |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `trace-complete` | 100,059 | 2168.4 MB | 1941.7 MB | 2358.2 MB | 1177.1 MB | 8.29 s | 2 | 0.20 / 0.79 ms | 9364 us | 28 ms | n/a | 0.0% |
| `ivm-interned-heap` | 100,058 | 760.2 MB (-64.9%) | 779.9 MB (-59.8%) | 847.6 MB | 95.1 MB (-91.9%) | 0.12 s (-98.6%) | 4 | 0.16 / 0.66 ms | 657 us | 12 ms | 1.13x | 100.0% |
| `ivm-interned-mmap` | 100,061 | 116.2 MB (-94.6%) | 493.8 MB (-74.6%) | 515.3 MB | 27.1 MB (-97.7%) | 0.55 s (-93.4%) | 21 | 0.33 / 0.79 ms | 1081 us | 22 ms | 1.15x | 100.0% |
| `ivm-lz4-heap` | 100,061 | 413.1 MB (-80.9%) | 460.0 MB (-76.3%) | 482.3 MB | 62.2 MB (-94.7%) | 0.17 s (-97.9%) | 6 | 0.33 / 0.79 ms | 518 us | 10 ms | 1.14x | 100.0% |
| `ivm-lz4-mmap` | 100,061 | 111.1 MB (-94.9%) | 305.5 MB (-84.3%) | 324.2 MB | 25.8 MB (-97.8%) | 0.53 s (-93.6%) | 21 | 0.33 / 0.79 ms | 1367 us | 15 ms | 1.16x | 100.0% |

### 1d. `50k spans/sec` Scaling Curve Point (`0.5M` live spans, `1%` error)
| Candidate | Achieved sps | Peak Heap (vs base) | Steady VmRSS (vs base) | VmHWM | Steady GC Scan (vs base) | GC CPU (vs base) | GC Cycles | GC Pause p50/p99 | Ingest p99 | Tick p99 | Frag | Zero-Unm |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `trace-complete` | 50,027 | 1117.9 MB | 1051.8 MB | 1227.5 MB | 603.8 MB | 4.27 s | 2 | 0.16 / 0.79 ms | 3449 us | 24 ms | n/a | 0.0% |
| `ivm-interned-heap` | 50,026 | 392.1 MB (-64.9%) | 421.5 MB (-59.9%) | 446.5 MB | 49.0 MB (-91.9%) | 0.08 s (-98.0%) | 4 | 0.26 / 0.52 ms | 757 us | 6 ms | 1.14x | 100.0% |
| `ivm-lz4-heap` | 50,026 | 224.1 MB (-80.0%) | 253.7 MB (-75.9%) | 264.0 MB | 37.0 MB (-93.9%) | 0.17 s (-96.0%) | 5 | 0.39 / 3.15 ms | 1030 us | 6 ms | 1.19x | 100.0% |
| `ivm-lz4-mmap` | 50,025 | 65.6 MB (-94.1%) | 174.9 MB (-83.4%) | 186.6 MB | 15.7 MB (-97.4%) | 0.38 s (-91.0%) | 17 | 0.33 / 0.92 ms | 1873 us | 7 ms | 1.19x | 100.0% |

### 1e. `num_shards=1` & `splitResourceSpansByTrace` Bypass Ablation (`100k spans/sec`, `mixed`, `1%` error)

#### `num_shards=1` (compared against `trace-complete` `shards=1`)
| Candidate | Split Bypass | Achieved sps | Peak Heap (vs base) | Steady VmRSS (vs base) | VmHWM | Steady GC Scan (vs base) | GC CPU (vs base) | GC Cycles | GC Pause p50/p99 | Ingest p99 | Tick p99 | Frag | Zero-Unm |
| :--- | :---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `trace-complete` | false | 100,055 | 2202.9 MB | 2060.5 MB | 2387.0 MB | 1214.4 MB | 8.53 s | 2 | 0.20 / 0.52 ms | 9134 us | 178 ms | n/a | 0.0% |
| `ivm-interned-heap` | true | 99,972 | 778.5 MB (-64.7%) | 760.7 MB (-63.1%) | 832.4 MB | 97.0 MB (-92.0%) | 0.05 s (-99.4%) | 2 | 0.02 / 0.66 ms | 33 us | 61 ms | 1.05x | 100.0% |
| `ivm-interned-heap` | false | 100,056 | 760.7 MB (-65.5%) | 824.2 MB (-60.0%) | 855.8 MB | 124.3 MB (-89.8%) | 0.61 s (-92.8%) | 5 | 1.84 / 8.39 ms | 33322 us | 67 ms | 1.06x | 100.0% |
| `ivm-lz4-heap` | true | 100,059 | 433.2 MB (-80.3%) | 452.5 MB (-78.0%) | 479.4 MB | 66.0 MB (-94.6%) | 0.10 s (-98.8%) | 4 | 0.33 / 1.84 ms | 32 us | 57 ms | 1.08x | 100.0% |
| `ivm-lz4-mmap` | true | 100,060 | 108.2 MB (-95.1%) | 303.2 MB (-85.3%) | 334.0 MB | 26.8 MB (-97.8%) | 0.44 s (-94.9%) | 14 | 0.16 / 3.15 ms | 32 us | 72 ms | 1.08x | 100.0% |

#### `num_shards=4` Bypass Ablation (compared against `trace-complete` `shards=4`)
| Candidate | Split Bypass | Achieved sps | Peak Heap (vs base) | Steady VmRSS (vs base) | VmHWM | Steady GC Scan (vs base) | GC CPU (vs base) | GC Cycles | GC Pause p50/p99 | Ingest p99 | Tick p99 | Frag | Zero-Unm |
| :--- | :---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `trace-complete` | false | 100,058 | 2166.0 MB | 1940.9 MB | 2357.2 MB | 1176.2 MB | 7.93 s | 2 | 0.33 / 0.79 ms | 10474 us | 30 ms | n/a | 0.0% |
| `ivm-interned-heap` | true | 100,059 | 826.0 MB (-61.9%) | 826.0 MB (-57.4%) | 891.7 MB | 106.3 MB (-91.0%) | 0.08 s (-99.0%) | 3 | 0.33 / 0.66 ms | 781 us | 12 ms | 1.10x | 100.0% |
| `ivm-interned-heap` | false | 100,064 | 780.9 MB (-63.9%) | 833.6 MB (-57.1%) | 864.9 MB | 123.8 MB (-89.5%) | 0.18 s (-97.8%) | 5 | 0.46 / 0.92 ms | 3152 us | 12 ms | 1.11x | 100.0% |
| `ivm-lz4-heap` | true | 100,061 | 434.5 MB (-79.9%) | 461.4 MB (-76.2%) | 488.2 MB | 63.7 MB (-94.6%) | 0.14 s (-98.2%) | 5 | 0.33 / 0.79 ms | 692 us | 14 ms | 1.15x | 100.0% |
| `ivm-lz4-heap` | false | 100,056 | 447.7 MB (-79.3%) | 465.4 MB (-76.0%) | 500.0 MB | 78.2 MB (-93.4%) | 0.24 s (-97.0%) | 9 | 0.33 / 0.66 ms | 1965 us | 14 ms | 1.14x | 100.0% |

## 2. W4-W2 Sustained Early Eject (`35%` `/healthz` drop + `2%` error + cross-batch `AND` + `3%` latency, `mixed`, `shards=4`)

### 2a. `100k spans/sec` (`1M` live spans pre-drop)
| Candidate | Achieved sps | Peak Heap (vs base) | Steady VmRSS (vs base) | VmHWM | Steady GC Scan (vs base) | GC CPU (vs base) | GC Cycles | GC Pause p50/p99 | Ingest p99 | Tick p99 | Frag | Zero-Unm |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `trace-complete` | 99,792 | 2233.7 MB | 1938.3 MB | 2416.9 MB | 1205.5 MB | 9.07 s | 2 | 0.46 / 1.31 ms | 11908 us | 98 ms | n/a | 0.0% |
| `span-ingest` | 99,999 | 1395.4 MB (-37.5%) | 1417.7 MB (-26.9%) | 1543.1 MB | 764.5 MB (-36.6%) | 7.90 s (-12.9%) | 3 | 0.46 / 0.66 ms | 2959 us | 0 ms* | n/a | 0.0% |
| `ivm-raw-slab` | 99,725 | 932.3 MB (-58.3%) | 920.8 MB (-52.5%) | 997.9 MB | 114.8 MB (-90.5%) | 0.12 s (-98.6%) | 3 | 0.20 / 0.79 ms | 761 us | 27 ms | 1.17x | 100.0% |
| `ivm-interned-heap` | 100,066 | 598.3 MB (-73.2%) | 610.4 MB (-68.5%) | 654.6 MB | 80.5 MB (-93.3%) | 0.17 s (-98.1%) | 5 | 0.39 / 0.66 ms | 715 us | 26 ms | 1.18x | 100.0% |
| `ivm-interned-mmap` | 99,724 | 99.5 MB (-95.5%) | 386.7 MB (-80.1%) | 404.5 MB | 26.1 MB (-97.8%) | 0.85 s (-90.6%) | 24 | 0.33 / 3.15 ms | 1064 us | 37 ms | 1.17x | 100.0% |
| `ivm-lz4-heap` | 100,057 | 321.9 MB (-85.6%) | 353.1 MB (-81.8%) | 371.9 MB | 56.4 MB (-95.3%) | 0.24 s (-97.4%) | 7 | 0.33 / 0.66 ms | 746 us | 30 ms | 1.23x | 100.0% |
| `ivm-lz4-mmap` | 100,065 | 98.9 MB (-95.6%) | 244.1 MB (-87.4%) | 257.2 MB | 24.6 MB (-98.0%) | 0.88 s (-90.3%) | 24 | 0.33 / 4.19 ms | 1276 us | 32 ms | 1.19x | 100.0% |

*Live parity & telemetry proof on W4-W2 `100k`*: `trace-complete` emitted `53,980` sampled spans (`3,296` sampled traces, `0` `dropped_too_early`); `ivm-lz4-heap` emitted exact `53,980` sampled spans (`3,296` sampled traces, `0` `dropped_too_early`); upstream `span-ingest` emitted **`0` sampled spans** (`0` sampled traces, silently dropping 100% of non-dropped traces) and recorded **`29,260` false `trace_dropped_too_early`** errors.

### 2b. `200k spans/sec` (`2M` live spans pre-drop)
| Candidate | Achieved sps | Peak Heap (vs base) | Steady VmRSS (vs base) | VmHWM | Steady GC Scan (vs base) | GC CPU (vs base) | GC Cycles | GC Pause p50/p99 | Ingest p99 | Tick p99 | Frag | Zero-Unm |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `trace-complete` | 199,517 | 4514.9 MB | 4084.5 MB | 4889.2 MB | 2396.7 MB | 20.11 s | 2 | 0.23 / 0.46 ms | 19581 us | 262 ms | n/a | 0.0% |
| `ivm-interned-heap` | 199,775 | 1161.6 MB (-74.3%) | 1197.6 MB (-70.7%) | 1265.0 MB | 164.0 MB (-93.2%) | 0.28 s (-98.6%) | 5 | 0.13 / 0.66 ms | 728 us | 52 ms | 1.19x | 100.0% |
| `ivm-interned-mmap` | 199,598 | 199.2 MB (-95.6%) | 741.0 MB (-81.9%) | 793.7 MB | 50.1 MB (-97.9%) | 1.62 s (-91.9%) | 25 | 0.33 / 2.62 ms | 3255 us | 82 ms | 1.18x | 100.0% |
| `ivm-lz4-heap` | 199,599 | 676.8 MB (-85.0%) | 700.9 MB (-82.8%) | 741.2 MB | 107.3 MB (-95.5%) | 0.43 s (-97.9%) | 7 | 0.20 / 0.79 ms | 676 us | 68 ms | 1.18x | 100.0% |
| `ivm-lz4-mmap` | 199,589 | 192.3 MB (-95.7%) | 451.7 MB (-88.9%) | 478.2 MB | 48.0 MB (-98.0%) | 1.29 s (-93.6%) | 25 | 0.33 / 0.66 ms | 773 us | 63 ms | 1.18x | 100.0% |

## 3. W4-W3 Sustained Worst Case (`0%` Short-Circuit, `100k spans/sec x 10s = 1M` Live Pending Spans, `shards=4`)

### 3 (mixed): `0` arena unmarshals across 100% of expired traces
| Candidate | Achieved sps | Peak Heap (vs base) | Steady VmRSS (vs base) | VmHWM | Steady GC Scan (vs base) | GC CPU (vs base) | GC Cycles | GC Pause p50/p99 | Ingest p99 | Tick p99 | Frag | Zero-Unm |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `trace-complete` | 100,062 | 2102.1 MB | 1932.0 MB | 2297.1 MB | 1174.2 MB | 11.24 s | 2 | 0.39 / 0.92 ms | 16657 us | 27 ms | n/a | 0.0% |
| `ivm-raw-slab` | 100,059 | 1360.8 MB (-35.3%) | 1262.9 MB (-34.6%) | 1449.0 MB | 122.3 MB (-89.6%) | 0.09 s (-99.2%) | 2 | 0.10 / 1.57 ms | 1175 us | 12 ms | 1.06x | 100.0% |
| `ivm-interned-heap` | 100,060 | 817.9 MB (-61.1%) | 822.0 MB (-57.5%) | 882.4 MB | 103.8 MB (-91.2%) | 0.08 s (-99.3%) | 3 | 0.26 / 0.79 ms | 751 us | 13 ms | 1.08x | 100.0% |
| `ivm-interned-mmap` | 100,064 | 111.3 MB (-94.7%) | 504.8 MB (-73.9%) | 525.8 MB | 25.7 MB (-97.8%) | 0.44 s (-96.1%) | 17 | 0.33 / 3.67 ms | 1277 us | 31 ms | 1.07x | 100.0% |
| `ivm-lz4-heap` | 100,059 | 443.2 MB (-78.9%) | 467.2 MB (-75.8%) | 500.8 MB | 68.2 MB (-94.2%) | 0.11 s (-99.0%) | 5 | 0.33 / 0.39 ms | 649 us | 16 ms | 1.13x | 100.0% |
| `ivm-lz4-mmap` | 100,040 | 113.6 MB (-94.6%) | 309.3 MB (-84.0%) | 326.2 MB | 24.9 MB (-97.9%) | 0.50 s (-95.5%) | 17 | 0.39 / 2.62 ms | 1242 us | 19 ms | 1.11x | 100.0% |

### 3 (sdk): `0` arena unmarshals across 100% of expired traces
| Candidate | Achieved sps | Peak Heap (vs base) | Steady VmRSS (vs base) | VmHWM | Steady GC Scan (vs base) | GC CPU (vs base) | GC Cycles | GC Pause p50/p99 | Ingest p99 | Tick p99 | Frag | Zero-Unm |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `trace-complete` | 100,037 | 1518.2 MB | 1478.3 MB | 1664.2 MB | 830.4 MB | 3.53 s | 1 | 0.20 / 0.92 ms | 2472 us | 34 ms | n/a | 0.0% |
| `ivm-interned-heap` | 100,056 | 808.8 MB (-46.7%) | 810.1 MB (-45.2%) | 881.7 MB | 100.4 MB (-87.9%) | 0.09 s (-97.4%) | 3 | 0.33 / 1.05 ms | 720 us | 9 ms | 1.11x | 100.0% |
| `ivm-interned-mmap` | 100,058 | 80.4 MB (-94.7%) | 477.9 MB (-67.7%) | 501.7 MB | 20.8 MB (-97.5%) | 0.58 s (-83.7%) | 22 | 0.33 / 3.15 ms | 1356 us | 26 ms | 1.08x | 100.0% |
| `ivm-lz4-heap` | 100,056 | 219.0 MB (-85.6%) | 255.1 MB (-82.7%) | 266.0 MB | 39.0 MB (-95.3%) | 0.24 s (-93.1%) | 9 | 0.46 / 0.66 ms | 912 us | 12 ms | 1.14x | 100.0% |
| `ivm-lz4-mmap` | 100,059 | 84.1 MB (-94.5%) | 187.3 MB (-87.3%) | 200.6 MB | 21.0 MB (-97.5%) | 0.56 s (-84.2%) | 22 | 0.46 / 0.92 ms | 1177 us | 10 ms | 1.15x | 100.0% |

### 3 (resource-per-trace): `0` arena unmarshals across 100% of expired traces
| Candidate | Achieved sps | Peak Heap (vs base) | Steady VmRSS (vs base) | VmHWM | Steady GC Scan (vs base) | GC CPU (vs base) | GC Cycles | GC Pause p50/p99 | Ingest p99 | Tick p99 | Frag | Zero-Unm |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `trace-complete` | 100,064 | 1507.5 MB | 1413.5 MB | 1652.4 MB | 822.3 MB | 2.83 s | 1 | 0.01 / 0.46 ms | 773 us | 38 ms | n/a | 0.0% |
| `ivm-interned-heap` | 100,064 | 814.7 MB (-46.0%) | 809.2 MB (-42.8%) | 878.6 MB | 113.9 MB (-86.1%) | 0.09 s (-96.9%) | 3 | 0.23 / 0.46 ms | 120 us | 10 ms | 1.08x | 100.0% |
| `ivm-interned-mmap` | 100,060 | 79.9 MB (-94.7%) | 479.8 MB (-66.1%) | 502.3 MB | 22.4 MB (-97.3%) | 0.57 s (-79.8%) | 21 | 0.33 / 0.79 ms | 170 us | 23 ms | 1.10x | 100.0% |
| `ivm-lz4-heap` | 100,061 | 224.5 MB (-85.1%) | 257.9 MB (-81.8%) | 271.0 MB | 44.6 MB (-94.6%) | 0.24 s (-91.4%) | 9 | 0.33 / 0.66 ms | 114 us | 9 ms | 1.11x | 100.0% |
| `ivm-lz4-mmap` | 100,061 | 89.6 MB (-94.1%) | 190.2 MB (-86.5%) | 200.8 MB | 22.7 MB (-97.2%) | 0.67 s (-76.2%) | 20 | 0.46 / 1.05 ms | 182 us | 11 ms | 1.12x | 100.0% |

### 3 (few-traces-per-resource): `0` arena unmarshals across 100% of expired traces
| Candidate | Achieved sps | Peak Heap (vs base) | Steady VmRSS (vs base) | VmHWM | Steady GC Scan (vs base) | GC CPU (vs base) | GC Cycles | GC Pause p50/p99 | Ingest p99 | Tick p99 | Frag | Zero-Unm |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `trace-complete` | 100,055 | 1461.3 MB | 1441.5 MB | 1625.1 MB | 836.1 MB | 2.97 s | 1 | 0.02 / 0.20 ms | 2601 us | 45 ms | n/a | 0.0% |
| `ivm-interned-heap` | 100,060 | 761.7 MB (-47.9%) | 794.4 MB (-44.9%) | 844.6 MB | 93.9 MB (-88.8%) | 0.08 s (-97.4%) | 3 | 0.01 / 1.57 ms | 4944 us | 13 ms | 1.08x | 100.0% |
| `ivm-lz4-heap` | 100,055 | 223.1 MB (-84.7%) | 257.2 MB (-82.2%) | 268.6 MB | 45.1 MB (-94.6%) | 0.28 s (-90.7%) | 10 | 0.39 / 0.79 ms | 1165 us | 8 ms | 1.14x | 100.0% |
| `ivm-lz4-mmap` | 100,056 | 92.6 MB (-93.7%) | 188.2 MB (-86.9%) | 198.0 MB | 23.6 MB (-97.2%) | 0.74 s (-75.1%) | 25 | 0.39 / 2.10 ms | 2018 us | 10 ms | 1.14x | 100.0% |

### 3 (ephemeral-unique-res): `0` arena unmarshals across 100% of expired traces
| Candidate | Achieved sps | Peak Heap (vs base) | Steady VmRSS (vs base) | VmHWM | Steady GC Scan (vs base) | GC CPU (vs base) | GC Cycles | GC Pause p50/p99 | Ingest p99 | Tick p99 | Frag | Zero-Unm |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `trace-complete` | 100,060 | 1545.2 MB | 1363.2 MB | 1700.3 MB | 820.7 MB | 6.37 s | 2 | 0.02 / 0.92 ms | 792 us | 35 ms | n/a | 0.0% |
| `ivm-interned-heap` | 100,059 | 904.6 MB (-41.5%) | 887.8 MB (-34.9%) | 972.3 MB | 114.0 MB (-86.1%) | 0.19 s (-97.0%) | 3 | 0.66 / 0.92 ms | 297 us | 20 ms | 1.09x | 100.0% |
| `ivm-interned-mmap` | 100,056 | 100.8 MB (-93.5%) | 543.1 MB (-60.2%) | 565.0 MB | 25.3 MB (-96.9%) | 0.69 s (-89.2%) | 19 | 0.46 / 1.57 ms | 338 us | 44 ms | 1.10x | 100.0% |
| `ivm-lz4-heap` | 100,061 | 291.4 MB (-81.1%) | 341.9 MB (-74.9%) | 356.5 MB | 52.7 MB (-93.6%) | 0.28 s (-95.6%) | 7 | 0.46 / 0.92 ms | 172 us | 13 ms | 1.12x | 100.0% |
| `ivm-lz4-mmap` | 100,064 | 106.9 MB (-93.1%) | 241.0 MB (-82.3%) | 253.1 MB | 26.5 MB (-96.8%) | 0.79 s (-87.5%) | 18 | 0.66 / 2.62 ms | 263 us | 21 ms | 1.12x | 100.0% |

## 4. Post-GC Retained Heap, Live HeapObjects, & Active CPU Breakdown (Key High-Scale Runs)

| Run | Retained Heap (post-GC) | Retained HeapObjects | Window TotalAlloc | User CPU | GC CPU | GC Pause CPU | Total Active CPU | Arena Mmap | Compactions |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `W4-W1-100k-e1-s4/trace-complete` | 1059.5 MB | 16,396,530 | 1722.7 MB | 8.32 s | 7.93 s | 0.025 s | 16.25 s | 0.0 MB | 0 |
| `W4-W1-100k-e1-s4/ivm-interned-heap` | 391.0 MB | 92,870 | 1339.0 MB | 4.01 s | 0.08 s | 0.024 s | 4.08 s | 0.0 MB | 341 |
| `W4-W1-100k-e1-s4/ivm-lz4-heap` | 213.4 MB | 92,244 | 1143.8 MB | 5.73 s | 0.14 s | 0.054 s | 5.87 s | 0.0 MB | 45 |
| `W4-W1-100k-e1-s4/ivm-lz4-mmap` | 58.1 MB | 91,816 | 981.2 MB | 6.75 s | 0.49 s | 0.185 s | 7.25 s | 171.2 MB | 43 |
| `W4-W1-200k-e1-s4/trace-complete` | 2128.6 MB | 32,934,664 | 3458.1 MB | 25.30 s | 21.60 s | 0.009 s | 46.89 s | 0.0 MB | 0 |
| `W4-W1-200k-e1-s4/ivm-interned-heap` | 775.5 MB | 181,994 | 2671.4 MB | 8.49 s | 0.11 s | 0.020 s | 8.60 s | 0.0 MB | 690 |
| `W4-W1-200k-e1-s4/ivm-lz4-heap` | 422.0 MB | 180,711 | 2280.1 MB | 11.10 s | 0.18 s | 0.043 s | 11.28 s | 0.0 MB | 91 |
| `W4-W1-200k-e1-s4/ivm-lz4-mmap` | 113.1 MB | 179,681 | 1960.7 MB | 13.51 s | 0.71 s | 0.203 s | 14.23 s | 341.0 MB | 93 |
| `W4-W2-200k-s4/trace-complete` | 2114.9 MB | 32,695,991 | 3414.9 MB | 22.89 s | 20.11 s | 0.017 s | 43.00 s | 0.0 MB | 0 |
| `W4-W2-200k-s4/ivm-interned-heap` | 574.9 MB | 214,947 | 2561.6 MB | 10.51 s | 0.28 s | 0.033 s | 10.79 s | 0.0 MB | 419 |
| `W4-W2-200k-s4/ivm-lz4-heap` | 309.8 MB | 213,967 | 2262.4 MB | 11.69 s | 0.43 s | 0.056 s | 12.11 s | 0.0 MB | 23 |
| `W4-W2-200k-s4/ivm-lz4-mmap` | 91.2 MB | 215,795 | 2040.7 MB | 12.73 s | 1.29 s | 0.203 s | 14.02 s | 239.1 MB | 32 |
| `W4-W3-100k-mixed-s4/trace-complete` | 1059.3 MB | 16,396,445 | 1722.4 MB | 10.21 s | 11.24 s | 0.036 s | 21.45 s | 0.0 MB | 0 |
| `W4-W3-100k-mixed-s4/ivm-lz4-heap` | 216.7 MB | 88,662 | 1060.9 MB | 5.83 s | 0.11 s | 0.038 s | 5.95 s | 0.0 MB | 23 |
| `W4-W3-100k-mixed-s4/ivm-lz4-mmap` | 57.4 MB | 88,184 | 898.0 MB | 7.05 s | 0.50 s | 0.248 s | 7.55 s | 174.6 MB | 24 |
| `W4-W3-100k-ephemeral-unique-res-s4/trace-complete` | 801.5 MB | 11,761,474 | 1009.6 MB | 7.64 s | 6.37 s | 0.024 s | 14.00 s | 0.0 MB | 0 |
| `W4-W3-100k-ephemeral-unique-res-s4/ivm-lz4-heap` | 150.8 MB | 88,789 | 991.2 MB | 8.48 s | 0.28 s | 0.088 s | 8.76 s | 0.0 MB | 12 |
| `W4-W3-100k-ephemeral-unique-res-s4/ivm-lz4-mmap` | 51.1 MB | 88,171 | 888.7 MB | 9.42 s | 0.79 s | 0.308 s | 10.21 s | 110.3 MB | 12 |

## 5. Unpaced Maximum Sustainable Throughput (`DecisionWait=2s`, `BlockOnOverflow=true`, `mixed`, `5s` measure)

*Interpretation note*: Unpaced runs sustain very different throughputs (`158k-949k sps` vs `200k-467k sps` baseline), so memory `% vs base` here is throughput-coupled; use Sections 1-3 for throughput-matched memory comparisons. At `num_shards=1` (`5a`), `ivm-lz4-heap` (`175k sps`) trails `trace-complete` (`200k sps`) because LZ4 compression serializes on the single shard goroutine while `trace-complete` distributes `splitResourceSpansByTrace` across all 4 producer goroutines; once `num_shards=4` (`5b`, `5c`) parallelizes shard work, `ivm-lz4-heap` leads `trace-complete` by `1.28x` on W1 (`598k` vs `467k sps`) and `1.55x` on W2 (`662k` vs `426k sps`), while `ivm-interned-heap` reaches `851k-949k sps` (`1.82x-2.23x`).

### 5a. W1 (`1%` error), `num_shards=1`
| Candidate | Split Bypass | Achieved sps | Peak Heap (vs base) | Steady VmRSS (vs base) | VmHWM | Steady GC Scan (vs base) | GC CPU (vs base) | GC Cycles | GC Pause p50/p99 | Ingest p99 | Tick p99 | Frag | Zero-Unm |
| :--- | :---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `trace-complete` | false | 200,188 | 2758.9 MB | 2301.1 MB | 3060.6 MB | 1545.1 MB | 8.47 s | 1 | 0.08 / 0.39 ms | 19957 us | 817 ms | n/a | 0.0% |
| `ivm-interned-heap` | true | 245,589 | 472.3 MB (-82.9%) | 512.4 MB (-77.7%) | 552.8 MB | 61.3 MB (-96.0%) | 0.32 s (-96.2%) | 6 | 0.20 / 1.31 ms | 12139 us | 142 ms | 1.04x | 100.0% |
| `ivm-interned-heap` | false | 170,624 | 411.2 MB (-85.1%) | 406.2 MB (-82.3%) | 458.5 MB | 76.4 MB (-95.1%) | 0.66 s (-92.2%) | 10 | 0.33 / 0.79 ms | 17850 us | 75 ms | 1.05x | 100.0% |
| `ivm-lz4-heap` | true | 175,209 | 223.7 MB (-91.9%) | 252.6 MB (-89.0%) | 286.5 MB | 36.8 MB (-97.6%) | 0.32 s (-96.2%) | 8 | 0.33 / 0.46 ms | 15574 us | 89 ms | 1.05x | 100.0% |
| `ivm-lz4-mmap` | true | 157,688 | 75.4 MB (-97.3%) | 166.6 MB (-92.8%) | 179.3 MB | 22.8 MB (-98.5%) | 0.56 s (-93.4%) | 17 | 0.23 / 0.66 ms | 83664 us | 80 ms | 1.06x | 100.0% |

### 5b. W1 (`1%` error), `num_shards=4`
| Candidate | Split Bypass | Achieved sps | Peak Heap (vs base) | Steady VmRSS (vs base) | VmHWM | Steady GC Scan (vs base) | GC CPU (vs base) | GC Cycles | GC Pause p50/p99 | Ingest p99 | Tick p99 | Frag | Zero-Unm |
| :--- | :---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `trace-complete` | false | 467,346 | 2748.1 MB | 2518.2 MB | 2996.0 MB | 1390.4 MB | 23.53 s | 3 | 0.52 / 1.84 ms | 17355 us | 222 ms | n/a | 0.0% |
| `ivm-interned-heap` | true | 851,427 | 1848.6 MB (-32.7%) | 1649.6 MB (-34.5%) | 2088.6 MB | 220.5 MB (-84.1%) | 0.75 s (-96.8%) | 8 | 0.92 / 3.15 ms | 3416 us | 98 ms | 1.03x | 100.0% |
| `ivm-interned-heap` | false | 570,135 | 1200.9 MB (-56.3%) | 1164.9 MB (-53.7%) | 1395.7 MB | 188.3 MB (-86.5%) | 0.82 s (-96.5%) | 10 | 0.66 / 3.15 ms | 5285 us | 60 ms | 1.04x | 100.0% |
| `ivm-lz4-heap` | true | 598,222 | 731.2 MB (-73.4%) | 741.3 MB (-70.6%) | 829.2 MB | 116.0 MB (-91.7%) | 0.85 s (-96.4%) | 11 | 0.79 / 2.10 ms | 5511 us | 76 ms | 1.04x | 100.0% |
| `ivm-lz4-heap` | false | 419,036 | 469.1 MB (-82.9%) | 505.6 MB (-79.9%) | 584.7 MB | 81.7 MB (-94.1%) | 1.20 s (-94.9%) | 18 | 0.79 / 3.15 ms | 8140 us | 44 ms | 1.05x | 100.0% |
| `ivm-lz4-mmap` | true | 509,525 | 160.3 MB (-94.2%) | 386.8 MB (-84.6%) | 462.6 MB | 40.2 MB (-97.1%) | 2.02 s (-91.4%) | 37 | 0.66 / 2.62 ms | 6616 us | 77 ms | 1.05x | 100.0% |

### 5c. W2 (`35%` `/healthz` drop + cross-batch `AND`), `num_shards=4`
| Candidate | Achieved sps | Peak Heap (vs base) | Steady VmRSS (vs base) | VmHWM | Steady GC Scan (vs base) | GC CPU (vs base) | GC Cycles | GC Pause p50/p99 | Ingest p99 | Tick p99 | Frag | Zero-Unm |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `trace-complete` | 426,176 | 2350.4 MB | 2116.6 MB | 2531.7 MB | 1213.1 MB | 23.21 s | 4 | 0.46 / 2.62 ms | 16404 us | 283 ms | n/a | 0.0% |
| `ivm-interned-heap` | 949,338 | 1690.6 MB (-28.1%) | 1625.7 MB (-23.2%) | 1831.0 MB | 240.1 MB (-80.2%) | 0.97 s (-95.8%) | 9 | 0.66 / 2.62 ms | 2400 us | 243 ms | 1.09x | 100.0% |
| `ivm-lz4-heap` | 661,757 | 533.5 MB (-77.3%) | 595.1 MB (-71.9%) | 676.1 MB | 91.4 MB (-92.5%) | 1.27 s (-94.5%) | 15 | 0.79 / 1.31 ms | 3785 us | 176 ms | 1.10x | 100.0% |
| `ivm-lz4-mmap` | 602,311 | 198.4 MB (-91.6%) | 376.4 MB (-82.2%) | 446.3 MB | 52.5 MB (-95.7%) | 3.65 s (-84.3%) | 44 | 0.66 / 5.24 ms | 4748 us | 167 ms | 1.09x | 100.0% |

## 6. Correctness Gate (`TestIVMBakeoffBenchmarkStreamParity`, `-race`) & Full Package Verification

- `go test -race -count=1 -v -run '^TestIVMBakeoffBenchmarkStreamParity$' .`: **PASS** (`16.58s`, `0` mismatches across `w1_err1`, `w1_err5`, `w2` x `in_order` & `out_of_order` x all 5 IVM arena candidates at `shards=1` + `ivm-lz4-mmap` at `shards=4`, explicitly verifying positive `probabilistic(1%)` matches beyond deterministic error+latency counts, reversed cross-batch `AND`, late-phase `/healthz` `drop > sample` precedence freeing buffered phase-0 arena chunks, and proving upstream `span-ingest` fails parity on all 6 stream subtests).
- `go test -race -timeout=240s -count=1 ./...`: **PASS** (re-verified post-R1 fixes; all subpackages green; `goleak` clean).
- `go vet ./...`, `GOOS=darwin GOARCH=arm64 go build ./...`, `GOOS=windows GOARCH=amd64 go build ./...`, `GOOS=linux GOARCH=386 go build ./...`: **PASS** (exit 0).

## 7. Full-Package `benchstat` Regression Gate (`upstream/main` `db80cc0daf0` vs `HEAD`)

Interleaved 6 passes (`-count=6`) strictly serially on compiled binaries (`./tailsampling_base.test` vs `./tailsampling_head.test`):
- **CPU/Throughput (`-benchtime=500ms`)**: `sec/op` geomean `-2.93%`, `B/s` geomean `-2.19%`, `B/op` geomean `+0.02%`, `allocs/op` geomean `-0.00%`, `sampled-spans/op` `-0.55%`, `consume-calls/op` `-0.55%`. **0 statistically significant regressions** (`p >= 0.121` across every benchmark and metric).
- **Memory (`BenchmarkProcessorMemory`, `-benchtime=1x`)**: `retained-B/span` geomean **`-0.00%`** (`p >= 0.472`), `retained-B/trace` geomean **`-0.00%`** (`p >= 0.545`), exact match across all 4 shapes (`893.0 -> 892.9`, `723.6 -> 723.6`, `725.2 -> 725.2`, `725.2 -> 725.2`).

## 8. Milestone 3 Exit Gate Evaluation (`PROTOTYPE_PLAN.md` Section 5)

1. **Memory Gate (#1)**: **PASS (exceeded by wide margin)**.
   - W4-W1 (`100k`/`200k`, `1%` & `5%` error): `ivm-lz4-heap` achieves `-80.9%` to `-79.9%` peak `HeapAlloc` (`>= 50%` gate) and `-78.0%` to `-76.2%` steady `VmRSS` (`>= 40%` gate); `ivm-lz4-mmap` achieves `-95.0%` to `-94.7%` peak `HeapAlloc` and `-85.7%` to `-84.1%` steady `VmRSS`.
   - W4-W2 (`100k`/`200k`, `35%` drop + `AND`): `ivm-lz4-heap` achieves `-85.6%` to `-85.0%` peak `HeapAlloc` and `-82.8%` to `-81.8%` steady `VmRSS`; `ivm-lz4-mmap` achieves `-95.7%` to `-95.6%` peak `HeapAlloc` and `-88.9%` to `-87.4%` steady `VmRSS`.
   - W4-W3 (`0%` short-circuit, `1M` live spans across all 5 shapes): `ivm-lz4-heap` achieves `-82.7%` to `-74.9%` steady `VmRSS` (`>= 30%` gate, including `-74.9%` on `ephemeral-unique-res` and `-82.7%` on `sdk`); `ivm-lz4-mmap` achieves `-87.3%` to `-82.3%` steady `VmRSS`.
2. **GC Gate (#2)**: **PASS on scan heap & GC CPU; PARTIAL PASS (documented variance on literal `STW p99 <= Baseline A`)**.
   - Scannable heap (`/gc/scan/heap:bytes`): across all 12 paced W4 cells, `ivm-lz4-heap` reduces steady scannable heap by **`-95.5%` to `-93.6%`** (gate: `>= 50%`), and retained live `HeapObjects` at `200k` W1 by **`-99.45%`** (`32,934,664 -> 180,711`).
   - Concurrent GC CPU (`/cpu/classes/gc/total:cpu-seconds`): across all 12 paced W4 cells, `ivm-lz4-heap` reduces GC CPU by **`-99.2%` to `-90.7%`** (e.g. `21.60s -> 0.18s` at 200k W1, `20.11s -> 0.43s` at 200k W2; gate: `>= 50%`).
   - **STW pause p99 (`/gc/pauses:seconds`) full accounting**: Across all 12 paced W4 cells (`11` at `shards=4` + `1` at `shards=1`), literal `p99 <= baseline` holds on **7/12** cells for `ivm-interned-heap`, **6/12** for `ivm-lz4-heap`, and **3/12** for `ivm-lz4-mmap`. In Go 1.27, STW pause duration is already sub-millisecond on multi-GB heaps (`0.20-1.31 ms` across 1-2 GC cycles in 10s on baseline, `0.003-0.042s` pause CPU). `ivm-lz4-heap` stays sub-millisecond (`0.39-0.92 ms`) on 10/12 paced cells (`0.029-0.111s` pause CPU), with higher p99 outliers on `W4-W1 50k` (`3.15` vs `0.79 ms`) and `W4-W1 100k shards=1` (`1.84` vs `0.52 ms`), and on unpaced W1 (`0.46` vs `0.39 ms` at `shards=1`; `2.10` vs `1.84 ms` at `shards=4`; `3.15 ms` with split), while cutting unpaced W2 p99 by `-50%` (`2.62 -> 1.31 ms`). This variance is a direct `GOGC=100` sample-size and cycle-frequency effect: shrinking live Go heap triggers `4-10` cycles (`8-20` pauses, where `p99` equals the single worst pause) vs baseline's `1-2` cycles (`2-4` pauses). On off-heap `mmap` (`ivm-lz4-mmap`), hiding payload memory shrinks retained live Go heap to `30.0-113.1 MB` (`next_gc ~60-225 MB`, `peak_heap = 65.6-214.6 MB`), driving `14-25` cycles in 10s paced (up to `44` in 5s unpaced), `2.23x-4.48x` higher paced GC CPU than `ivm-lz4-heap` (`1.74x-2.87x` unpaced), `0.160-0.311s` paced pause CPU (up to `0.748s` unpaced), and p99 spikes up to `4.19 ms` paced / `5.24 ms` unpaced. Raising `GOGC=400` on `ivm-lz4-mmap` (`W4-W1 100k`) cuts its GC cycles (`19 -> 5`) and GC CPU (`0.49s -> 0.15s`, p99 `0.92 ms`), confirming the mechanism, but pushes its steady VmRSS (`463.8 MB`) slightly above default-`GOGC=100` `ivm-lz4-heap` (`461.4 MB`, `0.14s` GC CPU).
3. **Correctness Gate (#3)**: **PASS** (`100.0%` sampled-TraceID & span-count parity on W1 and W2 under both in-order and out-of-order arrivals, including positive `probabilistic(1%)` matches, reversed cross-batch `AND`, and late-phase `/healthz` `drop > sample` arena deletion; `0` false positives, `0` false negatives; `span-ingest` proven failing).
4. **Full-Package `benchstat` Gate**: **PASS** (`0` statistically significant regressions `> 5%` at `p < 0.05`; `-0.00%` `retained-B/span` and `retained-B/trace`).
