#!/usr/bin/env python3
"""Serial isolated-subprocess bakeoff runner for Milestone 3."""
import json
import os
import subprocess
import sys
import time

BIN = "./tailsampling_head.test"
OUT_DIR = "prototype_docs"


def run_case(label, env_overrides, out_fh):
    env = os.environ.copy()
    env.setdefault("GOGC", "100")
    env["IVM_BAKEOFF_RUN"] = "1"
    for k, v in env_overrides.items():
        env[k] = str(v)
    t0 = time.monotonic()
    cmd = [BIN, "-test.run=^TestIVMBakeoffSubprocess$", "-test.v"]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=180)
    elapsed = time.monotonic() - t0
    if proc.returncode != 0:
        print(f"FAIL [{label}] rc={proc.returncode} ({elapsed:.1f}s)\n{proc.stdout}\n{proc.stderr}", flush=True)
        sys.exit(1)
    json_line = None
    for line in proc.stdout.splitlines():
        if line.startswith("BAKEOFF_JSON:"):
            json_line = line[len("BAKEOFF_JSON:"):]
            break
    if not json_line:
        print(f"ERROR [{label}] missing BAKEOFF_JSON:\n{proc.stdout}\n{proc.stderr}", flush=True)
        sys.exit(1)
    rec = json.loads(json_line)
    rec["label"] = label
    out_fh.write(json.dumps(rec) + "\n")
    out_fh.flush()
    print(
        f"OK [{label:42s}] {elapsed:4.1f}s | "
        f"sps={rec['achieved_sps']:8.0f} | "
        f"peakHeap={rec['peak_heap_alloc_bytes']/1e6:6.1f}MB | "
        f"steadyRSS={rec['steady_vm_rss_bytes']/1e6:6.1f}MB | "
        f"scan={rec['steady_gc_scan_heap_bytes']/1e6:5.1f}MB | "
        f"gcCPU={rec['window_gc_cpu_seconds']:5.2f}s | "
        f"p99GC={rec['gc_pause_p99_ms']:5.2f}ms | "
        f"inP99={rec['ingest_p99_us']:6.0f}us | "
        f"tkP99={rec['tick_p99_ms']:4.0f}ms | "
        f"zeroUnm={rec['zero_unmarshal_drop_ratio']:.3f}",
        flush=True,
    )


def batch1():
    cases = []
    # Block A1: W4-W1 100k spans/sec (1% error, mixed, shards=4) - all 7 candidates
    for c in [
        "trace-complete",
        "span-ingest",
        "ivm-raw-slab",
        "ivm-interned-heap",
        "ivm-interned-mmap",
        "ivm-lz4-heap",
        "ivm-lz4-mmap",
    ]:
        cases.append((f"W4-W1-100k-e1-s4/{c}", {
            "IVM_CANDIDATE": c, "IVM_WORKLOAD": "w1_err1", "IVM_SHAPE": "mixed",
            "IVM_SHARDS": 4, "IVM_RATE": 100000, "IVM_BYPASS_SPLIT": "true",
        }))

    # Block A2: W4-W1 200k spans/sec (1% error, mixed, shards=4) - 2M live spans
    for c in ["trace-complete", "ivm-interned-heap", "ivm-interned-mmap", "ivm-lz4-heap", "ivm-lz4-mmap"]:
        cases.append((f"W4-W1-200k-e1-s4/{c}", {
            "IVM_CANDIDATE": c, "IVM_WORKLOAD": "w1_err1", "IVM_SHAPE": "mixed",
            "IVM_SHARDS": 4, "IVM_RATE": 200000, "IVM_BYPASS_SPLIT": "true",
        }))

    # Block A3: W4-W1 100k spans/sec (5% error, mixed, shards=4)
    for c in ["trace-complete", "ivm-interned-heap", "ivm-interned-mmap", "ivm-lz4-heap", "ivm-lz4-mmap"]:
        cases.append((f"W4-W1-100k-e5-s4/{c}", {
            "IVM_CANDIDATE": c, "IVM_WORKLOAD": "w1_err5", "IVM_SHAPE": "mixed",
            "IVM_SHARDS": 4, "IVM_RATE": 100000, "IVM_BYPASS_SPLIT": "true",
        }))

    # Block A4: W4-W1 50k spans/sec scaling curve point (1% error, mixed, shards=4)
    for c in ["trace-complete", "ivm-interned-heap", "ivm-lz4-heap", "ivm-lz4-mmap"]:
        cases.append((f"W4-W1-50k-e1-s4/{c}", {
            "IVM_CANDIDATE": c, "IVM_WORKLOAD": "w1_err1", "IVM_SHAPE": "mixed",
            "IVM_SHARDS": 4, "IVM_RATE": 50000, "IVM_BYPASS_SPLIT": "true",
        }))

    # Block A5: num_shards=1 & splitResourceSpansByTrace bypass ablation (W4-W1 100k, mixed)
    for c, sh, byp in [
        ("trace-complete", 1, "true"),
        ("ivm-interned-heap", 1, "true"),
        ("ivm-interned-heap", 1, "false"),
        ("ivm-lz4-heap", 1, "true"),
        ("ivm-lz4-mmap", 1, "true"),
        ("ivm-interned-heap", 4, "false"),
        ("ivm-lz4-heap", 4, "false"),
    ]:
        tag = "byp" if byp == "true" else "withsplit"
        cases.append((f"W4-W1-100k-ablation-s{sh}-{tag}/{c}", {
            "IVM_CANDIDATE": c, "IVM_WORKLOAD": "w1_err1", "IVM_SHAPE": "mixed",
            "IVM_SHARDS": sh, "IVM_RATE": 100000, "IVM_BYPASS_SPLIT": byp,
        }))

    # Block B1: W4-W2 100k spans/sec (35% drop + 2% error + cross-batch AND, mixed, shards=4)
    for c in [
        "trace-complete",
        "span-ingest",
        "ivm-raw-slab",
        "ivm-interned-heap",
        "ivm-interned-mmap",
        "ivm-lz4-heap",
        "ivm-lz4-mmap",
    ]:
        cases.append((f"W4-W2-100k-s4/{c}", {
            "IVM_CANDIDATE": c, "IVM_WORKLOAD": "w2", "IVM_SHAPE": "mixed",
            "IVM_SHARDS": 4, "IVM_RATE": 100000, "IVM_BYPASS_SPLIT": "true",
        }))

    # Block B2: W4-W2 200k spans/sec (2M live spans pre-drop, mixed, shards=4)
    for c in ["trace-complete", "ivm-interned-heap", "ivm-interned-mmap", "ivm-lz4-heap", "ivm-lz4-mmap"]:
        cases.append((f"W4-W2-200k-s4/{c}", {
            "IVM_CANDIDATE": c, "IVM_WORKLOAD": "w2", "IVM_SHAPE": "mixed",
            "IVM_SHARDS": 4, "IVM_RATE": 200000, "IVM_BYPASS_SPLIT": "true",
        }))

    # Block E: GOGC=400 sensitivity on ivm-lz4-mmap (diagnostic for tiny Go heap GC frequency)
    cases.append(("W4-W1-100k-e1-s4-gogc400/ivm-lz4-mmap", {
        "GOGC": "400",
        "IVM_CANDIDATE": "ivm-lz4-mmap", "IVM_WORKLOAD": "w1_err1", "IVM_SHAPE": "mixed",
        "IVM_SHARDS": 4, "IVM_RATE": 100000, "IVM_BYPASS_SPLIT": "true",
    }))
    return cases


def batch2():
    cases = []
    # Block C: W4-W3 100k spans/sec x 10s = 1M live pending spans (0% short-circuit) across all 5 shapes
    shape_cands = [
        ("mixed", ["trace-complete", "ivm-raw-slab", "ivm-interned-heap", "ivm-interned-mmap", "ivm-lz4-heap", "ivm-lz4-mmap"]),
        ("sdk", ["trace-complete", "ivm-interned-heap", "ivm-interned-mmap", "ivm-lz4-heap", "ivm-lz4-mmap"]),
        ("resource-per-trace", ["trace-complete", "ivm-interned-heap", "ivm-interned-mmap", "ivm-lz4-heap", "ivm-lz4-mmap"]),
        ("few-traces-per-resource", ["trace-complete", "ivm-interned-heap", "ivm-lz4-heap", "ivm-lz4-mmap"]),
        ("ephemeral-unique-res", ["trace-complete", "ivm-interned-heap", "ivm-interned-mmap", "ivm-lz4-heap", "ivm-lz4-mmap"]),
    ]
    for shape, cands in shape_cands:
        for c in cands:
            cases.append((f"W4-W3-100k-{shape}-s4/{c}", {
                "IVM_CANDIDATE": c, "IVM_WORKLOAD": "w3", "IVM_SHAPE": shape,
                "IVM_SHARDS": 4, "IVM_RATE": 100000, "IVM_BYPASS_SPLIT": "true",
            }))

    # Block D: Unpaced max sustainable throughput (IVM_RATE=0, DW=2s, warmup=2s, measure=5s, BlockOnOverflow=true)
    unpaced = [
        ("w1_err1", 1, "trace-complete", "true"),
        ("w1_err1", 1, "ivm-interned-heap", "true"),
        ("w1_err1", 1, "ivm-interned-heap", "false"),
        ("w1_err1", 1, "ivm-lz4-heap", "true"),
        ("w1_err1", 1, "ivm-lz4-mmap", "true"),
        ("w1_err1", 4, "trace-complete", "true"),
        ("w1_err1", 4, "ivm-interned-heap", "true"),
        ("w1_err1", 4, "ivm-interned-heap", "false"),
        ("w1_err1", 4, "ivm-lz4-heap", "true"),
        ("w1_err1", 4, "ivm-lz4-heap", "false"),
        ("w1_err1", 4, "ivm-lz4-mmap", "true"),
        ("w2", 4, "trace-complete", "true"),
        ("w2", 4, "ivm-interned-heap", "true"),
        ("w2", 4, "ivm-lz4-heap", "true"),
        ("w2", 4, "ivm-lz4-mmap", "true"),
    ]
    for wl, sh, c, byp in unpaced:
        tag = "byp" if byp == "true" else "withsplit"
        cases.append((f"UNPACED-{wl}-s{sh}-{tag}/{c}", {
            "IVM_CANDIDATE": c, "IVM_WORKLOAD": wl, "IVM_SHAPE": "mixed",
            "IVM_SHARDS": sh, "IVM_RATE": 0, "IVM_BYPASS_SPLIT": byp,
            "IVM_DECISION_WAIT_SEC": 2, "IVM_WARMUP_SEC": 2, "IVM_MEASURE_SEC": 5,
            "IVM_BLOCK_ON_OVERFLOW": "true",
        }))
    return cases


def main():
    which = sys.argv[1]
    if which == "rerun_r1":
        # Re-run all 41 batch1 runs (W1/W2/ablation/span-ingest/GOGC=400) cleanly.
        b1_cases = batch1()
        b1_path = os.path.join(OUT_DIR, "m3_bakeoff_batch1.jsonl")
        print(f"Starting rerun_r1 batch1: {len(b1_cases)} isolated serial runs -> {b1_path}", flush=True)
        with open(b1_path, "w") as fh:
            for i, (label, env_ov) in enumerate(b1_cases, 1):
                print(f"[B1 {i:02d}/{len(b1_cases):02d}] ", end="", flush=True)
                run_case(label, env_ov, fh)
        # For batch2, keep untouched W4-W3 ivm-* runs (no probabilistic/AND/span-ingest/bypass change),
        # and re-run all 5 W4-W3 trace-complete runs + all 15 UNPACED runs!
        b2_path = os.path.join(OUT_DIR, "m3_bakeoff_batch2.jsonl")
        existing_b2 = {}
        if os.path.exists(b2_path):
            with open(b2_path) as f:
                for line in f:
                    r = json.loads(line)
                    existing_b2[r["label"]] = r
        b2_cases = batch2()
        rerun_labels = {
            lbl for lbl, _ in b2_cases
            if lbl.startswith("UNPACED-") or lbl.endswith("/trace-complete")
        }
        print(f"Starting rerun_r1 batch2 delta: {len(rerun_labels)} isolated serial runs -> {b2_path}", flush=True)
        tmp_b2 = b2_path + ".tmp"
        done_delta = 0
        with open(tmp_b2, "w") as fh:
            for label, env_ov in b2_cases:
                if label in rerun_labels:
                    done_delta += 1
                    print(f"[B2 {done_delta:02d}/{len(rerun_labels):02d}] ", end="", flush=True)
                    run_case(label, env_ov, fh)
                else:
                    fh.write(json.dumps(existing_b2[label]) + "\n")
                    fh.flush()
        os.replace(tmp_b2, b2_path)
        print(f"Completed rerun_r1 ({len(b1_cases) + len(rerun_labels)} fresh runs, 81 total).", flush=True)
        return

    out_path = os.path.join(OUT_DIR, f"m3_bakeoff_{which}.jsonl")
    cases = batch1() if which == "batch1" else batch2()
    print(f"Starting {which}: {len(cases)} isolated serial runs -> {out_path}", flush=True)
    with open(out_path, "w") as fh:
        for i, (label, env_ov) in enumerate(cases, 1):
            print(f"[{i:02d}/{len(cases):02d}] ", end="", flush=True)
            run_case(label, env_ov, fh)
    print(f"Completed {which} ({len(cases)} runs).", flush=True)


if __name__ == "__main__":
    main()
