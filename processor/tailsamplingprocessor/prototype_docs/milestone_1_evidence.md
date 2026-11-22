# Milestone 1 Verification Evidence (Gate 2 Converged)

- **Worktree**: `<repo-root>`
- **Base Commit**: `db80cc0daf0` (`upstream/main`)
- **`go.mod` / `go.sum` Changes**: `0` files modified

## Changed / Added Files (Post Gate 2 Convergence)

- `config.go` (modified)
- `processor.go` (modified)
- `sharded_processor.go` (modified)
- `ivm_circuit.go` (`1766` lines)
- `ivm_processor.go` (`1464` lines)
- `ivm_parity_test.go` (`2092` lines)

## Raw Command Outputs

### 1. Targeted IVM Parity & Lifecycle Suite (`-race`, pre-R2 run `task-317.log`)

```text
--- PASS: TestUpstreamSpanIngestKnownParityFailures (0.01s)
    --- PASS: TestUpstreamSpanIngestKnownParityFailures/Bug1_DropPlusPositiveDropsAllTraces (0.00s)
    --- PASS: TestUpstreamSpanIngestKnownParityFailures/Bug2_CrossBatchAND (0.00s)
    --- PASS: TestUpstreamSpanIngestKnownParityFailures/Bug3_CrossBatchLatency (0.00s)
    --- PASS: TestUpstreamSpanIngestKnownParityFailures/Bug4_NotAndInvertMatchPrematureFalsePositive (0.00s)
=== NAME  TestIVMParity
    ivm_parity_test.go:876: Verified 0 mismatches across 13800 randomized multi-batch traces
--- PASS: TestIVMParity (4.37s)
--- PASS: TestIVMParity_MultiShardAndOver32Nodes (0.17s)
--- PASS: TestIVMParity_OTTLErrorLatticeAndStatefulHybrid (0.08s)
--- PASS: TestIVMParityLatencyOracleB (0.00s)
--- PASS: TestIVMLifecycle (0.06s)
PASS
ok  	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor	5.993s
```

### 2. Targeted Gate 2 Regressions & Full Package Suite (`-race`, post-R2 run `task-393.log`)

Command:
```bash
cd processor/tailsamplingprocessor && \
  go test -race -count=1 -v -run 'TestIVMGate2Regressions' . && go test -race -count=1 ./...
```

```text
--- PASS: TestIVMGate2Regressions (0.12s)
    --- PASS: TestIVMGate2Regressions/F7_IsolatedSpanEmptyStringRegexAsymmetry (0.00s)
    --- PASS: TestIVMGate2Regressions/F5_F7_SampledAndNonSampledHooksParity_EarlyAndTick (0.00s)
    --- PASS: TestIVMGate2Regressions/F7_CachedMaximumTraceSizeBytes_ZeroDroppedTooEarly (0.01s)
    --- PASS: TestIVMGate2Regressions/F11_HybridAndNotRateLimiting_NoOrphanedNodes (0.00s)
    --- PASS: TestIVMGate2Regressions/F4_HybridDropPrefixNotDoubleEvaluated (0.05s)
    --- PASS: TestIVMGate2Regressions/F3_BlockOnOverflow_CtxCancelExitsCleanlyWithoutSpin (0.02s)
    --- PASS: TestIVMGate2Regressions/F2_FailingTailStorageAppend_NoSlotLeakOrOvercount (0.00s)
    --- PASS: TestIVMGate2Regressions/F8_WithPoliciesInjectedAlongsideNonEmptyPolicyCfgs (0.00s)
    --- PASS: TestIVMGate2Regressions/F10_SetSamplingPolicyPreservesLatchedPolicyNameUnderNopCache (0.00s)
    --- PASS: TestIVMGate2Regressions/F13_EarlyShortCircuitRecordsEvalErrorTelemetry (0.03s)
    --- PASS: TestIVMGate2Regressions/F14_FIFOEvictionOfLatchedNopCacheTrace_ZeroDroppedTooEarly (0.00s)
    --- PASS: TestIVMGate2Regressions/R2_1_PolicyNamesByVerPrunedUnderContinuousTraffic (0.00s)
    --- PASS: TestIVMGate2Regressions/R2_2_FailingTakeWithNonSampledHooksCallsDeleteFallback (0.00s)
PASS
ok  	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor	1.297s
ok  	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor	20.586s
ok  	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/cache	1.068s
ok  	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/idbatcher	1.394s
ok  	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/metadata	1.080s
ok  	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/metadatatest	1.107s
ok  	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/sampling	7.808s
?   	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/tailstorageextension	[no test files]
ok  	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/telemetry	1.792s
?   	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/pkg/samplingpolicy	[no test files]
```

*(Also verified by clean-room auditor: `go vet ./...` PASS and `GOOS=darwin GOARCH=arm64 go build ./...` PASS.)*

## Exit Gate Checklist (`PROTOTYPE_PLAN.md` Section 5)
1. `TestUpstreamSpanIngestKnownParityFailures` proves all 4 upstream `span-ingest` bugs non-tautologically: **PASS**.
2. `TestIVMParity` `>= 5,000` randomized multi-batch traces with `0` mismatches: **PASS (`13,800` traces, `0` mismatches)**, covering `EmptyAnd`, `EmptyDropOverridesAlwaysSample`, `SpanCount(0,0)`, numeric/boolean/string `InvertMatch`, `BoundedLatency_NonPrefixDivergent`, `PolicyName` winner parity (`useTracestate=false` and `true`), `TraceState` outgoing `th` parity, `sample_on_first_match`, `num_shards=4`, `>32` circuit nodes, `and`/`not`/`drop` stateful hybrids, `WithSampledHooks`/`WithNonSampledHooks` (including `ReceivedBatches`), isolated span-side `^$` regex asymmetry, `SetSamplingPolicy` hot-reload, continuous-traffic `policyNamesByVer` pruning (`R2-1`), failing-`Take` `Delete` fallback (`R2-2`), and Oracle B (`TestIVMParityLatencyOracleB`).
3. Lifecycle & telemetry gates (`trace_dropped_too_early == 0`, `earlyDropped == 1`, `sampling_policy_evaluation_error`, `block_on_overflow` + `ctx.Done()`, `NopCache` + `maximum_trace_size_bytes`, cached `maximum_trace_size_bytes`, `DecisionWaitAfterRootReceived`, `withPolicies(...)`, failing `TailStorage.Append`): **PASS**.
