# Milestone 2 Verification Evidence (Gate 2 Converged)

- **Worktree**: `<repo-root>`
- **Base Commit**: `db80cc0daf0` (`upstream/main`)
- **`go.mod` / `go.sum` Changes**: `0` files modified (zero external dependencies)

## Added / Modified Files (M2)

- `wire_codec.go` (`833` lines): Direct zero-alloc stdlib `encoding/binary` `pdata -> OTLP proto3` wire writer matching `pdata@v1.71.1` exact recursion depth boundaries (`F1`), skipping empty `spanIndices[pos]` (`F5`), preserving internal `Resource.EntityRefs` via zero-alloc `ResourceSpansSize` delta check (`F9`), plus raw wire frame splitter/reassembler (`splitRawWire` / `reassembleRawWire` supporting wire types `0–5`, bounded group depth `64`, `parseWireTag` checking `fieldNum <= (1<<29)-1` (`F11`), same-`TraceID` field 2 + field 1000 merging (`F10`), reverse field order, `schema_url`, omitted proto3 zero `TraceID`).
- `lz4_codec.go` (`308` lines): Zero-dep pure-Go LZ4 block encoder/decoder using generation-stamped `[4096]uint32` hash table, `>= 128 B` cutoff, `>= 10%` savings gate, strict bounds-checked decoder with overlapping RLE copy.
- `arena_storage.go` (`1611` lines): All 5 arena modes (`rawProtoSlab`, `internedHeap`, `internedMmap`, `lz4Heap`, `lz4Mmap`), separate `noscan` slab-backed `resIntern` & `scopeIntern` tables (`O(len(t.buckets))` compaction (`F3`)), unconditional capped `scratchComp` reuse on incompressible bailout (`F2`), zero dead-slab pooling for Go-heap `!mmapped` arenas (`F4`), `AppendGroup` index-mismatch & closed-store guards (`F5`), bounds/liveness/overflow guards (`F6`), `acquireSlab`-before-seal on `ENOMEM` (`F7`), batch raw resource header caching (`F9`), `slabs`/`freeSlabIDs` reset on idle after `>= 256` slab burst (`F12`), direct construction without `init()` global hook (`F14`).
- `arena_mmap_unix.go` (`46` lines, `//go:build linux || darwin`): `syscall.Mmap` with `syscall.MAP_ANON` + `size <= 0` / overflow guards (`F6`).
- `arena_madvise_linux.go` (`17` lines, `//go:build linux`): `syscall.Madvise(b, syscall.MADV_DONTNEED)`.
- `arena_madvise_other.go` (`10` lines, `//go:build !linux`): No-op stub (`arenaSupportsMadvDontNeed = false` triggers immediate `Munmap` on `!linux`).
- `arena_mmap_fallback.go` (`28` lines, `//go:build !linux && !darwin`): Portable heap `[]byte` fallback + `size <= 0` guard (`F6`).
- `arena_storage_test.go` (`1980` lines): Local canonical deep-comparison helper (zero `pdatatest` import) + full M2 exit gates + `TestArena_Gate2Regressions` (`F1`–`F12`).
- `ivm_processor.go` (`1475` lines): Calls `newArenaPayloadStore` directly (`F14`) and uses `ivmBatchAwareStore` lazy pin hook (`BeginResourceSpans`/`FinishResourceSpans`).

## Bonus Upstream Discovery

During `TestWireCodec_RawWireSplitAndUnknownFields`, we discovered an upstream bug in `go.opentelemetry.io/collector/pdata@v1.71.1/internal/proto/unmarshal.go:35-62` (`ConsumeUnknown`): inside a proto2 group (`WireTypeStartGroup`, `depth > 0`), `ConsumeUnknown` executes `return pos, err` immediately on the first inner `Varint`/`I64`/`Len`/`I32` field instead of continuing to scan until `WireTypeEndGroup`, causing `ErrUnexpectedEndOfGroup` on valid non-empty protobuf groups. Our `wire_codec.go` (`consumeWireField`) handles nested groups across wire types `0–5` losslessly; our test verifies both our verbatim wire round-trip on non-empty nested groups AND `pdata.ProtoUnmarshaler` compatibility on wire types `0, 1, 2, 5` + empty `3/4` groups.

## Raw Command Outputs

### 1. M2 Targeted Verification Suite (Non-Race, Clean `runtime.MemStats` with `runtime.KeepAlive`)

Command:
```bash
cd processor/tailsamplingprocessor && \
  go test -timeout=60s -count=1 -v -run 'TestWireCodec|TestLZ4|TestArena' .
```

Key test outputs:
```text
--- PASS: TestWireCodec_LosslessPdataAndExactProtoSize (0.00s)
--- PASS: TestWireCodec_RawWireSplitAndUnknownFields (0.00s)
--- PASS: TestLZ4_RoundTripAndCorruptSafety (0.00s)
--- PASS: TestArena_LosslessRoundTripAll5Modes (0.10s)
--- PASS: TestArena_ZeroLeaksLifecycleAndShutdown (0.61s)
    (includes ZeroUnmarshalOnW3ExpiredTraces across all 5 arena modes)
=== RUN   TestArena_SteadyStatePinningAndCompaction
    Chunk pinning [internedMmap]: livePayload=75710 B, liveSlabBytes=98304 B, totalAlloc=229376 B, pooled=4, liveRatio=1.30x, compactions=60
    Chunk pinning [internedHeap]: livePayload=75710 B, liveSlabBytes=98304 B, totalAlloc=98304 B, pooled=0, liveRatio=1.30x, compactions=60
    Chunk pinning [lz4Heap]:      livePayload=75710 B, liveSlabBytes=98304 B, totalAlloc=98304 B, pooled=0, liveRatio=1.30x, compactions=60
    Intern pinning after 10k churn: resEntries=1, resPeak=251, scopeEntries=2, internAllocBytes=196608, compactions=2
--- PASS: TestArena_SteadyStatePinningAndCompaction (0.26s)
--- PASS: TestArena_W3WorstCaseMemoryReduction (5.07s)
--- PASS: TestArena_EndToEndIVMParity (0.36s)
    (PureIVM_ZeroUnmarshalDropAndEarlySample + HybridWithBytesLimiting across all 5 modes + MultiShard4_ConcurrentRace + span content parity)
=== RUN   TestArena_Gate2Regressions
    (R2-1 update: F2_IncompressibleScratchCompReused removed testing.AllocsPerRun; verifies exact backing-array pointer identity & capacity on both as.scratchComp and as.resIntern.scratchComp across 50 cache-missing incompressible iterations; re-verified under -race + go vet in task-67.log)
--- PASS: TestArena_Gate2Regressions (0.05s)
    --- PASS: F1_ExactPdataRecursionDepthBoundaries (0.00s)
    --- PASS: F2_IncompressibleScratchCompReused (0.00s)
    --- PASS: F3_MapCompactionAndPeakReset_InternAndTraceChunks (0.02s)
    --- PASS: F5_EmptySpanIndicesMismatchAndClosedCheck (0.00s)
    --- PASS: F6_CorruptScopeCountScopeIDAndSizeGuards (0.00s)
    --- PASS: F7_ENOMEMDoesNotSealPartiallyFilledActiveSlab (0.00s)
    --- PASS: F8_NumTracesOverflow_FIFOAndBlockOnOverflow_WithArena (0.02s)
    --- PASS: F8_CompactSlabReseal_RollbackBatchActive_Collision3Node_LZ4AndEmptySpanID (0.00s)
    --- PASS: F9_LosslessEntityRefsAll5Modes (0.00s) [all 5 arena modes byte-for-byte identical!]
    --- PASS: F12_SlabHeaderSliceResetOnIdle (0.00s)
PASS
ok  	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor	6.479s
```

### 2. W3 Worst-Case Retained Memory Proof (`0%` Short-Circuit, `49,860` Pending Spans / `3,840` Traces)

*After each retained snapshot (`runtime.KeepAlive(template)` and `runtime.KeepAlive(p)` across dual `runtime.GC()` calls), `measureW3Retained` actively tick-expires all `3,840` traces (`DropPendingTracesOnShutdown: false`) and asserts `droppedWithoutUnmarshal == 3840`, `UnmarshalCount == 0`, `LiveSlabs == 0`, `LiveChunks == 0`, and `MmapActiveBytes == 0`.*

| Shape (`49,860` spans) | Baseline (`trace-complete`) | `internedHeap` | `lz4Heap` | `internedMmap` (Go Heap / `mmap`) | `lz4Mmap` (Go Heap / `mmap`) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **`mixed`** | `892.9 B/sp` (`42.46 MiB`, `657,764` objs) | **`394.1 B/sp`** (**-55.9%**, `578` objs **-99.9%**, payload `16.59 MiB`) | **`120.7 B/sp`** (**-86.5%**, `530` objs **-99.9%**, payload `3.85 MiB`, `12,060/12,060` comp) | **`34.0 B/sp`** (**-96.2%** heap, `502` objs) / `17.12 MiB` mmap | **`33.9 B/sp`** (**-96.2%** heap, `494` objs) / **`4.12 MiB`** mmap |
| **`resource-per-trace`** | `723.7 B/sp` (`34.41 MiB`, `507,590` objs) | `397.5 B/sp` (-45.1%, `256` objs **-99.9%**, payload `16.63 MiB`) | **`70.5 B/sp`** (**-90.3%**, `196` objs **-100.0%**, payload `1.52 MiB`, `3,840/3,840` comp) | **`25.6 B/sp`** (**-96.5%** heap, `187` objs) / `17.69 MiB` mmap | **`25.8 B/sp`** (**-96.4%** heap, `181` objs) / **`2.12 MiB`** mmap |
| **`few-traces-per-resource`** | `725.3 B/sp` (`34.49 MiB`, `507,572` objs) | `396.1 B/sp` (-45.4%, `267` objs **-99.9%**, payload `16.59 MiB`) | **`70.4 B/sp`** (**-90.3%**, `205` objs **-100.0%**, payload `1.48 MiB`, `3,840/3,840` comp) | **`25.4 B/sp`** (**-96.5%** heap, `198` objs) / `17.62 MiB` mmap | **`25.7 B/sp`** (**-96.5%** heap, `203` objs) / **`2.12 MiB`** mmap |
| **`sdk`** | `725.3 B/sp` (`34.49 MiB`, `507,573` objs) | `396.7 B/sp` (-45.3%, `740` objs **-99.9%**, payload `16.57 MiB`) | **`71.0 B/sp`** (**-90.2%**, `673` objs **-99.9%**, payload `1.47 MiB`, `3,840/3,840` comp) | **`26.1 B/sp`** (**-96.4%** heap, `660` objs) / `17.62 MiB` mmap | **`26.3 B/sp`** (**-96.4%** heap, `663` objs) / **`2.12 MiB`** mmap |
| **`ephemeral-unique-res`** | `770.3 B/sp` (`36.63 MiB`, `514,659` objs) | `445.8 B/sp` (-42.1%, `294` objs **-99.9%**, payload `18.61 MiB`) | **`115.2 B/sp`** (**-85.0%**, `232` objs **-100.0%**, payload `2.99 MiB`, `3,840/3,840` comp) | **`32.2 B/sp`** (**-95.8%** heap, `202` objs) / `19.69 MiB` mmap | **`32.4 B/sp`** (**-95.8%** heap, `199` objs) / **`3.94 MiB`** mmap |

### 3. Full Package `-race`, Cross-OS Compilation, & `go vet`

Command:
```bash
cd processor/tailsamplingprocessor && \
  go test -race -timeout=180s -count=1 ./... && \
  GOOS=darwin GOARCH=arm64 go build ./... && \
  GOOS=windows GOARCH=amd64 go build ./... && \
  GOOS=linux GOARCH=386 go build ./... && \
  go vet ./...
```

```text
ok  	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor	88.537s
ok  	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/cache	1.085s
ok  	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/idbatcher	1.475s
ok  	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/metadata	1.097s
ok  	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/metadatatest	1.114s
ok  	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/sampling	8.834s
?   	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/tailstorageextension	[no test files]
ok  	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/telemetry	1.965s
?   	github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/pkg/samplingpolicy	[no test files]
```
Exit code `0` (Linux amd64/386, Darwin arm64, Windows amd64 builds and `go vet ./...` clean).

## M2 Exit Gate Checklist (`PROTOTYPE_PLAN.md` Section 5)
1. **100% lossless round-trip** (`pdata` all `ValueType` variants incl. zero values, `Resource.EntityRefs` (`F9`), events, links, tracestate, flags, `schema_url`, dropped counts, multi-scope `TracesSize`, zero `TraceID`, empty `SpanID`, AND raw wire unknown fields across wire types `0–5` + reverse field order + same-TraceID field 2 & field 1000): **PASS**.
2. **Zero leaks & deterministic cleanup** (50k traces, 10k ephemeral `k8s.pod.uid`, FIFO + `block_on_overflow` overflow with arenas, `Start()` error path, `DropPendingTracesOnShutdown: true`, `ctx.Done()`, `liveSlabs == 0`, `liveChunks == 0`, `liveInternedEntries == 0`, `mmapActiveBytes == 0`, `peakEntries == 0`, `goleak` clean): **PASS**.
3. **Steady-state pinning & compaction proof** (chunk pinning `1.30x <= 2.0x` across `internedMmap`, `internedHeap` (`pooled=0`), AND `lz4Heap` (`pooled=0`); intern pinning `196,608 B <= 512 KiB` after 10k ephemeral resource churn): **PASS**.
4. **Non-tautological W3 worst-case memory proof**: `internedHeap` on `mixed` `-55.9% >= 50%` and `-99.9% >= 85%` objs; `lz4Heap` `-85.0%` to `-90.3% >= 50%` across ALL 5 variants; `internedMmap`/`lz4Mmap` `-95.8%` to `-96.5% >= 90%` Go heap across ALL 5 variants: **PASS**.
5. **Non-tautological zero-unmarshal proof**: All `3,840` traces actively tick-expired in `measureW3Retained` (plus `ZeroUnmarshalOnW3ExpiredTraces` across all 5 arena modes) asserting `droppedWithoutUnmarshal == 3840` and `UnmarshalCount == 0`: **PASS**.
