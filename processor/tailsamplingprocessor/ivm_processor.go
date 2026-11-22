// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tailsamplingprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	pkgsampling "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/sampling"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/cache"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/sampling"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/tailstorageextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/telemetry"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/pkg/samplingpolicy"
)

// arenaStorageMode selects the decoupled payload storage backend used by
// sampling_strategy: "incremental".
type arenaStorageMode uint8

const (
	// arenaModeInMemory uses the default inMemoryTailStorage (or configured tail_storage).
	arenaModeInMemory arenaStorageMode = iota
	// arenaModeRawProtoSlab stores raw per-batch ResourceSpans OTLP bytes in in-heap noscan slabs.
	arenaModeRawProtoSlab
	// arenaModeInternedHeap stores Resource/Scope-interned OTLP chunks in in-heap noscan slabs.
	arenaModeInternedHeap
	// arenaModeInternedMmap stores Resource/Scope-interned OTLP chunks and intern bytes in mmap slabs.
	arenaModeInternedMmap
	// arenaModeLZ4Heap adds zero-dep pure-Go LZ4 block compression to internedHeapArena.
	arenaModeLZ4Heap
	// arenaModeLZ4Mmap adds zero-dep pure-Go LZ4 block compression to internedMmapArena.
	arenaModeLZ4Mmap
)

// ArenaStats reports diagnostic counters for arena-backed payload storage.
type ArenaStats struct {
	LiveSlabs             int
	PooledSlabs           int
	TotalAllocatedBytes   uint64
	LivePayloadBytes      uint64
	LiveChunks            uint64
	MmapActiveBytes       uint64
	ResourceInternEntries int
	ScopeInternEntries    int
	ResourceInternPeak    int
	ScopeInternPeak       int
	ResourceInternBytes   uint64
	ScopeInternBytes      uint64
	InternAllocatedBytes  uint64
	RawBytesIn            uint64
	CompressedBytesStored uint64
	CompressedChunks      uint64
	UncompressedChunks    uint64
	CompactionCount       uint64
	UnmarshalCount        uint64
}

// ivmPayloadStore is an optional extension of TailStorage implemented by compact
// byte-arena backends (Milestone 2).
type ivmPayloadStore interface {
	tailstorageextension.TailStorage
	// AppendGroup stores all spans for traceID from a single incoming ResourceSpans,
	// preserving multi-scope grouping so TracesSize and lossless round-trip match.
	AppendGroup(traceID pcommon.TraceID, rs ptrace.ResourceSpans, scopeIndices []int, spanIndices [][]int) error
	// OnTick seals active slabs and triggers compaction on pinned low-utilization slabs.
	OnTick()
	// Close releases off-heap resources (e.g. syscall.Munmap).
	Close() error
	// Stats returns diagnostic counters.
	Stats() ArenaStats
}

// ivmRuntimeStats holds atomic counters observable by tests and benchmarks.
type ivmRuntimeStats struct {
	earlySampled            atomic.Int64
	earlyDropped            atomic.Int64
	earlyNotSampled         atomic.Int64
	droppedWithoutUnmarshal atomic.Int64
	unmarshaledForSample    atomic.Int64
	unmarshaledForFallback  atomic.Int64
	latchedSampledSpans     atomic.Int64
	latchedDroppedSpans     atomic.Int64
	positiveFastSkips       atomic.Int64
}

// withIncrementalBypassSplit toggles bypassing splitResourceSpansByTrace in
// incremental mode (true by default). Setting false enables the M3 ablation.
func withIncrementalBypassSplit(bypass bool) Option {
	return func(tsp *tailSamplingSpanProcessor) {
		tsp.ivmBypassSplit = bypass
		tsp.ivmBypassSplitSet = true
	}
}

// withArenaStorageMode selects the payload arena backend for incremental mode.
func withArenaStorageMode(mode arenaStorageMode) Option {
	return func(tsp *tailSamplingSpanProcessor) {
		tsp.ivmArenaMode = mode
	}
}

// withArenaSlabSize overrides the default 256 KiB arena slab size for tests/benchmarks.
func withArenaSlabSize(size int) Option {
	return func(tsp *tailSamplingSpanProcessor) {
		tsp.ivmSlabSize = size
	}
}

type ivmTraceGroup struct {
	id           pcommon.TraceID
	spanCount    int64
	hasRoot      bool
	scopeIndices []int
	spanIndices  [][]int
}

// ivmSizeCache caches protobuf wire header sizes for a single incoming ResourceSpans
// so multi-trace batches never re-measure Resource or Scope overhead per trace.
type ivmSizeCache struct {
	resHeaderSize    int
	scopeHeaderSizes []int
}

func (sc *ivmSizeCache) reset(numScopes int) {
	sc.resHeaderSize = -1
	if cap(sc.scopeHeaderSizes) < numScopes {
		sc.scopeHeaderSizes = make([]int, numScopes)
	} else {
		sc.scopeHeaderSizes = sc.scopeHeaderSizes[:numScopes]
	}
	for i := range sc.scopeHeaderSizes {
		sc.scopeHeaderSizes[i] = -1
	}
}

type ivmShardState struct {
	circuit          *ivmCircuit
	table            *ivmTraceTable
	rawWorkChan      chan ptrace.Traces
	bypassSplit      bool
	payloadStore     ivmPayloadStore
	stats            ivmRuntimeStats
	nextVer          uint32
	policyNamesByVer map[uint32][]string
	verRefCounts     map[uint32]int

	// Reusable per-ResourceSpans grouping buffers to avoid steady-state allocations.
	groupIndex map[pcommon.TraceID]int
	groups     []ivmTraceGroup
	sizeCache  ivmSizeCache
}

func (tsp *tailSamplingSpanProcessor) isIncremental() bool {
	return tsp.cfg.SamplingStrategy == samplingStrategyIncremental
}

func (tsp *tailSamplingSpanProcessor) usesRawWorkChan() bool {
	return tsp.ivm != nil && tsp.ivm.bypassSplit
}

func snapshotPolicyNames(policies []*policy) []string {
	names := make([]string, len(policies))
	for i, p := range policies {
		names[i] = p.name
	}
	return names
}

func (tsp *tailSamplingSpanProcessor) ivmPolicyName(ver uint32, idx int32) string {
	if idx < 0 || tsp.ivm == nil {
		return ""
	}
	if names, ok := tsp.ivm.policyNamesByVer[ver]; ok && int(idx) < len(names) {
		return names[idx]
	}
	if int(idx) < len(tsp.policies) {
		return tsp.policies[idx].name
	}
	return ""
}

func (tsp *tailSamplingSpanProcessor) ivmReleaseVer(ver uint32) {
	if tsp.ivm == nil || ver == 0 {
		return
	}
	cnt := tsp.ivm.verRefCounts[ver] - 1
	if cnt > 0 {
		tsp.ivm.verRefCounts[ver] = cnt
		return
	}
	delete(tsp.ivm.verRefCounts, ver)
	if ver != tsp.ivm.circuit.version {
		delete(tsp.ivm.policyNamesByVer, ver)
	}
}

func (tsp *tailSamplingSpanProcessor) ivmSetSlotVer(slot *ivmTraceSlot, newVer uint32) {
	if slot.circuitVer == newVer {
		return
	}
	if slot.circuitVer != 0 {
		tsp.ivmReleaseVer(slot.circuitVer)
	}
	slot.circuitVer = newVer
	tsp.ivm.verRefCounts[newVer]++
}

func (tsp *tailSamplingSpanProcessor) pruneOldPolicyVersions() {
	if tsp.ivm == nil || len(tsp.ivm.policyNamesByVer) <= 1 {
		return
	}
	curVer := tsp.ivm.circuit.version
	for ver := range tsp.ivm.policyNamesByVer {
		if ver != curVer && tsp.ivm.verRefCounts[ver] == 0 {
			delete(tsp.ivm.policyNamesByVer, ver)
			delete(tsp.ivm.verRefCounts, ver)
		}
	}
}

func (tsp *tailSamplingSpanProcessor) initIncremental(cfgs []PolicyCfg, policies []*policy) error {
	bypass := true
	if tsp.ivmBypassSplitSet {
		bypass = tsp.ivmBypassSplit
	}

	circuit, err := compileIVMCircuit(tsp.set.TelemetrySettings, cfgs, policies, 1)
	if err != nil {
		return err
	}

	var store ivmPayloadStore
	if tsp.cfg.TailStorageID == nil && tsp.ivmArenaMode != arenaModeInMemory {
		waitTicks := max(1, int(tsp.cfg.DecisionWait.Seconds()))
		store = newArenaPayloadStore(tsp.ivmArenaMode, tsp.ivmSlabSize, waitTicks)
		tsp.tailStorage = store
	}

	bufCap := cap(tsp.workChan)
	var rawCh chan ptrace.Traces
	if bypass {
		if bufCap == 0 {
			rawCh = make(chan ptrace.Traces)
		} else {
			rawCh = make(chan ptrace.Traces, bufCap)
		}
	}

	tsp.ivm = &ivmShardState{
		circuit:          circuit,
		table:            newIVMTraceTable(len(circuit.nodes)),
		rawWorkChan:      rawCh,
		bypassSplit:      bypass,
		payloadStore:     store,
		nextVer:          2,
		policyNamesByVer: map[uint32][]string{1: snapshotPolicyNames(policies)},
		verRefCounts:     make(map[uint32]int, 4),
		groupIndex:       make(map[pcommon.TraceID]int, 128),
	}
	return nil
}

func (tsp *tailSamplingSpanProcessor) closeIVMStorage() {
	if tsp.ivm != nil && tsp.ivm.payloadStore != nil {
		if err := tsp.ivm.payloadStore.Close(); err != nil {
			tsp.logger.Error("Failed to close IVM payload arena", zap.Error(err))
		}
	}
}

func (tsp *tailSamplingSpanProcessor) applyIVMPolicyUpdate(cmd newPolicyCmd) {
	tsp.policies = cmd.policies
	if tsp.ivm == nil {
		return
	}
	ver := tsp.ivm.nextVer
	tsp.ivm.nextVer++
	tsp.ivm.policyNamesByVer[ver] = snapshotPolicyNames(cmd.policies)

	circuit, err := compileIVMCircuit(tsp.set.TelemetrySettings, cmd.cfgs, cmd.policies, ver)
	if err != nil {
		tsp.logger.Error("Failed to compile updated IVM circuit; falling back to full evaluation", zap.Error(err))
		fallback := &ivmCircuit{
			version:      ver,
			fallbackOnly: true,
			hasAnyOpaque: true,
		}
		for _, p := range cmd.policies {
			if p.isDrop {
				fallback.numDrop++
				fallback.hasDropPolicies = true
				fallback.hasAnyStatefulDrop = true
			} else {
				fallback.hasAnyStatefulPositive = true
			}
			if _, ok := p.evaluator.(sampling.BatchEvaluator); ok {
				fallback.hasAnyBatchEvaluator = true
			}
		}
		tsp.ivm.circuit = fallback
		tsp.pruneOldPolicyVersions()
		return
	}
	tsp.ivm.circuit = circuit
	tsp.ivm.table.ensureStride(len(circuit.nodes))
	tsp.pruneOldPolicyVersions()
}

func (tsp *tailSamplingSpanProcessor) drainIVMConfigChannels() {
	select {
	case cmd := <-tsp.newPolicyChan:
		tsp.applyIVMPolicyUpdate(cmd)
		tsp.logger.Debug("New policies loaded", zap.Int("policies.len", len(tsp.policies)))
	default:
	}
	select {
	case maxTraceSize := <-tsp.newTraceSizeChan:
		tsp.maxTraceSizeBytes = maxTraceSize
		tsp.logger.Debug("New maximum trace size loaded", zap.Uint64("size", maxTraceSize))
	default:
	}
}

// iterIncremental runs one event-loop step when sampling_strategy == "incremental"
// with bypassSplit enabled (receiving ptrace.Traces directly on rawWorkChan).
func (tsp *tailSamplingSpanProcessor) iterIncremental(tickChan <-chan time.Time, rawChan <-chan ptrace.Traces) bool {
	select {
	case <-tsp.ctx.Done():
		return false
	case td, ok := <-rawChan:
		if !ok {
			tsp.decisionBatcher.Stop()
			if !tsp.cfg.DropPendingTracesOnShutdown {
				for tsp.incrementalOnTick() {
				}
			}
			return false
		}
		tsp.drainIVMConfigChannels()
		if !tsp.processIncrementalRawTraces(tickChan, td) {
			return false
		}
	case cmd := <-tsp.newPolicyChan:
		tsp.applyIVMPolicyUpdate(cmd)
		tsp.logger.Debug("New policies loaded", zap.Int("policies.len", len(tsp.policies)))
	case maxTraceSize := <-tsp.newTraceSizeChan:
		tsp.maxTraceSizeBytes = maxTraceSize
		tsp.logger.Debug("New maximum trace size loaded", zap.Uint64("size", maxTraceSize))
	case <-tickChan:
		tsp.incrementalOnTick()
	case done := <-tsp.tickChan:
		tsp.incrementalOnTick()
		done <- struct{}{}
	}
	return true
}

// partitionTracesByShard splits td into at most numShards independent ptrace.Traces
// buckets (rather than one ResourceSpans per trace), copying Resource at most
// numShards-1 times per ResourceSpans.
func (sp *shardedProcessor) partitionTracesByShard(td ptrace.Traces) ([]ptrace.Traces, []bool, int64, int64) {
	numShards := sp.numShards
	out := make([]ptrace.Traces, numShards)
	hasData := make([]bool, numShards)

	var totalSpans, totalResourceSpans int64
	rssSlice := td.ResourceSpans()
	for i := 0; i < rssSlice.Len(); i++ {
		rss := rssSlice.At(i)
		totalResourceSpans++
		scopes := rss.ScopeSpans()

		// Fast path: check if all non-empty spans belong to one shard.
		singleShard := true
		firstShard := uint32(0)
		haveFirst := false
		fastSpans := int64(0)
		hasEmptyScope := false
		for j := 0; j < scopes.Len(); j++ {
			spans := scopes.At(j).Spans()
			if spans.Len() == 0 {
				hasEmptyScope = true
				continue
			}
			for k := 0; k < spans.Len(); k++ {
				sh := sp.traceIDToShard(spans.At(k).TraceID())
				if !haveFirst {
					firstShard = sh
					haveFirst = true
				} else if sh != firstShard {
					singleShard = false
					break
				}
				fastSpans++
			}
			if !singleShard {
				break
			}
		}

		if !haveFirst {
			continue
		}

		if singleShard && !hasEmptyScope {
			if !hasData[firstShard] {
				out[firstShard] = ptrace.NewTraces()
				hasData[firstShard] = true
			}
			rss.MoveTo(out[firstShard].ResourceSpans().AppendEmpty())
			totalSpans += fastSpans
			continue
		}

		type shardBuilder struct {
			rs        ptrace.ResourceSpans
			destScope ptrace.ScopeSpans
			lastScope int
			opened    bool
		}
		builders := make([]shardBuilder, numShards)
		resourceOwner := -1
		ensureShardRS := func(sh uint32) *shardBuilder {
			b := &builders[sh]
			if b.opened {
				return b
			}
			if !hasData[sh] {
				out[sh] = ptrace.NewTraces()
				hasData[sh] = true
			}
			rs := out[sh].ResourceSpans().AppendEmpty()
			if resourceOwner < 0 {
				rss.Resource().MoveTo(rs.Resource())
				resourceOwner = int(sh)
			} else {
				builders[resourceOwner].rs.Resource().CopyTo(rs.Resource())
			}
			rs.SetSchemaUrl(rss.SchemaUrl())
			b.rs = rs
			b.lastScope = -1
			b.opened = true
			return b
		}

		for j := 0; j < scopes.Len(); j++ {
			srcScope := scopes.At(j)
			spans := srcScope.Spans()
			if spans.Len() == 0 {
				continue
			}
			var scopeOwner ptrace.ScopeSpans
			scopeOwned := false
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				sh := sp.traceIDToShard(span.TraceID())
				b := ensureShardRS(sh)
				if b.lastScope != j {
					dest := b.rs.ScopeSpans().AppendEmpty()
					if scopeOwned {
						scopeOwner.Scope().CopyTo(dest.Scope())
					} else {
						srcScope.Scope().MoveTo(dest.Scope())
						scopeOwner, scopeOwned = dest, true
					}
					dest.SetSchemaUrl(srcScope.SchemaUrl())
					b.destScope = dest
					b.lastScope = j
				}
				span.MoveTo(b.destScope.Spans().AppendEmpty())
				totalSpans++
			}
		}
	}

	return out, hasData, totalSpans, totalResourceSpans
}

// processIncrementalRawTraces processes an entire ptrace.Traces batch directly on
// the shard goroutine, evaluating Resource attributes once per ResourceSpans and
// avoiding splitResourceSpansByTrace's per-trace Resource copying.
// Returns false if tsp.ctx was cancelled while waiting for overflow space.
func (tsp *tailSamplingSpanProcessor) processIncrementalRawTraces(tickChan <-chan time.Time, td ptrace.Traces) bool {
	ivm := tsp.ivm
	rssSlice := td.ResourceSpans()
	for i := 0; i < rssSlice.Len(); i++ {
		rs := rssSlice.At(i)
		scopes := rs.ScopeSpans()
		if scopes.Len() == 0 {
			continue
		}

		// Group spans within this ResourceSpans by TraceID without mutating rs.
		clear(ivm.groupIndex)
		for g := range ivm.groups {
			ivm.groups[g].scopeIndices = ivm.groups[g].scopeIndices[:0]
			for s := range ivm.groups[g].spanIndices {
				ivm.groups[g].spanIndices[s] = ivm.groups[g].spanIndices[s][:0]
			}
			ivm.groups[g].spanIndices = ivm.groups[g].spanIndices[:0]
		}
		ivm.groups = ivm.groups[:0]

		for j := 0; j < scopes.Len(); j++ {
			spans := scopes.At(j).Spans()
			if spans.Len() == 0 {
				// Skip empty ScopeSpans identically to splitResourceSpansByTrace.
				continue
			}
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				id := span.TraceID()
				gIdx, ok := ivm.groupIndex[id]
				if !ok {
					gIdx = len(ivm.groups)
					ivm.groupIndex[id] = gIdx
					if gIdx < cap(ivm.groups) {
						ivm.groups = ivm.groups[:gIdx+1]
						ivm.groups[gIdx].id = id
						ivm.groups[gIdx].spanCount = 0
						ivm.groups[gIdx].hasRoot = false
					} else {
						ivm.groups = append(ivm.groups, ivmTraceGroup{id: id})
					}
				}
				grp := &ivm.groups[gIdx]
				grp.spanCount++
				if span.ParentSpanID().IsEmpty() {
					grp.hasRoot = true
				}
				numScopes := len(grp.scopeIndices)
				if numScopes == 0 || grp.scopeIndices[numScopes-1] != j {
					grp.scopeIndices = append(grp.scopeIndices, j)
					if len(grp.spanIndices) < cap(grp.spanIndices) {
						grp.spanIndices = grp.spanIndices[:len(grp.spanIndices)+1]
						grp.spanIndices[len(grp.spanIndices)-1] = append(grp.spanIndices[len(grp.spanIndices)-1][:0], k)
					} else {
						grp.spanIndices = append(grp.spanIndices, []int{k})
					}
				} else {
					grp.spanIndices[numScopes-1] = append(grp.spanIndices[numScopes-1], k)
				}
			}
		}

		if len(ivm.groups) == 0 {
			continue
		}

		// Evaluate all resource-level predicates ONCE for the entire ResourceSpans.
		circuit := ivm.circuit
		if !circuit.fallbackOnly && len(circuit.resourceNodes) > 0 {
			circuit.evalResource(rs.Resource())
		}

		ivm.sizeCache.reset(scopes.Len())
		singleTraceInRS := len(ivm.groups) == 1

		var batchStore ivmBatchAwareStore
		if !singleTraceInRS && ivm.payloadStore != nil {
			if bs, ok := ivm.payloadStore.(ivmBatchAwareStore); ok {
				batchStore = bs
				batchStore.BeginResourceSpans(scopes.Len())
			}
		}

		for gIdx := range ivm.groups {
			grp := &ivm.groups[gIdx]
			if !tsp.processIncrementalGroup(tickChan, rs, grp, singleTraceInRS, &ivm.sizeCache) {
				if batchStore != nil {
					batchStore.FinishResourceSpans()
				}
				return false
			}
		}
		if batchStore != nil {
			batchStore.FinishResourceSpans()
		}
	}
	return true
}

// processIncrementalTraceBatch handles batches arriving via the legacy split
// workChan when bypassSplit == false (used for the M3 split ablation).
// Returns false if tsp.ctx was cancelled while waiting for overflow space.
func (tsp *tailSamplingSpanProcessor) processIncrementalTraceBatch(tickChan <-chan time.Time, batch []traceBatch) bool {
	ivm := tsp.ivm
	for _, tb := range batch {
		scopes := tb.rss.ScopeSpans()
		if scopes.Len() == 0 {
			continue
		}
		if !ivm.circuit.fallbackOnly && len(ivm.circuit.resourceNodes) > 0 {
			ivm.circuit.evalResource(tb.rss.Resource())
		}
		grp := ivmTraceGroup{
			id:        tb.id,
			spanCount: tb.spanCount,
			hasRoot:   tb.hasRoot,
		}
		for j := 0; j < scopes.Len(); j++ {
			spans := scopes.At(j).Spans()
			if spans.Len() == 0 {
				continue
			}
			grp.scopeIndices = append(grp.scopeIndices, j)
			idxs := make([]int, spans.Len())
			for k := 0; k < spans.Len(); k++ {
				idxs[k] = k
			}
			grp.spanIndices = append(grp.spanIndices, idxs)
		}
		if len(grp.scopeIndices) == 0 {
			continue
		}
		ivm.sizeCache.reset(scopes.Len())
		if !tsp.processIncrementalGroup(tickChan, tb.rss, &grp, true, &ivm.sizeCache) {
			return false
		}
	}
	return true
}

func (tsp *tailSamplingSpanProcessor) processIncrementalGroup(
	tickChan <-chan time.Time,
	rs ptrace.ResourceSpans,
	grp *ivmTraceGroup,
	singleTraceInRS bool,
	sizeCache *ivmSizeCache,
) bool {
	ivm := tsp.ivm
	id := grp.id

	// 1. Check decision caches first (before NumTraces / waitForSpace check!).
	if md, ok := tsp.sampledIDCache.Get(id); ok {
		tsp.logger.Debug("Trace ID is in the sampled cache", zap.Stringer("id", id))
		traceTd := extractGroupTraces(rs, grp, singleTraceInRS)
		if tsp.recordPolicy {
			if md.PolicyName != "" {
				sampling.SetAttrOnScopeSpans(traceTd, "tailsampling.policy", md.PolicyName)
			}
			sampling.SetBoolAttrOnScopeSpans(traceTd, "tailsampling.cached_decision", true)
		}
		if tsp.useTracestate {
			sampling.WriteEffectiveThreshold(tsp.ctx, traceTd, md.Threshold, tsp.telemetry.ProcessorTailSamplingCountSpansWithUnparseableTracestate)
		}
		// Increment before forwardSpans so callers waking on consumer receipt see updated stats.
		ivm.stats.latchedSampledSpans.Add(grp.spanCount)
		tsp.telemetry.ProcessorTailSamplingEarlyReleasesFromCacheDecision.Add(tsp.ctx, grp.spanCount, attrSampledTrue)
		tsp.forwardSpans(tsp.ctx, traceTd)
		return true
	}
	if _, ok := tsp.nonSampledIDCache.Get(id); ok {
		tsp.logger.Debug("Trace ID is in the non-sampled cache", zap.Stringer("id", id))
		ivm.stats.latchedDroppedSpans.Add(grp.spanCount)
		tsp.telemetry.ProcessorTailSamplingEarlyReleasesFromCacheDecision.Add(tsp.ctx, grp.spanCount, attrSampledFalse)
		return true
	}

	// 2. Ensure capacity for new trace IDs.
	prevCircuit := ivm.circuit
	slotIdx, slot, exists := ivm.table.lookup(id)
	if !exists && uint64(ivm.table.len()) >= tsp.cfg.NumTraces {
		if !tsp.ivmWaitForSpace(tickChan) {
			return false
		}
		// Re-evaluate rs.Resource() if a policy update swapped ivm.circuit during waitForSpace.
		if ivm.circuit != prevCircuit && !ivm.circuit.fallbackOnly && len(ivm.circuit.resourceNodes) > 0 {
			ivm.circuit.evalResource(rs.Resource())
		}
	}

	circuit := ivm.circuit
	currNano := time.Now().UnixNano()
	var newTraceIDs int64
	if !exists {
		slotIdx, slot = ivm.table.allocSlot(id, !tsp.blockOnOverflow)
		slot.arrivalNano = currNano
		slot.spanCount = grp.spanCount
		slot.circuitVer = circuit.version
		ivm.verRefCounts[circuit.version]++
		slot.finalDecision = int32(samplingpolicy.Unspecified)
		slot.policyIdx = -1
		if !circuit.fallbackOnly {
			circuit.initTraceStates(id, ivm.table.statesFor(slotIdx))
		}
		tsp.tracesOnMemory.Add(1)
		slot.batchID = tsp.decisionBatcher.AddToCurrentBatch(id)
		newTraceIDs = 1
	} else {
		slot.spanCount += grp.spanCount
	}
	if newTraceIDs > 0 {
		tsp.telemetry.ProcessorTailSamplingNewTraceIDReceived.Add(tsp.ctx, newTraceIDs)
	}

	// Only accelerate via DecisionWaitAfterRootReceived if trace is not already latched.
	finalDecision := samplingpolicy.Decision(slot.finalDecision)
	if finalDecision == samplingpolicy.Unspecified && grp.hasRoot && tsp.cfg.DecisionWaitAfterRootReceived > 0 {
		slot.batchID = tsp.decisionBatcher.MoveToEarlierBatch(id, slot.batchID, uint64(tsp.cfg.DecisionWaitAfterRootReceived.Seconds()))
	}

	// 3. Fast path for already-latched NopCache traces (0 payload bytes stored).
	if finalDecision != samplingpolicy.Unspecified {
		switch finalDecision {
		case samplingpolicy.Sampled:
			traceTd := extractGroupTraces(rs, grp, singleTraceInRS)
			if tsp.recordPolicy {
				if polName := tsp.ivmPolicyName(slot.circuitVer, slot.policyIdx); polName != "" {
					sampling.SetAttrOnScopeSpans(traceTd, "tailsampling.policy", polName)
				}
			}
			if tsp.useTracestate {
				th, _ := pkgsampling.UnsignedToThreshold(slot.finalThreshold)
				sampling.WriteEffectiveThreshold(tsp.ctx, traceTd, th, tsp.telemetry.ProcessorTailSamplingCountSpansWithUnparseableTracestate)
			}
			ivm.stats.latchedSampledSpans.Add(grp.spanCount)
			tsp.forwardSpans(tsp.ctx, traceTd)
		case samplingpolicy.NotSampled, samplingpolicy.Dropped:
			// Silently discard late spans under NopCache: no warning logs, no hook re-fires.
			ivm.stats.latchedDroppedSpans.Add(grp.spanCount)
		default:
			tsp.logger.Warn("Unexpected sampling decision", zap.Int("decision", int(finalDecision)))
		}
		if slot.decisionNano != 0 {
			ageSec := (currNano - slot.decisionNano) / int64(time.Second)
			tsp.telemetry.ProcessorTailSamplingSamplingLateSpanAge.Record(tsp.ctx, ageSec)
		}
		return true
	}

	// 4. Size accounting & maximum_trace_size_bytes enforcement (checked live after waitForSpace).
	if tsp.maxTraceSizeBytes > 0 || telemetry.IsMetricStatCountBytesSampledEnabled() {
		slot.sizeBytes += uint64(measureGroupProtoSize(rs, grp, sizeCache))
	}
	if tsp.maxTraceSizeBytes > 0 && slot.sizeBytes > tsp.maxTraceSizeBytes {
		tsp.telemetry.ProcessorTailSamplingTracesDroppedTooLarge.Add(tsp.ctx, 1)
		slot.finalDecision = int32(samplingpolicy.NotSampled)
		slot.policyIdx = -1
		slot.decisionNano = currNano
		var hookBatches ptrace.Traces
		if len(tsp.nonSampledHooks) > 0 {
			hookBatches = tsp.collectEarlyBatches(id, exists, rs, grp, singleTraceInRS)
		} else if exists {
			if err := tsp.tailStorage.Delete(id); err != nil {
				tsp.logger.Error("Failed to delete oversized trace from tail storage", zap.Error(err))
			}
		}
		if len(tsp.nonSampledHooks) > 0 {
			tdView := tsp.slotToTraceData(slot, hookBatches)
			for _, hook := range tsp.nonSampledHooks {
				hook(context.Background(), id, tdView)
			}
		}
		tsp.nonSampledIDCache.Put(id, cache.DecisionMetadata{})
		if _, cached := tsp.nonSampledIDCache.Get(id); cached {
			// Cached mode: safe to remove from decisionBatcher and evict immediately.
			tsp.decisionBatcher.RemoveFromBatch(id, slot.batchID)
			tsp.ivmDropSlot(id, currNano, false)
		}
		// NopCache mode: do NOT call RemoveFromBatch so tick cleans up the latched
		// NotSampled slot at decision_wait expiry (prevents rebirth AND block_on_overflow deadlock).
		return true
	}

	// 5. Incremental IVM metadata evaluation (when circuit version matches and not fallback-only).
	if !circuit.fallbackOnly && slot.circuitVer == circuit.version {
		states := ivm.table.statesFor(slotIdx)
		dropOnly := circuit.positiveLatched(
			states,
			tsp.sampleOnFirstMatch,
			tsp.useTracestate,
			tsp.recordPolicy,
			len(tsp.sampledHooks) > 0,
		)
		if dropOnly {
			ivm.stats.positiveFastSkips.Add(grp.spanCount)
		}

		if len(circuit.resourceNodes) > 0 {
			circuit.applyResource(states)
		}

		scopes := rs.ScopeSpans()
		for sPos, sIdx := range grp.scopeIndices {
			ss := scopes.At(sIdx)
			spans := ss.Spans()
			for _, k := range grp.spanIndices[sPos] {
				circuit.applySpan(tsp.ctx, slot, states, rs, ss, spans.At(k), dropOnly, tsp.logger)
			}
		}
		circuit.updateAggregatesAndPropagate(slot, states)

		res := circuit.tryShortCircuit(
			states,
			tsp.sampleOnFirstMatch,
			tsp.useTracestate,
			tsp.recordPolicy,
			len(tsp.sampledHooks) > 0,
			len(tsp.nonSampledHooks) > 0,
		)

		switch res.decision {
		case samplingpolicy.Dropped:
			ivm.stats.earlyDropped.Add(1)
			slot.finalDecision = int32(samplingpolicy.Dropped)
			slot.policyIdx = int32(res.policyIdx)
			slot.decisionNano = currNano
			if res.evalErrors > 0 {
				tsp.telemetry.ProcessorTailSamplingSamplingPolicyEvaluationError.Add(tsp.ctx, res.evalErrors)
			}
			tsp.telemetry.ProcessorTailSamplingGlobalCountTracesSampled.Add(tsp.ctx, 1, attrDecisionDropped)
			tsp.ivmFinalizeNotSampledEarly(id, slot, exists, rs, grp, singleTraceInRS, currNano)
			return true

		case samplingpolicy.NotSampled:
			ivm.stats.earlyNotSampled.Add(1)
			slot.finalDecision = int32(samplingpolicy.NotSampled)
			slot.policyIdx = -1
			slot.decisionNano = currNano
			if res.evalErrors > 0 {
				tsp.telemetry.ProcessorTailSamplingSamplingPolicyEvaluationError.Add(tsp.ctx, res.evalErrors)
			}
			tsp.telemetry.ProcessorTailSamplingGlobalCountTracesSampled.Add(tsp.ctx, 1, attrDecisionNotSampled)
			tsp.ivmFinalizeNotSampledEarly(id, slot, exists, rs, grp, singleTraceInRS, currNano)
			return true

		case samplingpolicy.Sampled:
			ivm.stats.earlySampled.Add(1)
			slot.finalDecision = int32(samplingpolicy.Sampled)
			slot.policyIdx = int32(res.policyIdx)
			slot.finalThreshold = res.threshold.Unsigned()
			slot.decisionNano = currNano

			// Retrieve ONLY prior buffered spans (if any); skip buffering current batch.
			var priorSpans ptrace.Traces
			var err error
			if exists {
				priorSpans, err = tsp.tailStorage.Take(id)
				if err != nil {
					tsp.logger.Error("Failed to retrieve trace from tail storage", zap.Error(err))
					tsp.decisionBatcher.RemoveFromBatch(id, slot.batchID)
					tsp.ivmDropSlot(id, currNano, true)
					return true
				}
				if priorSpans.ResourceSpans().Len() > 0 {
					ivm.stats.unmarshaledForSample.Add(1)
				}
			}

			merged := ptrace.NewTraces()
			if exists && priorSpans.ResourceSpans().Len() > 0 {
				appendAllTraces(merged, priorSpans)
			}
			curTd := extractGroupTraces(rs, grp, singleTraceInRS)
			appendAllTraces(merged, curTd)

			if tsp.recordPolicy {
				if polName := tsp.ivmPolicyName(slot.circuitVer, slot.policyIdx); polName != "" {
					sampling.SetAttrOnScopeSpans(merged, "tailsampling.policy", polName)
				}
			}
			if tsp.useTracestate {
				sampling.WriteEffectiveThreshold(tsp.ctx, merged, res.threshold, tsp.telemetry.ProcessorTailSamplingCountSpansWithUnparseableTracestate)
			}
			if res.evalErrors > 0 {
				tsp.telemetry.ProcessorTailSamplingSamplingPolicyEvaluationError.Add(tsp.ctx, res.evalErrors)
			}
			tsp.telemetry.ProcessorTailSamplingGlobalCountTracesSampled.Add(tsp.ctx, 1, attrDecisionSampled)
			tsp.ivmFinalizeSampled(tsp.ctx, id, slot, merged, currNano, true)
			return true
		}
	}

	// 6. Trace remains Pending: buffer payload in arena (or default TailStorage).
	if ivm.payloadStore != nil {
		if err := ivm.payloadStore.AppendGroup(id, rs, grp.scopeIndices, grp.spanIndices); err != nil {
			tsp.logger.Error("Failed to append trace group to payload arena", zap.Error(err))
		}
		return true
	}

	traceTd := extractGroupTraces(rs, grp, singleTraceInRS)
	if err := tsp.tailStorage.Append(id, traceTd); err != nil {
		tsp.logger.Error("Failed to append trace to tail storage", zap.Error(err))
	}
	return true
}

func (tsp *tailSamplingSpanProcessor) collectEarlyBatches(
	id pcommon.TraceID,
	exists bool,
	rs ptrace.ResourceSpans,
	grp *ivmTraceGroup,
	singleTraceInRS bool,
) ptrace.Traces {
	merged := ptrace.NewTraces()
	if exists {
		prior, err := tsp.tailStorage.Take(id)
		if err != nil {
			tsp.logger.Error("Failed to retrieve trace from tail storage for nonSampledHooks", zap.Error(err))
			if delErr := tsp.tailStorage.Delete(id); delErr != nil {
				tsp.logger.Error("Failed to delete trace from tail storage after Take error", zap.Error(delErr))
			}
		} else if prior.ResourceSpans().Len() > 0 {
			appendAllTraces(merged, prior)
		}
	}
	appendAllTraces(merged, extractGroupTraces(rs, grp, singleTraceInRS))
	return merged
}

// incrementalOnTick handles decision-wait expiry for sampling_strategy: "incremental".
// For pure IVM circuits, traces resolving to NotSampled or Dropped are freed in O(1)
// WITHOUT unmarshaling payloads from storage (when no nonSampledHooks are registered).
func (tsp *tailSamplingSpanProcessor) incrementalOnTick() bool {
	tsp.logger.Debug("Incremental Sampling Policy Evaluation ticked")
	ivm := tsp.ivm
	circuit := ivm.circuit

	metrics := newPolicyEvaluationMetrics(len(tsp.policies))
	startTime := time.Now()
	globalDecisions := make(map[samplingpolicy.Decision]int64)

	batch, hasMore := tsp.decisionBatcher.CloseCurrentAndTakeFirstBatch()
	batchLen := len(batch)
	var span trace.Span
	ctx := context.Background()
	if batchLen > 0 {
		ctx, span = tsp.tracer.Start(ctx, "tailsampling.samplingPolicyOnTick")
		defer span.End()
	}

	type fallbackPending struct {
		id            pcommon.TraceID
		slotIdx       uint32
		slot          *ivmTraceSlot
		dropEvaluated bool
	}
	var fallbackList []fallbackPending
	var sampledIDs []pcommon.TraceID
	var sampledSlots []*ivmTraceSlot
	var sampledRes []ivmDecisionResult

	nowNano := time.Now().UnixNano()

	for id := range batch {
		slotIdx, slot, ok := ivm.table.lookup(id)
		if !ok {
			metrics.idNotFoundOnMapCount++
			continue
		}

		// Already early-decided under NopCache: tick expiry cleans up the tiny latched
		// slot so table never leaks and block_on_overflow never deadlocks.
		if samplingpolicy.Decision(slot.finalDecision) != samplingpolicy.Unspecified {
			tsp.ivmDropSlot(id, nowNano, false)
			continue
		}

		// Check if circuit matches the trace's version and can resolve in O(1) without payload.
		dropAlreadyEvaluated := false
		if !circuit.fallbackOnly && slot.circuitVer == circuit.version {
			states := ivm.table.statesFor(slotIdx)
			if !circuit.hasAnyOpaque {
				res := circuit.resolveAtTick(states, tsp.sampleOnFirstMatch)
				metrics.evaluateErrorCount += res.evalErrors
				switch res.decision {
				case samplingpolicy.Sampled:
					sampledIDs = append(sampledIDs, id)
					sampledSlots = append(sampledSlots, slot)
					sampledRes = append(sampledRes, res)
				case samplingpolicy.Dropped:
					slot.decisionNano = nowNano
					slot.finalDecision = int32(samplingpolicy.Dropped)
					slot.policyIdx = int32(res.policyIdx)
					metrics.decisionDropped++
					globalDecisions[samplingpolicy.Dropped]++
					tsp.ivmFinalizeNotSampledOnTick(id, slot, nowNano)
				default:
					slot.decisionNano = nowNano
					slot.finalDecision = int32(samplingpolicy.NotSampled)
					slot.policyIdx = -1
					metrics.decisionNotSampled++
					globalDecisions[samplingpolicy.NotSampled]++
					tsp.ivmFinalizeNotSampledOnTick(id, slot, nowNano)
				}
				continue
			}

			// Hybrid config (opaque/stateful positive policies, but pure drop prefix):
			// evaluate pure drop prefix once here; record dropAlreadyEvaluated so
			// fallback path never double-evaluates drops or re-runs latency.go prefix bugs.
			if !circuit.hasAnyStatefulDrop && circuit.hasDropPolicies {
				dropPolIdx, evalErrs := circuit.resolveDropAtTick(states)
				metrics.evaluateErrorCount += evalErrs
				dropAlreadyEvaluated = true
				if dropPolIdx >= 0 {
					slot.decisionNano = nowNano
					slot.finalDecision = int32(samplingpolicy.Dropped)
					slot.policyIdx = int32(dropPolIdx)
					metrics.decisionDropped++
					globalDecisions[samplingpolicy.Dropped]++
					tsp.ivmFinalizeNotSampledOnTick(id, slot, nowNano)
					continue
				}
			}
		}

		// Fallback path (opaque/stateful policies, withPolicies, or stale circuitVer).
		fallbackList = append(fallbackList, fallbackPending{
			id:            id,
			slotIdx:       slotIdx,
			slot:          slot,
			dropEvaluated: dropAlreadyEvaluated,
		})
	}

	// Unmarshal ONLY the traces that resolved to Sampled in the pure IVM path.
	if len(sampledIDs) > 0 {
		allSpans, errs := tsp.takeAll(sampledIDs)
		for i, id := range sampledIDs {
			slot := sampledSlots[i]
			res := sampledRes[i]
			if errs[i] != nil {
				tsp.logger.Error("Failed to retrieve sampled trace from tail storage", zap.Error(errs[i]))
				tsp.ivmDropSlot(id, nowNano, true)
				continue
			}
			if allSpans[i].ResourceSpans().Len() == 0 {
				metrics.idNotFoundOnMapCount++
				tsp.ivmDropSlot(id, nowNano, false)
				continue
			}
			ivm.stats.unmarshaledForSample.Add(1)
			metrics.decisionSampled++
			globalDecisions[samplingpolicy.Sampled]++
			slot.decisionNano = nowNano
			slot.finalDecision = int32(samplingpolicy.Sampled)
			slot.policyIdx = int32(res.policyIdx)
			slot.finalThreshold = res.threshold.Unsigned()
			if tsp.recordPolicy {
				if polName := tsp.ivmPolicyName(slot.circuitVer, slot.policyIdx); polName != "" {
					sampling.SetAttrOnScopeSpans(allSpans[i], "tailsampling.policy", polName)
				}
			}
			if tsp.useTracestate {
				sampling.WriteEffectiveThreshold(ctx, allSpans[i], res.threshold, tsp.telemetry.ProcessorTailSamplingCountSpansWithUnparseableTracestate)
			}
			tsp.ivmFinalizeSampledOnTick(ctx, id, slot, allSpans[i], nowNano)
		}
	}

	// Handle any fallback candidates via full upstream evaluator path.
	if len(fallbackList) > 0 {
		fbIDs := make([]pcommon.TraceID, len(fallbackList))
		for i, f := range fallbackList {
			fbIDs[i] = f.id
		}
		allSpans, errs := tsp.takeAll(fbIDs)
		candidates := make([]tickCandidate, 0, len(fallbackList))
		slotByIdx := make([]*ivmTraceSlot, 0, len(fallbackList))
		dropDoneByIdx := make([]bool, 0, len(fallbackList))
		for i, f := range fallbackList {
			if errs[i] != nil {
				tsp.logger.Error("Failed to retrieve trace from tail storage", zap.Error(errs[i]))
				tsp.ivmDropSlot(f.id, nowNano, true)
				continue
			}
			if allSpans[i].ResourceSpans().Len() == 0 {
				metrics.idNotFoundOnMapCount++
				tsp.ivmDropSlot(f.id, nowNano, false)
				continue
			}
			ivm.stats.unmarshaledForFallback.Add(1)
			td := tsp.slotToTraceData(f.slot, allSpans[i])
			candidates = append(candidates, tickCandidate{
				id:    f.id,
				trace: td,
				data: &samplingpolicy.TraceData{
					SpanCount:       f.slot.spanCount,
					SizeBytes:       f.slot.sizeBytes,
					ReceivedBatches: allSpans[i],
				},
			})
			slotByIdx = append(slotByIdx, f.slot)
			dropDoneByIdx = append(dropDoneByIdx, f.dropEvaluated)
		}

		numDropPolicies := 0
		for _, p := range tsp.policies {
			if !p.isDrop {
				break
			}
			numDropPolicies++
		}

		notDropped := make([]tickCandidate, 0, len(candidates))
		notDroppedSlots := make([]*ivmTraceSlot, 0, len(candidates))
		for i, c := range candidates {
			if dropDoneByIdx[i] {
				notDropped = append(notDropped, c)
				notDroppedSlots = append(notDroppedSlots, slotByIdx[i])
				continue
			}
			p := tsp.evaluateDropPolicies(ctx, numDropPolicies, c.id, c.data, metrics)
			if p == nil {
				notDropped = append(notDropped, c)
				notDroppedSlots = append(notDroppedSlots, slotByIdx[i])
				continue
			}
			slot := slotByIdx[i]
			tsp.ivmSetSlotVer(slot, circuit.version)
			slot.decisionNano = time.Now().UnixNano()
			slot.finalDecision = int32(samplingpolicy.Dropped)
			slot.policyIdx = int32(tsp.findPolicyIdx(p.name))
			metrics.decisionDropped++
			globalDecisions[samplingpolicy.Dropped]++
			tsp.ivmFinalizeNotSampledOnTickWithBatches(c.id, slot, c.data.ReceivedBatches, slot.decisionNano)
		}

		if tsp.useTracestate {
			tsp.runBatchPolicies(ctx, notDropped)
		}

		for i, c := range notDropped {
			slot := notDroppedSlots[i]
			decisionTime := time.Now().UnixNano()
			tsp.ivmSetSlotVer(slot, circuit.version)
			slot.decisionNano = decisionTime
			decision, policyName, threshold := tsp.makeDecision(ctx, numDropPolicies, c.id, c.data, metrics)
			globalDecisions[decision]++
			slot.finalDecision = int32(decision)
			slot.finalThreshold = threshold.Unsigned()
			slot.policyIdx = int32(tsp.findPolicyIdx(policyName))

			if decision == samplingpolicy.Sampled {
				tsp.ivmFinalizeSampledOnTick(ctx, c.id, slot, c.data.ReceivedBatches, decisionTime)
			} else {
				tsp.ivmFinalizeNotSampledOnTickWithBatches(c.id, slot, c.data.ReceivedBatches, decisionTime)
			}
		}
	}

	// Seal active arena slabs and run compaction on every tick.
	if ivm.payloadStore != nil {
		ivm.payloadStore.OnTick()
	}

	tsp.pruneOldPolicyVersions()

	tsp.telemetry.ProcessorTailSamplingSamplingDecisionTimerLatency.Record(tsp.ctx, time.Since(startTime).Milliseconds())
	tsp.telemetry.ProcessorTailSamplingSamplingTracesOnMemory.Record(tsp.ctx, tsp.tracesOnMemory.Load())
	tsp.telemetry.ProcessorTailSamplingSamplingTraceDroppedTooEarly.Add(tsp.ctx, metrics.idNotFoundOnMapCount)
	tsp.telemetry.ProcessorTailSamplingSamplingPolicyEvaluationError.Add(tsp.ctx, metrics.evaluateErrorCount)

	for decision, count := range globalDecisions {
		tsp.telemetry.ProcessorTailSamplingGlobalCountTracesSampled.Add(tsp.ctx, count, decisionToAttributes[decision])
	}
	tsp.recordPerPolicyEvaluationMetrics(metrics)

	if span != nil && span.IsRecording() {
		span.SetAttributes(
			attribute.Int("batch.size", batchLen),
			attribute.Int("policy.count", len(tsp.policies)),
			attribute.Int64("policy.errors", metrics.evaluateErrorCount),
			attribute.Int64("decisions.sampled", metrics.decisionSampled),
			attribute.Int64("decisions.not_sampled", metrics.decisionNotSampled),
			attribute.Int64("decisions.dropped", metrics.decisionDropped),
		)
	}
	if metrics.evaluateErrorCount > 0 && span != nil && span.IsRecording() {
		span.SetStatus(codes.Error, fmt.Sprintf("%d policy evaluation errors", metrics.evaluateErrorCount))
	}

	return hasMore
}

func (tsp *tailSamplingSpanProcessor) findPolicyIdx(name string) int {
	for i, p := range tsp.policies {
		if p.name == name {
			return i
		}
	}
	return -1
}

func (tsp *tailSamplingSpanProcessor) slotToTraceData(slot *ivmTraceSlot, batches ptrace.Traces) *TraceData {
	polName := tsp.ivmPolicyName(slot.circuitVer, slot.policyIdx)
	th, _ := pkgsampling.UnsignedToThreshold(slot.finalThreshold)
	td := &TraceData{
		TraceData: samplingpolicy.TraceData{
			SpanCount:       slot.spanCount,
			SizeBytes:       slot.sizeBytes,
			ReceivedBatches: batches,
		},
		FinalDecision:  samplingpolicy.Decision(slot.finalDecision),
		FinalThreshold: th,
		PolicyName:     polName,
		arrivalTime:    time.Unix(0, slot.arrivalNano),
		batchID:        slot.batchID,
	}
	if slot.decisionNano != 0 {
		td.decisionTime = time.Unix(0, slot.decisionNano)
	}
	return td
}

func (tsp *tailSamplingSpanProcessor) ivmFinalizeSampled(
	ctx context.Context,
	id pcommon.TraceID,
	slot *ivmTraceSlot,
	batches ptrace.Traces,
	nowNano int64,
	early bool,
) {
	polName := tsp.ivmPolicyName(slot.circuitVer, slot.policyIdx)
	th, _ := pkgsampling.UnsignedToThreshold(slot.finalThreshold)
	if len(tsp.sampledHooks) > 0 {
		td := tsp.slotToTraceData(slot, batches)
		for _, hook := range tsp.sampledHooks {
			hook(ctx, id, td)
		}
	}
	tsp.sampledIDCache.Put(id, cache.DecisionMetadata{
		PolicyName: polName,
		Threshold:  th,
	})
	tsp.forwardSpans(ctx, batches)
	if _, ok := tsp.sampledIDCache.Get(id); ok {
		if early {
			tsp.decisionBatcher.RemoveFromBatch(id, slot.batchID)
		}
		tsp.ivmDropSlot(id, nowNano, false)
	}
}

func (tsp *tailSamplingSpanProcessor) ivmFinalizeSampledOnTick(
	ctx context.Context,
	id pcommon.TraceID,
	slot *ivmTraceSlot,
	batches ptrace.Traces,
	nowNano int64,
) {
	tsp.ivmFinalizeSampled(ctx, id, slot, batches, nowNano, false)
	if _, _, stillPresent := tsp.ivm.table.lookup(id); stillPresent {
		tsp.ivmDropSlot(id, nowNano, false)
	}
}

func (tsp *tailSamplingSpanProcessor) ivmFinalizeNotSampledEarly(
	id pcommon.TraceID,
	slot *ivmTraceSlot,
	exists bool,
	rs ptrace.ResourceSpans,
	grp *ivmTraceGroup,
	singleTraceInRS bool,
	nowNano int64,
) {
	polName := tsp.ivmPolicyName(slot.circuitVer, slot.policyIdx)
	if len(tsp.nonSampledHooks) > 0 {
		batches := tsp.collectEarlyBatches(id, exists, rs, grp, singleTraceInRS)
		td := tsp.slotToTraceData(slot, batches)
		for _, hook := range tsp.nonSampledHooks {
			hook(context.Background(), id, td)
		}
	} else if exists {
		if err := tsp.tailStorage.Delete(id); err != nil {
			tsp.logger.Error("Failed to delete trace from tail storage", zap.Error(err))
		}
	}
	tsp.nonSampledIDCache.Put(id, cache.DecisionMetadata{PolicyName: polName})
	if _, ok := tsp.nonSampledIDCache.Get(id); ok {
		tsp.decisionBatcher.RemoveFromBatch(id, slot.batchID)
		tsp.ivmDropSlot(id, nowNano, false)
	}
}

func (tsp *tailSamplingSpanProcessor) ivmFinalizeNotSampledOnTick(id pcommon.TraceID, slot *ivmTraceSlot, nowNano int64) {
	polName := tsp.ivmPolicyName(slot.circuitVer, slot.policyIdx)
	if len(tsp.nonSampledHooks) > 0 {
		batches, err := tsp.tailStorage.Take(id)
		if err != nil {
			tsp.logger.Error("Failed to retrieve trace from tail storage for nonSampledHooks", zap.Error(err))
			if delErr := tsp.tailStorage.Delete(id); delErr != nil {
				tsp.logger.Error("Failed to delete trace from tail storage after Take error", zap.Error(delErr))
			}
			batches = ptrace.NewTraces()
		}
		td := tsp.slotToTraceData(slot, batches)
		for _, hook := range tsp.nonSampledHooks {
			hook(context.Background(), id, td)
		}
	} else {
		// Zero-unmarshal O(1) delete when no nonSampledHooks require payload bytes!
		tsp.ivm.stats.droppedWithoutUnmarshal.Add(1)
		if err := tsp.tailStorage.Delete(id); err != nil {
			tsp.logger.Error("Failed to delete trace from tail storage", zap.Error(err))
		}
	}
	tsp.nonSampledIDCache.Put(id, cache.DecisionMetadata{PolicyName: polName})
	tsp.ivmDropSlot(id, nowNano, false)
}

func (tsp *tailSamplingSpanProcessor) ivmFinalizeNotSampledOnTickWithBatches(
	id pcommon.TraceID,
	slot *ivmTraceSlot,
	batches ptrace.Traces,
	nowNano int64,
) {
	polName := tsp.ivmPolicyName(slot.circuitVer, slot.policyIdx)
	if len(tsp.nonSampledHooks) > 0 {
		td := tsp.slotToTraceData(slot, batches)
		for _, hook := range tsp.nonSampledHooks {
			hook(context.Background(), id, td)
		}
	}
	tsp.nonSampledIDCache.Put(id, cache.DecisionMetadata{PolicyName: polName})
	tsp.ivmDropSlot(id, nowNano, false)
}

func (tsp *tailSamplingSpanProcessor) ivmDropSlot(id pcommon.TraceID, deletionNano int64, deleteStorage bool) bool {
	slot, ok := tsp.ivm.table.freeSlot(id)
	if !ok {
		return false
	}
	tsp.ivmReleaseVer(slot.circuitVer)
	tsp.tracesOnMemory.Add(-1)
	if deleteStorage {
		if err := tsp.tailStorage.Delete(id); err != nil {
			tsp.logger.Error("Failed to delete trace from tail storage", zap.Error(err))
			return false
		}
	}
	ageSec := (deletionNano - slot.arrivalNano) / int64(time.Second)
	tsp.telemetry.ProcessorTailSamplingSamplingTraceRemovalAge.Record(tsp.ctx, ageSec)
	return true
}

// ivmWaitForSpace blocks or evicts the oldest trace until table.len() < NumTraces.
// Returns false if tsp.ctx was cancelled while waiting under block_on_overflow.
func (tsp *tailSamplingSpanProcessor) ivmWaitForSpace(tickChan <-chan time.Time) bool {
	if tsp.blockOnOverflow {
		for uint64(tsp.ivm.table.len()) >= tsp.cfg.NumTraces {
			var keepRunning bool
			if tsp.ivm.bypassSplit {
				keepRunning = tsp.iterIncremental(tickChan, nil)
			} else {
				keepRunning = tsp.iter(tickChan, nil)
			}
			if !keepRunning {
				return false
			}
		}
		return true
	}
	head := tsp.ivm.table.fifoHead
	if head == ivmNoSlot {
		tsp.logger.Error("IVM FIFO queue is empty while waiting for space")
		return true
	}
	slot := &tsp.ivm.table.slots[head]
	id := slot.traceID
	if samplingpolicy.Decision(slot.finalDecision) != samplingpolicy.Unspecified {
		// Already decided under NopCache: remove from batcher so tick doesn't count
		// it as dropped_too_early; payload was already freed when it latched.
		tsp.decisionBatcher.RemoveFromBatch(id, slot.batchID)
		tsp.ivmDropSlot(id, time.Now().UnixNano(), false)
		return true
	}
	tsp.ivmDropSlot(id, time.Now().UnixNano(), true)
	return true
}

// extractGroupTraces constructs a ptrace.Traces holding only the spans for grp
// from rs, preserving exact Resource, SchemaUrl, Scope, and multi-scope grouping.
func extractGroupTraces(rs ptrace.ResourceSpans, grp *ivmTraceGroup, singleTrace bool) ptrace.Traces {
	td := ptrace.NewTraces()
	dstRS := td.ResourceSpans().AppendEmpty()
	if singleTrace && len(grp.scopeIndices) == rs.ScopeSpans().Len() {
		allMoved := true
		for i, sIdx := range grp.scopeIndices {
			if sIdx != i || len(grp.spanIndices[i]) != rs.ScopeSpans().At(sIdx).Spans().Len() {
				allMoved = false
				break
			}
		}
		if allMoved {
			rs.MoveTo(dstRS)
			return td
		}
	}
	rs.Resource().CopyTo(dstRS.Resource())
	dstRS.SetSchemaUrl(rs.SchemaUrl())
	scopes := rs.ScopeSpans()
	for pos, sIdx := range grp.scopeIndices {
		srcSS := scopes.At(sIdx)
		dstSS := dstRS.ScopeSpans().AppendEmpty()
		srcSS.Scope().CopyTo(dstSS.Scope())
		dstSS.SetSchemaUrl(srcSS.SchemaUrl())
		srcSpans := srcSS.Spans()
		dstSpans := dstSS.Spans()
		for _, k := range grp.spanIndices[pos] {
			srcSpans.At(k).MoveTo(dstSpans.AppendEmpty())
		}
	}
	return td
}

// measureGroupProtoSize returns the exact protobuf wire size that
// ptrace.ProtoMarshaler.ResourceSpansSize would compute for this trace's
// ResourceSpans slice, caching Resource and Scope header sizes once per batch.
func measureGroupProtoSize(rs ptrace.ResourceSpans, grp *ivmTraceGroup, sc *ivmSizeCache) int {
	var m ptrace.ProtoMarshaler
	resSize := -1
	if sc != nil {
		resSize = sc.resHeaderSize
	}
	if resSize < 0 {
		tmpRS := ptrace.NewResourceSpans()
		rs.Resource().CopyTo(tmpRS.Resource())
		tmpRS.SetSchemaUrl(rs.SchemaUrl())
		resSize = m.ResourceSpansSize(tmpRS)
		if sc != nil {
			sc.resHeaderSize = resSize
		}
	}
	size := resSize

	scopes := rs.ScopeSpans()
	for pos, sIdx := range grp.scopeIndices {
		srcSS := scopes.At(sIdx)
		scopeBase := -1
		if sc != nil && sIdx < len(sc.scopeHeaderSizes) {
			scopeBase = sc.scopeHeaderSizes[sIdx]
		}
		if scopeBase < 0 {
			tmpSS := ptrace.NewScopeSpans()
			srcSS.Scope().CopyTo(tmpSS.Scope())
			tmpSS.SetSchemaUrl(srcSS.SchemaUrl())
			scopeBase = m.ScopeSpansSize(tmpSS)
			if sc != nil && sIdx < len(sc.scopeHeaderSizes) {
				sc.scopeHeaderSizes[sIdx] = scopeBase
			}
		}
		ssInner := scopeBase
		srcSpans := srcSS.Spans()
		for _, k := range grp.spanIndices[pos] {
			spSize := m.SpanSize(srcSpans.At(k))
			ssInner += 1 + uvarintLen(uint64(spSize)) + spSize
		}
		size += 1 + uvarintLen(uint64(ssInner)) + ssInner
	}
	return size
}

func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}
