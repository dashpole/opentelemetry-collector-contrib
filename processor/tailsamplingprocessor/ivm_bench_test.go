// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tailsamplingprocessor

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/metrics"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/metadata"
)

// ---------------------------------------------------------------------------
// Bakeoff Workload Definitions & Deterministic 3-Phase Trace Stream Generator
// ---------------------------------------------------------------------------

const (
	bakeoffCycleTraces = 300 // exact multiple of len(benchTraceSizes)=6 and 100
	bakeoffGroupTraces = 100 // 3 groups per 300-trace cycle
	bakeoffPhases      = 3   // every multi-span trace split across 3 incoming batches
)

// bakeoffTemplateGroup holds the 3 staggered arrival phase batches for 100 traces.
type bakeoffTemplateGroup struct {
	phases     [bakeoffPhases]ptrace.Traces
	totalSpans int64
}

func bakeoffPoliciesForWorkload(workload string) ([]PolicyCfg, error) {
	switch workload {
	case "w1_err1", "w1_err5":
		return []PolicyCfg{
			{
				sharedPolicyCfg: sharedPolicyCfg{
					Name:          "errors",
					Type:          StatusCode,
					StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}},
				},
			},
			{
				sharedPolicyCfg: sharedPolicyCfg{
					Name:       "slow-traces",
					Type:       Latency,
					LatencyCfg: LatencyCfg{ThresholdMs: 500},
				},
			},
			{
				sharedPolicyCfg: sharedPolicyCfg{
					Name: "prob-1pct",
					Type: Probabilistic,
					ProbabilisticCfg: ProbabilisticCfg{
						HashSalt:           "bench-w1",
						SamplingPercentage: 1,
					},
				},
			},
		}, nil
	case "w2":
		return []PolicyCfg{
			{
				sharedPolicyCfg: sharedPolicyCfg{
					Name: "drop-healthz",
					Type: Drop,
				},
				DropCfg: DropCfg{
					SubPolicyCfg: []AndSubPolicyCfg{
						{
							sharedPolicyCfg: sharedPolicyCfg{
								Name: "healthz-route",
								Type: StringAttribute,
								StringAttributeCfg: StringAttributeCfg{
									Key:    "http.route",
									Values: []string{"/healthz"},
								},
							},
						},
					},
				},
			},
			{
				sharedPolicyCfg: sharedPolicyCfg{
					Name:          "errors",
					Type:          StatusCode,
					StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}},
				},
			},
			{
				sharedPolicyCfg: sharedPolicyCfg{
					Name: "and-svc1-http500",
					Type: And,
				},
				AndCfg: AndCfg{
					SubPolicyCfg: []AndSubPolicyCfg{
						{
							sharedPolicyCfg: sharedPolicyCfg{
								Name: "svc1",
								Type: StringAttribute,
								StringAttributeCfg: StringAttributeCfg{
									Key:    "service.name",
									Values: []string{"service-1"},
								},
							},
						},
						{
							sharedPolicyCfg: sharedPolicyCfg{
								Name: "http-500",
								Type: NumericAttribute,
								NumericAttributeCfg: NumericAttributeCfg{
									Key:      "http.response.status_code",
									MinValue: 500,
									MaxValue: 599,
								},
							},
						},
					},
				},
			},
			{
				sharedPolicyCfg: sharedPolicyCfg{
					Name:       "slow-traces",
					Type:       Latency,
					LatencyCfg: LatencyCfg{ThresholdMs: 500},
				},
			},
		}, nil
	case "w3":
		return []PolicyCfg{
			{
				sharedPolicyCfg: sharedPolicyCfg{
					Name:          "errors",
					Type:          StatusCode,
					StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}},
				},
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown bakeoff workload %q", workload)
	}
}

func bakeoffShapeForName(name string) (benchShape, bool, error) {
	switch name {
	case "mixed":
		return benchShapes[0], false, nil
	case "resource-per-trace":
		return benchShapes[1], false, nil
	case "few-traces-per-resource":
		return benchShapes[2], false, nil
	case "sdk":
		return benchShapes[3], false, nil
	case "ephemeral-unique-res":
		return benchShapes[1], true, nil
	default:
		return benchShape{}, false, fmt.Errorf("unknown bakeoff shape %q", name)
	}
}

// buildBakeoffTemplateCycle constructs 3 template groups (300 traces total, 100 traces
// per group), each partitioned into 3 arrival phase batches. Every trace carries 14
// resource attributes, 2 scopes, and 7 base span attributes matching generateBenchBatchShaped.
func buildBakeoffTemplateCycle(workload, shapeName string, outOfOrder bool) ([]bakeoffTemplateGroup, bool, error) {
	shape, ephemeralRes, err := bakeoffShapeForName(shapeName)
	if err != nil {
		return nil, false, err
	}
	if _, err := bakeoffPoliciesForWorkload(workload); err != nil {
		return nil, false, err
	}

	numGroups := bakeoffCycleTraces / bakeoffGroupTraces
	groups := make([]bakeoffTemplateGroup, numGroups)
	baseStart := time.Unix(1700000000, 0)

	// Track non-dropped trace index across the 300-trace cycle for exact W2 rates.
	nonDroppedIdx := 0
	var spanID uint64

	for g := range numGroups {
		numRes := shape.numResources(bakeoffGroupTraces)
		// Initialize the 3 phase batches with matching resource/scope shells,
		// then prune empty scopes/resources at the end so empty containers never pollute batches.
		var phaseTraces [bakeoffPhases]ptrace.Traces
		var phaseSpans [bakeoffPhases][][2]ptrace.SpanSlice
		for p := range bakeoffPhases {
			td := ptrace.NewTraces()
			spansByRes := make([][2]ptrace.SpanSlice, numRes)
			for r := range numRes {
				rs := td.ResourceSpans().AppendEmpty()
				attrs := rs.Resource().Attributes()
				svcIdx := r % 4
				attrs.PutStr("service.name", fmt.Sprintf("service-%d", svcIdx))
				attrs.PutStr("service.namespace", "shop")
				attrs.PutStr("service.version", "1.2.3")
				attrs.PutStr("service.instance.id", fmt.Sprintf("service-%d-7d9f8b6c5-x2k4p", r))
				attrs.PutStr("deployment.environment.name", "production")
				attrs.PutStr("telemetry.sdk.name", "opentelemetry")
				attrs.PutStr("telemetry.sdk.language", "go")
				attrs.PutStr("telemetry.sdk.version", "1.38.0")
				attrs.PutStr("host.name", fmt.Sprintf("ip-10-0-0-%d.eu-west-1.compute.internal", r))
				attrs.PutStr("cloud.region", "eu-west-1")
				attrs.PutStr("k8s.cluster.name", "prod-eu-west-1")
				attrs.PutStr("k8s.namespace.name", "shop")
				attrs.PutStr("k8s.deployment.name", fmt.Sprintf("service-%d", svcIdx))
				attrs.PutStr("k8s.pod.name", fmt.Sprintf("service-%d-7d9f8b6c5-x2k4p", r))
				if ephemeralRes {
					attrs.PutStr("k8s.pod.uid", "pod-000000000000-000")
				}
				for sIdx, scopeName := range []string{"net/http", "database/sql"} {
					ss := rs.ScopeSpans().AppendEmpty()
					ss.Scope().SetName(scopeName)
					ss.Scope().SetVersion("0.63.0")
					spansByRes[r][sIdx] = ss.Spans()
				}
			}
			phaseTraces[p] = td
			phaseSpans[p] = spansByRes
		}

		var groupSpans int64
		for localIdx := range bakeoffGroupTraces {
			globalIdx := g*bakeoffGroupTraces + localIdx
			traceID := uInt64ToTraceID(uint64(localIdx + 1))
			size := benchTraceSizes[globalIdx%len(benchTraceSizes)]
			rootID := spanID + 1

			isDrop := false
			isError := false
			isCrossBatchAND := false
			isCrossBatchLatency := false

			switch workload {
			case "w1_err1":
				isError = globalIdx%100 == 7
				isCrossBatchLatency = globalIdx%100 == 10 || globalIdx%100 == 40 || globalIdx%100 == 70
			case "w1_err5":
				isError = globalIdx%20 == 7
				isCrossBatchLatency = globalIdx%100 == 10 || globalIdx%100 == 40 || globalIdx%100 == 70
			case "w2":
				if globalIdx%100 < 35 {
					isDrop = true
				} else {
					nd := nonDroppedIdx
					nonDroppedIdx++
					isError = nd%50 == 0
					// Cross-batch AND requires at least 4 spans and >= 2 resources (mixed/few-traces/resource-per-trace)
					// so batch 0 carries service-1 and batch 1 carries http.response.status_code==500 on service-0.
					if !isError && nd%33 == 1 && size >= 4 && numRes >= 2 {
						isCrossBatchAND = true
					}
					if !isError && !isCrossBatchAND && nd%33 == 2 {
						isCrossBatchLatency = true
					}
				}
			case "w3":
				// 0% error, 0% drop, 0% short-circuit.
			}

			// Deterministic span-to-arrival-phase mapping.
			spanPhase := func(j int) int {
				if size == 1 {
					return 0
				}
				if size == 2 {
					if outOfOrder && !isDrop && globalIdx%2 == 1 {
						return 1 - j
					}
					return j
				}
				base := j % bakeoffPhases
				if !outOfOrder || isDrop || isCrossBatchAND {
					return base
				}
				// Deterministic out-of-order phase permutation per trace.
				switch globalIdx % 3 {
				case 1:
					return (base + 1) % bakeoffPhases
				case 2:
					return (base + 2) % bakeoffPhases
				default:
					return base
				}
			}

			errSpanIdx := (globalIdx*7 + 3) % size
			// Choose distinct early/late latency spans in different arrival phases when size >= 2.
			latEarlySpan := 0
			latLateSpan := 0
			if isCrossBatchLatency && size >= 2 {
				p0 := spanPhase(0)
				for j := 1; j < size; j++ {
					if spanPhase(j) != p0 {
						latLateSpan = j
						break
					}
				}
			}

			for j := range size {
				spanID++
				groupSpans++
				p := spanPhase(j)
				resIdx := shape.resourceFor(localIdx, j)

				// For W2 cross-batch AND, explicitly separate service-1 from
				// http.response.status_code==500 (on service-0 with StatusCodeUnset)
				// so neither batch alone matches AND and status_code=ERROR cannot subsume it.
				// When outOfOrder is true, reverse phase 0 and phase 1 on half of these traces
				// so 500 arrives before service-1.
				reverseAND := isCrossBatchAND && outOfOrder && (globalIdx%2 == 1)
				if isCrossBatchAND {
					if (!reverseAND && p == 0) || (reverseAND && p == 1) {
						resIdx = 1 % numRes // service-1
					} else {
						resIdx = 0 // service-0 (never service-1)
					}
				}

				span := phaseSpans[p][resIdx][j%2].AppendEmpty()
				span.SetTraceID(traceID)
				span.SetSpanID(uInt64ToSpanID(spanID))
				if j > 0 {
					span.SetParentSpanID(uInt64ToSpanID(rootID))
				}
				span.SetName("GET /api/v1/items/{id}")
				span.SetKind(ptrace.SpanKindServer)

				spStart := baseStart
				spEnd := baseStart.Add(500 * time.Microsecond)
				if isCrossBatchLatency {
					if size == 1 {
						spStart = baseStart
						spEnd = baseStart.Add(700 * time.Millisecond)
					} else {
						switch j {
						case latEarlySpan:
							spStart = baseStart
							spEnd = baseStart.Add(1 * time.Millisecond)
						case latLateSpan:
							spStart = baseStart.Add(699 * time.Millisecond)
							spEnd = baseStart.Add(700 * time.Millisecond)
						default:
							spStart = baseStart.Add(10 * time.Millisecond)
							spEnd = baseStart.Add(11 * time.Millisecond)
						}
					}
				}
				span.SetStartTimestamp(pcommon.NewTimestampFromTime(spStart))
				span.SetEndTimestamp(pcommon.NewTimestampFromTime(spEnd))

				attrs := span.Attributes()
				attrs.PutStr("http.request.method", "GET")
				route := "/api/v1/items/{id}"
				if isDrop && j == 0 {
					route = "/healthz"
				}
				attrs.PutStr("http.route", route)
				statusCode := int64(200)
				if isCrossBatchAND && ((!reverseAND && p == 1 && j == 1) || (reverseAND && p == 0 && j == 0)) {
					statusCode = 500
					// Explicitly leave StatusCodeUnset so status_code=ERROR cannot subsume AND.
				}
				attrs.PutInt("http.response.status_code", statusCode)
				attrs.PutStr("server.address", "items.shop.svc.cluster.local")
				attrs.PutInt("server.port", 8080)
				attrs.PutStr("url.path", fmt.Sprintf("/api/v1/items/%d", globalIdx))
				attrs.PutStr("user_agent.original", "Mozilla/5.0 (X11; Linux x86_64)")

				if isError && j == errSpanIdx {
					span.Status().SetCode(ptrace.StatusCodeError)
					attrs.PutInt("http.response.status_code", 500)
				}
			}
		}

		// Prune empty ScopeSpans / ResourceSpans from each phase template.
		for p := range bakeoffPhases {
			td := phaseTraces[p]
			td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
				rs.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
					return ss.Spans().Len() == 0
				})
				return rs.ScopeSpans().Len() == 0
			})
		}

		groups[g] = bakeoffTemplateGroup{
			phases:     phaseTraces,
			totalSpans: groupSpans,
		}
	}

	return groups, ephemeralRes, nil
}

// splitmix64 is a bijective 64-bit mixing permutation used to give every trace
// independent high bits for FNV-1a probabilistic sampling and collision-free
// decision-cache keys (which key on the second 8 bytes of TraceID).
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// cloneBakeoffPhaseBatch clones template and stamps globally unique TraceIDs keyed by
// (workerID, groupSeq, localTraceIdx). When ephemeralRes is true, it also stamps a
// globally unique k8s.pod.uid per (workerID, groupSeq, localTraceIdx) so every trace
// owns a unique Resource across the entire run.
func cloneBakeoffPhaseBatch(template ptrace.Traces, workerID uint64, groupSeq uint64, ephemeralRes bool) ptrace.Traces {
	batch := ptrace.NewTraces()
	template.CopyTo(batch)

	hi := (workerID << 56) | (groupSeq & 0x00ffffffffffffff)
	loBase := ((workerID & 0xffff) << 48) | ((groupSeq & 0xffffffff) << 16)

	rss := batch.ResourceSpans()
	for r := range rss.Len() {
		rs := rss.At(r)
		var rsLocalID uint64
		hasLocalID := false
		sss := rs.ScopeSpans()
		for s := range sss.Len() {
			spans := sss.At(s).Spans()
			for i := range spans.Len() {
				sp := spans.At(i)
				id := sp.TraceID()
				localLow := binary.BigEndian.Uint64(id[8:]) & 0xffff
				if !hasLocalID {
					rsLocalID = localLow
					hasLocalID = true
				}
				lo := splitmix64(loBase | localLow)
				if hi == 0 && lo == 0 {
					lo = 1
				}
				binary.BigEndian.PutUint64(id[:8], hi)
				binary.BigEndian.PutUint64(id[8:], lo)
				sp.SetTraceID(id)
			}
		}
		if ephemeralRes && hasLocalID {
			rs.Resource().Attributes().PutStr("k8s.pod.uid", fmt.Sprintf("pod-%02x%010x-%03d", workerID, groupSeq, rsLocalID))
		}
	}
	return batch
}

// ---------------------------------------------------------------------------
// Candidate Config Builder
// ---------------------------------------------------------------------------

func applyBakeoffCandidateConfig(cfg *Config, candidate string, bypassSplit bool) error {
	switch candidate {
	case "trace-complete":
		cfg.SamplingStrategy = samplingStrategyTraceComplete
	case "span-ingest":
		cfg.SamplingStrategy = samplingStrategySpanIngest
	case "ivm-raw-slab":
		cfg.SamplingStrategy = samplingStrategyIncremental
		cfg.Options = append(cfg.Options, withArenaStorageMode(arenaModeRawProtoSlab), withIncrementalBypassSplit(bypassSplit))
	case "ivm-interned-heap":
		cfg.SamplingStrategy = samplingStrategyIncremental
		cfg.Options = append(cfg.Options, withArenaStorageMode(arenaModeInternedHeap), withIncrementalBypassSplit(bypassSplit))
	case "ivm-interned-mmap":
		cfg.SamplingStrategy = samplingStrategyIncremental
		cfg.Options = append(cfg.Options, withArenaStorageMode(arenaModeInternedMmap), withIncrementalBypassSplit(bypassSplit))
	case "ivm-lz4-heap":
		cfg.SamplingStrategy = samplingStrategyIncremental
		cfg.Options = append(cfg.Options, withArenaStorageMode(arenaModeLZ4Heap), withIncrementalBypassSplit(bypassSplit))
	case "ivm-lz4-mmap":
		cfg.SamplingStrategy = samplingStrategyIncremental
		cfg.Options = append(cfg.Options, withArenaStorageMode(arenaModeLZ4Mmap), withIncrementalBypassSplit(bypassSplit))
	default:
		return fmt.Errorf("unknown bakeoff candidate %q", candidate)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Zero-Dependency Lightweight Telemetry Recorder (Tick Latency & Decisions)
// ---------------------------------------------------------------------------

type bakeoffMeterProvider struct {
	noop.MeterProvider
	rec *bakeoffMetricsRecorder
}

type bakeoffMeter struct {
	noop.Meter
	rec *bakeoffMetricsRecorder
}

type bakeoffTickHistogram struct {
	noop.Int64Histogram
	rec *bakeoffMetricsRecorder
}

type bakeoffDecisionCounter struct {
	noop.Int64Counter
	rec  *bakeoffMetricsRecorder
	kind string
}

const bakeoffTickHistBuckets = 4096 // exact 1 ms buckets for 0..4095 ms

type bakeoffMetricsRecorder struct {
	recording    atomic.Bool
	tickBuckets  [bakeoffTickHistBuckets]atomic.Int64
	tickOverflow atomic.Int64
	tickCount    atomic.Int64
	tickMaxMs    atomic.Int64

	decidedSampled    atomic.Int64
	decidedNotSampled atomic.Int64
	decidedDropped    atomic.Int64
	droppedTooEarly   atomic.Int64
}

func newBakeoffMeterProvider() (*bakeoffMeterProvider, *bakeoffMetricsRecorder) {
	rec := &bakeoffMetricsRecorder{}
	return &bakeoffMeterProvider{
		MeterProvider: noop.NewMeterProvider(),
		rec:           rec,
	}, rec
}

func (r *bakeoffMetricsRecorder) tickPercentiles() (count int, p50, p95, p99, maxMs float64) {
	total := r.tickCount.Load()
	if total == 0 {
		return 0, 0, 0, 0, 0
	}
	maxV := float64(r.tickMaxMs.Load())
	quantile := func(q float64) float64 {
		target := int64(math.Ceil(q * float64(total)))
		if target < 1 {
			target = 1
		}
		var cum int64
		for ms := range bakeoffTickHistBuckets {
			cum += r.tickBuckets[ms].Load()
			if cum >= target {
				return float64(ms)
			}
		}
		return maxV
	}
	return int(total), quantile(0.50), quantile(0.95), quantile(0.99), maxV
}

func (p *bakeoffMeterProvider) Meter(_ string, _ ...metric.MeterOption) metric.Meter {
	return &bakeoffMeter{
		rec: p.rec,
	}
}

func (m *bakeoffMeter) Int64Histogram(name string, opts ...metric.Int64HistogramOption) (metric.Int64Histogram, error) {
	base, err := m.Meter.Int64Histogram(name, opts...)
	if name == "otelcol_processor_tail_sampling_sampling_decision_timer_latency" {
		return &bakeoffTickHistogram{
			rec: m.rec,
		}, nil
	}
	return base, err
}

func (m *bakeoffMeter) Int64Counter(name string, opts ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	base, err := m.Meter.Int64Counter(name, opts...)
	switch name {
	case "otelcol_processor_tail_sampling_global_count_traces_sampled":
		return &bakeoffDecisionCounter{rec: m.rec, kind: "global"}, nil
	case "otelcol_processor_tail_sampling_sampling_trace_dropped_too_early":
		return &bakeoffDecisionCounter{rec: m.rec, kind: "dropped_too_early"}, nil
	}
	return base, err
}

func (h *bakeoffTickHistogram) Record(_ context.Context, value int64, _ ...metric.RecordOption) {
	if !h.rec.recording.Load() {
		return
	}
	if value < 0 {
		value = 0
	}
	if value < bakeoffTickHistBuckets {
		h.rec.tickBuckets[value].Add(1)
	} else {
		h.rec.tickOverflow.Add(1)
	}
	h.rec.tickCount.Add(1)
	for {
		cur := h.rec.tickMaxMs.Load()
		if value <= cur || h.rec.tickMaxMs.CompareAndSwap(cur, value) {
			break
		}
	}
}

func (c *bakeoffDecisionCounter) Add(_ context.Context, incr int64, opts ...metric.AddOption) {
	if c.kind == "dropped_too_early" {
		c.rec.droppedTooEarly.Add(incr)
		return
	}
	cfg := metric.NewAddConfig(opts)
	attrs := cfg.Attributes()
	if val, ok := attrs.Value(attribute.Key("decision")); ok {
		switch val.AsString() {
		case "sampled":
			c.rec.decidedSampled.Add(incr)
		case "not_sampled":
			c.rec.decidedNotSampled.Add(incr)
		case "dropped":
			c.rec.decidedDropped.Add(incr)
		}
	}
}

// ---------------------------------------------------------------------------
// Correctness Gate #3: 100% Sampled-TraceID Parity on W1 & W2 Benchmark Streams
// ---------------------------------------------------------------------------

type traceIDCollectingSink struct {
	mu    sync.Mutex
	ids   map[pcommon.TraceID]int
	spans int64
}

func newTraceIDCollectingSink() *traceIDCollectingSink {
	return &traceIDCollectingSink{ids: make(map[pcommon.TraceID]int)}
}

func (*traceIDCollectingSink) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (s *traceIDCollectingSink) ConsumeTraces(_ context.Context, td ptrace.Traces) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rss := td.ResourceSpans()
	for r := range rss.Len() {
		sss := rss.At(r).ScopeSpans()
		for sc := range sss.Len() {
			spans := sss.At(sc).Spans()
			for i := range spans.Len() {
				tid := spans.At(i).TraceID()
				s.ids[tid]++
				s.spans++
			}
		}
	}
	return nil
}

func runBakeoffParityStream(
	t *testing.T,
	workload string,
	outOfOrder bool,
	candidate string,
	numShards uint32,
	numCycles int,
) map[pcommon.TraceID]int {
	t.Helper()
	templates, ephemeralRes, err := buildBakeoffTemplateCycle(workload, "mixed", outOfOrder)
	require.NoError(t, err)
	policies, err := bakeoffPoliciesForWorkload(workload)
	require.NoError(t, err)

	sink := newTraceIDCollectingSink()
	cfg := Config{
		DecisionWait:            3 * time.Second,
		NumTraces:               50_000,
		ExpectedNewTracesPerSec: 1_000,
		NumShards:               numShards,
		PolicyCfgs:              policies,
		DecisionCache: DecisionCacheConfig{
			SampledCacheSize:    50_000,
			NonSampledCacheSize: 50_000,
		},
	}
	var ctrl *testTSPController
	if numShards == 1 {
		ctrl = newTestTSPController()
		cfg.Options = append(cfg.Options, withTestController(ctrl), withIDBatcher())
	} else {
		// Use a wide decision window so all 3 staggered phases finish arriving before
		// Shutdown() deterministically flushes every remaining decision batch.
		cfg.DecisionWait = 30 * time.Second
		cfg.Options = append(cfg.Options, withTickerFrequency(100*time.Millisecond))
	}
	require.NoError(t, applyBakeoffCandidateConfig(&cfg, candidate, true))

	p, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), sink, cfg)
	require.NoError(t, err)
	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))

	// Emit all 3 phases per trace group.
	groupSeq := uint64(1)
	for range numCycles {
		for _, grp := range templates {
			for phase := range bakeoffPhases {
				td := cloneBakeoffPhaseBatch(grp.phases[phase], 0, groupSeq, ephemeralRes)
				if td.SpanCount() > 0 {
					require.NoError(t, p.ConsumeTraces(t.Context(), td))
				}
			}
			groupSeq++
		}
	}

	if ctrl != nil {
		for range 5 {
			ctrl.waitForTick()
		}
	}
	// Shutdown with DropPendingTracesOnShutdown=false flushes every remaining decision batch deterministically.
	require.NoError(t, p.Shutdown(t.Context()))
	return sink.ids
}

// TestIVMBakeoffBenchmarkStreamParity enforces M3 Correctness Gate #3:
// 100% sampled-TraceID set parity against Baseline A (trace-complete) on the exact
// W1 (1% & 5% error) and W2 (35% drop + 2% error + cross-batch AND + cross-batch latency)
// benchmark generators under both in-order and out-of-order 3-batch arrivals, plus
// non-tautological proof that Baseline B (span-ingest) fails parity on both streams.
func TestIVMBakeoffBenchmarkStreamParity(t *testing.T) {
	candidates := []string{
		"ivm-raw-slab",
		"ivm-interned-heap",
		"ivm-interned-mmap",
		"ivm-lz4-heap",
		"ivm-lz4-mmap",
	}

	for _, workload := range []string{"w1_err1", "w1_err5", "w2"} {
		for _, outOfOrder := range []bool{false, true} {
			orderLabel := "in_order"
			if outOfOrder {
				orderLabel = "out_of_order"
			}
			t.Run(fmt.Sprintf("%s/%s", workload, orderLabel), func(t *testing.T) {
				// 2 cycles = 600 traces = 7,900 spans per workload/order pair.
				baseIDs := runBakeoffParityStream(t, workload, outOfOrder, "trace-complete", 1, 2)
				require.NotEmpty(t, baseIDs, "baseline must sample traces on %s", workload)
				// Verify probabilistic(1%) contributes positive non-error/non-latency matches on W1
				// (600 traces have 6 error + 18 latency = 24 deterministic traces on w1_err1,
				// and 30 error + 18 latency = 48 deterministic traces on w1_err5).
				if workload == "w1_err1" {
					require.Greater(t, len(baseIDs), 24, "w1_err1 must include positive probabilistic(1%%) matches beyond 24 error+latency traces")
				} else if workload == "w1_err5" {
					require.Greater(t, len(baseIDs), 48, "w1_err5 must include positive probabilistic(1%%) matches beyond 48 error+latency traces")
				}

				// Non-tautological negative control: prove Baseline B (span-ingest) fails parity!
				spanIngestIDs := runBakeoffParityStream(t, workload, outOfOrder, "span-ingest", 1, 2)
				if workload == "w2" {
					require.Empty(t, spanIngestIDs, "span-ingest must drop 100%% of traces on W2 due to upstream drop bug")
				} else {
					// On W1, span-ingest misses cross-batch latency traces.
					require.Less(t, len(spanIngestIDs), len(baseIDs), "span-ingest must miss cross-batch latency traces on %s", workload)
				}

				for _, cand := range candidates {
					gotIDs := runBakeoffParityStream(t, workload, outOfOrder, cand, 1, 2)
					require.Equal(t, baseIDs, gotIDs, "candidate %s (shards=1) must match trace-complete TraceIDs & span counts 100%% on %s/%s", cand, workload, orderLabel)
				}

				// Also verify multi-shard (num_shards=4) exact TraceID set & span count parity.
				got4 := runBakeoffParityStream(t, workload, outOfOrder, "ivm-lz4-mmap", 4, 2)
				require.Equal(t, baseIDs, got4, "ivm-lz4-mmap (shards=4) must match trace-complete 100%% on %s/%s", workload, orderLabel)
			})
		}
	}

	// F5: Explicit W2 late-phase drop > sample precedence + buffered arena deletion parity.
	t.Run("w2/late_drop_overrides_positive_and_frees_arena", func(t *testing.T) {
		policies, err := bakeoffPoliciesForWorkload("w2")
		require.NoError(t, err)
		runLateDrop := func(cand string, numShards uint32) (map[pcommon.TraceID]int, uint64) {
			sink := newTraceIDCollectingSink()
			cfg := Config{
				DecisionWait:            30 * time.Second,
				NumTraces:               1000,
				ExpectedNewTracesPerSec: 100,
				NumShards:               numShards,
				PolicyCfgs:              policies,
				DecisionCache: DecisionCacheConfig{
					SampledCacheSize:    1000,
					NonSampledCacheSize: 1000,
				},
			}
			var ctrl *testTSPController
			if numShards == 1 {
				ctrl = newTestTSPController()
				cfg.Options = append(cfg.Options, withTestController(ctrl), withIDBatcher())
			} else {
				cfg.Options = append(cfg.Options, withTickerFrequency(100*time.Millisecond))
			}
			require.NoError(t, applyBakeoffCandidateConfig(&cfg, cand, true))
			p, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), sink, cfg)
			require.NoError(t, err)
			require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))

			const nTraces = 20
			baseStart := time.Unix(1700000000, 0)
			// Phase 0: error span (matches positive status_code=ERROR, MUST buffer while drop is pending).
			td0 := ptrace.NewTraces()
			rs0 := td0.ResourceSpans().AppendEmpty()
			rs0.Resource().Attributes().PutStr("service.name", "service-1")
			sp0s := rs0.ScopeSpans().AppendEmpty().Spans()
			for i := range nTraces {
				tid := uInt64ToTraceID(splitmix64(uint64(i + 100)))
				sp := sp0s.AppendEmpty()
				sp.SetTraceID(tid)
				sp.SetSpanID(uInt64ToSpanID(uint64(i*2 + 1)))
				sp.SetStartTimestamp(pcommon.NewTimestampFromTime(baseStart))
				sp.SetEndTimestamp(pcommon.NewTimestampFromTime(baseStart.Add(time.Millisecond)))
				sp.Status().SetCode(ptrace.StatusCodeError)
				sp.Attributes().PutStr("http.route", "/api/v1/items")
				sp.Attributes().PutInt("http.response.status_code", 500)
			}
			require.NoError(t, p.ConsumeTraces(t.Context(), td0))

			// Phase 1: late /healthz span on half the traces (odd i -> must drop & delete buffered arena chunks);
			// normal span on even i (must stay buffered and sample at tick).
			td1 := ptrace.NewTraces()
			rs1 := td1.ResourceSpans().AppendEmpty()
			rs1.Resource().Attributes().PutStr("service.name", "service-0")
			sp1s := rs1.ScopeSpans().AppendEmpty().Spans()
			for i := range nTraces {
				tid := uInt64ToTraceID(splitmix64(uint64(i + 100)))
				sp := sp1s.AppendEmpty()
				sp.SetTraceID(tid)
				sp.SetSpanID(uInt64ToSpanID(uint64(i*2 + 2)))
				sp.SetStartTimestamp(pcommon.NewTimestampFromTime(baseStart.Add(2 * time.Millisecond)))
				sp.SetEndTimestamp(pcommon.NewTimestampFromTime(baseStart.Add(3 * time.Millisecond)))
				if i%2 == 1 {
					sp.Attributes().PutStr("http.route", "/healthz")
				} else {
					sp.Attributes().PutStr("http.route", "/api/v1/items")
				}
			}
			require.NoError(t, p.ConsumeTraces(t.Context(), td1))

			var liveChunksPreTick uint64
			if ctrl != nil {
				// Synchronize shard 0 by waiting for 1 tick (well before DecisionWait=30s) or checking post-phase1.
				ctrl.waitForTick()
				_, _, _, _, _, st := collectAllShardIVMStats(p.(*shardedProcessor))
				liveChunksPreTick = st.LiveChunks
			}
			require.NoError(t, p.Shutdown(t.Context()))
			return sink.ids, liveChunksPreTick
		}

		baseIDs, _ := runLateDrop("trace-complete", 1)
		require.Len(t, baseIDs, 10, "half of overlapping error+/healthz traces must be dropped and half sampled")
		for _, cand := range candidates {
			gotIDs, liveChunks := runLateDrop(cand, 1)
			require.Equal(t, baseIDs, gotIDs, "late /healthz drop parity failed for %s", cand)
			// Only the 10 surviving traces (2 chunks each = 20 chunks) may remain; the 10 dropped traces' phase-0 chunks must be freed!
			require.Equal(t, uint64(20), liveChunks, "late /healthz drop must free buffered phase-0 arena chunks for %s", cand)
		}
		got4, _ := runLateDrop("ivm-lz4-mmap", 4)
		require.Equal(t, baseIDs, got4, "late /healthz drop parity failed for ivm-lz4-mmap shards=4")
	})
}

// ---------------------------------------------------------------------------
// Isolated Subprocess Bakeoff Harness (W4: 50k / 100k / 200k spans/sec & Unpaced)
// ---------------------------------------------------------------------------

type bakeoffResult struct {
	Candidate       string  `json:"candidate"`
	Workload        string  `json:"workload"`
	Shape           string  `json:"shape"`
	BypassSplit     bool    `json:"bypass_split"`
	NumShards       uint32  `json:"num_shards"`
	TargetRateSPS   int64   `json:"target_rate_sps"`
	DecisionWaitSec int     `json:"decision_wait_sec"`
	WarmupSec       int     `json:"warmup_sec"`
	MeasureSec      int     `json:"measure_sec"`
	Workers         int     `json:"workers"`
	GOGC            string  `json:"gogc"`
	GoVersion       string  `json:"go_version"`
	AchievedSPS     float64 `json:"achieved_sps"`
	MeasuredSpans   int64   `json:"measured_spans"`
	SampledSpansOut int64   `json:"sampled_spans_out"`

	// Live steady-state memory (STW-free runtime/metrics + /proc/self/status every 250ms, NO forced GC)
	SteadyHeapAllocBytes   uint64 `json:"steady_heap_alloc_bytes"`
	PeakHeapAllocBytes     uint64 `json:"peak_heap_alloc_bytes"`
	SteadyHeapInuseBytes   uint64 `json:"steady_heap_inuse_bytes"`
	PeakHeapInuseBytes     uint64 `json:"peak_heap_inuse_bytes"`
	SteadyHeapObjects      uint64 `json:"steady_heap_objects"`
	PeakHeapObjects        uint64 `json:"peak_heap_objects"`
	WindowTotalAllocBytes  uint64 `json:"window_total_alloc_bytes"`
	SteadyVmRSSBytes       uint64 `json:"steady_vm_rss_bytes"`
	PeakVmRSSBytes         uint64 `json:"peak_vm_rss_bytes"`
	VmHWMBytes             uint64 `json:"vm_hwm_bytes"`
	SteadyGCScanHeapBytes  uint64 `json:"steady_gc_scan_heap_bytes"`
	PeakGCScanHeapBytes    uint64 `json:"peak_gc_scan_heap_bytes"`

	// End-of-window post-runtime.GC() retained snapshot (captured before Shutdown)
	RetainedHeapAllocBytes  uint64 `json:"retained_heap_alloc_bytes"`
	RetainedHeapObjects     uint64 `json:"retained_heap_objects"`
	RetainedVmRSSBytes      uint64 `json:"retained_vm_rss_bytes"`
	RetainedGCScanHeapBytes uint64 `json:"retained_gc_scan_heap_bytes"`
	TracesOnMemoryAtEnd     int64  `json:"traces_on_memory_at_window_end"`
	TracesOnMemoryAtSnap    int64  `json:"traces_on_memory_at_snap"`

	// Go runtime/metrics GC profile over the steady-state measurement window
	WindowGCCycles         uint64  `json:"window_gc_cycles"`
	WindowAutoGCCycles     uint64  `json:"window_auto_gc_cycles"`
	WindowGCCPUSeconds     float64 `json:"window_gc_cpu_seconds"`
	WindowGCMarkAssistSec  float64 `json:"window_gc_mark_assist_seconds"`
	WindowGCPauseCPUSec    float64 `json:"window_gc_pause_cpu_seconds"`
	WindowUserCPUSeconds   float64 `json:"window_user_cpu_seconds"`
	WindowTotalCPUSeconds  float64 `json:"window_total_cpu_seconds"`
	GCPauseP50Ms           float64 `json:"gc_pause_p50_ms"`
	GCPauseP95Ms           float64 `json:"gc_pause_p95_ms"`
	GCPauseP99Ms           float64 `json:"gc_pause_p99_ms"`
	GCPauseMaxMs           float64 `json:"gc_pause_max_ms"`

	// Ingest & tick latencies during measurement window
	IngestP50Us float64 `json:"ingest_p50_us"`
	IngestP95Us float64 `json:"ingest_p95_us"`
	IngestP99Us float64 `json:"ingest_p99_us"`
	IngestMaxUs float64 `json:"ingest_max_us"`
	TickCount   int     `json:"tick_count"`
	TickP50Ms   float64 `json:"tick_p50_ms"`
	TickP95Ms   float64 `json:"tick_p95_ms"`
	TickP99Ms   float64 `json:"tick_p99_ms"`
	TickMaxMs   float64 `json:"tick_max_ms"`

	// Decisions, zero-unmarshal drop ratio (post-Shutdown exact totals), & arena stats
	WindowDecidedSampled        int64   `json:"window_decided_sampled"`
	WindowDecidedNotSampled     int64   `json:"window_decided_not_sampled"`
	WindowDecidedDropped        int64   `json:"window_decided_dropped"`
	TotalDecidedSampled         int64   `json:"total_decided_sampled"`
	TotalDecidedNotSampled      int64   `json:"total_decided_not_sampled"`
	TotalDecidedDropped         int64   `json:"total_decided_dropped"`
	DroppedTooEarly             int64   `json:"dropped_too_early"`
	TotalDroppedWithoutUnmarsh  int64   `json:"total_dropped_without_unmarshal"`
	TotalEarlySampled           int64   `json:"total_early_sampled"`
	TotalEarlyDropped           int64   `json:"total_early_dropped"`
	TotalEarlyNotSampled        int64   `json:"total_early_not_sampled"`
	TotalUnmarshaledForSample   int64   `json:"total_unmarshaled_for_sample"`
	ZeroUnmarshalDropRatio      float64 `json:"zero_unmarshal_drop_ratio"`

	ArenaLiveSlabs          int     `json:"arena_live_slabs"`
	ArenaPooledSlabs        int     `json:"arena_pooled_slabs"`
	ArenaAllocatedBytes     uint64  `json:"arena_allocated_bytes"`
	ArenaLivePayloadBytes   uint64  `json:"arena_live_payload_bytes"`
	ArenaInternAllocBytes   uint64  `json:"arena_intern_alloc_bytes"`
	ArenaInternLiveBytes    uint64  `json:"arena_intern_live_bytes"`
	ArenaMmapActiveBytes    uint64  `json:"arena_mmap_active_bytes"`
	ArenaCompressedChunks   uint64  `json:"arena_compressed_chunks"`
	ArenaUncompressedChunks uint64  `json:"arena_uncompressed_chunks"`
	ArenaCompactionCount    uint64  `json:"arena_compaction_count"`
	ArenaUnmarshalCount     uint64  `json:"arena_unmarshal_count"`
	WindowArenaUnmarshals   uint64  `json:"window_arena_unmarshals"`
	SlabFragRatio           float64 `json:"slab_frag_ratio"`
	PostShutdownMmapBytes   uint64  `json:"post_shutdown_mmap_bytes"`
	PostShutdownLiveSlabs   int     `json:"post_shutdown_live_slabs"`
}

func readProcStatusBytes() (vmRSS, vmHWM uint64) {
	f, err := os.Open("/proc/self/status")
	if err != nil {
		return 0, 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if kb, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
					vmRSS = kb * 1024
				}
			}
		} else if strings.HasPrefix(line, "VmHWM:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				if kb, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
					vmHWM = kb * 1024
				}
			}
		}
	}
	return vmRSS, vmHWM
}

type rtMetricsSnap struct {
	heapObjectsBytes uint64
	heapUnusedBytes  uint64
	heapObjectsCount uint64
	allocsBytes      uint64
	gcScanHeapBytes  uint64
	gcCyclesTotal    uint64
	gcCyclesAuto     uint64
	gcCPUSec         float64
	gcMarkAssistSec  float64
	gcPauseCPUSec    float64
	userCPUSec       float64
	totalCPUSec      float64
	pauseBuckets     []float64
	pauseCounts      []uint64
}

func readRuntimeMetricsSnap() rtMetricsSnap {
	samples := []metrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/heap/unused:bytes"},
		{Name: "/gc/heap/objects:objects"},
		{Name: "/gc/heap/allocs:bytes"},
		{Name: "/gc/scan/heap:bytes"},
		{Name: "/gc/cycles/total:gc-cycles"},
		{Name: "/gc/cycles/automatic:gc-cycles"},
		{Name: "/cpu/classes/gc/total:cpu-seconds"},
		{Name: "/cpu/classes/gc/mark/assist:cpu-seconds"},
		{Name: "/cpu/classes/gc/pause:cpu-seconds"},
		{Name: "/cpu/classes/user:cpu-seconds"},
		{Name: "/cpu/classes/total:cpu-seconds"},
		{Name: "/gc/pauses:seconds"},
	}
	metrics.Read(samples)
	s := rtMetricsSnap{
		heapObjectsBytes: samples[0].Value.Uint64(),
		heapUnusedBytes:  samples[1].Value.Uint64(),
		heapObjectsCount: samples[2].Value.Uint64(),
		allocsBytes:      samples[3].Value.Uint64(),
		gcScanHeapBytes:  samples[4].Value.Uint64(),
		gcCyclesTotal:    samples[5].Value.Uint64(),
		gcCyclesAuto:     samples[6].Value.Uint64(),
		gcCPUSec:         samples[7].Value.Float64(),
		gcMarkAssistSec:  samples[8].Value.Float64(),
		gcPauseCPUSec:    samples[9].Value.Float64(),
		userCPUSec:       samples[10].Value.Float64(),
		totalCPUSec:      samples[11].Value.Float64(),
	}
	if hist := samples[12].Value.Float64Histogram(); hist != nil {
		s.pauseBuckets = slices.Clone(hist.Buckets)
		s.pauseCounts = slices.Clone(hist.Counts)
	}
	return s
}

func histogramQuantilesMs(startBuckets, endBuckets []float64, startCounts, endCounts []uint64) (p50, p95, p99, maxMs float64) {
	if len(endCounts) == 0 || len(endBuckets) != len(endCounts)+1 {
		return 0, 0, 0, 0
	}
	if len(startBuckets) > 0 && len(startBuckets) != len(endBuckets) {
		return 0, 0, 0, 0
	}
	delta := make([]uint64, len(endCounts))
	var total uint64
	for i := range endCounts {
		var sc uint64
		if i < len(startCounts) {
			sc = startCounts[i]
		}
		if endCounts[i] > sc {
			delta[i] = endCounts[i] - sc
			total += delta[i]
		}
	}
	if total == 0 {
		return 0, 0, 0, 0
	}
	bucketUpperMs := func(i int) float64 {
		hi := endBuckets[i+1]
		if math.IsInf(hi, 1) || math.IsNaN(hi) {
			return endBuckets[i] * 1000.0
		}
		return hi * 1000.0
	}
	quantile := func(q float64) float64 {
		target := uint64(math.Ceil(q * float64(total)))
		if target == 0 {
			target = 1
		}
		var cum uint64
		for i, c := range delta {
			cum += c
			if cum >= target {
				return bucketUpperMs(i)
			}
		}
		return 0
	}
	p50 = quantile(0.50)
	p95 = quantile(0.95)
	p99 = quantile(0.99)
	for i := len(delta) - 1; i >= 0; i-- {
		if delta[i] > 0 {
			maxMs = bucketUpperMs(i)
			break
		}
	}
	return p50, p95, p99, maxMs
}

func int64Percentiles(vals []int64, scale float64) (p50, p95, p99, maxV float64) {
	if len(vals) == 0 {
		return 0, 0, 0, 0
	}
	cp := slices.Clone(vals)
	slices.Sort(cp)
	at := func(q float64) float64 {
		idx := int(math.Ceil(q*float64(len(cp)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(cp) {
			idx = len(cp) - 1
		}
		return float64(cp[idx]) * scale
	}
	return at(0.50), at(0.95), at(0.99), float64(cp[len(cp)-1]) * scale
}

func envStr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envBool(k string, def bool) bool {
	if v := os.Getenv(k); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func collectAllShardIVMStats(sp *shardedProcessor) (
	droppedWithoutUnmarshal, earlySampled, earlyDropped, earlyNotSampled, unmarshaledForSample int64,
	arenaSum ArenaStats,
) {
	for _, s := range sp.shards {
		if s.ivm == nil {
			continue
		}
		droppedWithoutUnmarshal += s.ivm.stats.droppedWithoutUnmarshal.Load()
		earlySampled += s.ivm.stats.earlySampled.Load()
		earlyDropped += s.ivm.stats.earlyDropped.Load()
		earlyNotSampled += s.ivm.stats.earlyNotSampled.Load()
		unmarshaledForSample += s.ivm.stats.unmarshaledForSample.Load()
		if s.ivm.payloadStore != nil {
			st := s.ivm.payloadStore.Stats()
			arenaSum.LiveSlabs += st.LiveSlabs
			arenaSum.PooledSlabs += st.PooledSlabs
			arenaSum.TotalAllocatedBytes += st.TotalAllocatedBytes
			arenaSum.LivePayloadBytes += st.LivePayloadBytes
			arenaSum.LiveChunks += st.LiveChunks
			arenaSum.MmapActiveBytes += st.MmapActiveBytes
			arenaSum.ResourceInternEntries += st.ResourceInternEntries
			arenaSum.ScopeInternEntries += st.ScopeInternEntries
			arenaSum.ResourceInternBytes += st.ResourceInternBytes
			arenaSum.ScopeInternBytes += st.ScopeInternBytes
			arenaSum.InternAllocatedBytes += st.InternAllocatedBytes
			arenaSum.CompressedChunks += st.CompressedChunks
			arenaSum.UncompressedChunks += st.UncompressedChunks
			arenaSum.CompactionCount += st.CompactionCount
			arenaSum.UnmarshalCount += st.UnmarshalCount
		}
	}
	return
}

// TestIVMBakeoffSubprocess executes a single isolated bakeoff point when IVM_BAKEOFF_RUN=1.
// Always run serially in a dedicated OS subprocess with GOGC=100 so VmRSS/VmHWM and Go GC
// runtime/metrics are uncontaminated across candidates.
func TestIVMBakeoffSubprocess(t *testing.T) {
	if os.Getenv("IVM_BAKEOFF_RUN") != "1" {
		t.Skip("set IVM_BAKEOFF_RUN=1 to run isolated bakeoff subprocess")
	}

	candidate := envStr("IVM_CANDIDATE", "trace-complete")
	workload := envStr("IVM_WORKLOAD", "w1_err1")
	shapeName := envStr("IVM_SHAPE", "mixed")
	bypassSplit := envBool("IVM_BYPASS_SPLIT", true)
	numShards := uint32(envInt("IVM_SHARDS", 4))
	targetRate := int64(envInt("IVM_RATE", 100_000)) // 0 = unpaced max throughput
	decisionWaitSec := envInt("IVM_DECISION_WAIT_SEC", 10)
	warmupSec := envInt("IVM_WARMUP_SEC", 10)
	measureSec := envInt("IVM_MEASURE_SEC", 10)
	numWorkers := envInt("IVM_WORKERS", 4)
	blockOnOverflow := envBool("IVM_BLOCK_ON_OVERFLOW", targetRate == 0)

	templates, ephemeralRes, err := buildBakeoffTemplateCycle(workload, shapeName, true)
	require.NoError(t, err)
	policies, err := bakeoffPoliciesForWorkload(workload)
	require.NoError(t, err)

	mp, mrec := newBakeoffMeterProvider()
	set := processortest.NewNopSettings(metadata.Type)
	set.MeterProvider = mp

	sink := &countingSink{}
	expNewTraces := uint64(max(1000, targetRate/13))
	if targetRate == 0 {
		expNewTraces = 25_000
	}
	cfg := Config{
		DecisionWait:                time.Duration(decisionWaitSec) * time.Second,
		NumTraces:                   250_000,
		ExpectedNewTracesPerSec:     expNewTraces,
		NumShards:                   numShards,
		BlockOnOverflow:             blockOnOverflow,
		DropPendingTracesOnShutdown: true,
		PolicyCfgs:                  policies,
		DecisionCache: DecisionCacheConfig{
			SampledCacheSize:    100_000,
			NonSampledCacheSize: 200_000,
		},
	}
	require.NoError(t, applyBakeoffCandidateConfig(&cfg, candidate, bypassSplit))

	runtime.GC()
	p, err := newTracesProcessor(t.Context(), set, sink, cfg)
	require.NoError(t, err)
	sp := p.(*shardedProcessor)
	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))

	// Per-worker phase stagger offset (1s between phase 0 -> phase 1 -> phase 2).
	avgSpansPerGroup := float64(templates[0].totalSpans+templates[1].totalSpans+templates[2].totalSpans) / float64(len(templates))
	perWorkerRate := float64(targetRate) / float64(numWorkers)
	groupsPerSecPerWorker := max(1, int(math.Round(perWorkerRate/avgSpansPerGroup)))
	if targetRate == 0 {
		// In unpaced mode, keep a fixed small stagger of 4 groups so 3 phases stay interleaved
		// while finishing well within DecisionWait.
		groupsPerSecPerWorker = 4
	}

	var measuring atomic.Bool
	var measuredSpans atomic.Int64
	var stopWorkers atomic.Bool
	var firstEmitErr atomic.Value

	workerLatencies := make([][]int64, numWorkers)
	for w := range numWorkers {
		workerLatencies[w] = make([]int64, 0, 8192)
	}

	var wg sync.WaitGroup
	startBarrier := make(chan struct{})

	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			<-startBarrier
			wStart := time.Now()
			var emittedSpans int64
			var step uint64

			// Ring delay queues for staggered phase 1 (+1s) and phase 2 (+2s) arrivals.
			type pendingPhase struct {
				groupSeq uint64
				tmplIdx  int
			}
			stagger := groupsPerSecPerWorker
			q1 := make([]pendingPhase, 0, stagger+8)
			q2 := make([]pendingPhase, 0, 2*stagger+8)
			ctx := t.Context()

			emit := func(td ptrace.Traces) {
				sc := int64(td.SpanCount())
				if sc == 0 {
					return
				}
				isMeas := measuring.Load()
				var t0 time.Time
				if isMeas {
					t0 = time.Now()
				}
				if err := p.ConsumeTraces(ctx, td); err != nil {
					firstEmitErr.CompareAndSwap(nil, err)
					return
				}
				if isMeas {
					workerLatencies[workerID] = append(workerLatencies[workerID], time.Since(t0).Nanoseconds())
					measuredSpans.Add(sc)
				}
				emittedSpans += sc
			}

			for !stopWorkers.Load() {
				step++
				tmplIdx := int((step - 1) % uint64(len(templates)))
				grp := templates[tmplIdx]

				// Emit phase 0 of current trace group.
				emit(cloneBakeoffPhaseBatch(grp.phases[0], uint64(workerID), step, ephemeralRes))
				q1 = append(q1, pendingPhase{groupSeq: step, tmplIdx: tmplIdx})
				q2 = append(q2, pendingPhase{groupSeq: step, tmplIdx: tmplIdx})

				// Emit phase 1 of group from ~1s ago.
				if len(q1) > stagger {
					item := q1[0]
					q1 = q1[1:]
					emit(cloneBakeoffPhaseBatch(templates[item.tmplIdx].phases[1], uint64(workerID), item.groupSeq, ephemeralRes))
				}
				// Emit phase 2 of group from ~2s ago.
				if len(q2) > 2*stagger {
					item := q2[0]
					q2 = q2[1:]
					emit(cloneBakeoffPhaseBatch(templates[item.tmplIdx].phases[2], uint64(workerID), item.groupSeq, ephemeralRes))
				}

				if targetRate > 0 {
					expectedElapsed := time.Duration(float64(emittedSpans) / perWorkerRate * float64(time.Second))
					if sleepDur := expectedElapsed - time.Since(wStart); sleepDur > 500*time.Microsecond {
						time.Sleep(sleepDur)
					}
				}
			}
		}(w)
	}

	close(startBarrier)

	// Phase 1: Full warmup window (NO forced GC!).
	time.Sleep(time.Duration(warmupSec) * time.Second)

	// Capture window-start baseline snapshots (STW-free!).
	startRT := readRuntimeMetricsSnap()
	startSampledOut := sink.spans.Load()
	startDecSampled := mrec.decidedSampled.Load()
	startDecNotSampled := mrec.decidedNotSampled.Load()
	startDecDropped := mrec.decidedDropped.Load()
	_, _, _, _, _, startArenaSnap := collectAllShardIVMStats(sp)

	mrec.recording.Store(true)
	measuring.Store(true)
	measureStart := time.Now()

	// Sample live steady-state HeapAlloc/HeapInuse/HeapObjects/GCScanHeap/VmRSS every 250ms (ZERO STW!).
	var (
		sampleCount        uint64
		sumHeapAlloc       uint64
		peakHeapAlloc      uint64
		sumHeapInuse       uint64
		peakHeapInuse      uint64
		sumHeapObjs        uint64
		peakHeapObjs       uint64
		sumVmRSS           uint64
		peakVmRSS          uint64
		sumGCScanHeap      uint64
		peakGCScanHeap     uint64
		stopSampler        = make(chan struct{})
		samplerDone        = make(chan struct{})
	)
	go func() {
		defer close(samplerDone)
		tk := time.NewTicker(250 * time.Millisecond)
		defer tk.Stop()
		recordSample := func() {
			rt := readRuntimeMetricsSnap()
			rss, _ := readProcStatusBytes()
			inuse := rt.heapObjectsBytes + rt.heapUnusedBytes
			sampleCount++
			sumHeapAlloc += rt.heapObjectsBytes
			if rt.heapObjectsBytes > peakHeapAlloc {
				peakHeapAlloc = rt.heapObjectsBytes
			}
			sumHeapInuse += inuse
			if inuse > peakHeapInuse {
				peakHeapInuse = inuse
			}
			sumHeapObjs += rt.heapObjectsCount
			if rt.heapObjectsCount > peakHeapObjs {
				peakHeapObjs = rt.heapObjectsCount
			}
			sumVmRSS += rss
			if rss > peakVmRSS {
				peakVmRSS = rss
			}
			sumGCScanHeap += rt.gcScanHeapBytes
			if rt.gcScanHeapBytes > peakGCScanHeap {
				peakGCScanHeap = rt.gcScanHeapBytes
			}
		}
		recordSample()
		for {
			select {
			case <-stopSampler:
				return
			case <-tk.C:
				recordSample()
			}
		}
	}()

	// Phase 2: Full steady-state measurement window (NO forced GC!).
	time.Sleep(time.Duration(measureSec) * time.Second)
	measureElapsed := time.Since(measureStart)

	measuring.Store(false)
	mrec.recording.Store(false)
	close(stopSampler)
	<-samplerDone

	// Capture window-end live metrics & arena stats BEFORE stopping workers or forcing GC!
	endRT := readRuntimeMetricsSnap()
	_, vmHWM := readProcStatusBytes()
	endSampledOut := sink.spans.Load()
	endDecSampled := mrec.decidedSampled.Load()
	endDecNotSampled := mrec.decidedNotSampled.Load()
	endDecDropped := mrec.decidedDropped.Load()
	_, _, _, _, _, arenaSnap := collectAllShardIVMStats(sp)
	tracesOnMemAtSnap := sp.shards[0].tracesOnMemory.Load()

	// Stop producers cleanly, then take end-of-window post-runtime.GC() retained snapshot before Shutdown.
	stopWorkers.Store(true)
	wg.Wait()
	if errVal := firstEmitErr.Load(); errVal != nil {
		require.NoError(t, errVal.(error))
	}

	var postGC runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&postGC)
	retVmRSS, _ := readProcStatusBytes()
	postGCRT := readRuntimeMetricsSnap()
	tracesOnMemPostGC := sp.shards[0].tracesOnMemory.Load()

	require.NoError(t, p.Shutdown(t.Context()))
	totalDropNoUnm, totalEarlySamp, totalEarlyDrop, totalEarlyNotSamp, totalUnmSamp, postShutdownArena := collectAllShardIVMStats(sp)
	totalDecSampled := mrec.decidedSampled.Load()
	totalDecNotSampled := mrec.decidedNotSampled.Load()
	totalDecDropped := mrec.decidedDropped.Load()

	// Merge ingest latencies across workers.
	totalLats := 0
	for _, wl := range workerLatencies {
		totalLats += len(wl)
	}
	allLats := make([]int64, 0, totalLats)
	for _, wl := range workerLatencies {
		allLats = append(allLats, wl...)
	}
	inP50, inP95, inP99, inMax := int64Percentiles(allLats, 1e-3) // ns -> us

	tickCount, tkP50, tkP95, tkP99, tkMax := mrec.tickPercentiles()
	actualBypassSplit := sp.shards[0].usesRawWorkChan()

	gcP50, gcP95, gcP99, gcMax := histogramQuantilesMs(startRT.pauseBuckets, endRT.pauseBuckets, startRT.pauseCounts, endRT.pauseCounts)

	divSafe := func(sum, n uint64) uint64 {
		if n == 0 {
			return 0
		}
		return sum / n
	}

	deltaDecNotSampled := endDecNotSampled - startDecNotSampled
	zeroUnmRatio := 0.0
	if totalNonSampled := totalDecNotSampled + totalDecDropped; totalNonSampled > 0 {
		zeroUnmRatio = float64(totalDropNoUnm+totalEarlyDrop+totalEarlyNotSamp) / float64(totalNonSampled)
	}
	fragRatio := 0.0
	if arenaSnap.LivePayloadBytes > 0 {
		fragRatio = float64(arenaSnap.TotalAllocatedBytes) / float64(arenaSnap.LivePayloadBytes)
	}

	res := bakeoffResult{
		Candidate:       candidate,
		Workload:        workload,
		Shape:           shapeName,
		BypassSplit:     actualBypassSplit,
		NumShards:       numShards,
		TargetRateSPS:   targetRate,
		DecisionWaitSec: decisionWaitSec,
		WarmupSec:       warmupSec,
		MeasureSec:      measureSec,
		Workers:         numWorkers,
		GOGC:            envStr("GOGC", "100"),
		GoVersion:       runtime.Version(),
		AchievedSPS:     float64(measuredSpans.Load()) / measureElapsed.Seconds(),
		MeasuredSpans:   measuredSpans.Load(),
		SampledSpansOut: endSampledOut - startSampledOut,

		SteadyHeapAllocBytes:   divSafe(sumHeapAlloc, sampleCount),
		PeakHeapAllocBytes:     peakHeapAlloc,
		SteadyHeapInuseBytes:   divSafe(sumHeapInuse, sampleCount),
		PeakHeapInuseBytes:     peakHeapInuse,
		SteadyHeapObjects:      divSafe(sumHeapObjs, sampleCount),
		PeakHeapObjects:        peakHeapObjs,
		WindowTotalAllocBytes:  endRT.allocsBytes - startRT.allocsBytes,
		SteadyVmRSSBytes:       divSafe(sumVmRSS, sampleCount),
		PeakVmRSSBytes:         peakVmRSS,
		VmHWMBytes:             vmHWM,
		SteadyGCScanHeapBytes:  divSafe(sumGCScanHeap, sampleCount),
		PeakGCScanHeapBytes:    peakGCScanHeap,

		RetainedHeapAllocBytes:  postGC.HeapAlloc,
		RetainedHeapObjects:     postGC.HeapObjects,
		RetainedVmRSSBytes:      retVmRSS,
		RetainedGCScanHeapBytes: postGCRT.gcScanHeapBytes,
		TracesOnMemoryAtEnd:     tracesOnMemAtSnap,
		TracesOnMemoryAtSnap:    tracesOnMemPostGC,

		WindowGCCycles:        endRT.gcCyclesTotal - startRT.gcCyclesTotal,
		WindowAutoGCCycles:    endRT.gcCyclesAuto - startRT.gcCyclesAuto,
		WindowGCCPUSeconds:    endRT.gcCPUSec - startRT.gcCPUSec,
		WindowGCMarkAssistSec: endRT.gcMarkAssistSec - startRT.gcMarkAssistSec,
		WindowGCPauseCPUSec:   endRT.gcPauseCPUSec - startRT.gcPauseCPUSec,
		WindowUserCPUSeconds:  endRT.userCPUSec - startRT.userCPUSec,
		WindowTotalCPUSeconds: endRT.totalCPUSec - startRT.totalCPUSec,
		GCPauseP50Ms:          gcP50,
		GCPauseP95Ms:          gcP95,
		GCPauseP99Ms:          gcP99,
		GCPauseMaxMs:          gcMax,

		IngestP50Us: inP50,
		IngestP95Us: inP95,
		IngestP99Us: inP99,
		IngestMaxUs: inMax,
		TickCount:   tickCount,
		TickP50Ms:   tkP50,
		TickP95Ms:   tkP95,
		TickP99Ms:   tkP99,
		TickMaxMs:   tkMax,

		WindowDecidedSampled:       endDecSampled - startDecSampled,
		WindowDecidedNotSampled:    deltaDecNotSampled,
		WindowDecidedDropped:       endDecDropped - startDecDropped,
		TotalDecidedSampled:        totalDecSampled,
		TotalDecidedNotSampled:     totalDecNotSampled,
		TotalDecidedDropped:        totalDecDropped,
		DroppedTooEarly:            mrec.droppedTooEarly.Load(),
		TotalDroppedWithoutUnmarsh: totalDropNoUnm,
		TotalEarlySampled:          totalEarlySamp,
		TotalEarlyDropped:          totalEarlyDrop,
		TotalEarlyNotSampled:       totalEarlyNotSamp,
		TotalUnmarshaledForSample:  totalUnmSamp,
		ZeroUnmarshalDropRatio:     zeroUnmRatio,

		ArenaLiveSlabs:          arenaSnap.LiveSlabs,
		ArenaPooledSlabs:        arenaSnap.PooledSlabs,
		ArenaAllocatedBytes:     arenaSnap.TotalAllocatedBytes,
		ArenaLivePayloadBytes:   arenaSnap.LivePayloadBytes,
		ArenaInternAllocBytes:   arenaSnap.InternAllocatedBytes,
		ArenaInternLiveBytes:    arenaSnap.ResourceInternBytes + arenaSnap.ScopeInternBytes,
		ArenaMmapActiveBytes:    arenaSnap.MmapActiveBytes,
		ArenaCompressedChunks:   arenaSnap.CompressedChunks,
		ArenaUncompressedChunks: arenaSnap.UncompressedChunks,
		ArenaCompactionCount:    arenaSnap.CompactionCount,
		ArenaUnmarshalCount:     arenaSnap.UnmarshalCount,
		WindowArenaUnmarshals:   arenaSnap.UnmarshalCount - startArenaSnap.UnmarshalCount,
		SlabFragRatio:           fragRatio,
		PostShutdownMmapBytes:   postShutdownArena.MmapActiveBytes,
		PostShutdownLiveSlabs:   postShutdownArena.LiveSlabs,
	}

	b, err := json.Marshal(res)
	require.NoError(t, err)
	fmt.Printf("BAKEOFF_JSON:%s\n", string(b))
	require.Equal(t, uint64(0), res.PostShutdownMmapBytes, "mmap bytes must be 0 after Shutdown")
	require.Equal(t, 0, res.PostShutdownLiveSlabs, "live slabs must be 0 after Shutdown")
}
