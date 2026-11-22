// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tailsamplingprocessor

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/tailstorageextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/pkg/samplingpolicy"
)

func cloneTracesSlice(batches []ptrace.Traces) []ptrace.Traces {
	out := make([]ptrace.Traces, len(batches))
	for i, b := range batches {
		cp := ptrace.NewTraces()
		b.CopyTo(cp)
		out[i] = cp
	}
	return out
}

func collectSampledTraceIDs(sink *consumertest.TracesSink) map[pcommon.TraceID]int {
	counts := make(map[pcommon.TraceID]int)
	for _, td := range sink.AllTraces() {
		for _, rs := range td.ResourceSpans().All() {
			for _, ss := range rs.ScopeSpans().All() {
				for _, sp := range ss.Spans().All() {
					counts[sp.TraceID()]++
				}
			}
		}
	}
	return counts
}

func collectSampledPolicyAttr(sink *consumertest.TracesSink) map[pcommon.TraceID]map[string]struct{} {
	res := make(map[pcommon.TraceID]map[string]struct{})
	for _, td := range sink.AllTraces() {
		for _, rs := range td.ResourceSpans().All() {
			for _, ss := range rs.ScopeSpans().All() {
				pol := ""
				if v, ok := ss.Scope().Attributes().Get("tailsampling.policy"); ok {
					pol = v.AsString()
				}
				for _, sp := range ss.Spans().All() {
					id := sp.TraceID()
					if res[id] == nil {
						res[id] = make(map[string]struct{})
					}
					res[id][pol] = struct{}{}
				}
			}
		}
	}
	return res
}

func collectSpanTraceStates(sink *consumertest.TracesSink) map[pcommon.SpanID]string {
	res := make(map[pcommon.SpanID]string)
	for _, td := range sink.AllTraces() {
		for _, rs := range td.ResourceSpans().All() {
			for _, ss := range rs.ScopeSpans().All() {
				for _, sp := range ss.Spans().All() {
					res[sp.SpanID()] = sp.TraceState().AsRaw()
				}
			}
		}
	}
	return res
}

func assertSumMetricValue(t *testing.T, s *testTelemetry, name string, want int64) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, s.reader.Collect(t.Context(), &rm))
	m := s.getMetric(name, rm)
	sum, ok := m.Data.(metricdata.Sum[int64])
	require.True(t, ok, "metric %q missing or not Sum[int64]: %+v", name, m)
	require.NotEmpty(t, sum.DataPoints, "metric %q has no datapoints", name)
	var total int64
	for _, dp := range sum.DataPoints {
		total += dp.Value
	}
	require.Equal(t, want, total, "metric %q total mismatch", name)
}

func makeSpanBatch(traceID pcommon.TraceID, spanID uint64, parentID uint64, serviceName string, start, end time.Time, status ptrace.StatusCode, attrs map[string]any) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	if serviceName != "" {
		rs.Resource().Attributes().PutStr("service.name", serviceName)
	}
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("test-scope")
	sp := ss.Spans().AppendEmpty()
	sp.SetTraceID(traceID)
	sp.SetSpanID(uInt64ToSpanID(spanID))
	if parentID != 0 {
		sp.SetParentSpanID(uInt64ToSpanID(parentID))
	}
	sp.SetName("op")
	sp.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
	sp.SetEndTimestamp(pcommon.NewTimestampFromTime(end))
	sp.Status().SetCode(status)
	for k, v := range attrs {
		switch tv := v.(type) {
		case string:
			sp.Attributes().PutStr(k, tv)
		case int64:
			sp.Attributes().PutInt(k, tv)
		case bool:
			sp.Attributes().PutBool(k, tv)
		}
	}
	return td
}

func runProcessorSync(t *testing.T, cfg Config, host component.Host, batches []ptrace.Traces) (*consumertest.TracesSink, *tailSamplingSpanProcessor) {
	t.Helper()
	sink := new(consumertest.TracesSink)
	controller := newTestTSPController()
	cfgCopy := cfg
	cfgCopy.Options = append(append([]Option(nil), cfg.Options...), withTestController(controller))
	p, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), sink, cfgCopy)
	require.NoError(t, err)
	if host == nil {
		host = componenttest.NewNopHost()
	}
	require.NoError(t, p.Start(t.Context(), host))
	for _, b := range cloneTracesSlice(batches) {
		require.NoError(t, p.ConsumeTraces(t.Context(), b))
	}
	controller.waitForTick()
	controller.waitForTick()
	require.NoError(t, p.Shutdown(t.Context()))
	return sink, shard0(p)
}

// TestUpstreamSpanIngestKnownParityFailures proves non-tautologically that
// upstream's existing `span-ingest` strategy FAILS parity against `trace-complete`
// on all 4 documented bug classes, while `incremental` achieves 100% parity!
func TestUpstreamSpanIngestKnownParityFailures(t *testing.T) {
	base := time.Unix(1700000000, 0)

	t.Run("Bug1_DropPlusPositiveDropsAllTraces", func(t *testing.T) {
		policies := []PolicyCfg{
			{
				sharedPolicyCfg: sharedPolicyCfg{Name: "drop-healthz", Type: Drop},
				DropCfg: DropCfg{
					SubPolicyCfg: []AndSubPolicyCfg{
						{
							sharedPolicyCfg: sharedPolicyCfg{
								Name:               "healthz",
								Type:               StringAttribute,
								StringAttributeCfg: StringAttributeCfg{Key: "http.route", Values: []string{"/healthz"}},
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
		}
		tidErr := uInt64ToTraceID(1)
		tidHealth := uInt64ToTraceID(2)
		batches := []ptrace.Traces{
			makeSpanBatch(tidErr, 1, 0, "checkout", base, base.Add(10*time.Millisecond), ptrace.StatusCodeError, map[string]any{"http.route": "/checkout"}),
			makeSpanBatch(tidHealth, 2, 0, "checkout", base, base.Add(10*time.Millisecond), ptrace.StatusCodeError, map[string]any{"http.route": "/healthz"}),
		}

		oracleSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyTraceComplete, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, batches)
		spanIngestSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategySpanIngest, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, batches)
		ivmSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyIncremental, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, batches)

		// Oracle samples tidErr (1 span) and drops tidHealth.
		require.Equal(t, map[pcommon.TraceID]int{tidErr: 1}, collectSampledTraceIDs(oracleSink))
		// Baseline B (upstream span-ingest) FAILS: drops 100% of traces!
		require.Empty(t, collectSampledTraceIDs(spanIngestSink), "expected upstream span-ingest Bug #1: drops all traces when any drop policy exists")
		// Incremental matches oracle 100%!
		require.Equal(t, collectSampledTraceIDs(oracleSink), collectSampledTraceIDs(ivmSink))
	})

	t.Run("Bug2_CrossBatchAND", func(t *testing.T) {
		policies := []PolicyCfg{
			{
				sharedPolicyCfg: sharedPolicyCfg{Name: "and-checkout-500", Type: And},
				AndCfg: AndCfg{
					SubPolicyCfg: []AndSubPolicyCfg{
						{
							sharedPolicyCfg: sharedPolicyCfg{
								Name:               "svc",
								Type:               StringAttribute,
								StringAttributeCfg: StringAttributeCfg{Key: "service.name", Values: []string{"service-1"}},
							},
						},
						{
							sharedPolicyCfg: sharedPolicyCfg{
								Name:                "status500",
								Type:                NumericAttribute,
								NumericAttributeCfg: NumericAttributeCfg{Key: "http.response.status_code", MinValue: 500, MaxValue: 599},
							},
						},
					},
				},
			},
		}
		tid := uInt64ToTraceID(10)
		batches := []ptrace.Traces{
			makeSpanBatch(tid, 1, 0, "service-1", base, base.Add(10*time.Millisecond), ptrace.StatusCodeUnset, map[string]any{"http.response.status_code": int64(200)}),
			makeSpanBatch(tid, 2, 1, "service-2", base.Add(5*time.Millisecond), base.Add(15*time.Millisecond), ptrace.StatusCodeUnset, map[string]any{"http.response.status_code": int64(500)}),
		}

		oracleSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyTraceComplete, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, batches)
		spanIngestSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategySpanIngest, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, batches)
		ivmSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyIncremental, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, batches)

		require.Equal(t, map[pcommon.TraceID]int{tid: 2}, collectSampledTraceIDs(oracleSink))
		require.Empty(t, collectSampledTraceIDs(spanIngestSink), "expected upstream span-ingest Bug #2: misses cross-batch AND")
		require.Equal(t, collectSampledTraceIDs(oracleSink), collectSampledTraceIDs(ivmSink))
	})

	t.Run("Bug3_CrossBatchLatency", func(t *testing.T) {
		policies := []PolicyCfg{
			{
				sharedPolicyCfg: sharedPolicyCfg{
					Name:       "slow",
					Type:       Latency,
					LatencyCfg: LatencyCfg{ThresholdMs: 500},
				},
			},
		}
		tid := uInt64ToTraceID(20)
		// Each individual span is 100ms, but combined trace duration is 700ms > 500ms.
		batches := []ptrace.Traces{
			makeSpanBatch(tid, 1, 0, "svc", base, base.Add(100*time.Millisecond), ptrace.StatusCodeOk, nil),
			makeSpanBatch(tid, 2, 1, "svc", base.Add(600*time.Millisecond), base.Add(700*time.Millisecond), ptrace.StatusCodeOk, nil),
		}

		oracleSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyTraceComplete, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, batches)
		spanIngestSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategySpanIngest, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, batches)
		ivmSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyIncremental, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, batches)

		require.Equal(t, map[pcommon.TraceID]int{tid: 2}, collectSampledTraceIDs(oracleSink))
		require.Empty(t, collectSampledTraceIDs(spanIngestSink), "expected upstream span-ingest Bug #3: misses cross-batch latency")
		require.Equal(t, collectSampledTraceIDs(oracleSink), collectSampledTraceIDs(ivmSink))
	})

	t.Run("Bug4_NotAndInvertMatchPrematureFalsePositive", func(t *testing.T) {
		notPolicies := []PolicyCfg{
			{
				sharedPolicyCfg: sharedPolicyCfg{Name: "not-error", Type: Not},
				NotCfg: NotCfg{
					SubPolicy: NotSubPolicyCfg{
						sharedPolicyCfg: sharedPolicyCfg{
							Name:          "errors",
							Type:          StatusCode,
							StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}},
						},
					},
				},
			},
		}
		tid := uInt64ToTraceID(30)
		batches := []ptrace.Traces{
			makeSpanBatch(tid, 1, 0, "svc", base, base.Add(10*time.Millisecond), ptrace.StatusCodeOk, nil),
			makeSpanBatch(tid, 2, 1, "svc", base.Add(5*time.Millisecond), base.Add(15*time.Millisecond), ptrace.StatusCodeError, nil),
		}

		oracleSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyTraceComplete, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: notPolicies}, nil, batches)
		spanIngestSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategySpanIngest, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: notPolicies}, nil, batches)
		ivmSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyIncremental, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: notPolicies}, nil, batches)

		require.Empty(t, collectSampledTraceIDs(oracleSink))
		require.NotEmpty(t, collectSampledTraceIDs(spanIngestSink), "expected upstream span-ingest Bug #4: premature false sample on NOT")
		require.Equal(t, collectSampledTraceIDs(oracleSink), collectSampledTraceIDs(ivmSink))

		// Same test for invert_match: true on string_attribute.
		invPolicies := []PolicyCfg{
			{
				sharedPolicyCfg: sharedPolicyCfg{
					Name: "inv-str",
					Type: StringAttribute,
					StringAttributeCfg: StringAttributeCfg{
						Key:         "http.route",
						Values:      []string{"/healthz"},
						InvertMatch: true,
					},
				},
			},
		}
		invBatches := []ptrace.Traces{
			makeSpanBatch(tid, 1, 0, "svc", base, base.Add(10*time.Millisecond), ptrace.StatusCodeOk, map[string]any{"http.route": "/api"}),
			makeSpanBatch(tid, 2, 1, "svc", base.Add(5*time.Millisecond), base.Add(15*time.Millisecond), ptrace.StatusCodeOk, map[string]any{"http.route": "/healthz"}),
		}
		oInv, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyTraceComplete, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: invPolicies}, nil, invBatches)
		sInv, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategySpanIngest, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: invPolicies}, nil, invBatches)
		iInv, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyIncremental, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: invPolicies}, nil, invBatches)

		require.Empty(t, collectSampledTraceIDs(oInv))
		require.NotEmpty(t, collectSampledTraceIDs(sInv), "expected upstream span-ingest Bug #4: premature false sample on invert_match")
		require.Equal(t, collectSampledTraceIDs(oInv), collectSampledTraceIDs(iInv))
	})
}

// customDropExtension simulates a custom samplingpolicy.Extension that can return Dropped.
type customDropExtension struct {
	dropKey string
}

func (e *customDropExtension) Start(context.Context, component.Host) error { return nil }
func (e *customDropExtension) Shutdown(context.Context) error              { return nil }
func (e *customDropExtension) NewEvaluator(_ string, _ map[string]any) (samplingpolicy.Evaluator, error) {
	return &customDropEval{key: e.dropKey}, nil
}

type customDropEval struct {
	key string
}

func (c *customDropEval) Evaluate(_ context.Context, _ pcommon.TraceID, trace *samplingpolicy.TraceData) (samplingpolicy.Decision, error) {
	for _, rs := range trace.ReceivedBatches.ResourceSpans().All() {
		for _, ss := range rs.ScopeSpans().All() {
			for _, sp := range ss.Spans().All() {
				if v, ok := sp.Attributes().Get(c.key); ok && v.Str() == "dropme" {
					return samplingpolicy.Dropped, nil
				}
			}
		}
	}
	return samplingpolicy.NotSampled, nil
}
func (*customDropEval) IsStateful() bool { return false }

type extHost struct {
	component.Host
	exts map[component.ID]component.Component
}

func (h *extHost) GetExtensions() map[component.ID]component.Component { return h.exts }

// generateRandomizedParityBatches builds numTraces traces split across multiple
// ResourceSpans batches under the specified arrival order: "in-order", "reversed",
// or "shuffled". Consistent tracestate `rv` is assigned per trace per W3C spec.
func generateRandomizedParityBatches(seed int64, numTraces int, order string, withTracestateRV bool) []ptrace.Traces {
	rng := rand.New(rand.NewSource(seed))
	base := time.Unix(1700000000, 0)

	type rawSpan struct {
		tid         pcommon.TraceID
		sid         uint64
		parentID    uint64
		svc         string
		resEmpty    bool
		resNonBool  bool
		resNonInt   bool
		start       time.Time
		end         time.Time
		status      ptrace.StatusCode
		route       string
		statusCode  int64
		flagSampled bool
		tsRaw       string
		boolAttr    *bool
		eventName   string
		extraKey    string
		extraStr    string
		nonIntAttr  bool
		nonBoolAttr bool
	}

	var all []rawSpan
	var spanSeq uint64
	services := []string{"service-0", "service-1", "service-2", "checkout", "payments"}
	routes := []string{"/api/v1/items", "/healthz", "/checkout", "/metrics", ""}

	for i := range numTraces {
		tid := uInt64ToTraceID(uint64(i + 1))
		nSpans := 1 + rng.Intn(6) // 1..6 spans per trace
		// Pick consistent W3C tracestate for this trace if enabled.
		tsVal := ""
		if withTracestateRV {
			mode := rng.Intn(4)
			switch mode {
			case 0:
				tsVal = fmt.Sprintf("ot=rv:%014x", rng.Uint64()&((1<<56)-1))
			case 1:
				tsVal = "ot=th:8"
			case 2:
				tsVal = "vendor=keepme"
			}
		} else if rng.Intn(10) == 0 {
			tsVal = "tier=gold"
		}

		rootID := spanSeq + 1
		traceSpanDur := 10 * time.Millisecond
		spreadLatency := rng.Intn(12) == 0 // ~8% slow cross-span traces
		for j := range nSpans {
			spanSeq++
			offset := time.Duration(j*15) * time.Millisecond
			if spreadLatency && j == nSpans-1 {
				offset = 650 * time.Millisecond
			}
			st := base.Add(offset)
			et := st.Add(traceSpanDur)
			status := ptrace.StatusCodeOk
			r := rng.Intn(40)
			if r == 0 {
				status = ptrace.StatusCodeError
			} else if r < 8 {
				status = ptrace.StatusCodeUnset
			}
			route := routes[rng.Intn(len(routes))]
			code := int64(200)
			if rng.Intn(15) == 0 {
				code = 500
			} else if rng.Intn(10) == 0 {
				code = 404
			}
			var bPtr *bool
			if rng.Intn(8) == 0 {
				v := rng.Intn(2) == 0
				bPtr = &v
			}
			evName := ""
			if rng.Intn(20) == 0 {
				evName = "exception"
			}
			pid := rootID
			if j == 0 {
				pid = 0
			}
			tsForSpan := ""
			if tsVal != "" && (j == nSpans-1 || rng.Intn(2) == 0) {
				tsForSpan = tsVal
			}
			extraStr := ""
			if rng.Intn(25) == 0 {
				extraStr = "dropme"
			}
			all = append(all, rawSpan{
				tid:         tid,
				sid:         spanSeq,
				parentID:    pid,
				svc:         services[rng.Intn(len(services))],
				resEmpty:    rng.Intn(25) == 0,
				resNonBool:  rng.Intn(30) == 0,
				resNonInt:   rng.Intn(30) == 0,
				start:       st,
				end:         et,
				status:      status,
				route:       route,
				statusCode:  code,
				flagSampled: rng.Intn(25) == 0,
				tsRaw:       tsForSpan,
				boolAttr:    bPtr,
				eventName:   evName,
				extraKey:    "custom.ext",
				extraStr:    extraStr,
				nonIntAttr:  rng.Intn(30) == 0,
				nonBoolAttr: rng.Intn(30) == 0,
			})
		}
	}

	switch order {
	case "reversed":
		for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
			all[i], all[j] = all[j], all[i]
		}
	case "shuffled":
		rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	}

	// Pack into multi-trace mixed ResourceSpans batches (16 spans per batch).
	const batchSize = 16
	var batches []ptrace.Traces
	for i := 0; i < len(all); i += batchSize {
		end := min(i+batchSize, len(all))
		td := ptrace.NewTraces()
		// Group by service within the batch so ResourceSpans has multiple traces sharing 1 Resource.
		bySvc := make(map[string]ptrace.ResourceSpans)
		for _, item := range all[i:end] {
			key := item.svc
			if item.resEmpty {
				key += "|empty"
			}
			if item.resNonBool {
				key += "|nb"
			}
			if item.resNonInt {
				key += "|ni"
			}
			rs, ok := bySvc[key]
			if !ok {
				rs = td.ResourceSpans().AppendEmpty()
				rs.Resource().Attributes().PutStr("service.name", item.svc)
				if item.resEmpty {
					rs.Resource().Attributes().PutStr("empty.res.key", "")
				}
				if item.resNonBool {
					rs.Resource().Attributes().PutStr("flag.bool", "not-a-bool")
				}
				if item.resNonInt {
					rs.Resource().Attributes().PutStr("num.zero", "not-an-int")
				}
				ss0 := rs.ScopeSpans().AppendEmpty()
				ss0.Scope().SetName("net/http")
				ss1 := rs.ScopeSpans().AppendEmpty()
				ss1.Scope().SetName("database/sql")
				bySvc[key] = rs
			}
			ss := rs.ScopeSpans().At(int(item.sid % 2))
			sp := ss.Spans().AppendEmpty()
			sp.SetTraceID(item.tid)
			sp.SetSpanID(uInt64ToSpanID(item.sid))
			if item.parentID != 0 {
				sp.SetParentSpanID(uInt64ToSpanID(item.parentID))
			}
			sp.SetName("GET /items")
			sp.SetStartTimestamp(pcommon.NewTimestampFromTime(item.start))
			sp.SetEndTimestamp(pcommon.NewTimestampFromTime(item.end))
			sp.Status().SetCode(item.status)
			sp.Attributes().PutStr("http.route", item.route)
			if item.nonIntAttr {
				sp.Attributes().PutStr("http.response.status_code", "not-an-int")
			} else {
				sp.Attributes().PutInt("http.response.status_code", item.statusCode)
			}
			if item.boolAttr != nil {
				sp.Attributes().PutBool("flag.bool", *item.boolAttr)
			} else if item.nonBoolAttr {
				sp.Attributes().PutStr("flag.bool", "str-val")
			}
			if item.extraStr != "" {
				sp.Attributes().PutStr(item.extraKey, item.extraStr)
			}
			if item.flagSampled {
				sp.SetFlags(1)
			}
			if item.tsRaw != "" {
				sp.TraceState().FromRaw(item.tsRaw)
			}
			if item.eventName != "" {
				ev := sp.Events().AppendEmpty()
				ev.SetName(item.eventName)
			}
		}
		batches = append(batches, td)
	}
	return batches
}

// TestIVMParity runs >= 5,000 randomized multi-batch traces across in-order,
// reversed, and shuffled out-of-order arrivals comparing `incremental` directly
// against the `trace-complete` oracle (asserting 0 mismatches!).
func TestIVMParity(t *testing.T) {
	suiteConfigs := []struct {
		name               string
		numTraces          int
		policies           []PolicyCfg
		recordPolicy       bool
		sampleOnFirstMatch bool
		useTracestate      bool
		enableCache        bool
		bypassSplit        bool
		withHost           bool
		checkPolicyNames   bool
		checkTraceState    bool
	}{
		{
			name:        "CoreAllMonotonicAndLatency_InOrderReversedShuffled",
			numTraces:   600,
			bypassSplit: true,
			enableCache: true,
			policies: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "slow", Type: Latency, LatencyCfg: LatencyCfg{ThresholdMs: 500}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "prob", Type: Probabilistic, ProbabilisticCfg: ProbabilisticCfg{SamplingPercentage: 10, HashSalt: "salt1"}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "str-regex", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "http.route", Values: []string{"^/check.*"}, EnabledRegexMatching: true}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "num-500", Type: NumericAttribute, NumericAttributeCfg: NumericAttributeCfg{Key: "http.response.status_code", MinValue: 500, MaxValue: 599}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "bool-true", Type: BooleanAttribute, BooleanAttributeCfg: BooleanAttributeCfg{Key: "flag.bool", Value: true}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "ts-gold", Type: TraceState, TraceStateCfg: TraceStateCfg{Key: "tier", Values: []string{"gold", ""}}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "flags", Type: TraceFlags}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "ottl-event", Type: OTTLCondition, OTTLConditionCfg: OTTLConditionCfg{ErrorMode: ottl.IgnoreError, SpanEventConditions: []string{`name == "exception"`}}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "span-min", Type: SpanCount, SpanCountCfg: SpanCountCfg{MinSpans: 5}}},
			},
		},
		{
			name:        "DropPlusPositiveAndNestedAndNotInvertMatch",
			numTraces:   600,
			bypassSplit: true,
			enableCache: true,
			policies: []PolicyCfg{
				{
					sharedPolicyCfg: sharedPolicyCfg{Name: "drop-healthz", Type: Drop},
					DropCfg: DropCfg{SubPolicyCfg: []AndSubPolicyCfg{
						{sharedPolicyCfg: sharedPolicyCfg{Name: "d1", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "http.route", Values: []string{"/healthz"}}}},
					}},
				},
				{
					sharedPolicyCfg: sharedPolicyCfg{Name: "drop-multi", Type: Drop},
					DropCfg: DropCfg{SubPolicyCfg: []AndSubPolicyCfg{
						{sharedPolicyCfg: sharedPolicyCfg{Name: "dm1", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "service.name", Values: []string{"service-0"}}}},
						{
							sharedPolicyCfg: sharedPolicyCfg{Name: "dm2", Type: Not},
							NotCfg: NotCfg{SubPolicy: NotSubPolicyCfg{
								sharedPolicyCfg: sharedPolicyCfg{Name: "dm2-err", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}},
							}},
						},
					}},
				},
				{
					sharedPolicyCfg: sharedPolicyCfg{Name: "and-cross", Type: And},
					AndCfg: AndCfg{SubPolicyCfg: []AndSubPolicyCfg{
						{sharedPolicyCfg: sharedPolicyCfg{Name: "a1", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "service.name", Values: []string{"service-1"}}}},
						{sharedPolicyCfg: sharedPolicyCfg{Name: "a2", Type: NumericAttribute, NumericAttributeCfg: NumericAttributeCfg{Key: "http.response.status_code", MinValue: 500, MaxValue: 599}}},
					}},
				},
				{
					sharedPolicyCfg: sharedPolicyCfg{Name: "not-metrics", Type: Not},
					NotCfg: NotCfg{SubPolicy: NotSubPolicyCfg{
						sharedPolicyCfg: sharedPolicyCfg{Name: "nm", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "http.route", Values: []string{"/metrics", "/api/v1/items", "/checkout"}}},
					}},
				},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "inv-bool", Type: BooleanAttribute, BooleanAttributeCfg: BooleanAttributeCfg{Key: "flag.bool", Value: true, InvertMatch: true}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
			},
		},
		{
			name:        "SubtleUpstreamQuirks_SpanCount_EmptyStrRegex_NonBool_NonInt_EmptyAnd",
			numTraces:   500,
			bypassSplit: true,
			enableCache: false, // NopCache parity!
			policies: []PolicyCfg{
				// Empty regex matching empty string on resource vs skipped on span!
				{sharedPolicyCfg: sharedPolicyCfg{Name: "empty-res-regex", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "empty.res.key", Values: []string{"^$"}, EnabledRegexMatching: true}}},
				// Non-bool value returns false, matching Value: false!
				{sharedPolicyCfg: sharedPolicyCfg{Name: "nonbool-false", Type: BooleanAttribute, BooleanAttributeCfg: BooleanAttributeCfg{Key: "flag.bool", Value: false}}},
				// Non-int value returns 0 (matches -1..1 when MinValue=-1, MaxValue=1)!
				{sharedPolicyCfg: sharedPolicyCfg{Name: "nonint-zero", Type: NumericAttribute, NumericAttributeCfg: NumericAttributeCfg{Key: "num.zero", MinValue: -1, MaxValue: 1}}},
				// SpanCount bounded, impossible max < min, negative max.
				{sharedPolicyCfg: sharedPolicyCfg{Name: "sc-bounded", Type: SpanCount, SpanCountCfg: SpanCountCfg{MinSpans: 3, MaxSpans: 4}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "sc-impossible", Type: SpanCount, SpanCountCfg: SpanCountCfg{MinSpans: 5, MaxSpans: 2}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "sc-negmax", Type: SpanCount, SpanCountCfg: SpanCountCfg{MinSpans: 1, MaxSpans: -1}}},
				// Invert numeric + invert string.
				{sharedPolicyCfg: sharedPolicyCfg{Name: "inv-num", Type: NumericAttribute, NumericAttributeCfg: NumericAttributeCfg{Key: "http.response.status_code", MinValue: 200, MaxValue: 200, InvertMatch: true}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "inv-str", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "http.route", Values: []string{"/healthz", "/metrics"}, InvertMatch: true}}},
				// TraceState exceeding 256 bytes ignored.
				{sharedPolicyCfg: sharedPolicyCfg{Name: "ts-long", Type: TraceState, TraceStateCfg: TraceStateCfg{Key: "tier", Values: []string{string(make([]byte, 260))}}}},
			},
		},
		{
			name:        "SpanCountZeroZero",
			numTraces:   200,
			bypassSplit: true,
			enableCache: true,
			policies: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "sc-zero", Type: SpanCount, SpanCountCfg: SpanCountCfg{MinSpans: 0, MaxSpans: 0}}},
			},
		},
		{
			name:        "BoundedLatency_NonPrefixDivergent",
			numTraces:   400,
			bypassSplit: true,
			enableCache: true,
			policies: []PolicyCfg{
				// Randomized spans have max duration 660ms (<= 1000ms upper bound), never exceeding upper bound.
				{sharedPolicyCfg: sharedPolicyCfg{Name: "lat-bounded", Type: Latency, LatencyCfg: LatencyCfg{ThresholdMs: 500, UpperThresholdMs: 1000}}},
			},
		},
		{
			// R2-1 check: probabilistic BEFORE status_code with recordPolicy=true and useTracestate=false!
			name:             "PolicyNameWinnerParity_ProbBeforeError_UseTracestateFalse",
			numTraces:        500,
			bypassSplit:      true,
			enableCache:      false,
			recordPolicy:     true,
			checkPolicyNames: true,
			policies: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "prob-50", Type: Probabilistic, ProbabilisticCfg: ProbabilisticCfg{SamplingPercentage: 50, HashSalt: "w1"}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "slow", Type: Latency, LatencyCfg: LatencyCfg{ThresholdMs: 500}}},
			},
		},
		{
			name:               "PolicyNameWinnerParity_SampleOnFirstMatch",
			numTraces:          400,
			bypassSplit:        true,
			enableCache:        false,
			recordPolicy:       true,
			sampleOnFirstMatch: true,
			checkPolicyNames:   true,
			policies: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "prob-50", Type: Probabilistic, ProbabilisticCfg: ProbabilisticCfg{SamplingPercentage: 50, HashSalt: "w2"}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "slow", Type: Latency, LatencyCfg: LatencyCfg{ThresholdMs: 500}}},
			},
		},
		{
			name:             "UseTracestate_ProbabilisticWithConsistentRV",
			numTraces:        500,
			bypassSplit:      true,
			enableCache:      false,
			useTracestate:    true,
			recordPolicy:     true,
			checkPolicyNames: true,
			checkTraceState:  true,
			policies: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "prob-30", Type: Probabilistic, ProbabilisticCfg: ProbabilisticCfg{SamplingPercentage: 30}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "prob-0", Type: Probabilistic, ProbabilisticCfg: ProbabilisticCfg{SamplingPercentage: 0}}},
			},
		},
		{
			name:        "BypassSplitFalse_Ablation",
			numTraces:   400,
			bypassSplit: false,
			enableCache: true,
			policies: []PolicyCfg{
				{
					sharedPolicyCfg: sharedPolicyCfg{Name: "drop-healthz", Type: Drop},
					DropCfg: DropCfg{SubPolicyCfg: []AndSubPolicyCfg{
						{sharedPolicyCfg: sharedPolicyCfg{Name: "d1", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "http.route", Values: []string{"/healthz"}}}},
					}},
				},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "slow", Type: Latency, LatencyCfg: LatencyCfg{ThresholdMs: 500}}},
			},
		},
		{
			name:        "CustomExtensionReturningDropped",
			numTraces:   300,
			bypassSplit: true,
			enableCache: true,
			withHost:    true,
			policies: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "ext-drop", Type: PolicyType("custom_drop")}},
			},
		},
		{
			name:        "EmptyAnd",
			numTraces:   100,
			bypassSplit: true,
			enableCache: true,
			policies: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "empty-and", Type: And}},
			},
		},
		{
			name:        "EmptyDropOverridesAlwaysSample",
			numTraces:   100,
			bypassSplit: true,
			enableCache: true,
			policies: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "empty-drop", Type: Drop}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "always", Type: AlwaysSample}},
			},
		},
	}

	orders := []string{"in-order", "reversed", "shuffled"}
	totalChecked := 0

	for sIdx, sc := range suiteConfigs {
		t.Run(sc.name, func(t *testing.T) {
			if sc.useTracestate {
				gate := metadata.ProcessorTailsamplingprocessorUsetracestateFeatureGate
				prev := gate.IsEnabled()
				require.NoError(t, featuregate.GlobalRegistry().Set(gate.ID(), true))
				t.Cleanup(func() {
					require.NoError(t, featuregate.GlobalRegistry().Set(gate.ID(), prev))
				})
			}

			var host component.Host = componenttest.NewNopHost()
			if sc.withHost {
				host = &extHost{
					Host: componenttest.NewNopHost(),
					exts: map[component.ID]component.Component{
						component.MustNewID("custom_drop"): &customDropExtension{dropKey: "custom.ext"},
					},
				}
			}

			for oIdx, order := range orders {
				t.Run(order, func(t *testing.T) {
					batches := generateRandomizedParityBatches(int64(1000+sIdx*10+oIdx), sc.numTraces, order, sc.useTracestate)
					totalChecked += sc.numTraces

					var baseOpts []Option
					if sc.recordPolicy {
						baseOpts = append(baseOpts, withRecordPolicy())
					}
					if sc.useTracestate {
						baseOpts = append(baseOpts, withUseTracestate())
					}

					var dCache DecisionCacheConfig
					if sc.enableCache {
						dCache = DecisionCacheConfig{SampledCacheSize: 4096, NonSampledCacheSize: 4096}
					}

					oracleCfg := Config{
						SamplingStrategy:   samplingStrategyTraceComplete,
						DecisionWait:       defaultTestDecisionWait,
						NumTraces:          uint64(sc.numTraces * 2),
						PolicyCfgs:         sc.policies,
						SampleOnFirstMatch: sc.sampleOnFirstMatch,
						DecisionCache:      dCache,
						Options:            baseOpts,
					}
					ivmOpts := append(append([]Option(nil), baseOpts...), withIncrementalBypassSplit(sc.bypassSplit))
					ivmCfg := Config{
						SamplingStrategy:   samplingStrategyIncremental,
						DecisionWait:       defaultTestDecisionWait,
						NumTraces:          uint64(sc.numTraces * 2),
						PolicyCfgs:         sc.policies,
						SampleOnFirstMatch: sc.sampleOnFirstMatch,
						DecisionCache:      dCache,
						Options:            ivmOpts,
					}

					oracleSink, _ := runProcessorSync(t, oracleCfg, host, batches)
					ivmSink, _ := runProcessorSync(t, ivmCfg, host, batches)

					oracleIDs := collectSampledTraceIDs(oracleSink)
					ivmIDs := collectSampledTraceIDs(ivmSink)
					require.Equal(t, oracleIDs, ivmIDs, "sampled TraceID/span counts diverged in %s/%s", sc.name, order)

					if sc.checkPolicyNames {
						oPol := collectSampledPolicyAttr(oracleSink)
						iPol := collectSampledPolicyAttr(ivmSink)
						require.Equal(t, oPol, iPol, "tailsampling.policy attribute diverged in %s/%s", sc.name, order)
					}
					if sc.checkTraceState {
						oTS := collectSpanTraceStates(oracleSink)
						iTS := collectSpanTraceStates(ivmSink)
						require.Equal(t, oTS, iTS, "outgoing span tracestate diverged in %s/%s", sc.name, order)
					}
				})
			}
		})
	}
	require.GreaterOrEqual(t, totalChecked, 5000, "must verify >= 5,000 randomized traces")
	t.Logf("Verified 0 mismatches across %d randomized multi-batch traces", totalChecked)
}

func TestIVMParity_MultiShardAndOver32Nodes(t *testing.T) {
	// Build 50 policies (> 32 circuit nodes) to verify flat stride slice resizing.
	policies := make([]PolicyCfg, 0, 50)
	for i := range 48 {
		policies = append(policies, PolicyCfg{
			sharedPolicyCfg: sharedPolicyCfg{
				Name: fmt.Sprintf("str-pol-%d", i),
				Type: StringAttribute,
				StringAttributeCfg: StringAttributeCfg{
					Key:    "http.route",
					Values: []string{fmt.Sprintf("/nonexistent-%d", i)},
				},
			},
		})
	}
	policies = append(policies,
		PolicyCfg{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
		PolicyCfg{sharedPolicyCfg: sharedPolicyCfg{Name: "slow", Type: Latency, LatencyCfg: LatencyCfg{ThresholdMs: 500}}},
	)

	batches := generateRandomizedParityBatches(777, 400, "shuffled", false)

	oracleSink := new(consumertest.TracesSink)
	oracleCfg := Config{
		SamplingStrategy: samplingStrategyTraceComplete,
		DecisionWait:     10 * time.Second,
		NumTraces:        2000,
		NumShards:        4,
		PolicyCfgs:       policies,
		DecisionCache:    DecisionCacheConfig{SampledCacheSize: 4096, NonSampledCacheSize: 4096},
		Options:          []Option{withTickerFrequency(time.Hour)},
	}
	op, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), oracleSink, oracleCfg)
	require.NoError(t, err)
	require.NoError(t, op.Start(t.Context(), componenttest.NewNopHost()))
	for _, b := range cloneTracesSlice(batches) {
		require.NoError(t, op.ConsumeTraces(t.Context(), b))
	}
	require.NoError(t, op.Shutdown(t.Context()))

	ivmSink := new(consumertest.TracesSink)
	ivmCfg := oracleCfg
	ivmCfg.SamplingStrategy = samplingStrategyIncremental
	ip, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), ivmSink, ivmCfg)
	require.NoError(t, err)
	require.NoError(t, ip.Start(t.Context(), componenttest.NewNopHost()))

	// Send batches concurrently across 4 goroutines to exercise -race under num_shards=4.
	var wg sync.WaitGroup
	clones := cloneTracesSlice(batches)
	for g := range 4 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := worker; i < len(clones); i += 4 {
				require.NoError(t, ip.ConsumeTraces(t.Context(), clones[i]))
			}
		}(g)
	}
	wg.Wait()
	require.NoError(t, ip.Shutdown(t.Context()))

	require.Equal(t, collectSampledTraceIDs(oracleSink), collectSampledTraceIDs(ivmSink))
}

func TestIVMParity_OTTLErrorLatticeAndStatefulHybrid(t *testing.T) {
	base := time.Unix(1700000000, 0)

	t.Run("NOT_Of_OTTL_Error_DoesNotInvertToSampled", func(t *testing.T) {
		// ParseJSON on invalid string triggers an OTTL error under PropagateError!
		policies := []PolicyCfg{
			{
				sharedPolicyCfg: sharedPolicyCfg{Name: "not-ottl-err", Type: Not},
				NotCfg: NotCfg{
					SubPolicy: NotSubPolicyCfg{
						sharedPolicyCfg: sharedPolicyCfg{
							Name: "ottl-bad-json",
							Type: OTTLCondition,
							OTTLConditionCfg: OTTLConditionCfg{
								ErrorMode:      ottl.PropagateError,
								SpanConditions: []string{`ParseJSON(attributes["payload"])["ok"] == true`},
							},
						},
					},
				},
			},
		}
		tidErr := uInt64ToTraceID(1)
		tidOK := uInt64ToTraceID(2)
		batches := []ptrace.Traces{
			makeSpanBatch(tidErr, 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"payload": "{not-json"}),
			makeSpanBatch(tidOK, 2, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"payload": `{"ok":false}`}),
		}

		oracleSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyTraceComplete, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, batches)
		ivmSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyIncremental, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, batches)

		// tidErr errored -> NOT(Error) = Error (NOT Sampled!). tidOK -> NOT(false) = Sampled!
		require.Equal(t, map[pcommon.TraceID]int{tidOK: 1}, collectSampledTraceIDs(oracleSink))
		require.Equal(t, collectSampledTraceIDs(oracleSink), collectSampledTraceIDs(ivmSink))
	})

	t.Run("AND_OrderedErrorSuppressionAndPropagation", func(t *testing.T) {
		// Subpolicy 0 = false suppresses Subpolicy 1 error; Subpolicy 0 = true propagates Subpolicy 1 error.
		policies := []PolicyCfg{
			{
				sharedPolicyCfg: sharedPolicyCfg{Name: "and-err", Type: And},
				AndCfg: AndCfg{SubPolicyCfg: []AndSubPolicyCfg{
					{sharedPolicyCfg: sharedPolicyCfg{Name: "s0", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "env", Values: []string{"prod"}}}},
					{sharedPolicyCfg: sharedPolicyCfg{Name: "s1", Type: OTTLCondition, OTTLConditionCfg: OTTLConditionCfg{
						ErrorMode:      ottl.PropagateError,
						SpanConditions: []string{`ParseJSON(attributes["payload"])["ok"] == true`},
					}}},
				}},
			},
		}
		tidSuppress := uInt64ToTraceID(11)
		tidPropagate := uInt64ToTraceID(12)
		tidMatch := uInt64ToTraceID(13)
		batches := []ptrace.Traces{
			makeSpanBatch(tidSuppress, 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"env": "dev", "payload": "{bad"}),
			makeSpanBatch(tidPropagate, 2, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"env": "prod", "payload": "{bad"}),
			makeSpanBatch(tidMatch, 3, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"env": "prod", "payload": `{"ok":true}`}),
		}

		oracleSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyTraceComplete, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, batches)
		ivmSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyIncremental, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, batches)

		require.Equal(t, map[pcommon.TraceID]int{tidMatch: 1}, collectSampledTraceIDs(oracleSink))
		require.Equal(t, collectSampledTraceIDs(oracleSink), collectSampledTraceIDs(ivmSink))
	})

	t.Run("HybridStatefulPoliciesAndNestedStatefulDrop", func(t *testing.T) {
		policies := []PolicyCfg{
			// Drop with pure subpolicy + stateful rate_limiting inside another drop.
			{
				sharedPolicyCfg: sharedPolicyCfg{Name: "drop-healthz", Type: Drop},
				DropCfg: DropCfg{SubPolicyCfg: []AndSubPolicyCfg{
					{sharedPolicyCfg: sharedPolicyCfg{Name: "d1", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "http.route", Values: []string{"/healthz"}}}},
				}},
			},
			{
				sharedPolicyCfg: sharedPolicyCfg{Name: "drop-rate", Type: Drop},
				DropCfg: DropCfg{SubPolicyCfg: []AndSubPolicyCfg{
					{sharedPolicyCfg: sharedPolicyCfg{Name: "rl", Type: RateLimiting, RateLimitingCfg: RateLimitingCfg{SpansPerSecond: 1000, BurstCapacity: 1000}}},
					{sharedPolicyCfg: sharedPolicyCfg{Name: "d2", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "http.route", Values: []string{"/metrics"}}}},
				}},
			},
			{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
			{sharedPolicyCfg: sharedPolicyCfg{Name: "bytes-lim", Type: BytesLimiting, BytesLimitingCfg: BytesLimitingCfg{BytesPerSecond: 1_000_000, BurstCapacity: 2_000_000}}},
			{
				sharedPolicyCfg: sharedPolicyCfg{Name: "comp", Type: Composite},
				CompositeCfg: CompositeCfg{
					MaxTotalSpansPerSecond: 1000,
					PolicyOrder:            []string{"c1"},
					SubPolicyCfg: []CompositeSubPolicyCfg{
						{sharedPolicyCfg: sharedPolicyCfg{Name: "c1", Type: AlwaysSample}},
					},
					RateAllocation: []RateAllocationCfg{{Policy: "c1", Percent: 100}},
				},
			},
		}
		batches := []ptrace.Traces{
			makeSpanBatch(uInt64ToTraceID(1), 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeError, map[string]any{"http.route": "/healthz"}),
			makeSpanBatch(uInt64ToTraceID(2), 2, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeError, map[string]any{"http.route": "/metrics"}),
			makeSpanBatch(uInt64ToTraceID(3), 3, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeError, map[string]any{"http.route": "/api"}),
		}

		oracleSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyTraceComplete, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, batches)
		ivmSink, tsp := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyIncremental, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies, DecisionCache: DecisionCacheConfig{NonSampledCacheSize: 100}}, nil, batches)

		// Only trace 3 is sampled; trace 1 early-dropped before drop-rate because drop_0 is pure!
		require.Equal(t, map[pcommon.TraceID]int{uInt64ToTraceID(3): 1}, collectSampledTraceIDs(oracleSink))
		require.Equal(t, collectSampledTraceIDs(oracleSink), collectSampledTraceIDs(ivmSink))
		require.Equal(t, int64(1), tsp.ivm.stats.earlyDropped.Load(), "pure drop_0 should still early-eject trace 1!")
		require.Equal(t, int64(0), tsp.ivm.stats.earlySampled.Load(), "stateful positive policies must disable early Sampled short-circuit!")
	})

	t.Run("HybridBatchEvaluatorUnderUseTracestate", func(t *testing.T) {
		gate := metadata.ProcessorTailsamplingprocessorUsetracestateFeatureGate
		prev := gate.IsEnabled()
		require.NoError(t, featuregate.GlobalRegistry().Set(gate.ID(), true))
		t.Cleanup(func() {
			require.NoError(t, featuregate.GlobalRegistry().Set(gate.ID(), prev))
		})

		policies := []PolicyCfg{
			{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
			{sharedPolicyCfg: sharedPolicyCfg{Name: "rl", Type: RateLimiting, RateLimitingCfg: RateLimitingCfg{SpansPerSecond: 1000, BurstCapacity: 1000}}},
		}
		batches := []ptrace.Traces{
			makeSpanBatch(uInt64ToTraceID(1), 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeError, nil),
			makeSpanBatch(uInt64ToTraceID(2), 2, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil),
		}
		oracleSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyTraceComplete, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies, Options: []Option{withUseTracestate()}}, nil, batches)
		ivmSink, tsp := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyIncremental, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies, Options: []Option{withUseTracestate()}}, nil, batches)

		require.Equal(t, collectSampledTraceIDs(oracleSink), collectSampledTraceIDs(ivmSink))
		require.Equal(t, int64(0), tsp.ivm.stats.earlySampled.Load(), "BatchEvaluator must disable early Sampled short-circuit!")
	})
}

// TestIVMParityLatencyOracleB tests the documented Oracle B divergence:
// upstream latency.go checks upper_threshold_ms per prefix in arrival order,
// while incremental computes exact order-independent trace-wide (minStart, maxEnd).
func TestIVMParityLatencyOracleB(t *testing.T) {
	base := time.Unix(1700000000, 0)
	policies := []PolicyCfg{
		{sharedPolicyCfg: sharedPolicyCfg{Name: "bounded-lat", Type: Latency, LatencyCfg: LatencyCfg{ThresholdMs: 5000, UpperThresholdMs: 10000}}},
	}

	tid := uInt64ToTraceID(99)
	span6s := makeSpanBatch(tid, 1, 0, "svc", base, base.Add(6*time.Second), ptrace.StatusCodeOk, nil)
	span15s := makeSpanBatch(tid, 2, 1, "svc", base, base.Add(15*time.Second), ptrace.StatusCodeOk, nil)

	// Demonstrate upstream latency.go arrival-order bug:
	oFirst6s, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyTraceComplete, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, []ptrace.Traces{span6s, span15s})
	oFirst15s, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyTraceComplete, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, []ptrace.Traces{span15s, span6s})
	require.NotEmpty(t, collectSampledTraceIDs(oFirst6s), "upstream latency.go returns Sampled when 6s arrives before 15s")
	require.Empty(t, collectSampledTraceIDs(oFirst15s), "upstream latency.go returns NotSampled when 15s arrives before 6s")

	// Incremental is 100% order-independent (true trace duration = 15s > 10s upper bound -> NotSampled in BOTH orders!).
	iFirst6s, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyIncremental, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, []ptrace.Traces{span6s, span15s})
	iFirst15s, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyIncremental, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, []ptrace.Traces{span15s, span6s})
	require.Empty(t, collectSampledTraceIDs(iFirst6s))
	require.Empty(t, collectSampledTraceIDs(iFirst15s))

	// Zero StartTimestamp preservation & clock skew (end < start) safety!
	t.Run("ZeroStartTimestampAndClockSkew", func(t *testing.T) {
		latPolicies := []PolicyCfg{
			{sharedPolicyCfg: sharedPolicyCfg{Name: "lat-1s", Type: Latency, LatencyCfg: LatencyCfg{ThresholdMs: 1000}}},
		}
		// Span 1 has StartTimestamp == 0, EndTimestamp == 500ms; Span 2 has Start == 2s, End == 2.5s.
		// True trace span: [0, 2.5s] = 2500ms > 1000ms.
		tdZero := ptrace.NewTraces()
		sp1 := tdZero.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		sp1.SetTraceID(uInt64ToTraceID(201))
		sp1.SetSpanID(uInt64ToSpanID(1))
		sp1.SetStartTimestamp(0)
		sp1.SetEndTimestamp(pcommon.Timestamp(500 * time.Millisecond))

		tdTwo := ptrace.NewTraces()
		sp2 := tdTwo.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		sp2.SetTraceID(uInt64ToTraceID(201))
		sp2.SetSpanID(uInt64ToSpanID(2))
		sp2.SetStartTimestamp(pcommon.Timestamp(2 * time.Second))
		sp2.SetEndTimestamp(pcommon.Timestamp(2500 * time.Millisecond))

		iZero, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyIncremental, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: latPolicies}, nil, []ptrace.Traces{tdZero, tdTwo})
		require.Equal(t, map[pcommon.TraceID]int{uInt64ToTraceID(201): 2}, collectSampledTraceIDs(iZero))

		// Clock skew: EndTimestamp < StartTimestamp must NOT underflow uint64!
		tdSkew := ptrace.NewTraces()
		spSkew := tdSkew.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		spSkew.SetTraceID(uInt64ToTraceID(202))
		spSkew.SetSpanID(uInt64ToSpanID(3))
		spSkew.SetStartTimestamp(pcommon.NewTimestampFromTime(base.Add(10 * time.Second)))
		spSkew.SetEndTimestamp(pcommon.NewTimestampFromTime(base))
		iSkew, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyIncremental, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: latPolicies}, nil, []ptrace.Traces{tdSkew})
		require.Empty(t, collectSampledTraceIDs(iSkew), "clock skew end < start must clamp duration to 0ms, not uint64 wrap")
	})
}

func TestIVMLifecycle(t *testing.T) {
	base := time.Unix(1700000000, 0)

	t.Run("EarlyShortCircuitKeep_ZeroTicksAndZeroBufferingForLateSpans", func(t *testing.T) {
		s := setupTestTelemetry()
		sink := new(consumertest.TracesSink)
		controller := newTestTSPController()
		cfg := Config{
			SamplingStrategy: samplingStrategyIncremental,
			DecisionWait:     defaultTestDecisionWait,
			NumTraces:        100,
			DecisionCache:    DecisionCacheConfig{SampledCacheSize: 64, NonSampledCacheSize: 64},
			PolicyCfgs: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
			},
			Options: []Option{withTestController(controller), withRecordPolicy()},
		}
		p, err := newTracesProcessor(t.Context(), s.newSettings(), sink, cfg)
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))

		tid := uInt64ToTraceID(1)
		// Span 1 is ERROR -> emits BEFORE any tick fires!
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(tid, 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeError, nil)))
		require.Eventually(t, func() bool { return sink.SpanCount() == 1 }, time.Second, 5*time.Millisecond)

		tsp := shard0(p)
		require.Equal(t, int64(1), tsp.ivm.stats.earlySampled.Load())

		// Span 2 arrives (OK) -> bypasses buffer and streams straight through via sampledIDCache!
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(tid, 2, 1, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil)))
		require.Eventually(t, func() bool { return sink.SpanCount() == 2 }, time.Second, 5*time.Millisecond)
		require.Equal(t, int64(1), tsp.ivm.stats.latchedSampledSpans.Load())

		// Tick must NOT see a false dropped_too_early because RemoveFromBatch was called!
		controller.waitForTick()
		controller.waitForTick()
		require.NoError(t, p.Shutdown(t.Context()))
		require.Equal(t, 0, tsp.ivm.table.len())
		assertSumMetricValue(t, &s, "otelcol_processor_tail_sampling_sampling_trace_dropped_too_early", 0)
	})

	t.Run("EarlyDrop_CachedVsNopCache_ZeroWarnLogsZeroHookRefires", func(t *testing.T) {
		policies := []PolicyCfg{
			{
				sharedPolicyCfg: sharedPolicyCfg{Name: "drop-healthz", Type: Drop},
				DropCfg: DropCfg{SubPolicyCfg: []AndSubPolicyCfg{
					{sharedPolicyCfg: sharedPolicyCfg{Name: "h", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "http.route", Values: []string{"/healthz"}}}},
				}},
			},
			{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
		}

		// Case A: Cached mode (NonSampledCacheSize > 0) -> immediate table eviction + dropped_too_early == 0!
		s := setupTestTelemetry()
		ct := s.newSettings()
		controller := newTestTSPController()
		cfg := Config{
			SamplingStrategy: samplingStrategyIncremental,
			DecisionWait:     defaultTestDecisionWait,
			NumTraces:        100,
			DecisionCache:    DecisionCacheConfig{NonSampledCacheSize: 64},
			PolicyCfgs:       policies,
			Options:          []Option{withTestController(controller)},
		}
		p, err := newTracesProcessor(t.Context(), ct, new(consumertest.TracesSink), cfg)
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))

		tid := uInt64ToTraceID(42)
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(tid, 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeError, map[string]any{"http.route": "/healthz"})))
		controller.waitForTick()
		controller.waitForTick()
		require.NoError(t, p.Shutdown(t.Context()))
		require.Equal(t, int64(1), shard0(p).ivm.stats.earlyDropped.Load(), "must early-drop trace 42")
		require.Equal(t, 0, shard0(p).ivm.table.len(), "cached early drop must evict traceIndex immediately")
		assertSumMetricValue(t, &s, "otelcol_processor_tail_sampling_sampling_trace_dropped_too_early", 0)

		// Case B: NopCache mode (NonSampledCacheSize == 0) -> zero warn logs, zero hook re-fires, tick cleans up!
		core, logs := observer.New(zap.WarnLevel)
		logger := zap.New(core)
		set := processortest.NewNopSettings(metadata.Type)
		set.Logger = logger
		var hookCalls int
		controller2 := newTestTSPController()
		cfg2 := Config{
			SamplingStrategy: samplingStrategyIncremental,
			DecisionWait:     defaultTestDecisionWait,
			NumTraces:        100,
			PolicyCfgs:       policies,
			Options: []Option{
				withTestController(controller2),
				WithNonSampledHooks(func(_ context.Context, _ pcommon.TraceID, _ *TraceData) {
					hookCalls++
				}),
			},
		}
		p2, err := newTracesProcessor(t.Context(), set, new(consumertest.TracesSink), cfg2)
		require.NoError(t, err)
		require.NoError(t, p2.Start(t.Context(), componenttest.NewNopHost()))

		require.NoError(t, p2.ConsumeTraces(t.Context(), makeSpanBatch(tid, 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"http.route": "/healthz"})))
		// Second span arrives after drop latch!
		require.NoError(t, p2.ConsumeTraces(t.Context(), makeSpanBatch(tid, 2, 1, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeError, map[string]any{"http.route": "/api"})))
		controller2.waitForTick()
		controller2.waitForTick()
		require.NoError(t, p2.Shutdown(t.Context()))

		require.Equal(t, 1, hookCalls, "nonSampledHook must fire once, never re-fire on late spans under NopCache")
		require.Equal(t, 0, logs.Len(), "late Dropped spans under NopCache must NOT emit Unexpected sampling decision warnings")
		require.Equal(t, 0, shard0(p2).ivm.table.len(), "tick must evict latched NopCache slot")
	})

	t.Run("BlockOnOverflow_And_MaxTraceSize_NopCache_NoDeadlockNoRebirth", func(t *testing.T) {
		sink := new(consumertest.TracesSink)
		controller := newTestTSPController()
		cfg := Config{
			SamplingStrategy:      samplingStrategyIncremental,
			DecisionWait:          defaultTestDecisionWait,
			NumTraces:             1,
			BlockOnOverflow:       true,
			MaximumTraceSizeBytes: 120,
			PolicyCfgs: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "always", Type: AlwaysSample}},
			},
			Options: []Option{withTestController(controller)},
		}
		p, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), sink, cfg)
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
		defer func() { require.NoError(t, p.Shutdown(t.Context())) }()

		// Oversized trace 1 exceeds 120 bytes -> latched NotSampled under NopCache!
		tid1 := uInt64ToTraceID(1)
		big := makeSpanBatch(tid1, 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{
			"big": string(make([]byte, 300)),
		})
		require.NoError(t, p.ConsumeTraces(t.Context(), big))
		// Late small span for tid1 MUST NOT resurrect tid1 into AlwaysSample!
		small := makeSpanBatch(tid1, 2, 1, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil)
		require.NoError(t, p.ConsumeTraces(t.Context(), small))
		require.Equal(t, 0, sink.SpanCount(), "oversized trace must NOT be reborn as Sampled on late span under NopCache")

		// Now send tid2 while table is full (NumTraces=1) and block_on_overflow=true!
		// Tick MUST evict tid1 so tid2 unblocks instead of deadlocking!
		done := make(chan error, 1)
		go func() {
			tid2 := uInt64ToTraceID(2)
			done <- p.ConsumeTraces(t.Context(), makeSpanBatch(tid2, 3, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil))
		}()

		controller.waitForTick()
		controller.waitForTick()

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("deadlocked under block_on_overflow + NopCache + maximum_trace_size_bytes!")
		}
		controller.waitForTick()
		controller.waitForTick()
		require.Equal(t, map[pcommon.TraceID]int{uInt64ToTraceID(2): 1}, collectSampledTraceIDs(sink))
	})

	t.Run("NopCachePostLatchStampsPolicyAttribute", func(t *testing.T) {
		sink := new(consumertest.TracesSink)
		controller := newTestTSPController()
		cfg := Config{
			SamplingStrategy: samplingStrategyIncremental,
			DecisionWait:     defaultTestDecisionWait,
			NumTraces:        100,
			PolicyCfgs: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
			},
			Options: []Option{withTestController(controller), withRecordPolicy()},
		}
		p, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), sink, cfg)
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))

		tid := uInt64ToTraceID(7)
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(tid, 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeError, nil)))
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(tid, 2, 1, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil)))
		controller.waitForTick()
		controller.waitForTick()
		require.NoError(t, p.Shutdown(t.Context()))

		require.Equal(t, 2, sink.SpanCount())
		polMap := collectSampledPolicyAttr(sink)
		require.Equal(t, map[string]struct{}{"errors": {}}, polMap[tid], "post-latch NopCache span must also carry tailsampling.policy")
	})

	t.Run("DecisionWaitAfterRootReceived_DoesNotShortenLatchedWindow", func(t *testing.T) {
		sink := new(consumertest.TracesSink)
		controller := newTestTSPController()
		cfg := Config{
			SamplingStrategy:              samplingStrategyIncremental,
			DecisionWait:                  5 * time.Second,
			DecisionWaitAfterRootReceived: 1 * time.Second,
			NumTraces:                     100,
			PolicyCfgs: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
			},
			Options: []Option{withTestController(controller), withIDBatcher()},
		}
		p, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), sink, cfg)
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))

		tid := uInt64ToTraceID(55)
		// Child span with error latches Sampled early (under NopCache).
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(tid, 2, 1, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeError, nil)))
		require.Eventually(t, func() bool { return sink.SpanCount() == 1 }, time.Second, 5*time.Millisecond)

		// Root span arrives later -> must NOT move latched trace to the 1s batch!
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(tid, 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil)))
		require.Eventually(t, func() bool { return sink.SpanCount() == 2 }, time.Second, 5*time.Millisecond)

		// Advance 2 ticks (> 1s DecisionWaitAfterRootReceived, < 5s DecisionWait).
		// If MoveToEarlierBatch had shortened the latched window, tid would already be evicted
		// and a 3rd OK span would be treated as a brand-new NotSampled trace!
		controller.waitForTick()
		controller.waitForTick()
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(tid, 3, 1, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil)))
		require.Eventually(t, func() bool { return sink.SpanCount() == 3 }, time.Second, 5*time.Millisecond)

		// Remaining ticks evict the latched slot cleanly after the full 5s window.
		for range 6 {
			controller.waitForTick()
		}
		require.NoError(t, p.Shutdown(t.Context()))
		require.Equal(t, int64(2), shard0(p).ivm.stats.latchedSampledSpans.Load())
		require.Equal(t, 0, shard0(p).ivm.table.len())
	})

	t.Run("SetSamplingPolicy_HotReloadParityForPendingTraces", func(t *testing.T) {
		sink := new(consumertest.TracesSink)
		controller := newTestTSPController()
		initialPolicies := []PolicyCfg{
			{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
		}
		cfg := Config{
			SamplingStrategy: samplingStrategyIncremental,
			DecisionWait:     defaultTestDecisionWait,
			NumTraces:        100,
			PolicyCfgs:       initialPolicies,
			Options:          []Option{withTestController(controller)},
		}
		p, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), sink, cfg)
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))

		// Ingest OK trace while policy is only ERROR -> remains pending under circuitVer=1.
		tid := uInt64ToTraceID(88)
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(tid, 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil)))

		// Hot-reload policy to always_sample (circuitVer=2)!
		p.(*shardedProcessor).SetSamplingPolicy([]PolicyCfg{
			{sharedPolicyCfg: sharedPolicyCfg{Name: "always", Type: AlwaysSample}},
		})
		controller.waitForTick()
		controller.waitForTick()
		require.NoError(t, p.Shutdown(t.Context()))

		// Pending trace created under v1 MUST fall back and be evaluated under v2 (AlwaysSample) -> sampled!
		require.Equal(t, map[pcommon.TraceID]int{tid: 1}, collectSampledTraceIDs(sink))
		require.Equal(t, int64(1), shard0(p).ivm.stats.unmarshaledForFallback.Load())
	})

	t.Run("EmptyScopeSpans_And_WithPoliciesFallback_And_NumericZeroGuard", func(t *testing.T) {
		// 1. Empty ScopeSpans skipped cleanly.
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		rs.ScopeSpans().AppendEmpty() // empty scope
		ss := rs.ScopeSpans().AppendEmpty()
		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(uInt64ToTraceID(5))
		sp.SetSpanID(uInt64ToSpanID(1))
		sp.Status().SetCode(ptrace.StatusCodeError)

		oSink, _ := runProcessorSync(t, Config{
			SamplingStrategy: samplingStrategyIncremental,
			DecisionWait:     defaultTestDecisionWait,
			NumTraces:        10,
			PolicyCfgs:       []PolicyCfg{{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}}},
		}, nil, []ptrace.Traces{td})
		require.Equal(t, 1, oSink.SpanCount())
		require.Equal(t, 1, oSink.AllTraces()[0].ResourceSpans().At(0).ScopeSpans().Len(), "empty ScopeSpans must be skipped")

		// 2. withPolicies(...) injected evaluator fallback (no PolicyCfgs).
		mpe := &mockPolicyEvaluator{}
		mpe.SetDecision(samplingpolicy.Sampled)
		injected := []*policy{{name: "mock", evaluator: mpe, attribute: metric.WithAttributes(attribute.String("policy", "mock"))}}
		iSink, _ := runProcessorSync(t, Config{
			SamplingStrategy: samplingStrategyIncremental,
			DecisionWait:     defaultTestDecisionWait,
			NumTraces:        10,
			Options:          []Option{withPolicies(injected)},
		}, nil, []ptrace.Traces{simpleTraces()})
		require.Equal(t, 1, iSink.SpanCount())

		// 3. numeric_attribute with min=0, max=0 returns clean compile error instead of nil-deref panic.
		cfgZero := Config{
			SamplingStrategy: samplingStrategyIncremental,
			DecisionWait:     defaultTestDecisionWait,
			NumTraces:        10,
			PolicyCfgs: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "bad-num", Type: NumericAttribute, NumericAttributeCfg: NumericAttributeCfg{Key: "k", MinValue: 0, MaxValue: 0}}},
			},
		}
		pBad, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), new(consumertest.TracesSink), cfgZero)
		require.NoError(t, err)
		err = pBad.Start(t.Context(), componenttest.NewNopHost())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "min_value or max_value")
	})

	t.Run("ExactMeasureGroupProtoSizeMatchesMarshaler", func(t *testing.T) {
		batch := generateBenchBatchShaped(16, 4, func(trace, span int) int { return (trace + span) % 4 })
		var m ptrace.ProtoMarshaler
		for i := 0; i < batch.ResourceSpans().Len(); i++ {
			rs := batch.ResourceSpans().At(i)
			byTrace := make(map[pcommon.TraceID]*ivmTraceGroup)
			for j := 0; j < rs.ScopeSpans().Len(); j++ {
				spans := rs.ScopeSpans().At(j).Spans()
				for k := 0; k < spans.Len(); k++ {
					id := spans.At(k).TraceID()
					g := byTrace[id]
					if g == nil {
						g = &ivmTraceGroup{id: id}
						byTrace[id] = g
					}
					if len(g.scopeIndices) == 0 || g.scopeIndices[len(g.scopeIndices)-1] != j {
						g.scopeIndices = append(g.scopeIndices, j)
						g.spanIndices = append(g.spanIndices, []int{k})
					} else {
						last := len(g.spanIndices) - 1
						g.spanIndices[last] = append(g.spanIndices[last], k)
					}
				}
			}
			var sc ivmSizeCache
			sc.reset(rs.ScopeSpans().Len())
			for _, g := range byTrace {
				got := measureGroupProtoSize(rs, g, &sc)
				cp := ptrace.NewResourceSpans()
				rs.CopyTo(cp)
				extracted := extractGroupTraces(cp, g, false)
				want := m.ResourceSpansSize(extracted.ResourceSpans().At(0))
				require.Equal(t, want, got)
			}
		}
	})
}

type failingAppendExt struct {
	component.StartFunc
	component.ShutdownFunc
	storage tailstorageextension.TailStorage
}

func (f *failingAppendExt) Append(_ pcommon.TraceID, _ ptrace.Traces) error {
	return errors.New("simulated append failure")
}

func (f *failingAppendExt) Take(id pcommon.TraceID) (ptrace.Traces, error) {
	return f.storage.Take(id)
}

func (f *failingAppendExt) Delete(id pcommon.TraceID) error {
	return f.storage.Delete(id)
}

type failingStorageHost struct {
	component.Host
	ext component.Component
}

func (h *failingStorageHost) GetExtensions() map[component.ID]component.Component {
	return map[component.ID]component.Component{testExtensionID: h.ext}
}

// TestIVMGate2Regressions covers targeted Gate 2 findings F2-F14 and isolated F7 parity checks.
func TestIVMGate2Regressions(t *testing.T) {
	base := time.Unix(1700000000, 0)

	t.Run("F7_IsolatedSpanEmptyStringRegexAsymmetry", func(t *testing.T) {
		// Upstream string_attribute matches ^$ on resource attrs ("") but skips "" on span attrs!
		policies := []PolicyCfg{
			{sharedPolicyCfg: sharedPolicyCfg{Name: "empty-regex", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{
				Key:                  "k",
				Values:               []string{"^$"},
				EnabledRegexMatching: true,
			}}},
		}
		tidRes := uInt64ToTraceID(1)
		tdRes := makeSpanBatch(tidRes, 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil)
		tdRes.ResourceSpans().At(0).Resource().Attributes().PutStr("k", "")

		tidSpan := uInt64ToTraceID(2)
		tdSpan := makeSpanBatch(tidSpan, 2, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"k": ""})

		batches := []ptrace.Traces{tdRes, tdSpan}
		oSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyTraceComplete, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, batches)
		iSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyIncremental, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, batches)

		require.Equal(t, map[pcommon.TraceID]int{tidRes: 1}, collectSampledTraceIDs(oSink), "resource empty attr matches ^$, span empty attr does not")
		require.Equal(t, collectSampledTraceIDs(oSink), collectSampledTraceIDs(iSink))
	})

	t.Run("F5_F7_SampledAndNonSampledHooksParity_EarlyAndTick", func(t *testing.T) {
		policies := []PolicyCfg{
			{
				sharedPolicyCfg: sharedPolicyCfg{Name: "drop-healthz", Type: Drop},
				DropCfg: DropCfg{SubPolicyCfg: []AndSubPolicyCfg{
					{sharedPolicyCfg: sharedPolicyCfg{Name: "h", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "http.route", Values: []string{"/healthz"}}}},
				}},
			},
			{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
			{sharedPolicyCfg: sharedPolicyCfg{Name: "slow", Type: Latency, LatencyCfg: LatencyCfg{ThresholdMs: 500}}},
		}

		// Multi-batch traces covering:
		// 1: early drop across 2 batches (batch 1 buffered OK, batch 2 triggers /healthz drop!)
		// 2: early sampled across 2 batches (batch 1 buffered OK, batch 2 ERROR — held by pending drop -> tick!)
		// 3: not sampled at tick (2 buffered spans)
		t1a := makeSpanBatch(uInt64ToTraceID(1), 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"http.route": "/api"})
		t1b := makeSpanBatch(uInt64ToTraceID(1), 2, 1, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"http.route": "/healthz"})
		t2a := makeSpanBatch(uInt64ToTraceID(2), 3, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"http.route": "/api"})
		t2b := makeSpanBatch(uInt64ToTraceID(2), 4, 3, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeError, map[string]any{"http.route": "/api"})
		t3a := makeSpanBatch(uInt64ToTraceID(3), 5, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"http.route": "/api"})
		t3b := makeSpanBatch(uInt64ToTraceID(3), 6, 5, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"http.route": "/api"})
		batches := []ptrace.Traces{t1a, t2a, t3a, t1b, t2b, t3b}

		type hookRecord struct {
			decision   samplingpolicy.Decision
			policyName string
			threshold  uint64
			spans      int
		}

		collectHooks := func(strategy samplingStrategy) (map[pcommon.TraceID]hookRecord, map[pcommon.TraceID]hookRecord) {
			var mu sync.Mutex
			sampled := make(map[pcommon.TraceID]hookRecord)
			nonSampled := make(map[pcommon.TraceID]hookRecord)
			cfg := Config{
				SamplingStrategy: strategy,
				DecisionWait:     defaultTestDecisionWait,
				NumTraces:        100,
				PolicyCfgs:       policies,
				Options: []Option{
					WithSampledHooks(func(_ context.Context, id pcommon.TraceID, td *TraceData) {
						mu.Lock()
						defer mu.Unlock()
						sampled[id] = hookRecord{
							decision:   td.FinalDecision,
							policyName: td.PolicyName,
							threshold:  td.FinalThreshold.Unsigned(),
							spans:      td.ReceivedBatches.SpanCount(),
						}
					}),
					WithNonSampledHooks(func(_ context.Context, id pcommon.TraceID, td *TraceData) {
						mu.Lock()
						defer mu.Unlock()
						nonSampled[id] = hookRecord{
							decision:   td.FinalDecision,
							policyName: td.PolicyName,
							threshold:  td.FinalThreshold.Unsigned(),
							spans:      td.ReceivedBatches.SpanCount(),
						}
					}),
				},
			}
			runProcessorSync(t, cfg, nil, batches)
			return sampled, nonSampled
		}

		oS, oNS := collectHooks(samplingStrategyTraceComplete)
		iS, iNS := collectHooks(samplingStrategyIncremental)
		require.Equal(t, oS, iS, "WithSampledHooks metadata and ReceivedBatches.SpanCount must match trace-complete")
		require.Equal(t, oNS, iNS, "WithNonSampledHooks metadata and ReceivedBatches.SpanCount must match trace-complete (early drop + tick NotSampled)")
		require.Equal(t, 2, iNS[uInt64ToTraceID(1)].spans, "early drop hook must receive all prior + triggering spans")
		require.Equal(t, 2, iNS[uInt64ToTraceID(3)].spans, "tick NotSampled hook must receive all buffered spans")
	})

	t.Run("F7_CachedMaximumTraceSizeBytes_ZeroDroppedTooEarly", func(t *testing.T) {
		s := setupTestTelemetry()
		controller := newTestTSPController()
		var hookSpans int
		cfg := Config{
			SamplingStrategy:      samplingStrategyIncremental,
			DecisionWait:          defaultTestDecisionWait,
			NumTraces:             10,
			MaximumTraceSizeBytes: 120,
			DecisionCache:         DecisionCacheConfig{NonSampledCacheSize: 64},
			PolicyCfgs: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "always", Type: AlwaysSample}},
			},
			Options: []Option{
				withTestController(controller),
				WithNonSampledHooks(func(_ context.Context, _ pcommon.TraceID, td *TraceData) {
					hookSpans += td.ReceivedBatches.SpanCount()
				}),
			},
		}
		sink := new(consumertest.TracesSink)
		p, err := newTracesProcessor(t.Context(), s.newSettings(), sink, cfg)
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))

		tid := uInt64ToTraceID(77)
		big := makeSpanBatch(tid, 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"big": string(make([]byte, 300))})
		require.NoError(t, p.ConsumeTraces(t.Context(), big))
		// Late span hits nonSampledIDCache!
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(tid, 2, 1, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil)))
		controller.waitForTick()
		controller.waitForTick()
		require.NoError(t, p.Shutdown(t.Context()))

		require.Equal(t, 0, sink.SpanCount())
		require.Equal(t, 1, hookSpans, "nonSampledHook must receive triggering oversized span")
		require.Equal(t, 0, shard0(p).ivm.table.len())
		assertSumMetricValue(t, &s, "otelcol_processor_tail_sampling_sampling_trace_dropped_too_early", 0)
	})

	t.Run("F11_HybridAndNotRateLimiting_NoOrphanedNodes", func(t *testing.T) {
		policies := []PolicyCfg{
			{
				sharedPolicyCfg: sharedPolicyCfg{Name: "and-rl", Type: And},
				AndCfg: AndCfg{SubPolicyCfg: []AndSubPolicyCfg{
					{sharedPolicyCfg: sharedPolicyCfg{Name: "str", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "http.route", Values: []string{"/api"}}}},
					{sharedPolicyCfg: sharedPolicyCfg{Name: "rl", Type: RateLimiting, RateLimitingCfg: RateLimitingCfg{SpansPerSecond: 1000, BurstCapacity: 1000}}},
				}},
			},
			{
				sharedPolicyCfg: sharedPolicyCfg{Name: "not-rl", Type: Not},
				NotCfg: NotCfg{SubPolicy: NotSubPolicyCfg{
					sharedPolicyCfg: sharedPolicyCfg{Name: "rl2", Type: RateLimiting, RateLimitingCfg: RateLimitingCfg{SpansPerSecond: 1000, BurstCapacity: 1000}},
				}},
			},
		}
		batches := []ptrace.Traces{
			makeSpanBatch(uInt64ToTraceID(1), 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"http.route": "/api"}),
			makeSpanBatch(uInt64ToTraceID(2), 2, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"http.route": "/other"}),
		}
		oSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyTraceComplete, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, batches)
		iSink, tsp := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyIncremental, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policies}, nil, batches)

		require.Equal(t, collectSampledTraceIDs(oSink), collectSampledTraceIDs(iSink))
		// Exactly 2 top-level opaque nodes, zero orphaned child nodes!
		require.Len(t, tsp.ivm.circuit.nodes, 2, "collapsing to ivmKindOpaque must truncate orphaned child nodes")
		require.Empty(t, tsp.ivm.circuit.spanNodes)
		require.Empty(t, tsp.ivm.circuit.resourceNodes)
	})

	t.Run("F4_HybridDropPrefixNotDoubleEvaluated", func(t *testing.T) {
		// Part A: drop(ottl_condition(propagate)) + rate_limiting must record error count == 1 (NOT 2).
		s := setupTestTelemetry()
		controller := newTestTSPController()
		policiesErr := []PolicyCfg{
			{
				sharedPolicyCfg: sharedPolicyCfg{Name: "drop-ottl", Type: Drop},
				DropCfg: DropCfg{SubPolicyCfg: []AndSubPolicyCfg{
					{sharedPolicyCfg: sharedPolicyCfg{Name: "bad-ottl", Type: OTTLCondition, OTTLConditionCfg: OTTLConditionCfg{
						ErrorMode:      ottl.PropagateError,
						SpanConditions: []string{`ParseJSON(attributes["payload"])["ok"] == true`},
					}}},
				}},
			},
			{sharedPolicyCfg: sharedPolicyCfg{Name: "rl", Type: RateLimiting, RateLimitingCfg: RateLimitingCfg{SpansPerSecond: 1000, BurstCapacity: 1000}}},
		}
		p, err := newTracesProcessor(t.Context(), s.newSettings(), new(consumertest.TracesSink), Config{
			SamplingStrategy: samplingStrategyIncremental,
			DecisionWait:     defaultTestDecisionWait,
			NumTraces:        100,
			PolicyCfgs:       policiesErr,
			Options:          []Option{withTestController(controller)},
		})
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(uInt64ToTraceID(1), 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"payload": "{invalid"})))
		controller.waitForTick()
		controller.waitForTick()
		require.NoError(t, p.Shutdown(t.Context()))
		assertSumMetricValue(t, &s, "otelcol_processor_tail_sampling_sampling_policy_evaluation_error", 1)

		// Part B: drop(latency(5s..10s)) + rate_limiting with [6s, 15s] must NOT drop (Oracle B preserved!).
		policiesLat := []PolicyCfg{
			{
				sharedPolicyCfg: sharedPolicyCfg{Name: "drop-lat", Type: Drop},
				DropCfg: DropCfg{SubPolicyCfg: []AndSubPolicyCfg{
					{sharedPolicyCfg: sharedPolicyCfg{Name: "lat", Type: Latency, LatencyCfg: LatencyCfg{ThresholdMs: 5000, UpperThresholdMs: 10000}}},
				}},
			},
			{sharedPolicyCfg: sharedPolicyCfg{Name: "rl", Type: RateLimiting, RateLimitingCfg: RateLimitingCfg{SpansPerSecond: 1000, BurstCapacity: 1000}}},
		}
		tid := uInt64ToTraceID(2)
		batches := []ptrace.Traces{
			makeSpanBatch(tid, 1, 0, "svc", base, base.Add(6*time.Second), ptrace.StatusCodeOk, nil),
			makeSpanBatch(tid, 2, 1, "svc", base, base.Add(15*time.Second), ptrace.StatusCodeOk, nil),
		}
		iSink, _ := runProcessorSync(t, Config{SamplingStrategy: samplingStrategyIncremental, DecisionWait: defaultTestDecisionWait, NumTraces: 100, PolicyCfgs: policiesLat}, nil, batches)
		require.Equal(t, map[pcommon.TraceID]int{tid: 2}, collectSampledTraceIDs(iSink), "hybrid tick must NOT re-run upstream latency.go prefix bug")
	})

	t.Run("F3_BlockOnOverflow_CtxCancelExitsCleanlyWithoutSpin", func(t *testing.T) {
		controller := newTestTSPController()
		runCtx, cancel := context.WithCancel(t.Context())
		defer cancel()
		cfg := Config{
			SamplingStrategy: samplingStrategyIncremental,
			DecisionWait:     10 * time.Second,
			NumTraces:        1,
			BlockOnOverflow:  true,
			PolicyCfgs: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
			},
			Options: []Option{withTestController(controller), withIDBatcher()},
		}
		p, err := newTracesProcessor(runCtx, processortest.NewNopSettings(metadata.Type), new(consumertest.TracesSink), cfg)
		require.NoError(t, err)
		require.NoError(t, p.Start(runCtx, componenttest.NewNopHost()))

		// Trace 1 fills NumTraces=1 (stays pending because status is OK).
		require.NoError(t, p.ConsumeTraces(runCtx, makeSpanBatch(uInt64ToTraceID(1), 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil)))
		// Trace 2 is delivered to loop() (unbuffered rawWorkChan returns once loop() receives it)
		// and immediately enters ivmWaitForSpace because NumTraces == 1!
		require.NoError(t, p.ConsumeTraces(runCtx, makeSpanBatch(uInt64ToTraceID(2), 2, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil)))
		time.Sleep(15 * time.Millisecond)
		cancel()

		shutdownDone := make(chan error, 1)
		go func() {
			shutdownDone <- p.Shutdown(t.Context())
		}()
		select {
		case err := <-shutdownDone:
			require.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("Shutdown hung / spun in ivmWaitForSpace after ctx cancellation!")
		}
	})

	t.Run("F2_FailingTailStorageAppend_NoSlotLeakOrOvercount", func(t *testing.T) {
		enableTailStorageFeatureGateForTest(t)
		s := setupTestTelemetry()
		controller := newTestTSPController()
		ext := &failingAppendExt{storage: tailstorageextension.NewInMemoryTailStorage()}
		host := &failingStorageHost{Host: componenttest.NewNopHost(), ext: ext}
		cfg := Config{
			SamplingStrategy: samplingStrategyIncremental,
			DecisionWait:     defaultTestDecisionWait,
			NumTraces:        10,
			TailStorageID:    &testExtensionID,
			PolicyCfgs: []PolicyCfg{
				// Pending until tick (NOT error on first span), then tick resolves Sampled!
				{sharedPolicyCfg: sharedPolicyCfg{Name: "not-err", Type: Not}, NotCfg: NotCfg{SubPolicy: NotSubPolicyCfg{
					sharedPolicyCfg: sharedPolicyCfg{Name: "err", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}},
				}}},
			},
			Options: []Option{withTestController(controller)},
		}
		sink := new(consumertest.TracesSink)
		p, err := newTracesProcessor(t.Context(), s.newSettings(), sink, cfg)
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), host))

		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(uInt64ToTraceID(1), 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil)))
		controller.waitForTick()
		controller.waitForTick()
		require.NoError(t, p.Shutdown(t.Context()))

		require.Equal(t, 0, shard0(p).ivm.table.len(), "empty Take at tick must drop slot from table without leaking")
		require.Equal(t, int64(0), shard0(p).tracesOnMemory.Load(), "tracesOnMemory must return to 0")
		assertSumMetricValue(t, &s, "otelcol_processor_tail_sampling_sampling_trace_dropped_too_early", 1)
		// Global sampled count must NOT be incremented for empty Take!
		var rm metricdata.ResourceMetrics
		require.NoError(t, s.reader.Collect(t.Context(), &rm))
		gm := s.getMetric("otelcol_processor_tail_sampling_global_count_traces_sampled", rm)
		if sum, ok := gm.Data.(metricdata.Sum[int64]); ok {
			for _, dp := range sum.DataPoints {
				if v, exists := dp.Attributes.Value("sampled"); exists && v.AsString() == "true" {
					require.Equal(t, int64(0), dp.Value, "empty Take must not increment global sampled counter")
				}
			}
		}
	})

	t.Run("F8_WithPoliciesInjectedAlongsideNonEmptyPolicyCfgs", func(t *testing.T) {
		// Empty injected policies + non-empty PolicyCfgs must NOT panic!
		iSinkEmpty, _ := runProcessorSync(t, Config{
			SamplingStrategy: samplingStrategyIncremental,
			DecisionWait:     defaultTestDecisionWait,
			NumTraces:        10,
			PolicyCfgs:       []PolicyCfg{{sharedPolicyCfg: sharedPolicyCfg{Name: "always", Type: AlwaysSample}}},
			Options:          []Option{withPolicies([]*policy{})},
		}, nil, []ptrace.Traces{simpleTraces()})
		require.Equal(t, 0, iSinkEmpty.SpanCount())

		// Named mock policy + conflicting PolicyCfgs must run mock evaluator, NOT PolicyCfgs!
		mpe := &mockPolicyEvaluator{}
		mpe.SetDecision(samplingpolicy.NotSampled)
		injected := []*policy{{name: "custom-mock", evaluator: mpe, attribute: metric.WithAttributes(attribute.String("policy", "custom-mock"))}}
		iSinkMock, _ := runProcessorSync(t, Config{
			SamplingStrategy: samplingStrategyIncremental,
			DecisionWait:     defaultTestDecisionWait,
			NumTraces:        10,
			PolicyCfgs:       []PolicyCfg{{sharedPolicyCfg: sharedPolicyCfg{Name: "always", Type: AlwaysSample}}},
			Options:          []Option{withPolicies(injected)},
		}, nil, []ptrace.Traces{simpleTraces()})
		require.Equal(t, 0, iSinkMock.SpanCount(), "injected mock returning NotSampled must win over AlwaysSample in PolicyCfgs")
	})

	t.Run("F10_SetSamplingPolicyPreservesLatchedPolicyNameUnderNopCache", func(t *testing.T) {
		sink := new(consumertest.TracesSink)
		controller := newTestTSPController()
		cfg := Config{
			SamplingStrategy: samplingStrategyIncremental,
			DecisionWait:     defaultTestDecisionWait,
			NumTraces:        100,
			PolicyCfgs: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "orig-errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
			},
			Options: []Option{withTestController(controller), withRecordPolicy()},
		}
		p, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), sink, cfg)
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))

		tid := uInt64ToTraceID(91)
		// Span 1 latches Sampled under circuitVer=1 ("orig-errors").
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(tid, 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeError, nil)))
		require.Eventually(t, func() bool { return sink.SpanCount() == 1 }, time.Second, 5*time.Millisecond)

		// Hot-reload policy to a completely different name at index 0!
		p.(*shardedProcessor).SetSamplingPolicy([]PolicyCfg{
			{sharedPolicyCfg: sharedPolicyCfg{Name: "reloaded-other", Type: Latency, LatencyCfg: LatencyCfg{ThresholdMs: 5000}}},
		})
		// Send late OK span for tid -> must STILL stamp "orig-errors", NOT "reloaded-other"!
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(tid, 2, 1, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil)))
		controller.waitForTick()
		controller.waitForTick()
		require.NoError(t, p.Shutdown(t.Context()))

		require.Equal(t, 2, sink.SpanCount())
		require.Equal(t, map[string]struct{}{"orig-errors": {}}, collectSampledPolicyAttr(sink)[tid])
	})

	t.Run("F13_EarlyShortCircuitRecordsEvalErrorTelemetry", func(t *testing.T) {
		s := setupTestTelemetry()
		controller := newTestTSPController()
		policies := []PolicyCfg{
			{sharedPolicyCfg: sharedPolicyCfg{Name: "bad-ottl", Type: OTTLCondition, OTTLConditionCfg: OTTLConditionCfg{
				ErrorMode:      ottl.PropagateError,
				SpanConditions: []string{`ParseJSON(attributes["payload"])["ok"] == true`},
			}}},
			{sharedPolicyCfg: sharedPolicyCfg{Name: "prob-zero", Type: Probabilistic, ProbabilisticCfg: ProbabilisticCfg{SamplingPercentage: 0}}},
		}
		p, err := newTracesProcessor(t.Context(), s.newSettings(), new(consumertest.TracesSink), Config{
			SamplingStrategy: samplingStrategyIncremental,
			DecisionWait:     defaultTestDecisionWait,
			NumTraces:        100,
			DecisionCache:    DecisionCacheConfig{NonSampledCacheSize: 64},
			PolicyCfgs:       policies,
			Options:          []Option{withTestController(controller)},
		})
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(uInt64ToTraceID(1), 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"payload": "{bad"})))
		controller.waitForTick()
		controller.waitForTick()
		require.NoError(t, p.Shutdown(t.Context()))

		require.Equal(t, int64(1), shard0(p).ivm.stats.earlyNotSampled.Load())
		assertSumMetricValue(t, &s, "otelcol_processor_tail_sampling_sampling_policy_evaluation_error", 1)
	})

	t.Run("F14_FIFOEvictionOfLatchedNopCacheTrace_ZeroDroppedTooEarly", func(t *testing.T) {
		s := setupTestTelemetry()
		controller := newTestTSPController()
		cfg := Config{
			SamplingStrategy: samplingStrategyIncremental,
			DecisionWait:     defaultTestDecisionWait,
			NumTraces:        1,
			BlockOnOverflow:  false,
			PolicyCfgs: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
			},
			Options: []Option{withTestController(controller)},
		}
		sink := new(consumertest.TracesSink)
		p, err := newTracesProcessor(t.Context(), s.newSettings(), sink, cfg)
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))

		// Trace 1 latches early as Sampled under NopCache (stays in FIFO at head).
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(uInt64ToTraceID(1), 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeError, nil)))
		// Trace 2 triggers FIFO eviction of latched Trace 1!
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(uInt64ToTraceID(2), 2, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil)))
		controller.waitForTick()
		controller.waitForTick()
		require.NoError(t, p.Shutdown(t.Context()))

		assertSumMetricValue(t, &s, "otelcol_processor_tail_sampling_sampling_trace_dropped_too_early", 0)
	})

	t.Run("R2_1_PolicyNamesByVerPrunedUnderContinuousTraffic", func(t *testing.T) {
		// With overlapping traces across multiple SetSamplingPolicy reloads,
		// table.len() NEVER drops to zero, yet expired versions must be pruned!
		controller := newTestTSPController()
		cfg := Config{
			SamplingStrategy:            samplingStrategyIncremental,
			DecisionWait:                3 * time.Second,
			NumTraces:                   100,
			DropPendingTracesOnShutdown: true,
			PolicyCfgs: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "v1-err", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
			},
			Options: []Option{withTestController(controller), withIDBatcher()},
		}
		p, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), new(consumertest.TracesSink), cfg)
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))

		// Tick 0: ingest trace under v1.
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(uInt64ToTraceID(1), 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil)))
		controller.waitForTick()

		// Hot-reload to v2, ingest trace under v2 (table has v1 AND v2).
		p.(*shardedProcessor).SetSamplingPolicy([]PolicyCfg{
			{sharedPolicyCfg: sharedPolicyCfg{Name: "v2-err", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
		})
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(uInt64ToTraceID(2), 2, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil)))
		controller.waitForTick()

		// Hot-reload to v3 (no traces on v3 yet, wait -> ingest trace under v3).
		p.(*shardedProcessor).SetSamplingPolicy([]PolicyCfg{
			{sharedPolicyCfg: sharedPolicyCfg{Name: "v3-err", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
		})
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(uInt64ToTraceID(3), 3, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil)))
		controller.waitForTick()

		// Tick 3: trace 1 (v1) expires while traces 2 (v2) and 3 (v3) are STILL live in table!
		controller.waitForTick()
		// Reload to v4 and ingest trace 4 so table stays non-empty while trace 2 (v2) expires too.
		p.(*shardedProcessor).SetSamplingPolicy([]PolicyCfg{
			{sharedPolicyCfg: sharedPolicyCfg{Name: "v4-err", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
		})
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(uInt64ToTraceID(4), 4, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil)))
		controller.waitForTick()

		// Shut down with DropPendingTracesOnShutdown=true while traces 3 & 4 remain live in table!
		require.NoError(t, p.Shutdown(t.Context()))

		ivm := shard0(p).ivm
		require.Positive(t, ivm.table.len(), "table must still be non-empty on shutdown (proving continuous non-zero traffic)")
		_, hasV1 := ivm.policyNamesByVer[1]
		_, hasV2 := ivm.policyNamesByVer[2]
		require.False(t, hasV1, "expired policy version 1 must be pruned despite non-empty table")
		require.False(t, hasV2, "expired policy version 2 must be pruned despite non-empty table")
		require.Contains(t, ivm.policyNamesByVer, uint32(4), "current policy version 4 must remain retained")
		require.Zero(t, ivm.verRefCounts[1])
		require.Zero(t, ivm.verRefCounts[2])
	})

	t.Run("R2_2_FailingTakeWithNonSampledHooksCallsDeleteFallback", func(t *testing.T) {
		enableTailStorageFeatureGateForTest(t)
		ext := &failingTakeStorageExt{
			storage: tailstorageextension.NewInMemoryTailStorage(),
			deleted: make(map[pcommon.TraceID]int),
		}
		host := &failingStorageHost{Host: componenttest.NewNopHost(), ext: ext}
		controller := newTestTSPController()
		var hookCalls int
		policies := []PolicyCfg{
			{
				sharedPolicyCfg: sharedPolicyCfg{Name: "drop-healthz", Type: Drop},
				DropCfg: DropCfg{SubPolicyCfg: []AndSubPolicyCfg{
					{sharedPolicyCfg: sharedPolicyCfg{Name: "h", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "http.route", Values: []string{"/healthz"}}}},
				}},
			},
			{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
		}
		cfg := Config{
			SamplingStrategy:      samplingStrategyIncremental,
			DecisionWait:          defaultTestDecisionWait,
			NumTraces:             100,
			MaximumTraceSizeBytes: 500,
			TailStorageID:         &testExtensionID,
			DecisionCache:         DecisionCacheConfig{NonSampledCacheSize: 64},
			PolicyCfgs:            policies,
			Options: []Option{
				withTestController(controller),
				WithNonSampledHooks(func(_ context.Context, _ pcommon.TraceID, _ *TraceData) {
					hookCalls++
				}),
			},
		}
		p, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), new(consumertest.TracesSink), cfg)
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), host))

		// 1. Early drop (span 1 buffered, span 2 triggers /healthz drop -> collectEarlyBatches Take fails -> Delete fallback!).
		tidEarlyDrop := uInt64ToTraceID(1)
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(tidEarlyDrop, 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"http.route": "/api"})))
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(tidEarlyDrop, 2, 1, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"http.route": "/healthz"})))

		// 2. Oversized drop (span 1 buffered, span 2 pushes trace > 500 bytes -> collectEarlyBatches Take fails -> Delete fallback!).
		tidOversize := uInt64ToTraceID(2)
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(tidOversize, 3, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"http.route": "/api"})))
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(tidOversize, 4, 3, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"big": string(make([]byte, 700))})))

		// 3. Tick expiry NotSampled (span 1 buffered -> ivmFinalizeNotSampledOnTick Take fails -> Delete fallback!).
		tidTick := uInt64ToTraceID(3)
		require.NoError(t, p.ConsumeTraces(t.Context(), makeSpanBatch(tidTick, 5, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, map[string]any{"http.route": "/api"})))

		controller.waitForTick()
		controller.waitForTick()
		require.NoError(t, p.Shutdown(t.Context()))

		require.Equal(t, 3, hookCalls, "all 3 non-sampled paths must still invoke nonSampledHooks")
		require.Equal(t, 1, ext.deleted[tidEarlyDrop], "early drop Take error must invoke Delete fallback")
		require.Equal(t, 1, ext.deleted[tidOversize], "oversized drop Take error must invoke Delete fallback")
		require.Equal(t, 1, ext.deleted[tidTick], "tick expiry Take error must invoke Delete fallback")
		require.Equal(t, 0, shard0(p).ivm.table.len())
	})
}

type failingTakeStorageExt struct {
	component.StartFunc
	component.ShutdownFunc
	mu      sync.Mutex
	storage tailstorageextension.TailStorage
	deleted map[pcommon.TraceID]int
}

func (f *failingTakeStorageExt) Append(id pcommon.TraceID, td ptrace.Traces) error {
	return f.storage.Append(id, td)
}

func (f *failingTakeStorageExt) Take(_ pcommon.TraceID) (ptrace.Traces, error) {
	return ptrace.NewTraces(), errors.New("simulated take failure")
}

func (f *failingTakeStorageExt) Delete(id pcommon.TraceID) error {
	f.mu.Lock()
	f.deleted[id]++
	f.mu.Unlock()
	return f.storage.Delete(id)
}
