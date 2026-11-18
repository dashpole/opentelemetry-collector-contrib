// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cumulativetodeltaprocessor

import (
	"math/rand/v2"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor/internal/tracking"
)

func buildSinglePointMetric(seriesIdx int, startTS, ts pcommon.Timestamp, val int64) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("bench")
	m := sm.Metrics().AppendEmpty()
	m.SetName("http_requests_total")
	sum := m.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := sum.DataPoints().AppendEmpty()
	dp.Attributes().PutInt("series", int64(seriesIdx))
	if startTS != 0 {
		dp.SetStartTimestamp(startTS)
	}
	dp.SetTimestamp(ts)
	dp.SetIntValue(val)
	return md
}

func buildBatchMetrics(startIdx, count int, startTS, ts pcommon.Timestamp, val int64) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("bench")
	m := sm.Metrics().AppendEmpty()
	m.SetName("http_requests_total")
	sum := m.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dps := sum.DataPoints()
	dps.EnsureCapacity(count)
	for i := range count {
		dp := dps.AppendEmpty()
		dp.Attributes().PutInt("series", int64(startIdx+i))
		if startTS != 0 {
			dp.SetStartTimestamp(startTS)
		}
		dp.SetTimestamp(ts)
		dp.SetIntValue(val)
	}
	return md
}

func measureHeapBytes() (uint64, uint64, uint64) {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc, ms.HeapObjects, ms.PauseTotalNs
}

func TestMemoryFootprint50kSeries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 50k series memory test in -short mode")
	}

	const numSeries = 50_000
	const batchSize = 500
	base := time.Now().Add(time.Hour)

	modes := []tracking.RetractionMode{
		tracking.RetractionModeOff,
		tracking.RetractionModeZSet,
		tracking.RetractionModeZSetCompact,
		tracking.RetractionModeNonNegative,
	}

	measure := func(mode tracking.RetractionMode, scrapeInterval time.Duration, steps int, rotatingGenerations int) (deltaBytes int64, deltaObjs int64, pauseMs float64, streams int64) {
		beforeBytes, beforeObjs, beforePause := measureHeapBytes()

		params := processortest.NewNopSettings(metadata.Type)
		params.Logger = zap.NewNop()
		cfg := &Config{
			MaxStaleness:     0, // Disable background sweeper during deterministic measurement.
			InitialValue:     tracking.InitialValueKeep,
			RetractionMode:   mode,
			ReorderWindow:    30 * time.Second,
			MaxHistoryPoints: 64,
		}
		p, err := createMetricsProcessor(t.Context(), params, cfg, consumertest.NewNop())
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))

		for step := range steps {
			ts := pcommon.NewTimestampFromTime(base.Add(time.Duration(step) * scrapeInterval))
			var startTS pcommon.Timestamp
			if rotatingGenerations > 0 {
				gen := step * rotatingGenerations / steps
				genStartStep := gen * steps / rotatingGenerations
				startTS = pcommon.NewTimestampFromTime(base.Add(time.Duration(genStartStep)*scrapeInterval - scrapeInterval/2))
			}
			for off := 0; off < numSeries; off += batchSize {
				md := buildBatchMetrics(off, batchSize, startTS, ts, int64((step+1)*10))
				require.NoError(t, p.ConsumeMetrics(t.Context(), md))
			}
		}

		afterBytes, afterObjs, afterPause := measureHeapBytes()
		runtime.KeepAlive(p)

		deltaBytes = int64(afterBytes) - int64(beforeBytes)
		deltaObjs = int64(afterObjs) - int64(beforeObjs)
		pauseMs = float64(afterPause-beforePause) / 1e6
		streams = numSeries
		if rotatingGenerations > 0 {
			streams = numSeries * int64(rotatingGenerations)
		}
		require.NoError(t, p.Shutdown(t.Context()))
		return deltaBytes, deltaObjs, pauseMs, streams
	}

	// Sub-case A: 5s scrape / 30s window -> steady state W = 8 points retained per series (run 12 steps = 60s).
	t.Logf("\n=== 50,000 Active Series Memory Footprint: 5s scrape / 30s window (W=8 steady state) ===")
	t.Logf("%-13s | %11s | %10s | %11s | %11s | %8s", "Mode", "HeapMiB", "B/series", "DeltaVsOff", "HeapObjs", "GCPause")
	var offBytes5s int64
	for _, m := range modes {
		b, objs, pause, _ := measure(m, 5*time.Second, 12, 0)
		if m == tracking.RetractionModeOff {
			offBytes5s = b
		}
		deltaBPerSeries := float64(b-offBytes5s) / float64(numSeries)
		t.Logf("%-13s | %8.2fMiB | %10.1f | %+9.1f B | %11d | %6.2fms",
			m.String(), float64(b)/(1024*1024), float64(b)/float64(numSeries), deltaBPerSeries, objs, pause)
		if m != tracking.RetractionModeOff {
			// Incremental heap budget at W=8: <= 256 B/series (<= 12.21 MiB for 50k series).
			assert.LessOrEqual(t, deltaBPerSeries, 256.0, "mode=%s exceeded 256 B/series incremental heap gate", m)
		}
	}

	// Sub-case B: 2s scrape / 30s window -> steady state W = 17 points retained per series (run 22 steps = 44s).
	t.Logf("\n=== 50,000 Active Series Memory Footprint: 2s scrape / 30s window (W=17 steady state) ===")
	t.Logf("%-13s | %11s | %10s | %11s | %11s | %8s", "Mode", "HeapMiB", "B/series", "DeltaVsOff", "HeapObjs", "GCPause")
	var offBytes2s int64
	for _, m := range modes {
		b, objs, pause, _ := measure(m, 2*time.Second, 22, 0)
		if m == tracking.RetractionModeOff {
			offBytes2s = b
		}
		deltaBPerSeries := float64(b-offBytes2s) / float64(numSeries)
		t.Logf("%-13s | %8.2fMiB | %10.1f | %+9.1f B | %11d | %6.2fms",
			m.String(), float64(b)/(1024*1024), float64(b)/float64(numSeries), deltaBPerSeries, objs, pause)
	}

	// Sub-case C: Rotating StartTimestamp (3 generations / 2 restarts per series = 150,000 map entries, R+1 = 3).
	t.Logf("\n=== Rotating StartTimestamp Scaling: 50,000 Series x 3 Generations / 2 Restarts (150,000 map entries) ===")
	t.Logf("%-13s | %11s | %10s | %11s", "Mode", "HeapMiB", "B/entry", "HeapObjs")
	for _, m := range []tracking.RetractionMode{tracking.RetractionModeOff, tracking.RetractionModeZSet} {
		b, objs, _, entries := measure(m, 5*time.Second, 9, 3)
		t.Logf("%-13s | %8.2fMiB | %10.1f | %11d",
			m.String(), float64(b)/(1024*1024), float64(b)/float64(entries), objs)
	}
}

func setup50kBenchProcessor(b *testing.B, mode tracking.RetractionMode) (processor.Metrics, time.Time, int64) {
	b.Helper()
	const numSeries = 50_000
	const batchSize = 500
	base := time.Now().Add(time.Hour)

	params := processortest.NewNopSettings(metadata.Type)
	params.Logger = zap.NewNop()
	cfg := &Config{
		MaxStaleness:     0,
		InitialValue:     tracking.InitialValueKeep,
		RetractionMode:   mode,
		ReorderWindow:    30 * time.Second,
		MaxHistoryPoints: 64,
	}
	p, err := createMetricsProcessor(b.Context(), params, cfg, consumertest.NewNop())
	require.NoError(b, err)
	require.NoError(b, p.Start(b.Context(), componenttest.NewNopHost()))
	b.Cleanup(func() { require.NoError(b, p.Shutdown(b.Context())) })

	// Pre-warm all 50k series into steady state (10 scrapes @ 5s = 50s -> W=8 retained + hasEvicted=true).
	for step := range 10 {
		ts := pcommon.NewTimestampFromTime(base.Add(time.Duration(step*5) * time.Second))
		for off := 0; off < numSeries; off += batchSize {
			md := buildBatchMetrics(off, batchSize, 0, ts, int64((step+1)*100))
			require.NoError(b, p.ConsumeMetrics(b.Context(), md))
		}
	}
	return p, base, 10
}

func Benchmark50kSeriesInOrder(b *testing.B) {
	const numSeries = 50_000
	modes := []tracking.RetractionMode{
		tracking.RetractionModeOff,
		tracking.RetractionModeZSet,
		tracking.RetractionModeZSetCompact,
		tracking.RetractionModeNonNegative,
	}

	for _, mode := range modes {
		b.Run(mode.String(), func(b *testing.B) {
			p, base, startStep := setup50kBenchProcessor(b, mode)
			steps := make([]int64, numSeries)
			for i := range steps {
				steps[i] = startStep
			}

			// Reuse a single pre-built Metrics container to measure pure conversion overhead.
			md := buildSinglePointMetric(0, 0, 0, 0)
			sum := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum()
			dp := sum.DataPoints().At(0)

			var i int
			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				s := i % numSeries
				i++
				st := steps[s]
				steps[s] = st + 1
				sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				sum.SetIsMonotonic(true)
				dp.Attributes().PutInt("series", int64(s))
				dp.SetStartTimestamp(0)
				dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(st*5) * time.Second)))
				dp.SetIntValue((st + 1) * 100)
				if err := p.ConsumeMetrics(b.Context(), md); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func Benchmark50kSeriesOutOfOrder15Pct(b *testing.B) {
	const numSeries = 50_000
	modes := []tracking.RetractionMode{
		tracking.RetractionModeOff,
		tracking.RetractionModeZSet,
		tracking.RetractionModeZSetCompact,
		tracking.RetractionModeNonNegative,
	}

	// Precompute deterministic action table (85% in-order, 15% out-of-order within 10s, ~3% resets).
	rng := rand.New(rand.NewPCG(777, 888))
	const tableLen = 1024
	type action struct {
		isOoO     bool
		isReset   bool
		lagSec    int64
		deltaFrac int64
	}
	actions := make([]action, tableLen)
	for i := range actions {
		r := rng.Float64()
		if r < 0.15 {
			actions[i] = action{
				isOoO:     true,
				lagSec:    1 + int64(rng.IntN(9)), // 1..9s behind high-water mark (within 30s window!)
				deltaFrac: int64(rng.IntN(80) + 1),
			}
		} else if r < 0.18 {
			actions[i] = action{isReset: true}
		}
	}

	for _, mode := range modes {
		b.Run(mode.String(), func(b *testing.B) {
			p, base, startStep := setup50kBenchProcessor(b, mode)
			curSec := make([]int64, numSeries)
			curVal := make([]int64, numSeries)
			for s := range numSeries {
				curSec[s] = (startStep - 1) * 5
				curVal[s] = startStep * 100
			}

			md := buildSinglePointMetric(0, 0, 0, 0)
			var idx int
			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				s := idx % numSeries
				act := actions[idx%tableLen]
				idx++

				var tsSecVal, val int64
				if act.isOoO {
					// Insert between the last two 5s scrapes (avoiding collisions by adding distinct second offset).
					tsSecVal = curSec[s] - act.lagSec
					if tsSecVal%5 == 0 {
						tsSecVal++
					}
					val = curVal[s] - act.deltaFrac
					if val < 0 {
						val = 10
					}
				} else if act.isReset {
					curSec[s] += 5
					curVal[s] = 25
					tsSecVal = curSec[s]
					val = curVal[s]
				} else {
					curSec[s] += 5
					curVal[s] += 100
					tsSecVal = curSec[s]
					val = curVal[s]
				}

				ts := pcommon.NewTimestampFromTime(base.Add(time.Duration(tsSecVal) * time.Second))
				if md.ResourceMetrics().Len() == 0 ||
					md.ResourceMetrics().At(0).ScopeMetrics().Len() == 0 ||
					md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().Len() == 0 ||
					md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().Len() != 1 {
					// Exclude input fixture reconstruction from timed/allocated conversion metrics.
					b.StopTimer()
					md = buildSinglePointMetric(s, 0, ts, val)
					b.StartTimer()
				} else {
					sum := md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum()
					sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
					sum.SetIsMonotonic(true)
					dp := sum.DataPoints().At(0)
					dp.Attributes().PutInt("series", int64(s))
					dp.SetStartTimestamp(0)
					dp.SetTimestamp(ts)
					dp.SetIntValue(val)
				}
				if err := p.ConsumeMetrics(b.Context(), md); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
