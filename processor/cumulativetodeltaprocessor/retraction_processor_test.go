// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cumulativetodeltaprocessor

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor/internal/metadatatest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor/internal/tracking"
)

func makeIntSumBatch(base time.Time, startOffsetSec int64, pts []struct {
	sec      int64
	val      int64
	exemplar bool
	noVal    bool
},
) pmetric.Metrics {
	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("requests_total")
	sum := m.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	for _, pt := range pts {
		dp := sum.DataPoints().AppendEmpty()
		if startOffsetSec >= 0 {
			dp.SetStartTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(startOffsetSec) * time.Second)))
		}
		dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(pt.sec) * time.Second)))
		dp.SetIntValue(pt.val)
		dp.Attributes().PutStr("service", "checkout")
		if pt.noVal {
			dp.SetFlags(noValueFlag)
		}
		if pt.exemplar {
			ex := dp.Exemplars().AppendEmpty()
			ex.SetIntValue(pt.val)
			ex.SetTimestamp(dp.Timestamp())
		}
	}
	return md
}

func TestConfigValidateRetraction(t *testing.T) {
	t.Parallel()

	assert.NoError(t, (&Config{}).Validate())
	assert.NoError(t, (&Config{
		RetractionMode:   tracking.RetractionModeZSet,
		ReorderWindow:    30 * time.Second,
		MaxHistoryPoints: 64,
	}).Validate())
	assert.NoError(t, (&Config{
		RetractionMode:    tracking.RetractionModeNonNegative,
		PreserveMonotonic: true,
	}).Validate())

	assert.ErrorContains(t, (&Config{RetractionMode: tracking.RetractionMode(99)}).Validate(), "invalid retraction_mode")
	assert.ErrorContains(t, (&Config{ReorderWindow: -time.Second}).Validate(), "reorder_window must be non-negative")
	assert.ErrorContains(t, (&Config{MaxHistoryPoints: -1}).Validate(), "max_history_points must be 0 (default) or >= 2")
	assert.ErrorContains(t, (&Config{MaxHistoryPoints: 1}).Validate(), "max_history_points must be 0 (default) or >= 2")

	// Verify newCumulativeToDeltaProcessor rejects unvalidated bad retraction configs directly.
	factory := NewFactory()
	for _, bad := range []*Config{
		{RetractionMode: tracking.RetractionMode(99)},
		{ReorderWindow: -time.Second},
		{MaxHistoryPoints: 1},
		{MaxHistoryPoints: -5},
	} {
		p, err := factory.CreateMetrics(t.Context(), processortest.NewNopSettings(metadata.Type), bad, consumertest.NewNop())
		require.Error(t, err)
		assert.Nil(t, p)
	}
}

func TestConfigUnmarshalRetractionConfmap(t *testing.T) {
	t.Parallel()

	raw := map[string]any{
		"retraction_mode":    "zset_compact",
		"reorder_window":     "45s",
		"max_history_points": 32,
		"preserve_monotonic": true,
	}
	cm := confmap.NewFromStringMap(raw)
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	require.NoError(t, cm.Unmarshal(cfg))
	require.NoError(t, confmap.Validate(cfg))
	assert.Equal(t, tracking.RetractionModeZSetCompact, cfg.RetractionMode)
	assert.Equal(t, 45*time.Second, cfg.ReorderWindow)
	assert.Equal(t, 32, cfg.MaxHistoryPoints)
	assert.True(t, cfg.PreserveMonotonic)

	badCM := confmap.NewFromStringMap(map[string]any{"retraction_mode": "invalid_mode"})
	badCfg := NewFactory().CreateDefaultConfig().(*Config)
	require.ErrorContains(t, badCM.Unmarshal(badCfg), "unknown retraction_mode")
}

// TestBaselineVsRetractionProof verifies the same cross-restart out-of-order sequence:
// legacy `off` mode drops points, emits an inverted interval, and fails conservation,
// while `zset`, `zset_compact`, and `non_negative` conserve the exact oracle sum.
func TestBaselineVsRetractionProof(t *testing.T) {
	base := time.Now().Add(time.Hour)
	// True timeline:
	//   Gen 1: t=10: 100, t=20: 200, t=24: 240
	//   Restart at t=25!
	//   Gen 2: t=30: 50, t=40: 120
	// Arrival sequence with jitter (t=20 late after t=24? Even clearer: t=24 arrives AFTER t=30, and t=20 arrives late!):
	//   (10, 100), (30, 50), (24, 240), (20, 200), (40, 120)
	// True cumulative work across both generations: Gen1 (240) + Gen2 (120) = 360.
	arrivals := []struct {
		sec int64
		val int64
	}{
		{10, 100},
		{30, 50},
		{24, 240},
		{20, 200},
		{40, 120},
	}

	type result struct {
		sum          int64
		emittedCount int
		negCount     int
		inverted     int
		isMonotonic  bool
	}

	runMode := func(mode tracking.RetractionMode, preserveMonotonic bool) result {
		next := new(consumertest.MetricsSink)
		cfg := &Config{
			InitialValue:      tracking.InitialValueKeep,
			RetractionMode:    mode,
			ReorderWindow:     60 * time.Second,
			MaxHistoryPoints:  32,
			PreserveMonotonic: preserveMonotonic,
		}
		p, err := NewFactory().CreateMetrics(t.Context(), processortest.NewNopSettings(metadata.Type), cfg, next)
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
		defer func() { require.NoError(t, p.Shutdown(t.Context())) }()

		for _, a := range arrivals {
			md := makeIntSumBatch(base, -1, []struct {
				sec      int64
				val      int64
				exemplar bool
				noVal    bool
			}{{sec: a.sec, val: a.val}})
			require.NoError(t, p.ConsumeMetrics(t.Context(), md))
		}

		var res result
		first := true
		for _, md := range next.AllMetrics() {
			for i := range md.ResourceMetrics().Len() {
				sms := md.ResourceMetrics().At(i).ScopeMetrics()
				for j := range sms.Len() {
					ms := sms.At(j).Metrics()
					for k := range ms.Len() {
						s := ms.At(k).Sum()
						if first {
							res.isMonotonic = s.IsMonotonic()
							first = false
						} else {
							assert.Equal(t, res.isMonotonic, s.IsMonotonic(), "IsMonotonic must not flip mid-stream")
						}
						for d := range s.DataPoints().Len() {
							dp := s.DataPoints().At(d)
							res.emittedCount++
							res.sum += dp.IntValue()
							if dp.IntValue() < 0 {
								res.negCount++
							}
							if dp.StartTimestamp() > dp.Timestamp() {
								res.inverted++
							}
						}
					}
				}
			}
		}
		return res
	}

	// Baseline `off`: drops (30,50) as reset, emits spurious +190 on inverted (30,24],
	// drops (20,200) and rewinds prevPoint, drops (40,120) as false reset!
	// Sum = 100 + 190 = 290 != 360 (-70 error!), 1 inverted interval!
	offRes := runMode(tracking.RetractionModeOff, false)
	assert.NotEqual(t, int64(360), offRes.sum, "baseline must exhibit conservation failure")
	assert.Equal(t, int64(290), offRes.sum)
	assert.Greater(t, offRes.inverted, 0, "baseline must exhibit inverted interval")
	assert.True(t, offRes.isMonotonic)

	// ZSet: exact sum 360, 0 inverted intervals, IsMonotonic=false by default.
	zsetRes := runMode(tracking.RetractionModeZSet, false)
	assert.Equal(t, int64(360), zsetRes.sum)
	assert.Equal(t, 0, zsetRes.inverted)
	assert.Greater(t, zsetRes.negCount, 0)
	assert.False(t, zsetRes.isMonotonic)

	// ZSet with PreserveMonotonic=true keeps IsMonotonic=true unconditionally.
	zsetMono := runMode(tracking.RetractionModeZSet, true)
	assert.Equal(t, int64(360), zsetMono.sum)
	assert.True(t, zsetMono.isMonotonic)

	// ZSetCompact: exact sum 360, fewer points than full zset, 0 inverted.
	compactRes := runMode(tracking.RetractionModeZSetCompact, false)
	assert.Equal(t, int64(360), compactRes.sum)
	assert.Equal(t, 0, compactRes.inverted)
	assert.Less(t, compactRes.emittedCount, zsetRes.emittedCount)
	assert.False(t, compactRes.isMonotonic)

	// NonNegative: exact sum 360, ZERO negative deltas, 0 inverted, IsMonotonic=true!
	nnRes := runMode(tracking.RetractionModeNonNegative, false)
	assert.Equal(t, int64(360), nnRes.sum)
	assert.Equal(t, 0, nnRes.negCount)
	assert.Equal(t, 0, nnRes.inverted)
	assert.True(t, nnRes.isMonotonic)
}

func TestRetractionExemplarClearingAndCausalOrder(t *testing.T) {
	base := time.Now().Add(time.Hour)

	// Test BOTH single-point batches (n == 1 fast path) AND multi-point batches (n > 1 rebuild path).
	for _, singleBatch := range []bool{false, true} {
		name := "single_point_batches"
		if singleBatch {
			name = "multi_point_batch"
		}
		t.Run(name, func(t *testing.T) {
			next := new(consumertest.MetricsSink)
			cfg := &Config{
				InitialValue:     tracking.InitialValueKeep,
				RetractionMode:   tracking.RetractionModeZSet,
				ReorderWindow:    60 * time.Second,
				MaxHistoryPoints: 16,
			}
			p, err := NewFactory().CreateMetrics(t.Context(), processortest.NewNopSettings(metadata.Type), cfg, next)
			require.NoError(t, err)
			require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
			defer func() { require.NoError(t, p.Shutdown(t.Context())) }()

			pts := []struct {
				sec      int64
				val      int64
				exemplar bool
				noVal    bool
			}{
				{sec: 10, val: 100, exemplar: true},
				{sec: 30, val: 300, exemplar: true},
				{sec: 20, val: 200, exemplar: true}, // Triggers [retract (10,30]:-200, (10,20]:+100, (20,30]:+100]
				{sec: 25, val: 0, noVal: true},      // NoRecordedValue dropped
				{sec: 40, val: 400, exemplar: true}, // Must appear AFTER retraction triple in strict causal order!
			}

			if singleBatch {
				require.NoError(t, p.ConsumeMetrics(t.Context(), makeIntSumBatch(base, -1, pts)))
			} else {
				for _, pt := range pts {
					require.NoError(t, p.ConsumeMetrics(t.Context(), makeIntSumBatch(base, -1, []struct {
						sec      int64
						val      int64
						exemplar bool
						noVal    bool
					}{pt})))
				}
			}

			type gotDP struct {
				startSec int64
				endSec   int64
				val      int64
				exLen    int
			}
			var got []gotDP
			for _, md := range next.AllMetrics() {
				for i := range md.ResourceMetrics().Len() {
					sms := md.ResourceMetrics().At(i).ScopeMetrics()
					for j := range sms.Len() {
						dps := sms.At(j).Metrics().At(0).Sum().DataPoints()
						for k := range dps.Len() {
							dp := dps.At(k)
							s := int64(0)
							if dp.StartTimestamp() != 0 {
								s = int64(dp.StartTimestamp().AsTime().Sub(base) / time.Second)
							}
							e := int64(dp.Timestamp().AsTime().Sub(base) / time.Second)
							got = append(got, gotDP{
								startSec: s,
								endSec:   e,
								val:      dp.IntValue(),
								exLen:    dp.Exemplars().Len(),
							})
						}
					}
				}
			}

			// Exact causal sequence:
			//   0: (0, 10]: +100 (native, keeps exemplar)
			//   1: (10, 30]: +200 (native, keeps exemplar)
			//   2: (10, 30]: -200 (synthetic retraction -> 0 exemplars)
			//   3: (10, 20]: +100 (native -> keeps exemplar)
			//   4: (20, 30]: +100 (synthetic split -> 0 exemplars)
			//   5: (30, 40]: +100 (native -> keeps exemplar)
			want := []gotDP{
				{0, 10, 100, 1},
				{10, 30, 200, 1},
				{10, 30, -200, 0},
				{10, 20, 100, 1},
				{20, 30, 100, 0},
				{30, 40, 100, 1},
			}
			assert.Equal(t, want, got)
		})
	}
}

func TestRetractionNaNPassthroughInBatch(t *testing.T) {
	base := time.Now().Add(time.Hour)
	next := new(consumertest.MetricsSink)
	cfg := &Config{
		InitialValue:   tracking.InitialValueKeep,
		RetractionMode: tracking.RetractionModeZSet,
	}
	p, err := NewFactory().CreateMetrics(t.Context(), processortest.NewNopSettings(metadata.Type), cfg, next)
	require.NoError(t, err)
	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, p.Shutdown(t.Context())) }()

	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("flt_nan")
	s := m.SetEmptySum()
	s.SetIsMonotonic(true)
	s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	for _, pt := range []struct {
		sec int64
		val float64
	}{{10, 10}, {30, 30}, {20, 20}, {35, math.NaN()}, {40, 40}} {
		dp := s.DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(pt.sec) * time.Second)))
		dp.SetDoubleValue(pt.val)
	}
	require.NoError(t, p.ConsumeMetrics(t.Context(), md))

	got := next.AllMetrics()
	require.Len(t, got, 1)
	dps := got[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints()
	// (0,10]:10, (10,30]:20, retract(10,30]:-20, (10,20]:10, (20,30]:10, NaN@35, (30,40]:10 => 7 points.
	require.Equal(t, 7, dps.Len())
	assert.True(t, math.IsNaN(dps.At(5).DoubleValue()))
	assert.Equal(t, 10.0, dps.At(6).DoubleValue())
}

func TestRetractionTelemetryRealSDKNoPanic(t *testing.T) {
	// Verifies a real sdk/metric MeterProvider (not nop) records all drop reasons without
	// nil MeasurementOption panic, and absorbed points do not increment datapoints_dropped.
	base := time.Now().Add(time.Hour)
	tel := componenttest.NewTelemetry()
	defer func() { require.NoError(t, tel.Shutdown(t.Context())) }()

	next := new(consumertest.MetricsSink)
	factory := NewFactory()
	set := metadatatest.NewSettings(tel)
	cfg := &Config{
		InitialValue:     tracking.InitialValueDrop,
		RetractionMode:   tracking.RetractionModeNonNegative,
		ReorderWindow:    10 * time.Second,
		MaxHistoryPoints: 3,
	}
	mp, err := factory.CreateMetrics(t.Context(), set, cfg, next)
	require.NoError(t, err)
	require.NoError(t, mp.Start(t.Context(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, mp.Shutdown(t.Context())) }()

	send := func(sec, val int64) {
		md := makeIntSumBatch(base, -1, []struct {
			sec      int64
			val      int64
			exemplar bool
			noVal    bool
		}{{sec: sec, val: val}})
		require.NoError(t, mp.ConsumeMetrics(t.Context(), md))
	}

	// 1. Initial drop (reason="initial")
	send(10, 100)
	// 2. Duplicate (reason="duplicate")
	send(10, 100)
	// 3. Invalid negative value (reason="invalid_value")
	send(15, -5)
	// 4. Tail append -> emits +200 (converted datapoints += 1)
	send(30, 300)
	// 5. Out-of-order in-gen reorder (net==0 -> OutcomeAbsorbed: NOT dropped, NOT emitted!)
	send(20, 200)
	// 6. Downward tail revision (30, 250) -> net=-50 -> rejected (reason="rejected_revision")
	send(30, 250)
	// 7. Advance tail to trigger eviction (max_history_points=3, 10s window -> anchor becomes 30, 10 & 20 evicted)
	send(50, 500)
	// 8. Point older than anchor (t=5) -> dropped (reason="window_exceeded")
	send(5, 50)

	sumAttrs := attribute.NewSet(attribute.String("metric_type", "sum"))
	dropAttrs := func(reason string) attribute.Set {
		return attribute.NewSet(
			attribute.String("metric_type", "sum"),
			attribute.String("reason", reason),
		)
	}

	metadatatest.AssertEqualCumulativetodeltaDatapoints(t, tel, []metricdata.DataPoint[int64]{
		{Attributes: sumAttrs, Value: 2}, // (10,30]:200 and (30,50]:200
	}, metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualCumulativetodeltaStreamsTracked(t, tel, []metricdata.DataPoint[int64]{
		{Value: 1},
	}, metricdatatest.IgnoreTimestamp())
	metadatatest.AssertEqualCumulativetodeltaDatapointsDropped(t, tel, []metricdata.DataPoint[int64]{
		{Attributes: dropAttrs(tracking.ReasonInitial), Value: 1},
		{Attributes: dropAttrs(tracking.ReasonDuplicate), Value: 1},
		{Attributes: dropAttrs(tracking.ReasonInvalidValue), Value: 1},
		{Attributes: dropAttrs(tracking.ReasonRejectedRevision), Value: 1},
		{Attributes: dropAttrs(tracking.ReasonWindowExceeded), Value: 1},
	}, metricdatatest.IgnoreTimestamp())
}

func TestRetractionProcessorEdgeCases(t *testing.T) {
	base := time.Now().Add(time.Hour)

	t.Run("n1_interior_revision_emits_4_deltas", func(t *testing.T) {
		next := new(consumertest.MetricsSink)
		cfg := &Config{
			InitialValue:     tracking.InitialValueKeep,
			RetractionMode:   tracking.RetractionModeZSet,
			ReorderWindow:    60 * time.Second,
			MaxHistoryPoints: 16,
		}
		p, err := NewFactory().CreateMetrics(t.Context(), processortest.NewNopSettings(metadata.Type), cfg, next)
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
		defer func() { require.NoError(t, p.Shutdown(t.Context())) }()

		send := func(sec, val int64, ex bool) {
			md := makeIntSumBatch(base, -1, []struct {
				sec      int64
				val      int64
				exemplar bool
				noVal    bool
			}{{sec: sec, val: val, exemplar: ex}})
			require.NoError(t, p.ConsumeMetrics(t.Context(), md))
		}
		send(10, 100, false)
		send(20, 200, false)
		send(30, 300, false)
		// Single-point batch revising interior point (20, 250):
		// emits 4 points: -(10,20]:-100, +(10,20]:+150 (keeps exemplar), -(20,30]:-100, +(20,30]:+50.
		send(20, 250, true)

		all := next.AllMetrics()
		require.Len(t, all, 4)
		dps := all[3].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints()
		require.Equal(t, 4, dps.Len())
		assert.Equal(t, []int64{-100, 150, -100, 50}, []int64{
			dps.At(0).IntValue(), dps.At(1).IntValue(), dps.At(2).IntValue(), dps.At(3).IntValue(),
		})
		assert.Equal(t, 0, dps.At(0).Exemplars().Len())
		assert.Equal(t, 1, dps.At(1).Exemplars().Len())
		assert.Equal(t, 0, dps.At(2).Exemplars().Len())
		assert.Equal(t, 0, dps.At(3).Exemplars().Len())
	})

	t.Run("single_nan_and_inf_through_consume", func(t *testing.T) {
		tel := componenttest.NewTelemetry()
		defer func() { require.NoError(t, tel.Shutdown(t.Context())) }()
		next := new(consumertest.MetricsSink)
		cfg := &Config{InitialValue: tracking.InitialValueKeep, RetractionMode: tracking.RetractionModeZSet}
		p, err := NewFactory().CreateMetrics(t.Context(), metadatatest.NewSettings(tel), cfg, next)
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
		defer func() { require.NoError(t, p.Shutdown(t.Context())) }()

		makeFlt := func(vals ...float64) pmetric.Metrics {
			md := pmetric.NewMetrics()
			m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			m.SetName("flt_edge")
			s := m.SetEmptySum()
			s.SetIsMonotonic(true)
			s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			for i, v := range vals {
				dp := s.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(10+i*5) * time.Second)))
				dp.SetDoubleValue(v)
			}
			return md
		}

		// n == 1 NaN passes through unchanged.
		require.NoError(t, p.ConsumeMetrics(t.Context(), makeFlt(math.NaN())))
		require.Len(t, next.AllMetrics(), 1)
		assert.True(t, math.IsNaN(next.AllMetrics()[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints().At(0).DoubleValue()))

		// n == 1 +Inf and -Inf dropped as invalid_value (empty metric removed).
		require.NoError(t, p.ConsumeMetrics(t.Context(), makeFlt(math.Inf(1))))
		require.NoError(t, p.ConsumeMetrics(t.Context(), makeFlt(math.Inf(-1))))
		assert.Equal(t, 0, next.AllMetrics()[1].ResourceMetrics().Len())
		assert.Equal(t, 0, next.AllMetrics()[2].ResourceMetrics().Len())

		// n > 1 batch where NaN is at i == 0 BEFORE startRebuild triggers on +Inf at i == 1.
		require.NoError(t, p.ConsumeMetrics(t.Context(), makeFlt(math.NaN(), math.Inf(1), 42.0)))
		dps := next.AllMetrics()[3].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints()
		require.Equal(t, 2, dps.Len())
		assert.True(t, math.IsNaN(dps.At(0).DoubleValue()))
		assert.Equal(t, 42.0, dps.At(1).DoubleValue())

		// n > 1 all-dropped batch (both -Inf and +Inf) removes metric cleanly.
		require.NoError(t, p.ConsumeMetrics(t.Context(), makeFlt(math.Inf(-1), math.Inf(1))))
		assert.Equal(t, 0, next.AllMetrics()[4].ResourceMetrics().Len())
	})
}
