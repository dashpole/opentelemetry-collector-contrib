// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tracking

import (
	"bytes"
	"math"
	"math/big"
	"math/rand/v2"
	"runtime"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

func makeIntIdentity(name string, startTS pcommon.Timestamp) MetricIdentity {
	return MetricIdentity{
		Resource:               pcommon.NewResource(),
		InstrumentationLibrary: pcommon.NewInstrumentationScope(),
		MetricType:             pmetric.MetricTypeSum,
		MetricIsMonotonic:      true,
		MetricName:             name,
		MetricUnit:             "1",
		StartTimestamp:         startTS,
		Attributes:             pcommon.NewMap(),
		MetricValueType:        pmetric.NumberDataPointValueTypeInt,
	}
}

func makeFloatIdentity(name string, startTS pcommon.Timestamp) MetricIdentity {
	id := makeIntIdentity(name, startTS)
	id.MetricValueType = pmetric.NumberDataPointValueTypeDouble
	return id
}

func tsSec(base time.Time, sec int64) pcommon.Timestamp {
	return pcommon.NewTimestampFromTime(base.Add(time.Duration(sec) * time.Second))
}

func TestRetractionStructSizes(t *testing.T) {
	if unsafe.Sizeof(uintptr(0)) == 8 {
		assert.Equal(t, uintptr(16), unsafe.Sizeof(sumObs{}), "sumObs must remain compact 16B noscan struct")
		assert.Equal(t, uintptr(48), unsafe.Sizeof(retractionState{}), "retractionState must remain 48B (same size class as legacy state)")
	}
}

func TestRetractionModeUnmarshalAndValidate(t *testing.T) {
	cases := []struct {
		in      string
		want    RetractionMode
		wantStr string
		wantErr bool
	}{
		{"", RetractionModeOff, "off", false},
		{"off", RetractionModeOff, "off", false},
		{"zset", RetractionModeZSet, "zset", false},
		{"zset_compact", RetractionModeZSetCompact, "zset_compact", false},
		{"non_negative", RetractionModeNonNegative, "non_negative", false},
		{"bogus", RetractionModeOff, "", true},
	}
	for _, tc := range cases {
		var m RetractionMode
		err := m.UnmarshalText([]byte(tc.in))
		if tc.wantErr {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		assert.Equal(t, tc.want, m)
		assert.Equal(t, tc.wantStr, m.String())
		assert.True(t, m.IsValid())
	}
	assert.False(t, RetractionMode(-1).IsValid())
	assert.False(t, RetractionMode(99).IsValid())

	eff := RetractionConfig{Mode: RetractionModeZSet}.Effective()
	assert.Equal(t, DefaultReorderWindow, eff.ReorderWindow)
	assert.Equal(t, DefaultMaxHistoryPoints, eff.MaxHistoryPoints)

	offEff := RetractionConfig{Mode: RetractionModeOff}.Effective()
	assert.Equal(t, time.Duration(0), offEff.ReorderWindow)
	assert.Equal(t, 0, offEff.MaxHistoryPoints)
}

func TestMidWindowAll6PermutationsInt64(t *testing.T) {
	base := time.Now().Add(time.Hour)
	id := makeIntIdentity("perm_int", 0)

	type triple struct {
		name      string
		a, x, b   int64
		wantTotal int64 // C([(10,a),(20,x),(30,b)]) under Keep
	}
	triples := []triple{
		{name: "case1_monotonic_a<=x<=b", a: 100, x: 200, b: 300, wantTotal: 300},
		{name: "case1_equal_all", a: 100, x: 100, b: 100, wantTotal: 100},
		{name: "case2_post_reset_b<a<=x", a: 100, x: 240, b: 50, wantTotal: 290},
		{name: "case2b_post_reset_x<=b<a", a: 100, x: 30, b: 50, wantTotal: 150},
		{name: "case2c_double_reset_b<x<a", a: 100, x: 80, b: 50, wantTotal: 230},
		{name: "case3_hidden_reset_a<=b<x", a: 100, x: 180, b: 120, wantTotal: 300},
		{name: "case3b_hidden_reset_x<a<=b", a: 100, x: 40, b: 120, wantTotal: 220},
		{name: "case3b_zero_x", a: 100, x: 0, b: 120, wantTotal: 220},
		{name: "near_max_int64_no_overflow", a: 100, x: math.MaxInt64, b: math.MaxInt64 - 1, wantTotal: 0}, // checked with big.Int
	}

	modes := []RetractionMode{RetractionModeZSet, RetractionModeZSetCompact, RetractionModeNonNegative}
	for _, mode := range modes {
		for _, tc := range triples {
			t.Run(mode.String()+"/"+tc.name, func(t *testing.T) {
				tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, InitialValueKeep, RetractionConfig{
					Mode:             mode,
					ReorderWindow:    60 * time.Second,
					MaxHistoryPoints: 16,
				})
				var buf [4]SumDelta
				sum := new(big.Int)
				buckets := map[pcommon.Timestamp]*big.Int{}
				type ivKey struct{ s, e pcommon.Timestamp }
				zsetMap := map[ivKey]*big.Int{}

				feed := func(sec, val int64) {
					deltas, outcome, reason := tr.ConvertSum(MetricPoint{
						Identity: id,
						Value:    ValuePoint{ObservedTimestamp: tsSec(base, sec), IntValue: val},
					}, buf[:0])
					assert.Empty(t, reason)
					if outcome == OutcomeAbsorbed {
						assert.Empty(t, deltas)
						return
					}
					require.Equal(t, OutcomeEmitted, outcome)
					for _, d := range deltas {
						assert.LessOrEqual(t, d.StartTimestamp, d.Timestamp, "inverted interval")
						if mode == RetractionModeNonNegative {
							assert.GreaterOrEqual(t, d.IntValue, int64(0), "non_negative emitted negative delta")
						}
						v := big.NewInt(d.IntValue)
						sum.Add(sum, v)
						if buckets[d.Timestamp] == nil {
							buckets[d.Timestamp] = new(big.Int)
						}
						buckets[d.Timestamp].Add(buckets[d.Timestamp], v)
						k := ivKey{d.StartTimestamp, d.Timestamp}
						if zsetMap[k] == nil {
							zsetMap[k] = new(big.Int)
						}
						zsetMap[k].Add(zsetMap[k], v)
						if zsetMap[k].Sign() == 0 {
							delete(zsetMap, k)
						}
					}
				}

				feed(10, tc.a)
				feed(30, tc.b)
				feed(20, tc.x)

				want := new(big.Int).SetInt64(tc.a)
				d1 := intervalDeltaInt(tc.a, tc.x)
				d2 := intervalDeltaInt(tc.x, tc.b)
				want.Add(want, big.NewInt(d1))
				want.Add(want, big.NewInt(d2))
				assert.Equal(t, 0, sum.Cmp(want), "global sum mismatch: got=%s want=%s", sum, want)

				if mode == RetractionModeZSet || mode == RetractionModeZSetCompact {
					assert.Equal(t, 0, bucketOrZero(buckets, tsSec(base, 10)).Cmp(big.NewInt(tc.a)))
					assert.Equal(t, 0, bucketOrZero(buckets, tsSec(base, 20)).Cmp(big.NewInt(d1)))
					assert.Equal(t, 0, bucketOrZero(buckets, tsSec(base, 30)).Cmp(big.NewInt(d2)))
				}
				if mode == RetractionModeZSet {
					wantMap := map[ivKey]*big.Int{}
					putNZ := func(s, e pcommon.Timestamp, v int64) {
						if v != 0 {
							wantMap[ivKey{s, e}] = big.NewInt(v)
						}
					}
					putNZ(0, tsSec(base, 10), tc.a)
					putNZ(tsSec(base, 10), tsSec(base, 20), d1)
					putNZ(tsSec(base, 20), tsSec(base, 30), d2)
					assert.Equal(t, wantMap, zsetMap)
				}
			})
		}
	}
}

func bucketOrZero(m map[pcommon.Timestamp]*big.Int, ts pcommon.Timestamp) *big.Int {
	if v, ok := m[ts]; ok {
		return v
	}
	return new(big.Int)
}

func TestFloat64IEEE754Counterexamples(t *testing.T) {
	// IEEE-754 binary64 triples where naive (d1+d2)-dOld > 0 rounding causes macroscopic overcounting.
	base := time.Now().Add(time.Hour)
	id := makeFloatIdentity("f64_ulp", 0)

	cases := []struct {
		name    string
		a, x, b float64
	}{
		{"scale_0.1_0.2_2.0", 0.1, 0.2, 2.0},
		{"scale_0.1_100.3_1000.0", 0.1, 100.3, 1000.0},
		{"post_reset_ulp", 5000.0, 1057.562965768663, 3672.887320536073},
		{"near_max_float64", 1.5e308, 1.5e308, 1.5e308},
	}

	for _, mode := range []RetractionMode{RetractionModeZSet, RetractionModeZSetCompact, RetractionModeNonNegative} {
		for _, tc := range cases {
			t.Run(mode.String()+"/"+tc.name, func(t *testing.T) {
				tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, InitialValueKeep, RetractionConfig{
					Mode:             mode,
					ReorderWindow:    60 * time.Second,
					MaxHistoryPoints: 16,
				})
				var buf [4]SumDelta
				var emitted []SumDelta
				for _, pt := range []struct {
					sec int64
					v   float64
				}{{10, tc.a}, {30, tc.b}, {20, tc.x}} {
					ds, outcome, reason := tr.ConvertSum(MetricPoint{
						Identity: id,
						Value:    ValuePoint{ObservedTimestamp: tsSec(base, pt.sec), FloatValue: pt.v},
					}, buf[:0])
					assert.Empty(t, reason)
					if pt.sec == 20 && mode == RetractionModeNonNegative {
						// All 4 cases have net == 0: MUST absorb with ZERO emission!
						assert.Equal(t, OutcomeAbsorbed, outcome, "ULP misclassification emitted spurious delta: %+v", ds)
						assert.Empty(t, ds)
					}
					emitted = append(emitted, ds...)
				}
				var total float64
				for _, d := range emitted {
					assert.False(t, math.IsNaN(d.FloatValue) || math.IsInf(d.FloatValue, 0))
					assert.False(t, math.Signbit(d.FloatValue) && d.FloatValue == 0, "negative zero leaked")
					total += d.FloatValue
				}
				want := tc.a + intervalDeltaFloat(tc.a, tc.x) + intervalDeltaFloat(tc.x, tc.b)
				if math.IsInf(want, 0) {
					// When tc.a = 1.5e308, a + 0 + 0 = 1.5e308 (finite).
					t.Fatalf("unexpected inf oracle")
				}
				tol := 1e-12 * math.Max(1, math.Abs(want))
				assert.InDelta(t, want, total, tol)
			})
		}
	}
}

func TestFloat64SterbenzCrossRegimeAndRevisions(t *testing.T) {
	// Adversarial IEEE-754 cross-regime, scale-mismatch, near-MaxFloat64, and subnormal cases.
	base := time.Now().Add(time.Hour)
	id := makeFloatIdentity("sterbenz", 0)

	t.Run("near_max_float64_no_inf_nan_interior_rev", func(t *testing.T) {
		tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, InitialValueKeep, RetractionConfig{
			Mode: RetractionModeNonNegative,
		})
		var buf [4]SumDelta
		for _, pt := range []struct {
			s int64
			v float64
		}{{10, 1.5e308}, {20, 1.0e308}, {30, 1.0}} {
			tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, pt.s), FloatValue: pt.v}}, buf[:0])
		}
		ds, out, reason := tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, 20), FloatValue: 0.9e308}}, buf[:0])
		assert.Equal(t, OutcomeDropped, out)
		assert.Equal(t, ReasonRejectedRevision, reason)
		assert.Empty(t, ds)

		ds, out, reason = tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, 20), FloatValue: 1.2e308}}, buf[:0])
		assert.Equal(t, OutcomeEmitted, out)
		assert.Empty(t, reason)
		require.Len(t, ds, 1)
		assert.False(t, math.IsNaN(ds[0].FloatValue) || math.IsInf(ds[0].FloatValue, 0))
		assert.InDelta(t, 0.2e308, ds[0].FloatValue, 1e295)
	})

	t.Run("double_reset_scale_mismatch_1e16", func(t *testing.T) {
		tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, InitialValueKeep, RetractionConfig{
			Mode: RetractionModeNonNegative,
		})
		var buf [4]SumDelta
		for _, pt := range []struct {
			s int64
			v float64
		}{{10, 1e16}, {20, 2.0}, {30, 1.0}} {
			tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, pt.s), FloatValue: pt.v}}, buf[:0])
		}
		// Exact net = -0.5 < 0; must reject WITHOUT mutating H.
		_, out, reason := tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, 20), FloatValue: 1.5}}, buf[:0])
		assert.Equal(t, OutcomeDropped, out)
		assert.Equal(t, ReasonRejectedRevision, reason)
	})

	t.Run("cross_regime_near_boundary_sterbenz", func(t *testing.T) {
		tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, InitialValueKeep, RetractionConfig{
			Mode: RetractionModeNonNegative,
		})
		var buf [4]SumDelta
		a := 9754.26672142323
		oldV := 17491.203805505967
		newV := 7736.937084082736
		b := 6104.924411807546
		for _, pt := range []struct {
			s int64
			v float64
		}{{10, a}, {20, oldV}, {30, b}} {
			tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, pt.s), FloatValue: pt.v}}, buf[:0])
		}
		// oldV - a = 7736.9370840827378 > newV = 7736.937084082736 -> exact net ~ -1.82e-12 < 0 -> MUST reject!
		_, out, reason := tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, 20), FloatValue: newV}}, buf[:0])
		assert.Equal(t, OutcomeDropped, out)
		assert.Equal(t, ReasonRejectedRevision, reason)
	})

	t.Run("interior_rev_1e16_positive_edge_exact", func(t *testing.T) {
		// a=1, b=1, old=1e16, new=1e16+2: must emit +2 on (t_a, t_i], not drop or emit on (t_i, t_b].
		tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, InitialValueKeep, RetractionConfig{
			Mode: RetractionModeNonNegative,
		})
		var buf [4]SumDelta
		for _, pt := range []struct {
			s int64
			v float64
		}{{10, 1.0}, {20, 1e16}, {30, 1.0}} {
			tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, pt.s), FloatValue: pt.v}}, buf[:0])
		}
		ds, out, reason := tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, 20), FloatValue: 1e16 + 2.0}}, buf[:0])
		assert.Empty(t, reason)
		require.Equal(t, OutcomeEmitted, out)
		require.Len(t, ds, 1)
		assert.Equal(t, tsSec(base, 10), ds[0].StartTimestamp)
		assert.Equal(t, tsSec(base, 20), ds[0].Timestamp)
		assert.Equal(t, 2.0, ds[0].FloatValue)
	})

	t.Run("tail_rev_1e16_downward_rejected", func(t *testing.T) {
		tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, InitialValueKeep, RetractionConfig{
			Mode: RetractionModeNonNegative,
		})
		var buf [4]SumDelta
		tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, 10), FloatValue: 1.0}}, buf[:0])
		tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, 20), FloatValue: 1e16 + 2.0}}, buf[:0])
		_, out, reason := tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, 20), FloatValue: 1e16}}, buf[:0])
		assert.Equal(t, OutcomeDropped, out)
		assert.Equal(t, ReasonRejectedRevision, reason)
	})

	t.Run("subnormal_cmpDiffFloat", func(t *testing.T) {
		u := math.SmallestNonzeroFloat64
		s, m := cmpDiffFloat(5*u, 2*u, 1*u)
		assert.Equal(t, 1, s)
		assert.Equal(t, 2*u, m)
		s, m = cmpDiffFloat(3*u, 2*u, 1*u)
		assert.Equal(t, 0, s)
		assert.Equal(t, 0.0, m)
		s, m = cmpDiffFloat(3*u, 2*u, 1.5*u) // 1.5*u rounds to 2*u
		assert.Equal(t, -1, s)
		assert.Greater(t, m, 0.0)
	})
}

func TestHeadPrependAutoFlipAndStateChaining(t *testing.T) {
	base := time.Now().Add(time.Hour)
	startTS := tsSec(base, 20)
	id := makeIntIdentity("auto_flip", startTS)

	t.Run("non_negative_auto_flip_zero_then_head_rev_reject", func(t *testing.T) {
		// Sequence:
		// 1. (30, 100) emits +100 (initEmitted=true).
		// 2. head-prepend (20, 0) where ts==StartTimestamp flips initEmitted to false & absorbs.
		// 3. head-rev (20, 50) MUST see initEmitted==false and REJECT net=-50 (NOT accept as 0!).
		tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, InitialValueAuto, RetractionConfig{
			Mode: RetractionModeNonNegative,
		})
		var buf [4]SumDelta
		ds, out, _ := tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, 30), IntValue: 100}}, buf[:0])
		require.Equal(t, OutcomeEmitted, out)
		require.Len(t, ds, 1)
		assert.Equal(t, int64(100), ds[0].IntValue)

		ds, out, reason := tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, 20), IntValue: 0}}, buf[:0])
		assert.Equal(t, OutcomeAbsorbed, out)
		assert.Empty(t, reason)
		assert.Empty(t, ds)

		ds, out, reason = tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, 20), IntValue: 50}}, buf[:0])
		assert.Equal(t, OutcomeDropped, out)
		assert.Equal(t, ReasonRejectedRevision, reason)
		assert.Empty(t, ds)
	})

	t.Run("non_negative_auto_flip_clock_skew_ts_lt_start", func(t *testing.T) {
		tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, InitialValueAuto, RetractionConfig{
			Mode: RetractionModeNonNegative,
		})
		var buf [4]SumDelta
		_, out, _ := tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, 30), IntValue: 200}}, buf[:0])
		require.Equal(t, OutcomeEmitted, out)

		// Clock-skewed point ts=15 < StartTimestamp=20 with v=50 <= 200 -> net = -50 -> MUST reject!
		_, out, reason := tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, 15), IntValue: 50}}, buf[:0])
		assert.Equal(t, OutcomeDropped, out)
		assert.Equal(t, ReasonRejectedRevision, reason)
	})

	t.Run("non_negative_auto_zero_start_timestamp_head_prepend", func(t *testing.T) {
		idZero := makeIntIdentity("auto_zero_start", 0)
		tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, InitialValueAuto, RetractionConfig{
			Mode: RetractionModeNonNegative,
		})
		var buf [4]SumDelta
		// First point (20, 100) dropped as initial (!initEmitted).
		_, out, reason := tr.ConvertSum(MetricPoint{Identity: idZero, Value: ValuePoint{ObservedTimestamp: tsSec(base, 20), IntValue: 100}}, buf[:0])
		assert.Equal(t, OutcomeDropped, out)
		assert.Equal(t, ReasonInitial, reason)

		// Head-prepend (10, 40) -> (!oldInitEmit, !newInitEmit) -> MUST emit (10, 20]: +60!
		ds, out, reason := tr.ConvertSum(MetricPoint{Identity: idZero, Value: ValuePoint{ObservedTimestamp: tsSec(base, 10), IntValue: 40}}, buf[:0])
		assert.Empty(t, reason)
		require.Equal(t, OutcomeEmitted, out)
		require.Len(t, ds, 1)
		assert.Equal(t, tsSec(base, 10), ds[0].StartTimestamp)
		assert.Equal(t, tsSec(base, 20), ds[0].Timestamp)
		assert.Equal(t, int64(60), ds[0].IntValue)
	})

	t.Run("zset_auto_flip_retracts_and_matches_oracle", func(t *testing.T) {
		tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, InitialValueAuto, RetractionConfig{
			Mode: RetractionModeZSet,
		})
		var buf [4]SumDelta
		ds1, _, _ := tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, 30), IntValue: 200}}, buf[:0])
		require.Len(t, ds1, 1)
		initEmittedVal := ds1[0].IntValue
		assert.Equal(t, int64(200), initEmittedVal)

		// Prepend at ts==StartTimestamp (20, 80) -> retracts (20, 30]: -200, emits (20, 30]: +120. Net = +120!
		ds2, out, _ := tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, 20), IntValue: 80}}, buf[:0])
		require.Equal(t, OutcomeEmitted, out)
		require.Len(t, ds2, 2)
		assert.Equal(t, int64(-200), ds2[0].IntValue)
		assert.Equal(t, int64(120), ds2[1].IntValue)
		assert.Equal(t, int64(120), initEmittedVal+ds2[0].IntValue+ds2[1].IntValue)
	})

	t.Run("auto_clock_skew_first_point_never_inverted", func(t *testing.T) {
		tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, InitialValueAuto, RetractionConfig{
			Mode: RetractionModeZSet,
		})
		var buf [4]SumDelta
		_, out, reason := tr.ConvertSum(MetricPoint{Identity: id, Value: ValuePoint{ObservedTimestamp: tsSec(base, 10), IntValue: 50}}, buf[:0])
		assert.Equal(t, OutcomeDropped, out)
		assert.Equal(t, ReasonInitial, reason)
	})
}

func TestZeroDeltaTransitionsAbsorbedAndInvalidValues(t *testing.T) {
	base := time.Now().Add(time.Hour)
	idInt := makeIntIdentity("absorbed_int", 0)
	idFlt := makeFloatIdentity("invalid_flt", 0)
	var buf [4]SumDelta

	t.Run("zset_tail_rev_same_delta_absorbed", func(t *testing.T) {
		tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, InitialValueKeep, RetractionConfig{Mode: RetractionModeZSet})
		tr.ConvertSum(MetricPoint{Identity: idInt, Value: ValuePoint{ObservedTimestamp: tsSec(base, 10), IntValue: 100}}, buf[:0])
		tr.ConvertSum(MetricPoint{Identity: idInt, Value: ValuePoint{ObservedTimestamp: tsSec(base, 20), IntValue: 50}}, buf[:0])
		// prev=100, old=50 (delta=50), new=150 (delta=50) -> 0 emissions, OutcomeAbsorbed!
		ds, out, reason := tr.ConvertSum(MetricPoint{Identity: idInt, Value: ValuePoint{ObservedTimestamp: tsSec(base, 20), IntValue: 150}}, buf[:0])
		assert.Equal(t, OutcomeAbsorbed, out)
		assert.Empty(t, reason)
		assert.Empty(t, ds)
	})

	t.Run("zset_interior_rev_same_edges_absorbed", func(t *testing.T) {
		tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, InitialValueKeep, RetractionConfig{Mode: RetractionModeZSet})
		tr.ConvertSum(MetricPoint{Identity: idInt, Value: ValuePoint{ObservedTimestamp: tsSec(base, 10), IntValue: 100}}, buf[:0])
		tr.ConvertSum(MetricPoint{Identity: idInt, Value: ValuePoint{ObservedTimestamp: tsSec(base, 20), IntValue: 50}}, buf[:0])
		tr.ConvertSum(MetricPoint{Identity: idInt, Value: ValuePoint{ObservedTimestamp: tsSec(base, 30), IntValue: 10}}, buf[:0])
		ds, out, reason := tr.ConvertSum(MetricPoint{Identity: idInt, Value: ValuePoint{ObservedTimestamp: tsSec(base, 20), IntValue: 150}}, buf[:0])
		assert.Equal(t, OutcomeAbsorbed, out)
		assert.Empty(t, reason)
		assert.Empty(t, ds)
	})

	t.Run("negative_inf_nan_and_neg_zero", func(t *testing.T) {
		tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, InitialValueKeep, RetractionConfig{Mode: RetractionModeZSet})
		_, out, reason := tr.ConvertSum(MetricPoint{Identity: idInt, Value: ValuePoint{ObservedTimestamp: tsSec(base, 10), IntValue: -1}}, buf[:0])
		assert.Equal(t, OutcomeDropped, out)
		assert.Equal(t, ReasonInvalidValue, reason)

		for _, bad := range []float64{-0.1, math.Inf(1), math.Inf(-1)} {
			_, out, reason = tr.ConvertSum(MetricPoint{Identity: idFlt, Value: ValuePoint{ObservedTimestamp: tsSec(base, 10), FloatValue: bad}}, buf[:0])
			assert.Equal(t, OutcomeDropped, out)
			assert.Equal(t, ReasonInvalidValue, reason)
		}
		_, out, reason = tr.ConvertSum(MetricPoint{Identity: idFlt, Value: ValuePoint{ObservedTimestamp: tsSec(base, 10), FloatValue: math.NaN()}}, buf[:0])
		assert.Equal(t, OutcomeDropped, out)
		assert.Empty(t, reason)

		// -0.0 normalized to +0.0 so subsequent +0.0 at same timestamp is detected as exact duplicate.
		ds, out, _ := tr.ConvertSum(MetricPoint{Identity: idFlt, Value: ValuePoint{ObservedTimestamp: tsSec(base, 10), FloatValue: math.Copysign(0, -1)}}, buf[:0])
		require.Equal(t, OutcomeEmitted, out)
		assert.False(t, math.Signbit(ds[0].FloatValue))
		_, out, reason = tr.ConvertSum(MetricPoint{Identity: idFlt, Value: ValuePoint{ObservedTimestamp: tsSec(base, 10), FloatValue: 0.0}}, buf[:0])
		assert.Equal(t, OutcomeDropped, out)
		assert.Equal(t, ReasonDuplicate, reason)
	})
}

func TestInsertAndCompactMatchesReference20k(t *testing.T) {
	rng := rand.New(rand.NewPCG(42, 99))
	for trial := range 20000 {
		maxPts := 2 + rng.IntN(10)
		win := time.Duration(rng.IntN(40)) * time.Second
		tr := &MetricTracker{
			retractionCfg: RetractionConfig{
				Mode:             RetractionModeZSet,
				ReorderWindow:    win,
				MaxHistoryPoints: maxPts,
			},
		}
		s := &retractionState{points: make([]sumObs, 0, min(8, maxPts))}
		var ref []sumObs
		refEvicted := false

		steps := 1 + rng.IntN(30)
		var baseTS int64 = 100
		for range steps {
			// Generate varied timestamps: tail, mid-window, head-prepend, bursty, large jumps.
			var tsVal int64
			switch rng.IntN(4) {
			case 0:
				tsVal = baseTS + int64(rng.IntN(20)+1)
				baseTS = tsVal
			case 1:
				tsVal = max(1, baseTS-int64(rng.IntN(50)))
			case 2:
				tsVal = baseTS + int64(rng.IntN(200))
				baseTS = tsVal
			default:
				tsVal = int64(rng.IntN(100) + 1)
			}
			ts := pcommon.Timestamp(tsVal) * pcommon.Timestamp(time.Second)

			// Skip exact duplicate timestamp in this layout test.
			idx := sort.Search(len(ref), func(i int) bool { return ref[i].ts >= ts })
			if idx < len(ref) && ref[idx].ts == ts {
				continue
			}
			obs := obsFromInt(ts, rng.Int64N(1000))

			// Execute SUT.
			if len(s.points) == 0 {
				s.points = append(s.points, obs)
			} else {
				tr.insertAndCompactLocked(s, idx, obs)
			}

			// Execute naive reference insert-then-compact.
			ref = slices.Insert(ref, idx, obs)
			maxTS := ref[len(ref)-1].ts
			dropTime := 0
			if win > 0 && maxTS > pcommon.Timestamp(win) {
				cutoff := maxTS - pcommon.Timestamp(win)
				firstGE := sort.Search(len(ref), func(k int) bool { return ref[k].ts >= cutoff })
				if firstGE >= 2 {
					dropTime = firstGE - 1
				}
			}
			dropCap := 0
			if len(ref) > maxPts {
				dropCap = len(ref) - maxPts
			}
			drop := max(dropTime, dropCap)
			if drop > 0 {
				ref = ref[drop:]
				refEvicted = true
			}

			require.Equal(t, ref, s.points, "trial=%d maxPts=%d win=%s", trial, maxPts, win)
			require.Equal(t, refEvicted, s.hasEvicted(), "trial=%d hasEvicted mismatch", trial)
			require.LessOrEqual(t, len(s.points), maxPts)
			require.LessOrEqual(t, cap(s.points), maxPts, "capacity overshot maxPts")
		}
	}
}

func TestSteadyState5sScrapeNoSliceDoubling(t *testing.T) {
	base := time.Now().Add(time.Hour)
	id := makeIntIdentity("steady_cap", 0)
	tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, InitialValueKeep, RetractionConfig{
		Mode:             RetractionModeZSet,
		ReorderWindow:    30 * time.Second,
		MaxHistoryPoints: 64,
	})
	var buf [4]SumDelta
	for i := range int64(200) {
		tr.ConvertSum(MetricPoint{
			Identity: id,
			Value:    ValuePoint{ObservedTimestamp: tsSec(base, i*5), IntValue: i * 10},
		}, buf[:0])
	}
	raw, ok := tr.retractionStates.Load(identityKey(id))
	require.True(t, ok)
	s := raw.(*retractionState)
	// At 5s scrape / 30s window: retained = 7 points inside [max-30, max] + 1 left anchor = 8!
	assert.Equal(t, 8, len(s.points))
	assert.Equal(t, 8, cap(s.points), "backing array must stay pinned at cap=8 without doubling to 16")
	assert.True(t, s.hasEvicted())

	// Evicted point check: strictly older than anchor (t < H[0].ts) & anchor revision (t == H[0].ts) MUST drop with window_exceeded.
	anchor := s.points[0]
	_, out, reason := tr.ConvertSum(MetricPoint{
		Identity: id,
		Value:    ValuePoint{ObservedTimestamp: anchor.ts - pcommon.Timestamp(time.Second), IntValue: 1},
	}, buf[:0])
	assert.Equal(t, OutcomeDropped, out)
	assert.Equal(t, ReasonWindowExceeded, reason)

	_, out, reason = tr.ConvertSum(MetricPoint{
		Identity: id,
		Value:    ValuePoint{ObservedTimestamp: anchor.ts, IntValue: anchor.intVal() + 1},
	}, buf[:0])
	assert.Equal(t, OutcomeDropped, out)
	assert.Equal(t, ReasonWindowExceeded, reason)

	// Anchor exact duplicate -> ReasonDuplicate.
	_, out, reason = tr.ConvertSum(MetricPoint{
		Identity: id,
		Value:    ValuePoint{ObservedTimestamp: anchor.ts, IntValue: anchor.intVal()},
	}, buf[:0])
	assert.Equal(t, OutcomeDropped, out)
	assert.Equal(t, ReasonDuplicate, reason)
}

func identityKey(id MetricIdentity) string {
	b := identityBufferPool.Get().(*bytes.Buffer)
	b.Reset()
	id.Write(b)
	key := b.String()
	identityBufferPool.Put(b)
	return key
}

// oracleIntHistory tracks the accepted history of a stream (last-write-wins for revisions)
// and computes the exact conservation sum via math/big.Int.
func oracleInt(accepted map[pcommon.Timestamp]int64, initVal InitialValue, startTS, trackerStart pcommon.Timestamp) (*big.Int, map[pcommon.Timestamp]*big.Int, map[[2]pcommon.Timestamp]*big.Int) {
	keys := make([]pcommon.Timestamp, 0, len(accepted))
	for k := range accepted {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	total := new(big.Int)
	buckets := map[pcommon.Timestamp]*big.Int{}
	ivMap := map[[2]pcommon.Timestamp]*big.Int{}
	if len(keys) == 0 {
		return total, buckets, ivMap
	}
	t0 := keys[0]
	v0 := accepted[t0]
	emit := false
	var initStart pcommon.Timestamp
	switch initVal {
	case InitialValueKeep:
		emit = true
		if startTS <= t0 {
			initStart = startTS
		}
	case InitialValueAuto:
		if startTS >= trackerStart && t0 > startTS {
			emit = true
			initStart = startTS
		}
	case InitialValueDrop:
	}
	if emit {
		b0 := big.NewInt(v0)
		total.Add(total, b0)
		buckets[t0] = new(big.Int).Set(b0)
		if v0 != 0 {
			ivMap[[2]pcommon.Timestamp{initStart, t0}] = new(big.Int).Set(b0)
		}
	}
	for i := 1; i < len(keys); i++ {
		prev := accepted[keys[i-1]]
		curr := accepted[keys[i]]
		d := intervalDeltaInt(prev, curr)
		bd := big.NewInt(d)
		total.Add(total, bd)
		buckets[keys[i]] = new(big.Int).Set(bd)
		if d != 0 {
			ivMap[[2]pcommon.Timestamp{keys[i-1], keys[i]}] = new(big.Int).Set(bd)
		}
	}
	return total, buckets, ivMap
}

func TestRandomizedPropertyInt64(t *testing.T) {
	// 1,000+ seeded trials per mode covering resets, out-of-order, duplicates,
	// same-ts revisions, head-prepends, auto/keep/drop, and eviction windows.
	base := time.Now().Add(2 * time.Hour)
	rng := rand.New(rand.NewPCG(12345, 67890))
	modes := []RetractionMode{RetractionModeZSet, RetractionModeZSetCompact, RetractionModeNonNegative}
	initVals := []InitialValue{InitialValueKeep, InitialValueDrop, InitialValueAuto}

	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			for trial := range 1000 {
				iv := initVals[trial%len(initVals)]
				var startTS pcommon.Timestamp
				if iv == InitialValueAuto {
					if trial%2 == 0 {
						startTS = tsSec(base, 5) // >= tracker.startTime; exercises ts <= StartTimestamp auto flips too!
					}
				} else if trial%3 == 0 {
					startTS = tsSec(base, 5)
				}
				id := makeIntIdentity("prop_int", startTS)
				useEviction := trial%4 == 0
				win := time.Duration(0)
				maxPts := 128
				if useEviction {
					win = 20 * time.Second
					maxPts = 4 + rng.IntN(8)
				}
				tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, iv, RetractionConfig{
					Mode:             mode,
					ReorderWindow:    win,
					MaxHistoryPoints: maxPts,
				})

				accepted := map[pcommon.Timestamp]int64{}
				emittedSum := new(big.Int)
				buckets := map[pcommon.Timestamp]*big.Int{}
				ivMap := map[[2]pcommon.Timestamp]*big.Int{}
				var buf [4]SumDelta

				steps := 15 + rng.IntN(25)
				for range steps {
					tsOffset := int64(rng.IntN(40))
					if trial%5 == 0 {
						tsOffset += int64(rng.IntN(30))
					}
					ts := tsSec(base, tsOffset)
					var val int64
					if rng.IntN(10) == 0 {
						// Near-MaxInt64 stress value.
						val = math.MaxInt64 - int64(rng.IntN(500))
					} else {
						val = int64(rng.IntN(500))
					}

					ds, outcome, reason := tr.ConvertSum(MetricPoint{
						Identity: id,
						Value:    ValuePoint{ObservedTimestamp: ts, IntValue: val},
					}, buf[:0])

					if outcome == OutcomeDropped {
						if reason == ReasonInitial {
							accepted[ts] = val
						}
						// ReasonDuplicate, ReasonWindowExceeded, ReasonRejectedRevision do NOT mutate accepted!
						continue
					}
					accepted[ts] = val
					for _, d := range ds {
						require.LessOrEqual(t, d.StartTimestamp, d.Timestamp, "inverted interval mode=%s trial=%d", mode, trial)
						if mode == RetractionModeNonNegative {
							require.GreaterOrEqual(t, d.IntValue, int64(0), "negative delta in non_negative mode trial=%d", trial)
						}
						bv := big.NewInt(d.IntValue)
						emittedSum.Add(emittedSum, bv)
						if buckets[d.Timestamp] == nil {
							buckets[d.Timestamp] = new(big.Int)
						}
						buckets[d.Timestamp].Add(buckets[d.Timestamp], bv)
						k := [2]pcommon.Timestamp{d.StartTimestamp, d.Timestamp}
						if ivMap[k] == nil {
							ivMap[k] = new(big.Int)
						}
						ivMap[k].Add(ivMap[k], bv)
						if ivMap[k].Sign() == 0 {
							delete(ivMap, k)
						}
					}
				}

				wantTotal, wantBuckets, wantIV := oracleInt(accepted, iv, startTS, tr.startTime)
				require.Equal(t, 0, emittedSum.Cmp(wantTotal), "trial=%d iv=%s useEvict=%v got=%s want=%s", trial, iv.String(), useEviction, emittedSum, wantTotal)

				if mode == RetractionModeZSet || mode == RetractionModeZSetCompact {
					for ts, wv := range wantBuckets {
						got := bucketOrZero(buckets, ts)
						require.Equal(t, 0, got.Cmp(wv), "bucket mismatch mode=%s trial=%d ts=%d got=%s want=%s", mode, trial, ts, got, wv)
					}
					for ts, gv := range buckets {
						if _, ok := wantBuckets[ts]; !ok {
							require.Equal(t, 0, gv.Sign(), "extra non-zero bucket mode=%s trial=%d ts=%d got=%s", mode, trial, ts, gv)
						}
					}
				}
				if mode == RetractionModeZSet {
					require.Equal(t, wantIV, ivMap, "zset interval map mismatch trial=%d", trial)
				}
			}
		})
	}
}

func TestRandomizedPropertyFloat64(t *testing.T) {
	base := time.Now().Add(2 * time.Hour)
	rng := rand.New(rand.NewPCG(54321, 98765))
	modes := []RetractionMode{RetractionModeZSet, RetractionModeZSetCompact, RetractionModeNonNegative}

	for _, mode := range modes {
		t.Run(mode.String(), func(t *testing.T) {
			for trial := range 1000 {
				iv := InitialValue(trial % 3)
				var startTS pcommon.Timestamp
				if iv == InitialValueAuto && trial%2 == 0 {
					startTS = tsSec(base, 5)
				}
				id := makeFloatIdentity("prop_flt", startTS)
				tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, iv, RetractionConfig{
					Mode:             mode,
					ReorderWindow:    30 * time.Second,
					MaxHistoryPoints: 32,
				})

				accepted := map[pcommon.Timestamp]float64{}
				var emittedSum float64
				buckets := map[pcommon.Timestamp]float64{}
				maxVal := 1.0
				var buf [4]SumDelta

				for range 25 {
					ts := tsSec(base, int64(rng.IntN(45)))
					v := rng.Float64() * 1e5
					if rng.IntN(8) == 0 {
						v = rng.Float64() * 10
					}
					if v > maxVal {
						maxVal = v
					}
					ds, outcome, reason := tr.ConvertSum(MetricPoint{
						Identity: id,
						Value:    ValuePoint{ObservedTimestamp: ts, FloatValue: v},
					}, buf[:0])
					if outcome == OutcomeDropped {
						if reason == ReasonInitial {
							accepted[ts] = normFloat(v)
						}
						continue
					}
					accepted[ts] = normFloat(v)
					for _, d := range ds {
						require.LessOrEqual(t, d.StartTimestamp, d.Timestamp)
						require.False(t, math.IsNaN(d.FloatValue) || math.IsInf(d.FloatValue, 0))
						require.False(t, math.Signbit(d.FloatValue) && d.FloatValue == 0)
						if mode == RetractionModeNonNegative {
							require.GreaterOrEqual(t, d.FloatValue, 0.0)
						}
						emittedSum += d.FloatValue
						buckets[d.Timestamp] += d.FloatValue
					}
				}

				keys := make([]pcommon.Timestamp, 0, len(accepted))
				for k := range accepted {
					keys = append(keys, k)
				}
				slices.Sort(keys)
				var wantSum float64
				wantBuckets := map[pcommon.Timestamp]float64{}
				if len(keys) > 0 {
					t0 := keys[0]
					emit := iv == InitialValueKeep || (iv == InitialValueAuto && startTS >= tr.startTime && t0 > startTS)
					if emit {
						wantSum += accepted[t0]
						wantBuckets[t0] = accepted[t0]
					}
					for i := 1; i < len(keys); i++ {
						d := intervalDeltaFloat(accepted[keys[i-1]], accepted[keys[i]])
						wantSum += d
						wantBuckets[keys[i]] = d
					}
				}

				tol := 1e-9 * math.Max(1, math.Max(maxVal, math.Abs(wantSum)))
				assert.InDelta(t, wantSum, emittedSum, tol, "mode=%s trial=%d", mode, trial)
				if mode == RetractionModeZSet || mode == RetractionModeZSetCompact {
					for ts, wv := range wantBuckets {
						assert.InDelta(t, wv, buckets[ts], tol, "bucket mode=%s trial=%d ts=%d", mode, trial, ts)
					}
				}
			}
		})
	}
}

func TestRetractionRemoveStaleConcurrentStreamsNonNegative(t *testing.T) {
	// Verifies ConvertSum + removeStaleRetraction locks unshared newS.mu across
	// LoadOrStore until t.streams.Add(1) and calls CompareAndDelete under s.mu,
	// so Streams() never transiently dips negative, with zero spinning and zero races.
	base := time.Now().Add(-time.Hour)
	tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, InitialValueKeep, RetractionConfig{
		Mode:             RetractionModeZSet,
		ReorderWindow:    30 * time.Second,
		MaxHistoryPoints: 16,
	})

	const workers = 4
	const keys = 64
	var workerWG, sweeperWG sync.WaitGroup
	stop := make(chan struct{})

	sweeperWG.Add(1)
	go func() {
		defer sweeperWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				tr.removeStale(tsSec(base, 5000))
				assert.GreaterOrEqual(t, tr.Streams(), int64(0), "retraction Streams() transiently dipped negative!")
				runtime.Gosched()
			}
		}
	}()

	for w := range workers {
		workerWG.Add(1)
		go func(workerID int) {
			defer workerWG.Done()
			var buf [4]SumDelta
			for iter := range 300 {
				k := (workerID*17 + iter) % keys
				sumID := makeIntIdentity("sum_conc", pcommon.Timestamp(k))
				tr.ConvertSum(MetricPoint{
					Identity: sumID,
					Value: ValuePoint{
						ObservedTimestamp: tsSec(base, int64(iter*10)),
						IntValue:          int64(iter * 5),
					},
				}, buf[:0])
				assert.GreaterOrEqual(t, tr.Streams(), int64(0), "retraction Streams() dipped negative!")
			}
		}(w)
	}

	workerWG.Wait()
	close(stop)
	sweeperWG.Wait()

	tr.removeStale(pcommon.NewTimestampFromTime(time.Now().Add(time.Hour)))
	assert.Equal(t, int64(0), tr.Streams())
}

func TestMixedSumAndHistogramRemoveStaleConcurrent(t *testing.T) {
	// Verifies mixed Sum (*retractionState) + Histogram (*state) under concurrent
	// Convert + ConvertSum + removeStale triggers zero type-assertion panics and zero races (-race).
	// Note: legacy tracker.go:165-169 (153-157 on upstream/main) performs t.streams.Add(1) after
	// LoadOrStore on t.states (pre-existing race for legacy Histogram entries), so Streams() >= 0
	// is strictly asserted on retractionStates alone in TestRetractionRemoveStaleConcurrentStreamsNonNegative.
	base := time.Now().Add(-time.Hour)
	tr := NewMetricTrackerWithRetraction(t.Context(), zap.NewNop(), 0, InitialValueKeep, RetractionConfig{
		Mode:             RetractionModeZSet,
		ReorderWindow:    30 * time.Second,
		MaxHistoryPoints: 16,
	})

	const workers = 4
	const keys = 64
	var workerWG, sweeperWG sync.WaitGroup
	stop := make(chan struct{})

	sweeperWG.Add(1)
	go func() {
		defer sweeperWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
				tr.removeStale(tsSec(base, 5000))
				runtime.Gosched()
			}
		}
	}()

	for w := range workers {
		workerWG.Add(1)
		go func(workerID int) {
			defer workerWG.Done()
			var buf [4]SumDelta
			for iter := range 300 {
				k := (workerID*17 + iter) % keys
				sumID := makeIntIdentity("sum_mixed", pcommon.Timestamp(k))
				tr.ConvertSum(MetricPoint{
					Identity: sumID,
					Value: ValuePoint{
						ObservedTimestamp: tsSec(base, int64(iter*10)),
						IntValue:          int64(iter * 5),
					},
				}, buf[:0])

				histID := MetricIdentity{
					Resource:               pcommon.NewResource(),
					InstrumentationLibrary: pcommon.NewInstrumentationScope(),
					MetricType:             pmetric.MetricTypeHistogram,
					MetricName:             "hist_conc",
					StartTimestamp:         pcommon.Timestamp(k),
					Attributes:             pcommon.NewMap(),
				}
				tr.Convert(MetricPoint{
					Identity: histID,
					Value: ValuePoint{
						ObservedTimestamp: tsSec(base, int64(iter*10)),
						HistogramValue: &HistogramPoint{
							Count:        uint64(iter + 1),
							Sum:          float64(iter + 1),
							BucketBounds: []float64{1},
							BucketCounts: []uint64{uint64(iter + 1), 0},
						},
					},
				})
			}
		}(w)
	}

	workerWG.Wait()
	close(stop)
	sweeperWG.Wait()

	// Final sweep evicts everything older than future timestamp.
	tr.removeStale(pcommon.NewTimestampFromTime(time.Now().Add(time.Hour)))
	assert.Equal(t, int64(0), tr.Streams())
}

func TestConvertSumGuardRejectsInvalidOrOffMode(t *testing.T) {
	base := time.Now().Add(time.Hour)
	id := makeIntIdentity("guard", 0)
	for _, badMode := range []RetractionMode{RetractionModeOff, RetractionMode(-1), RetractionMode(99)} {
		tr := &MetricTracker{
			logger:        zap.NewNop(),
			retractionCfg: RetractionConfig{Mode: badMode},
		}
		var buf [4]SumDelta
		ds, out, reason := tr.ConvertSum(MetricPoint{
			Identity: id,
			Value:    ValuePoint{ObservedTimestamp: tsSec(base, 10), IntValue: 100},
		}, buf[:0])
		assert.Equal(t, OutcomeDropped, out)
		assert.Empty(t, reason)
		assert.Empty(t, ds)
		assert.Equal(t, int64(0), tr.Streams(), "invalid/off mode must not leak into retractionStates")
	}
}

func BenchmarkTracker50kInOrder(b *testing.B) {
	const numSeries = 50_000
	base := time.Now().Add(time.Hour)
	ids := make([]MetricIdentity, numSeries)
	for s := range numSeries {
		attrs := pcommon.NewMap()
		attrs.PutInt("series", int64(s))
		ids[s] = MetricIdentity{
			Resource:               pcommon.NewResource(),
			InstrumentationLibrary: pcommon.NewInstrumentationScope(),
			MetricType:             pmetric.MetricTypeSum,
			MetricIsMonotonic:      true,
			MetricName:             "http_requests_total",
			MetricUnit:             "1",
			Attributes:             attrs,
			MetricValueType:        pmetric.NumberDataPointValueTypeInt,
		}
	}

	for _, mode := range []RetractionMode{RetractionModeOff, RetractionModeZSet, RetractionModeZSetCompact, RetractionModeNonNegative} {
		b.Run(mode.String(), func(b *testing.B) {
			tr := NewMetricTrackerWithRetraction(b.Context(), zap.NewNop(), 0, InitialValueKeep, RetractionConfig{
				Mode:             mode,
				ReorderWindow:    30 * time.Second,
				MaxHistoryPoints: 64,
			})
			var buf [4]SumDelta
			for step := range int64(10) {
				ts := tsSec(base, step*5)
				for s := range numSeries {
					mp := MetricPoint{Identity: ids[s], Value: ValuePoint{ObservedTimestamp: ts, IntValue: (step + 1) * 100}}
					if mode == RetractionModeOff {
						tr.Convert(mp)
					} else {
						tr.ConvertSum(mp, buf[:0])
					}
				}
			}
			steps := make([]int64, numSeries)
			for s := range steps {
				steps[s] = 10
			}
			var i int
			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				s := i % numSeries
				i++
				st := steps[s]
				steps[s] = st + 1
				mp := MetricPoint{Identity: ids[s], Value: ValuePoint{ObservedTimestamp: tsSec(base, st*5), IntValue: (st + 1) * 100}}
				if mode == RetractionModeOff {
					tr.Convert(mp)
				} else {
					tr.ConvertSum(mp, buf[:0])
				}
			}
		})
	}
}

func BenchmarkTracker50kOutOfOrder15Pct(b *testing.B) {
	const numSeries = 50_000
	base := time.Now().Add(time.Hour)
	ids := make([]MetricIdentity, numSeries)
	for s := range numSeries {
		attrs := pcommon.NewMap()
		attrs.PutInt("series", int64(s))
		ids[s] = MetricIdentity{
			Resource:               pcommon.NewResource(),
			InstrumentationLibrary: pcommon.NewInstrumentationScope(),
			MetricType:             pmetric.MetricTypeSum,
			MetricIsMonotonic:      true,
			MetricName:             "http_requests_total",
			MetricUnit:             "1",
			Attributes:             attrs,
			MetricValueType:        pmetric.NumberDataPointValueTypeInt,
		}
	}

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
				lagSec:    1 + int64(rng.IntN(9)),
				deltaFrac: int64(rng.IntN(80) + 1),
			}
		} else if r < 0.18 {
			actions[i] = action{isReset: true}
		}
	}

	for _, mode := range []RetractionMode{RetractionModeOff, RetractionModeZSet, RetractionModeZSetCompact, RetractionModeNonNegative} {
		b.Run(mode.String(), func(b *testing.B) {
			tr := NewMetricTrackerWithRetraction(b.Context(), zap.NewNop(), 0, InitialValueKeep, RetractionConfig{
				Mode:             mode,
				ReorderWindow:    30 * time.Second,
				MaxHistoryPoints: 64,
			})
			var buf [4]SumDelta
			for step := range int64(10) {
				ts := tsSec(base, step*5)
				for s := range numSeries {
					mp := MetricPoint{Identity: ids[s], Value: ValuePoint{ObservedTimestamp: ts, IntValue: (step + 1) * 100}}
					if mode == RetractionModeOff {
						tr.Convert(mp)
					} else {
						tr.ConvertSum(mp, buf[:0])
					}
				}
			}

			curSec := make([]int64, numSeries)
			curVal := make([]int64, numSeries)
			for s := range numSeries {
				curSec[s] = 9 * 5
				curVal[s] = 10 * 100
			}

			var idx int
			b.ResetTimer()
			b.ReportAllocs()
			for b.Loop() {
				s := idx % numSeries
				act := actions[idx%tableLen]
				idx++

				var tsSecVal, val int64
				if act.isOoO {
					tsSecVal = curSec[s] - act.lagSec
					if tsSecVal%5 == 0 {
						tsSecVal++
					}
					val = max(curVal[s]-act.deltaFrac, 10)
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

				mp := MetricPoint{Identity: ids[s], Value: ValuePoint{ObservedTimestamp: tsSec(base, tsSecVal), IntValue: val}}
				if mode == RetractionModeOff {
					tr.Convert(mp)
				} else {
					tr.ConvertSum(mp, buf[:0])
				}
			}
		})
	}
}
