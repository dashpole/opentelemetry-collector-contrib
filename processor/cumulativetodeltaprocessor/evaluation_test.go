// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cumulativetodeltaprocessor

import (
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor/internal/metadatatest"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor/internal/tracking"
)

type tracePoint struct {
	seriesIdx   int
	startTS     pcommon.Timestamp // 0 for shared identity, > tracker.startTime for rotating StartTimestamp
	ts          pcommon.Timestamp
	arrivalTime time.Duration
	intVal      int64
	floatVal    float64
	isFloat     bool
	isRevision  bool
	isDuplicate bool
}

type evalResult struct {
	mode               tracking.RetractionMode
	totalInputs        int
	emittedPoints      int
	negDeltas          int
	errorDrops         int64
	benignInitial      int64
	benignDuplicates   int64
	absorbed           int64
	rejectedRevisions  int64
	tailRewindSpikes   int
	prefixOracleExcess int
	invertedIntervals  int
	globalErrAbs       float64
	globalErrBig       *big.Int
	oracleTotalBig     *big.Int
	oracleTotalFloat   float64
	bucketMAE          float64
	bucketMaxRelErr    float64
	zsetMismatchedKeys int
	amplification      float64
	dropByReason       map[string]int64
}

func readDropCounts(t *testing.T, tel *componenttest.Telemetry) (map[string]int64, int64) {
	t.Helper()
	reasons := map[string]int64{}
	m, err := tel.GetMetric("otelcol_cumulativetodelta_datapoints_dropped")
	if err == nil {
		if sumData, ok := m.Data.(metricdata.Sum[int64]); ok {
			for _, dp := range sumData.DataPoints {
				r, _ := dp.Attributes.Value("reason")
				reasons[r.AsString()] += dp.Value
			}
		}
	}
	var converted int64
	mc, err := tel.GetMetric("otelcol_cumulativetodelta_datapoints")
	if err == nil {
		if sumData, ok := mc.Data.(metricdata.Sum[int64]); ok {
			for _, dp := range sumData.DataPoints {
				converted += dp.Value
			}
		}
	}
	return reasons, converted
}

func intervalDeltaIntEval(prev, curr int64) int64 {
	if curr >= prev {
		return curr - prev
	}
	return curr
}

func intervalDeltaFloatEval(prev, curr float64) float64 {
	if curr >= prev {
		return curr - prev
	}
	return curr
}

// generateSyntheticTrace builds a deterministic Kubernetes-style trace with out-of-order
// delivery (oooRate in [0,1], jitter up to maxJitter), periodic container restarts, and flat
// zero-increment heartbeats. All timestamps are anchored strictly after base (now + 1h).
func generateSyntheticTrace(
	seed uint64,
	numSeries int,
	samplesPerSeries int,
	scrapeInterval time.Duration,
	oooRate float64,
	maxJitter time.Duration,
	rotatingStartTS bool,
	isFloat bool,
	floatScale float64,
	includeDupRev bool,
	base time.Time,
) []tracePoint {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	pts := make([]tracePoint, 0, numSeries*samplesPerSeries)

	for s := range numSeries {
		var curStartTS pcommon.Timestamp
		if rotatingStartTS {
			// Pod started 1s before its first scrape (strictly after tracker.startTime).
			curStartTS = pcommon.NewTimestampFromTime(base.Add(-scrapeInterval))
		}
		var curInt int64 = int64(rng.IntN(50) + 10)
		var curFlt float64 = (float64(rng.IntN(50)) + 10.0) * floatScale
		nextResetAt := 18 + rng.IntN(20)

		for i := range samplesPerSeries {
			tsOffset := time.Duration(i) * scrapeInterval
			ts := pcommon.NewTimestampFromTime(base.Add(tsOffset))

			if i > 0 {
				if i == nextResetAt {
					// Container restart!
					nextResetAt = i + 18 + rng.IntN(20)
					curInt = int64(rng.IntN(40) + 5)
					curFlt = (float64(rng.IntN(40)) + 5.0) * floatScale
					if rotatingStartTS {
						curStartTS = pcommon.NewTimestampFromTime(base.Add(tsOffset - scrapeInterval/2))
					}
				} else if rng.Float64() < 0.12 {
					// Flat +0 heartbeat step (same value as previous sample).
				} else {
					inc := int64(rng.IntN(100) + 1)
					curInt += inc
					curFlt += (float64(inc) + rng.Float64()) * floatScale
				}
			}

			// Compute network arrival delay.
			delay := time.Duration(0)
			if rng.Float64() < oooRate {
				// Delayed between scrapeInterval and maxJitter (strictly <= maxJitter).
				minDelay := scrapeInterval + time.Millisecond
				if minDelay >= maxJitter {
					delay = maxJitter
				} else {
					delay = minDelay + time.Duration(rng.Int64N(int64(maxJitter-minDelay+1)))
				}
			}

			tp := tracePoint{
				seriesIdx:   s,
				startTS:     curStartTS,
				ts:          ts,
				arrivalTime: tsOffset + delay,
				intVal:      curInt,
				floatVal:    curFlt,
				isFloat:     isFloat,
			}
			pts = append(pts, tp)

			if includeDupRev && i > 0 {
				r := rng.Float64()
				if r < 0.08 {
					dup := tp
					dup.arrivalTime = tsOffset + delay + 500*time.Millisecond
					dup.isDuplicate = true
					pts = append(pts, dup)
				} else if r < 0.13 {
					rev := tp
					rev.arrivalTime = tsOffset + delay + time.Second
					rev.isRevision = true
					delta := int64(rng.IntN(60)) - 30 // Mix of upward & downward revisions!
					if delta == 0 {
						delta = 15
					}
					if rev.intVal+delta < 0 {
						delta = -delta
					}
					rev.intVal += delta
					rev.floatVal = math.Max(0, rev.floatVal+float64(delta)*floatScale)
					pts = append(pts, rev)
				}
			}
		}
	}

	// Sort by simulated arrivalTime (stable to keep deterministic ordering on ties).
	sort.SliceStable(pts, func(i, j int) bool {
		if pts[i].arrivalTime != pts[j].arrivalTime {
			return pts[i].arrivalTime < pts[j].arrivalTime
		}
		if pts[i].seriesIdx != pts[j].seriesIdx {
			return pts[i].seriesIdx < pts[j].seriesIdx
		}
		return pts[i].ts < pts[j].ts
	})
	return pts
}

func runEvaluation(
	t *testing.T,
	trace []tracePoint,
	mode tracking.RetractionMode,
	initVal tracking.InitialValue,
	reorderWindow time.Duration,
	maxHistory int,
) evalResult {
	t.Helper()

	tel := componenttest.NewTelemetry()
	defer func() { require.NoError(t, tel.Shutdown(t.Context())) }()

	next := new(consumertest.MetricsSink)
	set := metadatatest.NewSettings(tel)
	cfg := &Config{
		InitialValue:     initVal,
		RetractionMode:   mode,
		ReorderWindow:    reorderWindow,
		MaxHistoryPoints: maxHistory,
	}
	require.NoError(t, cfg.Validate())
	p, err := NewFactory().CreateMetrics(t.Context(), set, cfg, next)
	require.NoError(t, err)
	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, p.Shutdown(t.Context())) }()

	type streamKey struct {
		series  int
		startTS pcommon.Timestamp
	}
	type ivKey struct {
		stream streamKey
		s, e   pcommon.Timestamp
	}

	acceptedInt := map[streamKey]map[pcommon.Timestamp]int64{}
	acceptedFlt := map[streamKey]map[pcommon.Timestamp]float64{}
	prevMaxEmitted := map[streamKey]pcommon.Timestamp{}

	emittedSumBig := new(big.Int)
	var emittedSumFlt float64
	bucketsInt := map[streamKey]map[pcommon.Timestamp]*big.Int{}
	bucketsFlt := map[streamKey]map[pcommon.Timestamp]float64{}
	zsetInt := map[ivKey]*big.Int{}
	zsetFlt := map[ivKey]float64{}

	var res evalResult
	res.mode = mode
	res.totalInputs = len(trace)
	isFloat := len(trace) > 0 && trace[0].isFloat

	var prevInitial int64
	var prevRejected int64
	var prevWindowExceeded int64
	var prevInvalid int64
	var prevDup int64
	var prevReset int64
	var maxVal float64 = 1.0

	prefixOracleGapInt := func(sk streamKey, start, end pcommon.Timestamp, isInit bool) int64 {
		if start > end {
			return 0
		}
		h := acceptedInt[sk]
		if len(h) == 0 {
			return 0
		}
		keys := make([]pcommon.Timestamp, 0, len(h))
		for k := range h {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		if isInit && end == keys[0] {
			return h[keys[0]]
		}
		// Sum consecutive interval deltas inside (start, end].
		var sum int64
		if start == 0 && end == keys[0] {
			return h[keys[0]]
		}
		for i := 1; i < len(keys); i++ {
			if keys[i-1] >= start && keys[i] <= end {
				sum += intervalDeltaIntEval(h[keys[i-1]], h[keys[i]])
			}
		}
		// If start == sk.startTS and end == keys[0] (rotating StartTimestamp initial emission).
		if sk.startTS != 0 && start == sk.startTS && end == keys[0] {
			return h[keys[0]]
		}
		// Also cover cases where emitted start lies strictly before keys[0] (e.g. InitialValueKeep with start=0).
		if start < keys[0] && end >= keys[0] {
			sum += h[keys[0]]
		}
		return sum
	}

	prefixOracleGapFlt := func(sk streamKey, start, end pcommon.Timestamp) float64 {
		if start > end {
			return 0
		}
		h := acceptedFlt[sk]
		if len(h) == 0 {
			return 0
		}
		keys := make([]pcommon.Timestamp, 0, len(h))
		for k := range h {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		if (start == 0 || (sk.startTS != 0 && start == sk.startTS)) && end == keys[0] {
			return h[keys[0]]
		}
		var sum float64
		for i := 1; i < len(keys); i++ {
			if keys[i-1] >= start && keys[i] <= end {
				sum += intervalDeltaFloatEval(h[keys[i-1]], h[keys[i]])
			}
		}
		if start < keys[0] && end >= keys[0] {
			sum += h[keys[0]]
		}
		return sum
	}

	for _, tp := range trace {
		sk := streamKey{series: tp.seriesIdx, startTS: tp.startTS}
		if tp.isFloat && tp.floatVal > maxVal {
			maxVal = tp.floatVal
		}

		md := pmetric.NewMetrics()
		m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName(fmt.Sprintf("http_requests_%d", tp.seriesIdx))
		s := m.SetEmptySum()
		s.SetIsMonotonic(true)
		s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		dp := s.DataPoints().AppendEmpty()
		dp.Attributes().PutInt("pod_idx", int64(tp.seriesIdx))
		if tp.startTS != 0 {
			dp.SetStartTimestamp(tp.startTS)
		}
		dp.SetTimestamp(tp.ts)
		if tp.isFloat {
			dp.SetDoubleValue(tp.floatVal)
		} else {
			dp.SetIntValue(tp.intVal)
		}

		beforeLen := len(next.AllMetrics())
		require.NoError(t, p.ConsumeMetrics(t.Context(), md))
		afterAll := next.AllMetrics()

		var emittedDPs []pmetric.NumberDataPoint
		if len(afterAll) > beforeLen {
			outMD := afterAll[len(afterAll)-1]
			if outMD.ResourceMetrics().Len() > 0 {
				outDPs := outMD.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Sum().DataPoints()
				for k := range outDPs.Len() {
					emittedDPs = append(emittedDPs, outDPs.At(k))
				}
			}
		}

		// Determine if this input was accepted into H_accepted (only need telemetry lookup
		// when 0 points were emitted for a non-initial point in retraction modes!).
		inputAccepted := true
		if len(emittedDPs) == 0 && mode != tracking.RetractionModeOff {
			reasons, _ := readDropCounts(t, tel)
			if reasons[tracking.ReasonRejectedRevision] > prevRejected ||
				reasons[tracking.ReasonWindowExceeded] > prevWindowExceeded ||
				reasons[tracking.ReasonInvalidValue] > prevInvalid ||
				reasons[tracking.ReasonDuplicate] > prevDup {
				inputAccepted = false
				prevRejected = reasons[tracking.ReasonRejectedRevision]
				prevWindowExceeded = reasons[tracking.ReasonWindowExceeded]
				prevInvalid = reasons[tracking.ReasonInvalidValue]
				prevDup = reasons[tracking.ReasonDuplicate]
			} else if reasons[tracking.ReasonInitial] > prevInitial {
				prevInitial = reasons[tracking.ReasonInitial]
			} else {
				// Expected zero-delta absorption (OutcomeAbsorbed).
				res.absorbed++
			}
		}
		if mode == tracking.RetractionModeOff && len(emittedDPs) == 0 {
			// Check whether baseline dropped as reset (keep full trace in oracle for off so we measure real loss!).
			reasons, _ := readDropCounts(t, tel)
			if reasons[tracking.ReasonReset] > prevReset {
				prevReset = reasons[tracking.ReasonReset]
			}
		}

		if inputAccepted {
			if tp.isFloat {
				if acceptedFlt[sk] == nil {
					acceptedFlt[sk] = map[pcommon.Timestamp]float64{}
				}
				acceptedFlt[sk][tp.ts] = tp.floatVal
			} else {
				if acceptedInt[sk] == nil {
					acceptedInt[sk] = map[pcommon.Timestamp]int64{}
				}
				acceptedInt[sk][tp.ts] = tp.intVal
			}
		}

		for _, edp := range emittedDPs {
			res.emittedPoints++
			st := edp.StartTimestamp()
			et := edp.Timestamp()
			if st > et {
				res.invertedIntervals++
			}
			// Gate #2(a): Tail-Rewind Overlapping Spikes.
			// Advances high-water mark while starting strictly behind prevMaxEmittedTimestamp.
			if prevMax, ok := prevMaxEmitted[sk]; ok {
				if et > prevMax && st < prevMax {
					res.tailRewindSpikes++
				}
			}
			if et > prevMaxEmitted[sk] {
				prevMaxEmitted[sk] = et
			}

			if tp.isFloat {
				v := edp.DoubleValue()
				if v < 0 {
					res.negDeltas++
				}
				emittedSumFlt += v
				if bucketsFlt[sk] == nil {
					bucketsFlt[sk] = map[pcommon.Timestamp]float64{}
				}
				bucketsFlt[sk][et] += v
				ik := ivKey{stream: sk, s: st, e: et}
				zsetFlt[ik] += v
				if math.Abs(zsetFlt[ik]) <= 1e-12*math.Max(1, maxVal) {
					delete(zsetFlt, ik)
				}
				// Gate #2(b): Prefix-Oracle Excess (positive emissions only).
				if v > 0 {
					slack := 1e-9 * math.Max(1, maxVal)
					oracleGap := prefixOracleGapFlt(sk, st, et)
					if v > oracleGap+slack {
						res.prefixOracleExcess++
					}
				}
			} else {
				v := edp.IntValue()
				if v < 0 {
					res.negDeltas++
				}
				bv := big.NewInt(v)
				emittedSumBig.Add(emittedSumBig, bv)
				if bucketsInt[sk] == nil {
					bucketsInt[sk] = map[pcommon.Timestamp]*big.Int{}
				}
				if bucketsInt[sk][et] == nil {
					bucketsInt[sk][et] = new(big.Int)
				}
				bucketsInt[sk][et].Add(bucketsInt[sk][et], bv)
				ik := ivKey{stream: sk, s: st, e: et}
				if zsetInt[ik] == nil {
					zsetInt[ik] = new(big.Int)
				}
				zsetInt[ik].Add(zsetInt[ik], bv)
				if zsetInt[ik].Sign() == 0 {
					delete(zsetInt, ik)
				}
				if v > 0 {
					oracleGap := prefixOracleGapInt(sk, st, et, len(acceptedInt[sk]) == 1)
					if v > oracleGap {
						res.prefixOracleExcess++
					}
				}
			}
		}
	}

	reasons, _ := readDropCounts(t, tel)
	res.dropByReason = reasons
	res.benignInitial = reasons[tracking.ReasonInitial]
	res.benignDuplicates = reasons[tracking.ReasonDuplicate]
	res.rejectedRevisions = reasons[tracking.ReasonRejectedRevision]
	res.errorDrops = reasons[tracking.ReasonReset] + reasons[tracking.ReasonWindowExceeded] + reasons[tracking.ReasonInvalidValue] + reasons[tracking.ReasonRejectedRevision]
	if res.totalInputs > 0 {
		res.amplification = float64(res.emittedPoints) / float64(res.totalInputs)
	}

	// Compute oracle totals, per-bucket MAE, and Z-set map mismatches.
	if !isFloat {
		oracleTotal := new(big.Int)
		wantZSet := map[ivKey]*big.Int{}
		var bucketErrSum float64
		var bucketCount int
		for sk, hist := range acceptedInt {
			keys := make([]pcommon.Timestamp, 0, len(hist))
			for k := range hist {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			if len(keys) == 0 {
				continue
			}
			wantB := map[pcommon.Timestamp]*big.Int{}
			t0 := keys[0]
			emitInit := initVal == tracking.InitialValueKeep || (initVal == tracking.InitialValueAuto && sk.startTS != 0 && t0 > sk.startTS)
			if emitInit {
				v0 := hist[t0]
				bv0 := big.NewInt(v0)
				oracleTotal.Add(oracleTotal, bv0)
				wantB[t0] = bv0
				if v0 != 0 {
					var ist pcommon.Timestamp
					if sk.startTS <= t0 {
						ist = sk.startTS
					}
					wantZSet[ivKey{sk, ist, t0}] = new(big.Int).Set(bv0)
				}
			}
			for i := 1; i < len(keys); i++ {
				d := intervalDeltaIntEval(hist[keys[i-1]], hist[keys[i]])
				bd := big.NewInt(d)
				oracleTotal.Add(oracleTotal, bd)
				wantB[keys[i]] = bd
				if d != 0 {
					wantZSet[ivKey{sk, keys[i-1], keys[i]}] = new(big.Int).Set(bd)
				}
			}
			allTS := map[pcommon.Timestamp]struct{}{}
			for ts := range wantB {
				allTS[ts] = struct{}{}
			}
			for ts := range bucketsInt[sk] {
				allTS[ts] = struct{}{}
			}
			for ts := range allTS {
				var g, w big.Int
				if v := bucketsInt[sk][ts]; v != nil {
					g.Set(v)
				}
				if v := wantB[ts]; v != nil {
					w.Set(v)
				}
				diff := new(big.Int).Sub(&g, &w)
				diff.Abs(diff)
				fdiff, _ := new(big.Float).SetInt(diff).Float64()
				bucketErrSum += fdiff
				bucketCount++
			}
		}
		diffTotal := new(big.Int).Sub(emittedSumBig, oracleTotal)
		diffTotal.Abs(diffTotal)
		res.globalErrBig = diffTotal
		res.oracleTotalBig = oracleTotal
		res.globalErrAbs, _ = new(big.Float).SetInt(diffTotal).Float64()
		if bucketCount > 0 {
			res.bucketMAE = bucketErrSum / float64(bucketCount)
		}
		for k, wv := range wantZSet {
			gv := zsetInt[k]
			if gv == nil || gv.Cmp(wv) != 0 {
				res.zsetMismatchedKeys++
			}
		}
		for k, gv := range zsetInt {
			if _, ok := wantZSet[k]; !ok && gv.Sign() != 0 {
				res.zsetMismatchedKeys++
			}
		}
	} else {
		var oracleTotal float64
		wantZSetFlt := map[ivKey]float64{}
		var bucketErrSum float64
		var bucketCount int
		sortedStreams := make([]streamKey, 0, len(acceptedFlt))
		for sk := range acceptedFlt {
			sortedStreams = append(sortedStreams, sk)
		}
		sort.Slice(sortedStreams, func(i, j int) bool {
			if sortedStreams[i].series != sortedStreams[j].series {
				return sortedStreams[i].series < sortedStreams[j].series
			}
			return sortedStreams[i].startTS < sortedStreams[j].startTS
		})
		for _, sk := range sortedStreams {
			hist := acceptedFlt[sk]
			keys := make([]pcommon.Timestamp, 0, len(hist))
			for k := range hist {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			if len(keys) == 0 {
				continue
			}
			wantB := map[pcommon.Timestamp]float64{}
			t0 := keys[0]
			emitInit := initVal == tracking.InitialValueKeep || (initVal == tracking.InitialValueAuto && sk.startTS != 0 && t0 > sk.startTS)
			if emitInit {
				oracleTotal += hist[t0]
				wantB[t0] = hist[t0]
				if hist[t0] != 0 {
					var ist pcommon.Timestamp
					if sk.startTS <= t0 {
						ist = sk.startTS
					}
					wantZSetFlt[ivKey{sk, ist, t0}] = hist[t0]
				}
			}
			for i := 1; i < len(keys); i++ {
				d := intervalDeltaFloatEval(hist[keys[i-1]], hist[keys[i]])
				oracleTotal += d
				wantB[keys[i]] = d
				if d != 0 {
					wantZSetFlt[ivKey{sk, keys[i-1], keys[i]}] = d
				}
			}
			allTS := map[pcommon.Timestamp]struct{}{}
			for ts := range wantB {
				allTS[ts] = struct{}{}
			}
			for ts := range bucketsFlt[sk] {
				allTS[ts] = struct{}{}
			}
			for ts := range allTS {
				errVal := math.Abs(bucketsFlt[sk][ts] - wantB[ts])
				bucketErrSum += errVal
				rel := errVal / math.Max(1, maxVal)
				if rel > res.bucketMaxRelErr {
					res.bucketMaxRelErr = rel
				}
				bucketCount++
			}
		}
		res.oracleTotalFloat = oracleTotal
		res.globalErrAbs = math.Abs(emittedSumFlt - oracleTotal)
		if bucketCount > 0 {
			res.bucketMAE = bucketErrSum / float64(bucketCount)
		}
		tolZ := 1e-9 * math.Max(1, maxVal)
		for k, wv := range wantZSetFlt {
			gv := zsetFlt[k]
			if math.Abs(gv-wv) > tolZ {
				res.zsetMismatchedKeys++
			}
		}
		for k, gv := range zsetFlt {
			if _, ok := wantZSetFlt[k]; !ok && math.Abs(gv) > tolZ {
				res.zsetMismatchedKeys++
			}
		}
	}

	return res
}

func logEvalTable(t *testing.T, title string, results []evalResult) {
	t.Helper()
	t.Logf("\n=== %s ===", title)
	t.Logf("%-13s | %6s | %7s | %8s | %6s | %6s | %4s | %12s | %10s | %6s | %5s | %5s",
		"Mode", "Inputs", "Emitted", "ErrDrop%", "SpkT", "SpkP", "Inv", "GlobalErr", "BucketMAE", "ZSetMM", "Amp", "Neg#")
	for _, r := range results {
		dropPct := 100.0 * float64(r.errorDrops) / float64(max(1, r.totalInputs))
		globalStr := fmt.Sprintf("%.4g", r.globalErrAbs)
		if r.globalErrBig != nil {
			globalStr = r.globalErrBig.String()
		}
		t.Logf("%-13s | %6d | %7d | %7.2f%% | %6d | %6d | %4d | %12s | %10.4g | %6d | %5.2f | %5d",
			r.mode.String(), r.totalInputs, r.emittedPoints, dropPct,
			r.tailRewindSpikes, r.prefixOracleExcess, r.invertedIntervals,
			globalStr, r.bucketMAE, r.zsetMismatchedKeys, r.amplification, r.negDeltas)
	}
}

func TestEvaluationSyntheticTraces(t *testing.T) {
	base := time.Now().Add(time.Hour)
	modes := []tracking.RetractionMode{
		tracking.RetractionModeOff,
		tracking.RetractionModeZSet,
		tracking.RetractionModeZSetCompact,
		tracking.RetractionModeNonNegative,
	}

	// Scenario A: Shared identity (StartTimestamp == 0) + frequent container restarts across 5%/10%/15% OoO & 5s/2s scrape intervals.
	configs := []struct {
		name           string
		scrapeInterval time.Duration
		oooRate        float64
	}{
		{"ScenarioA_SharedID_5s_5pctOoO", 5 * time.Second, 0.05},
		{"ScenarioA_SharedID_5s_10pctOoO", 5 * time.Second, 0.10},
		{"ScenarioA_SharedID_5s_15pctOoO", 5 * time.Second, 0.15},
		{"ScenarioA_SharedID_2s_15pctOoO", 2 * time.Second, 0.15},
	}

	for _, sc := range configs {
		t.Run(sc.name, func(t *testing.T) {
			trace := generateSyntheticTrace(1001, 30, 120, sc.scrapeInterval, sc.oooRate, 10*time.Second, false, false, 1, false, base)
			var results []evalResult
			for _, m := range modes {
				r := runEvaluation(t, trace, m, tracking.InitialValueKeep, 30*time.Second, 64)
				results = append(results, r)
			}
			logEvalTable(t, sc.name, results)

			offRes, zsetRes, compactRes, nnRes := results[0], results[1], results[2], results[3]

			// Baseline-vs-Change Proof: `off` MUST exhibit drops, spikes, inverted intervals, and non-zero error.
			assert.Greater(t, offRes.errorDrops, int64(0), "baseline should drop resets/OoO points")
			assert.Greater(t, offRes.tailRewindSpikes, 0, "baseline should trigger tail-rewind overlapping spikes")
			assert.Greater(t, offRes.prefixOracleExcess, 0, "baseline should trigger prefix-oracle excess spikes")
			assert.Greater(t, offRes.invertedIntervals, 0, "baseline should emit inverted intervals")
			assert.NotEqual(t, 0, offRes.globalErrBig.Sign(), "baseline should fail global conservation")

			// ZSet exit gates: 0% error drops, 0 spikes, 0 inverted, exact 0 conservation, 0 MAE, 0 ZSet mismatch.
			assert.Equal(t, int64(0), zsetRes.errorDrops)
			assert.Equal(t, 0, zsetRes.tailRewindSpikes)
			assert.Equal(t, 0, zsetRes.prefixOracleExcess)
			assert.Equal(t, 0, zsetRes.invertedIntervals)
			assert.Equal(t, 0, zsetRes.globalErrBig.Sign())
			assert.Equal(t, 0.0, zsetRes.bucketMAE)
			assert.Equal(t, 0, zsetRes.zsetMismatchedKeys)

			// ZSetCompact exit gates: 0% error drops, 0 spikes, 0 inverted, exact 0 conservation, 0 MAE, lower amplification than zset.
			assert.Equal(t, int64(0), compactRes.errorDrops)
			assert.Equal(t, 0, compactRes.tailRewindSpikes)
			assert.Equal(t, 0, compactRes.prefixOracleExcess)
			assert.Equal(t, 0, compactRes.invertedIntervals)
			assert.Equal(t, 0, compactRes.globalErrBig.Sign())
			assert.Equal(t, 0.0, compactRes.bucketMAE)
			assert.LessOrEqual(t, compactRes.emittedPoints, zsetRes.emittedPoints)

			// NonNegative exit gates: 0% error drops, 0 spikes, 0 inverted, exact 0 conservation, ZERO negative deltas.
			assert.Equal(t, int64(0), nnRes.errorDrops)
			assert.Equal(t, 0, nnRes.tailRewindSpikes)
			assert.Equal(t, 0, nnRes.prefixOracleExcess)
			assert.Equal(t, 0, nnRes.invertedIntervals)
			assert.Equal(t, 0, nnRes.globalErrBig.Sign())
			assert.Equal(t, 0, nnRes.negDeltas)
		})
	}

	// Scenario B: Rotating StartTimestamp (> tracker.startTime) + InitialValueAuto + flat heartbeats + 15% OoO.
	t.Run("ScenarioB_RotatingStartTS_5s_15pctOoO_Auto", func(t *testing.T) {
		trace := generateSyntheticTrace(2002, 15, 120, 5*time.Second, 0.15, 10*time.Second, true, false, 1, false, base)
		var results []evalResult
		for _, m := range modes {
			results = append(results, runEvaluation(t, trace, m, tracking.InitialValueAuto, 30*time.Second, 64))
		}
		logEvalTable(t, "ScenarioB_RotatingStartTS_5s_15pctOoO_Auto", results)

		offRes, zsetRes, compactRes, nnRes := results[0], results[1], results[2], results[3]
		assert.Greater(t, offRes.errorDrops, int64(0))
		assert.Greater(t, offRes.tailRewindSpikes, 0, "baseline should trigger tail-rewind spikes even with rotating StartTS")
		assert.Greater(t, offRes.invertedIntervals, 0, "baseline should emit inverted intervals on flat +0 heartbeats")
		assert.NotEqual(t, 0, offRes.globalErrBig.Sign())

		for _, r := range []evalResult{zsetRes, compactRes, nnRes} {
			assert.Equal(t, int64(0), r.errorDrops, "mode=%s", r.mode)
			assert.Equal(t, 0, r.tailRewindSpikes, "mode=%s", r.mode)
			assert.Equal(t, 0, r.prefixOracleExcess, "mode=%s", r.mode)
			assert.Equal(t, 0, r.invertedIntervals, "mode=%s", r.mode)
			assert.Equal(t, 0, r.globalErrBig.Sign(), "mode=%s", r.mode)
		}
		assert.Equal(t, 0.0, zsetRes.bucketMAE)
		assert.Equal(t, 0.0, compactRes.bucketMAE)
		assert.Equal(t, 0, zsetRes.zsetMismatchedKeys)
		assert.Equal(t, 0, nnRes.negDeltas)
	})

	// Scenario C: Duplicates + same-timestamp revisions (upward & downward).
	t.Run("ScenarioC_DuplicatesAndRevisions_15pctOoO", func(t *testing.T) {
		trace := generateSyntheticTrace(3003, 10, 80, 5*time.Second, 0.15, 10*time.Second, false, false, 1, true, base)
		var results []evalResult
		for _, m := range modes {
			results = append(results, runEvaluation(t, trace, m, tracking.InitialValueKeep, 30*time.Second, 64))
		}
		logEvalTable(t, "ScenarioC_DuplicatesAndRevisions_15pctOoO", results)

		zsetRes, compactRes, nnRes := results[1], results[2], results[3]
		assert.Equal(t, int64(0), zsetRes.errorDrops)
		assert.Greater(t, zsetRes.benignDuplicates, int64(0))
		assert.Equal(t, 0, zsetRes.globalErrBig.Sign())
		assert.Equal(t, 0.0, zsetRes.bucketMAE)
		assert.Equal(t, 0, zsetRes.zsetMismatchedKeys)
		assert.Equal(t, 0, zsetRes.tailRewindSpikes)
		assert.Equal(t, 0, zsetRes.prefixOracleExcess)

		assert.Equal(t, int64(0), compactRes.errorDrops)
		assert.Equal(t, 0, compactRes.globalErrBig.Sign())
		assert.Equal(t, 0.0, compactRes.bucketMAE)
		assert.Equal(t, 0, compactRes.tailRewindSpikes)
		assert.Equal(t, 0, compactRes.prefixOracleExcess)

		// NonNegative intentionally rejects net<0 revisions (preserving conservation on H_accepted).
		assert.Greater(t, nnRes.rejectedRevisions, int64(0))
		assert.Equal(t, 0, nnRes.globalErrBig.Sign())
		assert.Equal(t, 0, nnRes.negDeltas)
		assert.Equal(t, 0, nnRes.tailRewindSpikes)
		assert.Equal(t, 0, nnRes.prefixOracleExcess)
	})

	// Scenario D: Float64 high-dynamic-range trace (values ~1e8, testing scaled relative MAE <= 1e-9).
	t.Run("ScenarioD_Float64_1e8Scale_15pctOoO", func(t *testing.T) {
		trace := generateSyntheticTrace(4004, 10, 100, 5*time.Second, 0.15, 10*time.Second, false, true, 1e5, false, base)
		var results []evalResult
		for _, m := range modes {
			results = append(results, runEvaluation(t, trace, m, tracking.InitialValueKeep, 30*time.Second, 64))
		}
		logEvalTable(t, "ScenarioD_Float64_1e8Scale_15pctOoO", results)

		zsetRes, compactRes, nnRes := results[1], results[2], results[3]
		for _, r := range []evalResult{zsetRes, compactRes, nnRes} {
			relErr := r.globalErrAbs / math.Max(1, r.oracleTotalFloat)
			assert.LessOrEqual(t, relErr, 1e-9, "mode=%s relErr=%g", r.mode, relErr)
			assert.Equal(t, 0, r.tailRewindSpikes, "mode=%s", r.mode)
			assert.Equal(t, 0, r.prefixOracleExcess, "mode=%s", r.mode)
			assert.Equal(t, 0, r.invertedIntervals, "mode=%s", r.mode)
		}
		assert.LessOrEqual(t, zsetRes.bucketMaxRelErr, 1e-9)
		assert.Equal(t, 0, zsetRes.zsetMismatchedKeys)
		assert.LessOrEqual(t, compactRes.bucketMaxRelErr, 1e-9)
		assert.Equal(t, 0, nnRes.negDeltas)
		assert.Greater(t, nnRes.absorbed, int64(0), "non_negative should absorb in-gen reorders")
	})

	// Scenario E: InitialValueDrop with 15% OoO verifying res.absorbed tracks accurately even after initial drops.
	t.Run("ScenarioE_InitialValueDrop_AbsorbedTracking", func(t *testing.T) {
		trace := generateSyntheticTrace(4005, 10, 80, 5*time.Second, 0.15, 10*time.Second, false, false, 1, false, base)
		var results []evalResult
		for _, m := range modes {
			results = append(results, runEvaluation(t, trace, m, tracking.InitialValueDrop, 30*time.Second, 64))
		}
		logEvalTable(t, "ScenarioE_InitialValueDrop_AbsorbedTracking", results)
		nnRes := results[3]
		assert.Equal(t, int64(10), nnRes.benignInitial, "expected 1 initial drop per series")
		assert.Equal(t, int64(0), nnRes.errorDrops)
		assert.Greater(t, nnRes.absorbed, int64(0), "absorbed counter must keep incrementing after initial drops")
		assert.Equal(t, 0, nnRes.globalErrBig.Sign())
		assert.Equal(t, 0, nnRes.negDeltas)
	})
}

// deltaToCumulativeProbe faithfully replicates the exact guards from
// processor/deltatocumulativeprocessor/internal/delta/delta.go:46-58 and
// internal/data/add.go:34-44 (Go forbids cross-module internal/ imports).
type deltaToCumulativeProbe struct {
	stateStart map[string]pcommon.Timestamp
	stateTS    map[string]pcommon.Timestamp
	stateVal   map[string]int64
	accepted   int
	olderStart int
	outOfOrder int
}

func newDeltaToCumulativeProbe() *deltaToCumulativeProbe {
	return &deltaToCumulativeProbe{
		stateStart: map[string]pcommon.Timestamp{},
		stateTS:    map[string]pcommon.Timestamp{},
		stateVal:   map[string]int64{},
	}
}

func (p *deltaToCumulativeProbe) consume(series string, startTS, ts pcommon.Timestamp, val int64) {
	lastTS, ok := p.stateTS[series]
	if !ok || lastTS == 0 {
		p.stateStart[series] = startTS
		p.stateTS[series] = ts
		p.stateVal[series] = val
		p.accepted++
		return
	}
	// Checked first in deltatocumulativeprocessor/internal/delta/delta.go:52-54.
	if startTS < p.stateStart[series] {
		p.olderStart++
		return
	}
	// Checked second in deltatocumulativeprocessor/internal/delta/delta.go:55-57.
	if ts <= lastTS {
		p.outOfOrder++
		return
	}
	// Signed addition (deltatocumulativeprocessor/internal/data/add.go:34-44).
	p.stateVal[series] += val
	p.stateTS[series] = ts
	p.accepted++
}

func (p *deltaToCumulativeProbe) total() int64 {
	var sum int64
	for _, v := range p.stateVal {
		sum += v
	}
	return sum
}

func TestDownstreamCompatibility(t *testing.T) {
	base := time.Now().Add(time.Hour)
	trace := generateSyntheticTrace(5005, 10, 100, 5*time.Second, 0.15, 10*time.Second, false, false, 1, false, base)

	// Compute true oracle sum across all 10 series.
	perSeries := map[int]map[pcommon.Timestamp]int64{}
	for _, tp := range trace {
		if perSeries[tp.seriesIdx] == nil {
			perSeries[tp.seriesIdx] = map[pcommon.Timestamp]int64{}
		}
		perSeries[tp.seriesIdx][tp.ts] = tp.intVal
	}
	var oracleTotal int64
	for _, hist := range perSeries {
		keys := make([]pcommon.Timestamp, 0, len(hist))
		for k := range hist {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		oracleTotal += hist[keys[0]]
		for i := 1; i < len(keys); i++ {
			oracleTotal += intervalDeltaIntEval(hist[keys[i-1]], hist[keys[i]])
		}
	}

	modes := []tracking.RetractionMode{
		tracking.RetractionModeOff,
		tracking.RetractionModeZSet,
		tracking.RetractionModeZSetCompact,
		tracking.RetractionModeNonNegative,
	}

	t.Logf("\n=== Downstream Compatibility Matrix (Oracle Total = %d, 10 series x 100 pts, 15%% OoO <= 10s jitter) ===", oracleTotal)
	t.Logf("%-13s | %-8s | %10s | %10s | %9s | %10s | %10s | %10s",
		"Mode", "OutMono", "DBSP_ZSet", "Add_TSDB", "DTC_Acc", "DTC_OldSt", "DTC_OoO", "DTC_Err")

	for _, mode := range modes {
		tel := componenttest.NewTelemetry()
		next := new(consumertest.MetricsSink)
		cfg := &Config{
			InitialValue:     tracking.InitialValueKeep,
			RetractionMode:   mode,
			ReorderWindow:    30 * time.Second,
			MaxHistoryPoints: 64,
		}
		proc, err := NewFactory().CreateMetrics(t.Context(), metadatatest.NewSettings(tel), cfg, next)
		require.NoError(t, err)
		require.NoError(t, proc.Start(t.Context(), componenttest.NewNopHost()))

		for _, tp := range trace {
			md := pmetric.NewMetrics()
			m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			m.SetName(fmt.Sprintf("series_%d", tp.seriesIdx))
			s := m.SetEmptySum()
			s.SetIsMonotonic(true)
			s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			dp := s.DataPoints().AppendEmpty()
			dp.SetTimestamp(tp.ts)
			dp.SetIntValue(tp.intVal)
			require.NoError(t, proc.ConsumeMetrics(t.Context(), md))
		}
		require.NoError(t, proc.Shutdown(t.Context()))
		require.NoError(t, tel.Shutdown(t.Context()))

		// Downstream 1: DBSP Interval-Keyed Z-Set Integrator (with zero-weight key deletion).
		type zKey struct {
			name string
			s, e pcommon.Timestamp
		}
		zMap := map[zKey]int64{}

		// Downstream 2: End-Timestamp Additive TSDB Accumulator.
		type bKey struct {
			name string
			e    pcommon.Timestamp
		}
		bMap := map[bKey]int64{}

		// Downstream 3: Standalone deltatocumulativeprocessor probe.
		dtc := newDeltaToCumulativeProbe()

		var outMono bool
		seenMono := false
		for _, md := range next.AllMetrics() {
			for i := range md.ResourceMetrics().Len() {
				sms := md.ResourceMetrics().At(i).ScopeMetrics()
				for j := range sms.Len() {
					ms := sms.At(j).Metrics()
					for k := range ms.Len() {
						m := ms.At(k)
						sum := m.Sum()
						if !seenMono {
							outMono = sum.IsMonotonic()
							seenMono = true
						}
						for d := range sum.DataPoints().Len() {
							dp := sum.DataPoints().At(d)
							v := dp.IntValue()
							zk := zKey{m.Name(), dp.StartTimestamp(), dp.Timestamp()}
							zMap[zk] += v
							if zMap[zk] == 0 {
								delete(zMap, zk)
							}
							bMap[bKey{m.Name(), dp.Timestamp()}] += v
							dtc.consume(m.Name(), dp.StartTimestamp(), dp.Timestamp(), v)
						}
					}
				}
			}
		}

		var zSum, bSum int64
		for _, v := range zMap {
			zSum += v
		}
		for _, v := range bMap {
			bSum += v
		}
		dtcErr := dtc.total() - oracleTotal

		t.Logf("%-13s | %-8v | %10d | %10d | %9d | %10d | %10d | %+10d",
			mode.String(), outMono, zSum-oracleTotal, bSum-oracleTotal,
			dtc.accepted, dtc.olderStart, dtc.outOfOrder, dtcErr)

		if mode != tracking.RetractionModeOff {
			assert.Equal(t, oracleTotal, zSum, "DBSP Z-set sum must equal oracle for %s", mode)
			assert.Equal(t, oracleTotal, bSum, "Additive TSDB sum must equal oracle for %s", mode)
			// Verify deltatocumulativeprocessor DOES drop out-of-order retractions (proving the compatibility constraint!).
			assert.Greater(t, dtc.outOfOrder+dtc.olderStart, 0, "deltatocumulative must drop late/retraction points for %s", mode)
		}
	}

	// Subcase exercising ErrOlderStart (checked before ErrOutOfOrder in deltatocumulativeprocessor):
	// Under InitialValueDrop, the first emitted delta is (t1, t2] with stateStart = t1 > 0.
	// A subsequent head-prepend t0 < t1 emits (t0, t1] where startTS (t0) < stateStart (t1),
	// triggering ErrOlderStart across all 3 retraction modes.
	t.Run("InitialValueDrop_HeadPrependTriggersErrOlderStart", func(t *testing.T) {
		for _, mode := range []tracking.RetractionMode{
			tracking.RetractionModeZSet,
			tracking.RetractionModeZSetCompact,
			tracking.RetractionModeNonNegative,
		} {
			tel := componenttest.NewTelemetry()
			next := new(consumertest.MetricsSink)
			cfg := &Config{
				InitialValue:     tracking.InitialValueDrop,
				RetractionMode:   mode,
				ReorderWindow:    60 * time.Second,
				MaxHistoryPoints: 16,
			}
			proc, err := NewFactory().CreateMetrics(t.Context(), metadatatest.NewSettings(tel), cfg, next)
			require.NoError(t, err)
			require.NoError(t, proc.Start(t.Context(), componenttest.NewNopHost()))

			// Arrival order: t=20 (initial dropped), t=30 (emits (20,30]:+100 -> stateStart=20),
			// then late head-prepend t=10 (emits (10,20]:+50 -> start=10 < stateStart=20 -> ErrOlderStart!).
			for _, pt := range []struct {
				sec int64
				val int64
			}{{20, 100}, {30, 200}, {10, 50}} {
				md := pmetric.NewMetrics()
				m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
				m.SetName("series_older_start")
				s := m.SetEmptySum()
				s.SetIsMonotonic(true)
				s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
				dp := s.DataPoints().AppendEmpty()
				dp.SetTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(pt.sec) * time.Second)))
				dp.SetIntValue(pt.val)
				require.NoError(t, proc.ConsumeMetrics(t.Context(), md))
			}
			require.NoError(t, proc.Shutdown(t.Context()))
			require.NoError(t, tel.Shutdown(t.Context()))

			dtc := newDeltaToCumulativeProbe()
			for _, md := range next.AllMetrics() {
				for i := range md.ResourceMetrics().Len() {
					sms := md.ResourceMetrics().At(i).ScopeMetrics()
					for j := range sms.Len() {
						dps := sms.At(j).Metrics().At(0).Sum().DataPoints()
						for k := range dps.Len() {
							dp := dps.At(k)
							dtc.consume("series_older_start", dp.StartTimestamp(), dp.Timestamp(), dp.IntValue())
						}
					}
				}
			}
			assert.Equal(t, 1, dtc.accepted, "mode=%s", mode)
			assert.Equal(t, 1, dtc.olderStart, "mode=%s must trigger ErrOlderStart on head-prepend under InitialValueDrop", mode)
			assert.Equal(t, 0, dtc.outOfOrder, "mode=%s ErrOlderStart precedes ErrOutOfOrder", mode)
		}
	})
}
