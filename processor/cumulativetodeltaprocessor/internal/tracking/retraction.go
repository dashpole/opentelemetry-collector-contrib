// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tracking // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor/internal/tracking"

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// Additional drop reason constants surfaced by ConvertSum in retraction mode.
const (
	ReasonWindowExceeded   = "window_exceeded"
	ReasonDuplicate        = "duplicate"
	ReasonInvalidValue     = "invalid_value"
	ReasonRejectedRevision = "rejected_revision"
)

// Default runtime limits applied when retraction mode is enabled and the
// corresponding Config fields are left at their Go zero value.
const (
	DefaultReorderWindow    = 30 * time.Second
	DefaultMaxHistoryPoints = 64
)

// RetractionMode controls how cumulative-to-delta handles out-of-order points,
// same-timestamp revisions, and counter resets on monotonic sums.
type RetractionMode int

const (
	// RetractionModeOff preserves the legacy single-point behavior (default).
	RetractionModeOff RetractionMode = iota
	// RetractionModeZSet emits full DBSP Z-set retraction triples:
	// [-d_old on (t_a, t_b], +d_1 on (t_a, t_x], +d_2 on (t_x, t_b]].
	RetractionModeZSet
	// RetractionModeZSetCompact collapses same-endpoint retractions/insertions
	// into a single net adjustment at Timestamp t_b (fewer points, exact per-bucket sums).
	RetractionModeZSetCompact
	// RetractionModeNonNegative keeps exact global cumulative conservation without
	// ever emitting negative delta values (absorbing net-zero in-generation reorders
	// and rejecting negative-net revisions/head-prepends without mutating state).
	RetractionModeNonNegative
)

func (m RetractionMode) String() string {
	switch m {
	case RetractionModeOff:
		return "off"
	case RetractionModeZSet:
		return "zset"
	case RetractionModeZSetCompact:
		return "zset_compact"
	case RetractionModeNonNegative:
		return "non_negative"
	default:
		return fmt.Sprintf("unknown(%d)", int(m))
	}
}

// IsValid reports whether m is one of the known RetractionMode enum values.
func (m RetractionMode) IsValid() bool {
	return m >= RetractionModeOff && m <= RetractionModeNonNegative
}

func (m *RetractionMode) UnmarshalText(text []byte) error {
	switch string(text) {
	case "", "off":
		*m = RetractionModeOff
	case "zset":
		*m = RetractionModeZSet
	case "zset_compact":
		*m = RetractionModeZSetCompact
	case "non_negative":
		*m = RetractionModeNonNegative
	default:
		return fmt.Errorf("unknown retraction_mode: %q (valid: off, zset, zset_compact, non_negative)", text)
	}
	return nil
}

// RetractionConfig configures retraction-safe ordered tracking for Sum streams.
type RetractionConfig struct {
	Mode             RetractionMode
	ReorderWindow    time.Duration
	MaxHistoryPoints int
}

// Effective returns a copy of c with runtime defaults applied when Mode != Off.
func (c RetractionConfig) Effective() RetractionConfig {
	if c.Mode == RetractionModeOff {
		return c
	}
	if c.ReorderWindow == 0 {
		c.ReorderWindow = DefaultReorderWindow
	}
	if c.MaxHistoryPoints == 0 {
		c.MaxHistoryPoints = DefaultMaxHistoryPoints
	}
	return c
}

// Outcome distinguishes emitted deltas, benign zero-delta state absorptions,
// and dropped/rejected inputs.
type Outcome uint8

const (
	// OutcomeDropped indicates the input was not emitted and was dropped by the
	// tracker (reason is non-empty for recorded drops, or empty for internal
	// contract cases such as unsupported metric types or NaN floats handled upstream).
	OutcomeDropped Outcome = iota
	// OutcomeEmitted indicates one or more SumDelta points were emitted.
	OutcomeEmitted
	// OutcomeAbsorbed indicates the input was accepted into stream history without
	// emitting any delta datapoint (zero net change to publish).
	OutcomeAbsorbed
)

// SumDelta represents a single delta datapoint emitted by ConvertSum.
type SumDelta struct {
	StartTimestamp pcommon.Timestamp
	Timestamp      pcommon.Timestamp
	IntValue       int64
	FloatValue     float64
	IsFloat        bool
	IsRetraction   bool
	// Synthetic is true when this delta is a retraction or re-timestamped interval
	// that should not inherit exemplars from the triggering input datapoint.
	Synthetic bool
}

// sumObs is a compact 16-byte pointer-free observation stored in timestamp order.
// Go's runtime marks []sumObs as noscan (zero GC pointer scanning).
type sumObs struct {
	ts   pcommon.Timestamp
	bits uint64 // uint64(int64) for int sums, math.Float64bits(float64) for float sums
}

func obsFromInt(ts pcommon.Timestamp, v int64) sumObs {
	return sumObs{ts: ts, bits: uint64(v)}
}

func obsFromFloat(ts pcommon.Timestamp, v float64) sumObs {
	return sumObs{ts: ts, bits: math.Float64bits(normFloat(v))}
}

func (o sumObs) intVal() int64 {
	return int64(o.bits)
}

func (o sumObs) floatVal() float64 {
	return math.Float64frombits(o.bits)
}

const (
	flagInitEmitted uint8 = 1 << iota
	flagHasEvicted
	flagDeleted
)

// retractionState tracks bounded, timestamp-ordered observations for a single Sum stream.
// points is field 0 so Go GC ptrdata is minimal (8 bytes); total struct size is 48 bytes
// on 64-bit platforms (same size class as legacy state).
type retractionState struct {
	points      []sumObs
	mu          sync.Mutex
	initStartTS pcommon.Timestamp
	flags       uint8
}

func (s *retractionState) initEmitted() bool { return s.flags&flagInitEmitted != 0 }
func (s *retractionState) hasEvicted() bool  { return s.flags&flagHasEvicted != 0 }
func (s *retractionState) deleted() bool     { return s.flags&flagDeleted != 0 }

func (s *retractionState) setInitEmitted(v bool) {
	if v {
		s.flags |= flagInitEmitted
	} else {
		s.flags &^= flagInitEmitted
	}
}

func normFloat(v float64) float64 {
	if v == 0 {
		return 0
	}
	return v
}

func intervalDeltaInt(prev, curr int64) int64 {
	if curr >= prev {
		return curr - prev
	}
	return curr
}

func intervalDeltaFloat(prev, curr float64) float64 {
	if curr >= prev {
		return normFloat(curr - prev)
	}
	return normFloat(curr)
}

// cmpDiffInt returns the exact sign (-1, 0, +1) and non-negative magnitude of (big - base) - other
// for non-negative int64 values with 0 <= base <= big and other >= 0.
// Cannot overflow signed int64 because 0 <= big - base <= math.MaxInt64.
func cmpDiffInt(big, base, other int64) (int, int64) {
	d := big - base
	if d > other {
		return 1, d - other
	}
	if d < other {
		return -1, other - d
	}
	return 0, 0
}

// cmpDiffFloat returns the exact sign (-1, 0, +1) and non-negative magnitude of (big - base) - other
// for finite float64 values satisfying 0 <= other < base <= big.
// Uses big*0.5 > base (never overflows MaxFloat64) and Sterbenz's Lemma when base <= big <= 2*base.
func cmpDiffFloat(big, base, other float64) (int, float64) {
	if big*0.5 > base {
		return 1, normFloat((big - base) - other)
	}
	// Sterbenz's Lemma: for base <= big <= 2*base, (big - base) is bit-exact in binary64.
	// Subnormal inputs also subtract exactly as integer multiples of 2^-1074.
	d := big - base
	if d > other {
		return 1, normFloat(d - other)
	}
	if d < other {
		return -1, normFloat(other - d)
	}
	return 0, 0
}

func (t *MetricTracker) shouldEmitInitial(startTS, obsTS pcommon.Timestamp) (emit bool, emitStartTS pcommon.Timestamp) {
	switch t.initialValue {
	case InitialValueKeep:
		if startTS <= obsTS {
			return true, startTS
		}
		return true, 0
	case InitialValueAuto:
		if startTS >= t.startTime && obsTS > startTS {
			return true, startTS
		}
		return false, 0
	case InitialValueDrop:
		return false, 0
	default:
		return false, 0
	}
}

func emitInt(dst []SumDelta, start, end pcommon.Timestamp, val int64, isRetraction, synthetic bool) []SumDelta {
	return append(dst, SumDelta{
		StartTimestamp: start,
		Timestamp:      end,
		IntValue:       val,
		IsFloat:        false,
		IsRetraction:   isRetraction,
		Synthetic:      synthetic,
	})
}

func emitFloat(dst []SumDelta, start, end pcommon.Timestamp, val float64, isRetraction, synthetic bool) []SumDelta {
	return append(dst, SumDelta{
		StartTimestamp: start,
		Timestamp:      end,
		FloatValue:     normFloat(val),
		IsFloat:        true,
		IsRetraction:   isRetraction,
		Synthetic:      synthetic,
	})
}

// ConvertSum converts a single monotonic cumulative Sum datapoint using bounded
// timestamp-ordered history and the configured RetractionMode.
// Callers may supply a small stack-allocated slice (e.g. buf[:0], cap >= 4)
// to avoid heap allocation on every call; the returned slice aliases outBuf and
// must not be retained across subsequent calls reusing the same buffer.
func (t *MetricTracker) ConvertSum(in MetricPoint, outBuf []SumDelta) (out []SumDelta, outcome Outcome, reason string) {
	out = outBuf[:0]
	mode := t.retractionCfg.Mode
	if mode != RetractionModeZSet && mode != RetractionModeZSetCompact && mode != RetractionModeNonNegative {
		return out, OutcomeDropped, ""
	}
	metricID := in.Identity
	mp := in.Value
	if metricID.MetricType != pmetric.MetricTypeSum {
		return out, OutcomeDropped, ""
	}

	isFloat := metricID.IsFloatVal()
	var incoming sumObs
	if isFloat {
		v := mp.FloatValue
		if math.IsNaN(v) {
			return out, OutcomeDropped, ""
		}
		if math.IsInf(v, 0) || v < 0 {
			return out, OutcomeDropped, ReasonInvalidValue
		}
		incoming = obsFromFloat(mp.ObservedTimestamp, v)
	} else {
		if mp.IntValue < 0 {
			return out, OutcomeDropped, ReasonInvalidValue
		}
		incoming = obsFromInt(mp.ObservedTimestamp, mp.IntValue)
	}

	b := identityBufferPool.Get().(*bytes.Buffer)
	b.Reset()
	metricID.Write(b)
	hashableID := b.String()
	identityBufferPool.Put(b)

	for {
		raw, loaded := t.retractionStates.Load(hashableID)
		if !loaded {
			emit, initStart := t.shouldEmitInitial(metricID.StartTimestamp, incoming.ts)
			initCap := 8
			if maxPts := t.retractionCfg.MaxHistoryPoints; maxPts > 0 && maxPts < initCap {
				initCap = maxPts
			}
			newS := &retractionState{
				points:      make([]sumObs, 1, initCap),
				initStartTS: initStart,
			}
			newS.points[0] = incoming
			newS.setInitEmitted(emit)

			// Lock unshared newS before publishing so removeStale cannot delete
			// and decrement t.streams before t.streams.Add(1) runs.
			newS.mu.Lock()
			raw, loaded = t.retractionStates.LoadOrStore(hashableID, newS)
			if !loaded {
				t.streams.Add(1)
				newS.mu.Unlock()
				if emit {
					if isFloat {
						out = emitFloat(out, initStart, incoming.ts, incoming.floatVal(), false, false)
					} else {
						out = emitInt(out, initStart, incoming.ts, incoming.intVal(), false, false)
					}
					return out, OutcomeEmitted, ""
				}
				return out, OutcomeDropped, ReasonInitial
			}
			newS.mu.Unlock()
		}

		s := raw.(*retractionState)
		s.mu.Lock()
		if s.deleted() {
			s.mu.Unlock()
			continue
		}

		out, outcome, reason = t.processLocked(s, metricID.StartTimestamp, incoming, isFloat, mode, out)
		s.mu.Unlock()
		return out, outcome, reason
	}
}

func (t *MetricTracker) processLocked(
	s *retractionState,
	metricStartTS pcommon.Timestamp,
	incoming sumObs,
	isFloat bool,
	mode RetractionMode,
	out []SumDelta,
) ([]SumDelta, Outcome, string) {
	n := len(s.points)
	last := s.points[n-1]

	// Fast path: strictly increasing timestamp (tail append, 85%-95% of traffic).
	if incoming.ts > last.ts {
		if isFloat {
			d := intervalDeltaFloat(last.floatVal(), incoming.floatVal())
			out = emitFloat(out, last.ts, incoming.ts, d, false, false)
		} else {
			d := intervalDeltaInt(last.intVal(), incoming.intVal())
			out = emitInt(out, last.ts, incoming.ts, d, false, false)
		}
		t.insertAndCompactLocked(s, n, incoming)
		return out, OutcomeEmitted, ""
	}

	// Binary search for incoming.ts in s.points.
	idx := sort.Search(n, func(i int) bool {
		return s.points[i].ts >= incoming.ts
	})

	// Case 1: Exact timestamp match (duplicate or revision).
	if idx < n && s.points[idx].ts == incoming.ts {
		existing := s.points[idx]
		if existing.bits == incoming.bits {
			return out, OutcomeDropped, ReasonDuplicate
		}
		if idx == 0 && s.hasEvicted() {
			return out, OutcomeDropped, ReasonWindowExceeded
		}
		if isFloat {
			return t.reviseFloatLocked(s, idx, incoming, mode, out)
		}
		return t.reviseIntLocked(s, idx, incoming, mode, out)
	}

	// Case 2: Strictly older than earliest retained point.
	if idx == 0 {
		if s.hasEvicted() {
			return out, OutcomeDropped, ReasonWindowExceeded
		}
		if isFloat {
			return t.prependFloatLocked(s, metricStartTS, incoming, mode, out)
		}
		return t.prependIntLocked(s, metricStartTS, incoming, mode, out)
	}

	// Case 3: Mid-window insertion between s.points[idx-1] and s.points[idx].
	if isFloat {
		out = t.insertMidFloatLocked(s, idx, incoming, mode, out)
	} else {
		out = t.insertMidIntLocked(s, idx, incoming, mode, out)
	}
	t.insertAndCompactLocked(s, idx, incoming)
	if len(out) == 0 {
		return out, OutcomeAbsorbed, ""
	}
	return out, OutcomeEmitted, ""
}

// insertAndCompactLocked inserts x at sorted position insertIdx and compacts any prefix
// outside reorder_window (retaining the single newest anchor < cutoff) or exceeding
// max_history_points, using an in-place shift when drop >= 1 so cap(s.points) stays tight.
func (t *MetricTracker) insertAndCompactLocked(s *retractionState, insertIdx int, x sumObs) {
	n := len(s.points)
	newN := n + 1

	atTS := func(k int) pcommon.Timestamp {
		if k < insertIdx {
			return s.points[k].ts
		}
		if k == insertIdx {
			return x.ts
		}
		return s.points[k-1].ts
	}

	maxTS := atTS(newN - 1)
	dropTime := 0
	if win := t.retractionCfg.ReorderWindow; win > 0 && maxTS > pcommon.Timestamp(win) {
		cutoff := maxTS - pcommon.Timestamp(win)
		firstGE := sort.Search(newN, func(k int) bool {
			return atTS(k) >= cutoff
		})
		if firstGE >= 2 {
			dropTime = firstGE - 1
		}
	}

	dropCap := 0
	if maxPts := t.retractionCfg.MaxHistoryPoints; maxPts > 0 && newN > maxPts {
		dropCap = newN - maxPts
	}

	drop := max(dropTime, dropCap)
	if drop == 0 {
		// Ensure capacity without overshooting MaxHistoryPoints.
		// Step by +8 once cap >= 16 so steady-state windows just above a power of two
		// (e.g. W=17 at 2s scrape / 30s window) allocate 24 slots (384 B) instead of 32 (512 B).
		if len(s.points) == cap(s.points) {
			var nextCap int
			if cap(s.points) < 16 {
				nextCap = max(cap(s.points)*2, 4)
			} else {
				nextCap = cap(s.points) + 8
			}
			if maxPts := t.retractionCfg.MaxHistoryPoints; maxPts > 0 && nextCap > maxPts {
				nextCap = maxPts
			}
			grown := make([]sumObs, n+1, nextCap)
			copy(grown[:insertIdx], s.points[:insertIdx])
			grown[insertIdx] = x
			copy(grown[insertIdx+1:], s.points[insertIdx:])
			s.points = grown
			return
		}
		s.points = append(s.points, sumObs{})
		copy(s.points[insertIdx+1:], s.points[insertIdx:])
		s.points[insertIdx] = x
		return
	}

	s.flags |= flagHasEvicted
	newLen := newN - drop // Guaranteed <= n <= cap(s.points); zero allocation.
	if insertIdx < drop {
		if drop > 1 {
			copy(s.points[:newLen], s.points[drop-1:])
		}
		s.points = s.points[:newLen]
		return
	}
	p := insertIdx - drop
	copy(s.points[:p], s.points[drop:insertIdx])
	if drop > 1 {
		copy(s.points[p+1:newLen], s.points[insertIdx:])
	}
	s.points[p] = x
	s.points = s.points[:newLen]
}

// --- Mid-window insertions ---

func (t *MetricTracker) insertMidIntLocked(s *retractionState, idx int, incoming sumObs, mode RetractionMode, out []SumDelta) []SumDelta {
	left := s.points[idx-1]
	right := s.points[idx]
	a, x, b := left.intVal(), incoming.intVal(), right.intVal()
	dOld := intervalDeltaInt(a, b)
	d1 := intervalDeltaInt(a, x)
	d2 := intervalDeltaInt(x, b)

	if mode == RetractionModeZSet {
		if dOld != 0 {
			out = emitInt(out, left.ts, right.ts, -dOld, true, true)
		}
		out = emitInt(out, left.ts, incoming.ts, d1, false, false)
		out = emitInt(out, incoming.ts, right.ts, d2, false, true)
		return out
	}

	// Exact structural 5-case partition (zero overflow, identical for compact & non_negative on resets).
	if (a <= x && x <= b) || (b < a && x <= b) {
		// Case 1 & 2: net == 0.
		if mode == RetractionModeNonNegative || d1 == 0 {
			return out
		}
		out = emitInt(out, left.ts, incoming.ts, d1, false, false)
		out = emitInt(out, left.ts, right.ts, -d1, true, true)
		return out
	}
	if b < a {
		// Case 3: b < a && x > b -> d2 == dOld == b, adj(t_b) == 0.
		out = emitInt(out, left.ts, incoming.ts, d1, false, false)
		return out
	}
	if x > b {
		// Case 4: a <= b < x -> d1 = x - a, adj(t_b) = +a.
		out = emitInt(out, left.ts, incoming.ts, d1, false, false)
		if a > 0 {
			out = emitInt(out, incoming.ts, right.ts, a, false, true)
		}
		return out
	}
	// Case 5: x < a <= b -> d1 = x, adj(t_b) = +(a - x) > 0.
	out = emitInt(out, left.ts, incoming.ts, d1, false, false)
	out = emitInt(out, incoming.ts, right.ts, a-x, false, true)
	return out
}

func (t *MetricTracker) insertMidFloatLocked(s *retractionState, idx int, incoming sumObs, mode RetractionMode, out []SumDelta) []SumDelta {
	left := s.points[idx-1]
	right := s.points[idx]
	a, x, b := left.floatVal(), incoming.floatVal(), right.floatVal()
	dOld := intervalDeltaFloat(a, b)
	d1 := intervalDeltaFloat(a, x)
	d2 := intervalDeltaFloat(x, b)

	if mode == RetractionModeZSet {
		if dOld != 0 {
			out = emitFloat(out, left.ts, right.ts, -dOld, true, true)
		}
		out = emitFloat(out, left.ts, incoming.ts, d1, false, false)
		out = emitFloat(out, incoming.ts, right.ts, d2, false, true)
		return out
	}

	if (a <= x && x <= b) || (b < a && x <= b) {
		if mode == RetractionModeNonNegative || d1 == 0 {
			return out
		}
		out = emitFloat(out, left.ts, incoming.ts, d1, false, false)
		out = emitFloat(out, left.ts, right.ts, -d1, true, true)
		return out
	}
	if b < a {
		out = emitFloat(out, left.ts, incoming.ts, d1, false, false)
		return out
	}
	if x > b {
		out = emitFloat(out, left.ts, incoming.ts, d1, false, false)
		if a > 0 {
			out = emitFloat(out, incoming.ts, right.ts, a, false, true)
		}
		return out
	}
	out = emitFloat(out, left.ts, incoming.ts, d1, false, false)
	out = emitFloat(out, incoming.ts, right.ts, a-x, false, true)
	return out
}

// --- Head prepends (idx == 0, !hasEvicted) ---

func (t *MetricTracker) prependIntLocked(s *retractionState, metricStartTS pcommon.Timestamp, incoming sumObs, mode RetractionMode, out []SumDelta) ([]SumDelta, Outcome, string) {
	h0 := s.points[0]
	vx, v0 := incoming.intVal(), h0.intVal()
	oldInitEmit := s.initEmitted()
	oldStart := s.initStartTS
	newInitEmit, newStart := t.shouldEmitInitial(metricStartTS, incoming.ts)
	d1 := intervalDeltaInt(vx, v0)

	switch mode {
	case RetractionModeZSet:
		if oldInitEmit && v0 != 0 {
			out = emitInt(out, oldStart, h0.ts, -v0, true, true)
		}
		if newInitEmit {
			out = emitInt(out, newStart, incoming.ts, vx, false, false)
		}
		out = emitInt(out, incoming.ts, h0.ts, d1, false, true)
	case RetractionModeZSetCompact:
		if newInitEmit && vx != 0 {
			out = emitInt(out, newStart, incoming.ts, vx, false, false)
		}
		if !oldInitEmit {
			if d1 != 0 {
				out = emitInt(out, incoming.ts, h0.ts, d1, false, true)
			}
		} else if vx <= v0 {
			// adj = (v0 - vx) - v0 = -vx (exact).
			if vx != 0 {
				out = emitInt(out, oldStart, h0.ts, -vx, true, true)
			}
		}
		// else vx > v0: reset d1 == v0 == oldInit -> adj == 0.
	case RetractionModeNonNegative:
		// Strictly keyed on the 3 reachable (oldInitEmit, newInitEmit) pairs.
		// (false, true) is impossible when incoming.ts < h0.ts under identical metricStartTS.
		switch {
		case !oldInitEmit && !newInitEmit:
			out = emitInt(out, incoming.ts, h0.ts, d1, false, true)
		case oldInitEmit && newInitEmit:
			if vx > v0 {
				out = emitInt(out, newStart, incoming.ts, vx, false, false)
			}
		case oldInitEmit && !newInitEmit:
			// Auto flip for incoming.ts <= metricStartTS.
			if vx <= v0 && vx > 0 {
				return out, OutcomeDropped, ReasonRejectedRevision
			}
		}
	}

	s.setInitEmitted(newInitEmit)
	s.initStartTS = newStart
	t.insertAndCompactLocked(s, 0, incoming)
	if len(out) == 0 {
		return out, OutcomeAbsorbed, ""
	}
	return out, OutcomeEmitted, ""
}

func (t *MetricTracker) prependFloatLocked(s *retractionState, metricStartTS pcommon.Timestamp, incoming sumObs, mode RetractionMode, out []SumDelta) ([]SumDelta, Outcome, string) {
	h0 := s.points[0]
	vx, v0 := incoming.floatVal(), h0.floatVal()
	oldInitEmit := s.initEmitted()
	oldStart := s.initStartTS
	newInitEmit, newStart := t.shouldEmitInitial(metricStartTS, incoming.ts)
	d1 := intervalDeltaFloat(vx, v0)

	switch mode {
	case RetractionModeZSet:
		if oldInitEmit && v0 != 0 {
			out = emitFloat(out, oldStart, h0.ts, -v0, true, true)
		}
		if newInitEmit {
			out = emitFloat(out, newStart, incoming.ts, vx, false, false)
		}
		out = emitFloat(out, incoming.ts, h0.ts, d1, false, true)
	case RetractionModeZSetCompact:
		if newInitEmit && vx != 0 {
			out = emitFloat(out, newStart, incoming.ts, vx, false, false)
		}
		if !oldInitEmit {
			if d1 != 0 {
				out = emitFloat(out, incoming.ts, h0.ts, d1, false, true)
			}
		} else if vx <= v0 {
			if vx != 0 {
				out = emitFloat(out, oldStart, h0.ts, -vx, true, true)
			}
		}
	case RetractionModeNonNegative:
		switch {
		case !oldInitEmit && !newInitEmit:
			out = emitFloat(out, incoming.ts, h0.ts, d1, false, true)
		case oldInitEmit && newInitEmit:
			if vx > v0 {
				out = emitFloat(out, newStart, incoming.ts, vx, false, false)
			}
		case oldInitEmit && !newInitEmit:
			if vx <= v0 && vx > 0 {
				return out, OutcomeDropped, ReasonRejectedRevision
			}
		}
	}

	s.setInitEmitted(newInitEmit)
	s.initStartTS = newStart
	t.insertAndCompactLocked(s, 0, incoming)
	if len(out) == 0 {
		return out, OutcomeAbsorbed, ""
	}
	return out, OutcomeEmitted, ""
}

// --- Same-timestamp revisions ---

func (t *MetricTracker) reviseIntLocked(s *retractionState, idx int, incoming sumObs, mode RetractionMode, out []SumDelta) ([]SumDelta, Outcome, string) {
	n := len(s.points)
	oldObs := s.points[idx]
	oldVal, newVal := oldObs.intVal(), incoming.intVal()

	// Head revision (idx == 0, !hasEvicted already verified).
	if idx == 0 {
		if n == 1 {
			if !s.initEmitted() {
				s.points[0] = incoming
				return out, OutcomeAbsorbed, ""
			}
			switch mode {
			case RetractionModeZSet:
				if oldVal != 0 {
					out = emitInt(out, s.initStartTS, oldObs.ts, -oldVal, true, true)
				}
				out = emitInt(out, s.initStartTS, oldObs.ts, newVal, false, false)
			case RetractionModeZSetCompact:
				diff := newVal - oldVal
				out = emitInt(out, s.initStartTS, oldObs.ts, diff, diff < 0, diff < 0)
			case RetractionModeNonNegative:
				if newVal < oldVal {
					return out, OutcomeDropped, ReasonRejectedRevision
				}
				out = emitInt(out, s.initStartTS, oldObs.ts, newVal-oldVal, false, false)
			}
			s.points[0] = incoming
			return out, OutcomeEmitted, ""
		}

		// Head revision with right neighbor H[1].
		right := s.points[1]
		v1 := right.intVal()
		oldR := intervalDeltaInt(oldVal, v1)
		newR := intervalDeltaInt(newVal, v1)

		switch mode {
		case RetractionModeZSet:
			if s.initEmitted() {
				if oldVal != 0 {
					out = emitInt(out, s.initStartTS, oldObs.ts, -oldVal, true, true)
				}
				out = emitInt(out, s.initStartTS, oldObs.ts, newVal, false, false)
			}
			if oldR != newR {
				if oldR != 0 {
					out = emitInt(out, oldObs.ts, right.ts, -oldR, true, true)
				}
				out = emitInt(out, oldObs.ts, right.ts, newR, false, true)
			}
		case RetractionModeZSetCompact:
			if s.initEmitted() {
				diff := newVal - oldVal
				out = emitInt(out, s.initStartTS, oldObs.ts, diff, diff < 0, diff < 0)
			}
			if newR != oldR {
				diffR := newR - oldR
				out = emitInt(out, oldObs.ts, right.ts, diffR, diffR < 0, true)
			}
		case RetractionModeNonNegative:
			if s.initEmitted() {
				// v1 cancels: net = ([v1 < new]*new) - ([v1 < old]*old).
				switch {
				case oldVal <= v1 && newVal <= v1:
					// net == 0.
				case oldVal <= v1 && newVal > v1:
					out = emitInt(out, s.initStartTS, oldObs.ts, newVal-oldVal, false, false)
					if oldVal > 0 {
						out = emitInt(out, oldObs.ts, right.ts, oldVal, false, true)
					}
				case oldVal > v1 && newVal <= v1:
					return out, OutcomeDropped, ReasonRejectedRevision
				default: // oldVal > v1 && newVal > v1
					if newVal < oldVal {
						return out, OutcomeDropped, ReasonRejectedRevision
					}
					out = emitInt(out, s.initStartTS, oldObs.ts, newVal-oldVal, false, false)
				}
			} else {
				// !initEmitted: net = intervalDelta(new, v1) - intervalDelta(old, v1).
				switch {
				case oldVal <= v1 && newVal <= v1:
					if newVal > oldVal {
						return out, OutcomeDropped, ReasonRejectedRevision
					}
					out = emitInt(out, oldObs.ts, right.ts, oldVal-newVal, false, true)
				case oldVal <= v1 && newVal > v1:
					if oldVal > 0 {
						out = emitInt(out, oldObs.ts, right.ts, oldVal, false, true)
					}
				case oldVal > v1 && newVal <= v1:
					if newVal > 0 {
						return out, OutcomeDropped, ReasonRejectedRevision
					}
				default: // oldVal > v1 && newVal > v1 -> net == 0.
				}
			}
		}
		s.points[0] = incoming
		if len(out) == 0 {
			return out, OutcomeAbsorbed, ""
		}
		return out, OutcomeEmitted, ""
	}

	// Tail revision (idx == n-1, n > 1).
	if idx == n-1 {
		prev := s.points[idx-1]
		a := prev.intVal()
		oldL := intervalDeltaInt(a, oldVal)
		newL := intervalDeltaInt(a, newVal)

		switch mode {
		case RetractionModeZSet:
			if oldL != newL {
				if oldL != 0 {
					out = emitInt(out, prev.ts, oldObs.ts, -oldL, true, true)
				}
				out = emitInt(out, prev.ts, oldObs.ts, newL, false, false)
			}
		case RetractionModeZSetCompact:
			if newL != oldL {
				diff := newL - oldL
				out = emitInt(out, prev.ts, oldObs.ts, diff, diff < 0, diff < 0)
			}
		case RetractionModeNonNegative:
			switch {
			case (oldVal >= a) == (newVal >= a):
				if newVal < oldVal {
					return out, OutcomeDropped, ReasonRejectedRevision
				}
				out = emitInt(out, prev.ts, oldObs.ts, newVal-oldVal, false, false)
			case oldVal < a: // newVal >= a
				sgn, mag := cmpDiffInt(newVal, a, oldVal)
				if sgn < 0 {
					return out, OutcomeDropped, ReasonRejectedRevision
				}
				if sgn > 0 {
					out = emitInt(out, prev.ts, oldObs.ts, mag, false, false)
				}
			default: // oldVal >= a && newVal < a
				sgn, mag := cmpDiffInt(oldVal, a, newVal)
				if sgn > 0 {
					return out, OutcomeDropped, ReasonRejectedRevision
				}
				if sgn < 0 {
					out = emitInt(out, prev.ts, oldObs.ts, mag, false, false)
				}
			}
		}
		s.points[idx] = incoming
		if len(out) == 0 {
			return out, OutcomeAbsorbed, ""
		}
		return out, OutcomeEmitted, ""
	}

	// Interior revision (0 < idx < n-1).
	left := s.points[idx-1]
	right := s.points[idx+1]
	a, b := left.intVal(), right.intVal()
	oldL := intervalDeltaInt(a, oldVal)
	newL := intervalDeltaInt(a, newVal)
	oldR := intervalDeltaInt(oldVal, b)
	newR := intervalDeltaInt(newVal, b)

	switch mode {
	case RetractionModeZSet:
		if oldL != newL {
			if oldL != 0 {
				out = emitInt(out, left.ts, oldObs.ts, -oldL, true, true)
			}
			out = emitInt(out, left.ts, oldObs.ts, newL, false, false)
		}
		if oldR != newR {
			if oldR != 0 {
				out = emitInt(out, oldObs.ts, right.ts, -oldR, true, true)
			}
			out = emitInt(out, oldObs.ts, right.ts, newR, false, true)
		}
	case RetractionModeZSetCompact:
		if newL != oldL {
			diffL := newL - oldL
			out = emitInt(out, left.ts, oldObs.ts, diffL, diffL < 0, diffL < 0)
		}
		if newR != oldR {
			diffR := newR - oldR
			out = emitInt(out, oldObs.ts, right.ts, diffR, diffR < 0, true)
		}
	case RetractionModeNonNegative:
		oA, oB := oldVal < a, b < oldVal
		nA, nB := newVal < a, b < newVal

		var sgn int
		var mag int64
		switch {
		case oA == nA:
			switch {
			case !nB && !oB:
				sgn = 0
			case nB && !oB:
				sgn, mag = 1, newVal
			case !nB && oB:
				sgn = -1
			default: // nB && oB
				if newVal > oldVal {
					sgn, mag = 1, newVal-oldVal
				} else {
					sgn = -1
				}
			}
		case nA && !oA: // new < a <= old
			switch {
			case !nB && !oB:
				sgn, mag = 1, a
			case !nB && oB:
				if oldVal == a {
					sgn = 0
				} else {
					sgn = -1
				}
			default: // nB && oB: b < new < a <= old
				cs, cm := cmpDiffInt(oldVal, a, newVal)
				// net = (a + new) - old = new - (old - a) = -((old - a) - new).
				sgn, mag = -cs, cm
			}
		default: // !nA && oA: old < a <= new
			switch {
			case !nB && !oB:
				sgn = -1
			case nB && !oB:
				if newVal == a {
					sgn = 0
				} else {
					sgn, mag = 1, newVal-a
				}
			default: // nB && oB: b < old < a <= new
				sgn, mag = cmpDiffInt(newVal, a, oldVal)
			}
		}

		if sgn < 0 {
			return out, OutcomeDropped, ReasonRejectedRevision
		}
		if sgn > 0 {
			// Pure-structural 4-branch positive-edge emission (zero rounding/overflow error).
			switch {
			case oB && nB:
				// dr == 0 across all 4 (oA, nA) cases when both > b.
				out = emitInt(out, left.ts, oldObs.ts, mag, false, false)
			case oA == nA && !oB && nB:
				// old <= b < new; new - old > 0, (new - old) + old == new == mag.
				out = emitInt(out, left.ts, oldObs.ts, newVal-oldVal, false, false)
				if oldVal > 0 {
					out = emitInt(out, oldObs.ts, right.ts, oldVal, false, true)
				}
			case nA && !oA && !nB && !oB:
				// new < a <= old <= b, mag == a.
				cs, dmag := cmpDiffInt(oldVal, a, newVal)
				if cs < 0 {
					out = emitInt(out, left.ts, oldObs.ts, dmag, false, false)
					out = emitInt(out, oldObs.ts, right.ts, oldVal-newVal, false, true)
				} else {
					out = emitInt(out, oldObs.ts, right.ts, a, false, true)
				}
			default:
				// !nA && oA && nB && !oB: old < a <= new, old <= b < new, mag == new - a > 0.
				cs, dmag := cmpDiffInt(newVal, a, oldVal)
				if cs > 0 {
					out = emitInt(out, left.ts, oldObs.ts, dmag, false, false)
					if oldVal > 0 {
						out = emitInt(out, oldObs.ts, right.ts, oldVal, false, true)
					}
				} else {
					out = emitInt(out, oldObs.ts, right.ts, newVal-a, false, true)
				}
			}
		}
	}

	s.points[idx] = incoming
	if len(out) == 0 {
		return out, OutcomeAbsorbed, ""
	}
	return out, OutcomeEmitted, ""
}

func (t *MetricTracker) reviseFloatLocked(s *retractionState, idx int, incoming sumObs, mode RetractionMode, out []SumDelta) ([]SumDelta, Outcome, string) {
	n := len(s.points)
	oldObs := s.points[idx]
	oldVal, newVal := oldObs.floatVal(), incoming.floatVal()

	// Head revision (idx == 0, !hasEvicted already verified).
	if idx == 0 {
		if n == 1 {
			if !s.initEmitted() {
				s.points[0] = incoming
				return out, OutcomeAbsorbed, ""
			}
			switch mode {
			case RetractionModeZSet:
				if oldVal != 0 {
					out = emitFloat(out, s.initStartTS, oldObs.ts, -oldVal, true, true)
				}
				out = emitFloat(out, s.initStartTS, oldObs.ts, newVal, false, false)
			case RetractionModeZSetCompact:
				diff := normFloat(newVal - oldVal)
				out = emitFloat(out, s.initStartTS, oldObs.ts, diff, diff < 0, diff < 0)
			case RetractionModeNonNegative:
				if newVal < oldVal {
					return out, OutcomeDropped, ReasonRejectedRevision
				}
				out = emitFloat(out, s.initStartTS, oldObs.ts, newVal-oldVal, false, false)
			}
			s.points[0] = incoming
			return out, OutcomeEmitted, ""
		}

		// Head revision with right neighbor H[1].
		right := s.points[1]
		v1 := right.floatVal()
		oldR := intervalDeltaFloat(oldVal, v1)
		newR := intervalDeltaFloat(newVal, v1)

		switch mode {
		case RetractionModeZSet:
			if s.initEmitted() {
				if oldVal != 0 {
					out = emitFloat(out, s.initStartTS, oldObs.ts, -oldVal, true, true)
				}
				out = emitFloat(out, s.initStartTS, oldObs.ts, newVal, false, false)
			}
			if oldR != newR {
				if oldR != 0 {
					out = emitFloat(out, oldObs.ts, right.ts, -oldR, true, true)
				}
				out = emitFloat(out, oldObs.ts, right.ts, newR, false, true)
			}
		case RetractionModeZSetCompact:
			if s.initEmitted() {
				diff := normFloat(newVal - oldVal)
				out = emitFloat(out, s.initStartTS, oldObs.ts, diff, diff < 0, diff < 0)
			}
			if newR != oldR {
				diffR := normFloat(newR - oldR)
				out = emitFloat(out, oldObs.ts, right.ts, diffR, diffR < 0, true)
			}
		case RetractionModeNonNegative:
			if s.initEmitted() {
				switch {
				case oldVal <= v1 && newVal <= v1:
					// net == 0.
				case oldVal <= v1 && newVal > v1:
					out = emitFloat(out, s.initStartTS, oldObs.ts, newVal-oldVal, false, false)
					if oldVal > 0 {
						out = emitFloat(out, oldObs.ts, right.ts, oldVal, false, true)
					}
				case oldVal > v1 && newVal <= v1:
					return out, OutcomeDropped, ReasonRejectedRevision
				default: // oldVal > v1 && newVal > v1
					if newVal < oldVal {
						return out, OutcomeDropped, ReasonRejectedRevision
					}
					out = emitFloat(out, s.initStartTS, oldObs.ts, newVal-oldVal, false, false)
				}
			} else {
				switch {
				case oldVal <= v1 && newVal <= v1:
					if newVal > oldVal {
						return out, OutcomeDropped, ReasonRejectedRevision
					}
					out = emitFloat(out, oldObs.ts, right.ts, oldVal-newVal, false, true)
				case oldVal <= v1 && newVal > v1:
					if oldVal > 0 {
						out = emitFloat(out, oldObs.ts, right.ts, oldVal, false, true)
					}
				case oldVal > v1 && newVal <= v1:
					if newVal > 0 {
						return out, OutcomeDropped, ReasonRejectedRevision
					}
				default:
				}
			}
		}
		s.points[0] = incoming
		if len(out) == 0 {
			return out, OutcomeAbsorbed, ""
		}
		return out, OutcomeEmitted, ""
	}

	// Tail revision (idx == n-1, n > 1).
	if idx == n-1 {
		prev := s.points[idx-1]
		a := prev.floatVal()
		oldL := intervalDeltaFloat(a, oldVal)
		newL := intervalDeltaFloat(a, newVal)

		switch mode {
		case RetractionModeZSet:
			if oldL != newL {
				if oldL != 0 {
					out = emitFloat(out, prev.ts, oldObs.ts, -oldL, true, true)
				}
				out = emitFloat(out, prev.ts, oldObs.ts, newL, false, false)
			}
		case RetractionModeZSetCompact:
			if newL != oldL {
				diff := normFloat(newL - oldL)
				out = emitFloat(out, prev.ts, oldObs.ts, diff, diff < 0, diff < 0)
			}
		case RetractionModeNonNegative:
			switch {
			case (oldVal >= a) == (newVal >= a):
				if newVal < oldVal {
					return out, OutcomeDropped, ReasonRejectedRevision
				}
				out = emitFloat(out, prev.ts, oldObs.ts, newVal-oldVal, false, false)
			case oldVal < a: // newVal >= a
				sgn, mag := cmpDiffFloat(newVal, a, oldVal)
				if sgn < 0 {
					return out, OutcomeDropped, ReasonRejectedRevision
				}
				if sgn > 0 {
					out = emitFloat(out, prev.ts, oldObs.ts, mag, false, false)
				}
			default: // oldVal >= a && newVal < a
				sgn, mag := cmpDiffFloat(oldVal, a, newVal)
				if sgn > 0 {
					return out, OutcomeDropped, ReasonRejectedRevision
				}
				if sgn < 0 {
					out = emitFloat(out, prev.ts, oldObs.ts, mag, false, false)
				}
			}
		}
		s.points[idx] = incoming
		if len(out) == 0 {
			return out, OutcomeAbsorbed, ""
		}
		return out, OutcomeEmitted, ""
	}

	// Interior revision (0 < idx < n-1).
	left := s.points[idx-1]
	right := s.points[idx+1]
	a, b := left.floatVal(), right.floatVal()
	oldL := intervalDeltaFloat(a, oldVal)
	newL := intervalDeltaFloat(a, newVal)
	oldR := intervalDeltaFloat(oldVal, b)
	newR := intervalDeltaFloat(newVal, b)

	switch mode {
	case RetractionModeZSet:
		if oldL != newL {
			if oldL != 0 {
				out = emitFloat(out, left.ts, oldObs.ts, -oldL, true, true)
			}
			out = emitFloat(out, left.ts, oldObs.ts, newL, false, false)
		}
		if oldR != newR {
			if oldR != 0 {
				out = emitFloat(out, oldObs.ts, right.ts, -oldR, true, true)
			}
			out = emitFloat(out, oldObs.ts, right.ts, newR, false, true)
		}
	case RetractionModeZSetCompact:
		if newL != oldL {
			diffL := normFloat(newL - oldL)
			out = emitFloat(out, left.ts, oldObs.ts, diffL, diffL < 0, diffL < 0)
		}
		if newR != oldR {
			diffR := normFloat(newR - oldR)
			out = emitFloat(out, oldObs.ts, right.ts, diffR, diffR < 0, true)
		}
	case RetractionModeNonNegative:
		oA, oB := oldVal < a, b < oldVal
		nA, nB := newVal < a, b < newVal

		var sgn int
		var mag float64
		switch {
		case oA == nA:
			switch {
			case !nB && !oB:
				sgn = 0
			case nB && !oB:
				sgn, mag = 1, newVal
			case !nB && oB:
				sgn = -1
			default: // nB && oB
				if newVal > oldVal {
					sgn, mag = 1, normFloat(newVal-oldVal)
				} else {
					sgn = -1
				}
			}
		case nA && !oA: // new < a <= old
			switch {
			case !nB && !oB:
				sgn, mag = 1, a
			case !nB && oB:
				if oldVal == a {
					sgn = 0
				} else {
					sgn = -1
				}
			default: // nB && oB: b < new < a <= old
				cs, cm := cmpDiffFloat(oldVal, a, newVal)
				sgn, mag = -cs, cm
			}
		default: // !nA && oA: old < a <= new
			switch {
			case !nB && !oB:
				sgn = -1
			case nB && !oB:
				if newVal == a {
					sgn = 0
				} else {
					sgn, mag = 1, normFloat(newVal-a)
				}
			default: // nB && oB: b < old < a <= new
				sgn, mag = cmpDiffFloat(newVal, a, oldVal)
			}
		}

		if sgn < 0 {
			return out, OutcomeDropped, ReasonRejectedRevision
		}
		if sgn > 0 {
			switch {
			case oB && nB:
				out = emitFloat(out, left.ts, oldObs.ts, mag, false, false)
			case oA == nA && !oB && nB:
				out = emitFloat(out, left.ts, oldObs.ts, newVal-oldVal, false, false)
				if oldVal > 0 {
					out = emitFloat(out, oldObs.ts, right.ts, oldVal, false, true)
				}
			case nA && !oA && !nB && !oB:
				cs, dmag := cmpDiffFloat(oldVal, a, newVal)
				if cs < 0 {
					out = emitFloat(out, left.ts, oldObs.ts, dmag, false, false)
					out = emitFloat(out, oldObs.ts, right.ts, oldVal-newVal, false, true)
				} else {
					out = emitFloat(out, oldObs.ts, right.ts, a, false, true)
				}
			default:
				cs, dmag := cmpDiffFloat(newVal, a, oldVal)
				if cs > 0 {
					out = emitFloat(out, left.ts, oldObs.ts, dmag, false, false)
					if oldVal > 0 {
						out = emitFloat(out, oldObs.ts, right.ts, oldVal, false, true)
					}
				} else {
					out = emitFloat(out, oldObs.ts, right.ts, newVal-a, false, true)
				}
			}
		}
	}

	s.points[idx] = incoming
	if len(out) == 0 {
		return out, OutcomeAbsorbed, ""
	}
	return out, OutcomeEmitted, ""
}

// removeStaleRetraction sweeps expired retractionState entries safely without
// spinning: CompareAndDelete runs while holding s.mu (strict lock order s.mu -> sync.Map).
func (t *MetricTracker) removeStaleRetraction(staleBefore pcommon.Timestamp) {
	t.retractionStates.Range(func(key, value any) bool {
		s := value.(*retractionState)
		s.mu.Lock()
		if !s.deleted() && len(s.points) > 0 && s.points[len(s.points)-1].ts < staleBefore {
			s.flags |= flagDeleted
			if t.retractionStates.CompareAndDelete(key, s) {
				t.streams.Add(-1)
				if ce := t.logger.Check(zap.DebugLevel, "removing stale retraction state key"); ce != nil {
					ce.Write(zap.String("key", key.(string)))
				}
			}
		}
		s.mu.Unlock()
		return true
	})
}
