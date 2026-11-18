// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cumulativetodeltaprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor"

import (
	"context"
	"fmt"
	"math"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/filterset"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor/internal/tracking"
)

// metricTypeAttrs and droppedReasonAttrs hold the precomputed attribute sets
// used by the processor's internal telemetry. They are static (independent of
// per-processor configuration) so they live at package scope. Drop reasons
// are surfaced by the tracker (see tracking.Reason*) and must match the
// `reason` attribute enum declared in metadata.yaml.
var (
	metricTypeAttrs = map[pmetric.MetricType]metric.MeasurementOption{
		pmetric.MetricTypeSum:                  metric.WithAttributes(attribute.String("metric_type", "sum")),
		pmetric.MetricTypeHistogram:            metric.WithAttributes(attribute.String("metric_type", "histogram")),
		pmetric.MetricTypeExponentialHistogram: metric.WithAttributes(attribute.String("metric_type", "exponential_histogram")),
	}
	droppedReasonAttrs = map[pmetric.MetricType]map[string]metric.MeasurementOption{
		pmetric.MetricTypeSum: {
			tracking.ReasonReset:            metric.WithAttributes(attribute.String("metric_type", "sum"), attribute.String("reason", tracking.ReasonReset)),
			tracking.ReasonInitial:          metric.WithAttributes(attribute.String("metric_type", "sum"), attribute.String("reason", tracking.ReasonInitial)),
			tracking.ReasonWindowExceeded:   metric.WithAttributes(attribute.String("metric_type", "sum"), attribute.String("reason", tracking.ReasonWindowExceeded)),
			tracking.ReasonDuplicate:        metric.WithAttributes(attribute.String("metric_type", "sum"), attribute.String("reason", tracking.ReasonDuplicate)),
			tracking.ReasonInvalidValue:     metric.WithAttributes(attribute.String("metric_type", "sum"), attribute.String("reason", tracking.ReasonInvalidValue)),
			tracking.ReasonRejectedRevision: metric.WithAttributes(attribute.String("metric_type", "sum"), attribute.String("reason", tracking.ReasonRejectedRevision)),
		},
		pmetric.MetricTypeHistogram: {
			tracking.ReasonReset:          metric.WithAttributes(attribute.String("metric_type", "histogram"), attribute.String("reason", tracking.ReasonReset)),
			tracking.ReasonInitial:        metric.WithAttributes(attribute.String("metric_type", "histogram"), attribute.String("reason", tracking.ReasonInitial)),
			tracking.ReasonBucketMismatch: metric.WithAttributes(attribute.String("metric_type", "histogram"), attribute.String("reason", tracking.ReasonBucketMismatch)),
		},
		pmetric.MetricTypeExponentialHistogram: {
			tracking.ReasonReset:   metric.WithAttributes(attribute.String("metric_type", "exponential_histogram"), attribute.String("reason", tracking.ReasonReset)),
			tracking.ReasonInitial: metric.WithAttributes(attribute.String("metric_type", "exponential_histogram"), attribute.String("reason", tracking.ReasonInitial)),
		},
	}
)

func metricTypeLabel(mt pmetric.MetricType) string {
	switch mt {
	case pmetric.MetricTypeSum:
		return "sum"
	case pmetric.MetricTypeHistogram:
		return "histogram"
	case pmetric.MetricTypeExponentialHistogram:
		return "exponential_histogram"
	default:
		return strings.ToLower(mt.String())
	}
}

type cumulativeToDeltaProcessor struct {
	includeFS          filterset.FilterSet
	excludeFS          filterset.FilterSet
	includeMetricTypes map[pmetric.MetricType]bool
	excludeMetricTypes map[pmetric.MetricType]bool
	logger             *zap.Logger
	deltaCalculator    *tracking.MetricTracker
	cancelFunc         context.CancelFunc
	telemetry          *metadata.TelemetryBuilder
	retractionMode     tracking.RetractionMode
	preserveMonotonic  bool
}

func newCumulativeToDeltaProcessor(config *Config, logger *zap.Logger, telemetry *metadata.TelemetryBuilder) (*cumulativeToDeltaProcessor, error) {
	if !config.RetractionMode.IsValid() {
		return nil, fmt.Errorf("invalid retraction_mode: %s", config.RetractionMode.String())
	}
	if config.ReorderWindow < 0 {
		return nil, fmt.Errorf("reorder_window must be non-negative, got %s", config.ReorderWindow)
	}
	if config.MaxHistoryPoints < 0 || config.MaxHistoryPoints == 1 {
		return nil, fmt.Errorf("max_history_points must be 0 (default) or >= 2, got %d", config.MaxHistoryPoints)
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &cumulativeToDeltaProcessor{
		logger:            logger,
		cancelFunc:        cancel,
		telemetry:         telemetry,
		retractionMode:    config.RetractionMode,
		preserveMonotonic: config.PreserveMonotonic,
	}
	if len(config.Include.Metrics) > 0 {
		includeFS, err := filterset.CreateFilterSet(config.Include.Metrics, &config.Include.Config)
		if err != nil {
			return nil, fmt.Errorf("invalid include filter: %w", err)
		}
		p.includeFS = includeFS
	}
	if len(config.Exclude.Metrics) > 0 {
		excludeFS, err := filterset.CreateFilterSet(config.Exclude.Metrics, &config.Exclude.Config)
		if err != nil {
			return nil, fmt.Errorf("invalid exclude filter: %w", err)
		}
		p.excludeFS = excludeFS
	}

	if len(config.Include.MetricTypes) > 0 {
		includeMetricTypeFilter, err := getMetricTypeFilter(config.Include.MetricTypes)
		if err != nil {
			return nil, err
		}
		p.includeMetricTypes = includeMetricTypeFilter
	}

	if len(config.Exclude.MetricTypes) > 0 {
		excludeMetricTypeFilter, err := getMetricTypeFilter(config.Exclude.MetricTypes)
		if err != nil {
			return nil, err
		}
		p.excludeMetricTypes = excludeMetricTypeFilter
	}

	p.deltaCalculator = tracking.NewMetricTrackerWithRetraction(
		ctx,
		logger,
		config.MaxStaleness,
		config.InitialValue,
		tracking.RetractionConfig{
			Mode:             config.RetractionMode,
			ReorderWindow:    config.ReorderWindow,
			MaxHistoryPoints: config.MaxHistoryPoints,
		},
	)

	return p, nil
}

func getMetricTypeFilter(types []string) (map[pmetric.MetricType]bool, error) {
	res := map[pmetric.MetricType]bool{}
	for _, t := range types {
		switch strings.ToLower(t) {
		case strings.ToLower(pmetric.MetricTypeSum.String()):
			res[pmetric.MetricTypeSum] = true
		case strings.ToLower(pmetric.MetricTypeHistogram.String()):
			res[pmetric.MetricTypeHistogram] = true
		case strings.ToLower(pmetric.MetricTypeExponentialHistogram.String()):
			res[pmetric.MetricTypeExponentialHistogram] = true
		default:
			return nil, fmt.Errorf("unsupported metric type filter: %s", t)
		}
	}
	return res, nil
}

// processMetrics implements the ProcessMetricsFunc type.
func (ctdp *cumulativeToDeltaProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rm.ScopeMetrics().RemoveIf(func(ilm pmetric.ScopeMetrics) bool {
			ilm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
				if !ctdp.shouldConvertMetric(m) {
					return false
				}
				switch m.Type() {
				case pmetric.MetricTypeSum:
					ms := m.Sum()
					if ms.AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
						return false
					}

					// Ignore any metrics that aren't monotonic
					if !ms.IsMonotonic() {
						return false
					}

					baseIdentity := tracking.MetricIdentity{
						Resource:               rm.Resource(),
						InstrumentationLibrary: ilm.Scope(),
						MetricType:             m.Type(),
						MetricName:             m.Name(),
						MetricUnit:             m.Unit(),
						MetricIsMonotonic:      ms.IsMonotonic(),
					}
					if ctdp.retractionMode == tracking.RetractionModeOff {
						ctdp.convertNumberDataPoints(ctx, ms.DataPoints(), baseIdentity)
					} else {
						ctdp.convertNumberDataPointsRetraction(ctx, ms.DataPoints(), baseIdentity)
						if (ctdp.retractionMode == tracking.RetractionModeZSet || ctdp.retractionMode == tracking.RetractionModeZSetCompact) && !ctdp.preserveMonotonic {
							ms.SetIsMonotonic(false)
						}
					}
					ms.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
					return ms.DataPoints().Len() == 0
				case pmetric.MetricTypeHistogram:
					ms := m.Histogram()
					if ms.AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
						return false
					}

					if ms.DataPoints().Len() == 0 {
						return false
					}

					baseIdentity := tracking.MetricIdentity{
						Resource:               rm.Resource(),
						InstrumentationLibrary: ilm.Scope(),
						MetricType:             m.Type(),
						MetricName:             m.Name(),
						MetricUnit:             m.Unit(),
						MetricIsMonotonic:      true,
						MetricValueType:        pmetric.NumberDataPointValueTypeInt,
					}

					ctdp.convertHistogramDataPoints(ctx, ms.DataPoints(), baseIdentity)

					ms.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
					return ms.DataPoints().Len() == 0

				case pmetric.MetricTypeExponentialHistogram:
					ms := m.ExponentialHistogram()
					if ms.AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
						return false
					}
					if ms.DataPoints().Len() == 0 {
						return false
					}

					baseIdentity := tracking.MetricIdentity{
						Resource:               rm.Resource(),
						InstrumentationLibrary: ilm.Scope(),
						MetricType:             m.Type(),
						MetricName:             m.Name(),
						MetricUnit:             m.Unit(),
					}
					ctdp.convertExponentialHistogramDataPoints(ctx, ms.DataPoints(), baseIdentity)

					ms.SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
					return ms.DataPoints().Len() == 0

				default:
					return false
				}
			})
			return ilm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})
	return md, nil
}

func (ctdp *cumulativeToDeltaProcessor) shutdown(context.Context) error {
	ctdp.cancelFunc()
	ctdp.telemetry.Shutdown()
	return nil
}

func (ctdp *cumulativeToDeltaProcessor) shouldConvertMetric(metric pmetric.Metric) bool {
	return (ctdp.includeFS == nil || ctdp.includeFS.Matches(metric.Name())) &&
		(len(ctdp.includeMetricTypes) == 0 || ctdp.includeMetricTypes[metric.Type()]) &&
		(ctdp.excludeFS == nil || !ctdp.excludeFS.Matches(metric.Name())) &&
		(len(ctdp.excludeMetricTypes) == 0 || !ctdp.excludeMetricTypes[metric.Type()])
}

func (ctdp *cumulativeToDeltaProcessor) recordDropped(ctx context.Context, id tracking.MetricIdentity, reason string) {
	// Convert can return valid=false with an empty reason on internal-contract
	// edge cases (unsupported metric type, NaN float). Don't record an empty
	// reason — it isn't in the metadata.yaml enum and would create a useless
	// `reason=""` series.
	if reason == "" {
		return
	}
	opt, ok := droppedReasonAttrs[id.MetricType][reason]
	if !ok {
		opt = metric.WithAttributes(
			attribute.String("metric_type", metricTypeLabel(id.MetricType)),
			attribute.String("reason", reason),
		)
	}
	ctdp.telemetry.CumulativetodeltaDatapointsDropped.Add(ctx, 1, opt)
	if reason == tracking.ReasonReset {
		// Log at Debug alongside the metric: logs absorb the per-stream
		// cardinality that would be prohibitive on the metric, while still
		// letting operators identify which stream is wrapping/restarting.
		// Logger.Check skips the AsRaw() allocation when Debug isn't enabled.
		if ce := ctdp.logger.Check(zap.DebugLevel, "reset detected during cumulative-to-delta conversion"); ce != nil {
			ce.Write(
				zap.String("metric_name", id.MetricName),
				zap.String("metric_type", id.MetricType.String()),
				zap.Any("attributes", id.Attributes.AsRaw()),
			)
		}
	}
}

func (ctdp *cumulativeToDeltaProcessor) convertNumberDataPoints(ctx context.Context, dps pmetric.NumberDataPointSlice, baseIdentity tracking.MetricIdentity) {
	attrs := metricTypeAttrs[pmetric.MetricTypeSum]
	dps.RemoveIf(func(dp pmetric.NumberDataPoint) bool {
		id := baseIdentity
		id.StartTimestamp = dp.StartTimestamp()
		id.Attributes = dp.Attributes()
		id.MetricValueType = dp.ValueType()
		point := tracking.ValuePoint{
			ObservedTimestamp: dp.Timestamp(),
		}

		if dp.Flags().NoRecordedValue() {
			// drop points with no value
			return true
		}
		if id.IsFloatVal() {
			// Do not attempt to transform NaN values
			if math.IsNaN(dp.DoubleValue()) {
				return false
			}
			point.FloatValue = dp.DoubleValue()
		} else {
			point.IntValue = dp.IntValue()
		}
		trackingPoint := tracking.MetricPoint{
			Identity: id,
			Value:    point,
		}
		delta, valid, reason := ctdp.deltaCalculator.Convert(trackingPoint)
		if !valid {
			ctdp.recordDropped(ctx, id, reason)
			return true
		}
		ctdp.telemetry.CumulativetodeltaDatapoints.Add(ctx, 1, attrs)
		dp.SetStartTimestamp(delta.StartTimestamp)
		if id.IsFloatVal() {
			dp.SetDoubleValue(delta.FloatValue)
		} else {
			dp.SetIntValue(delta.IntValue)
		}
		return false
	})
}

func applySumDelta(dst pmetric.NumberDataPoint, d tracking.SumDelta) {
	dst.SetStartTimestamp(d.StartTimestamp)
	dst.SetTimestamp(d.Timestamp)
	if d.IsFloat {
		dst.SetDoubleValue(d.FloatValue)
	} else {
		dst.SetIntValue(d.IntValue)
	}
}

func clearExemplars(dp pmetric.NumberDataPoint) {
	if dp.Exemplars().Len() > 0 {
		dp.Exemplars().RemoveIf(func(pmetric.Exemplar) bool { return true })
	}
}

func (ctdp *cumulativeToDeltaProcessor) convertNumberDataPointsRetraction(ctx context.Context, dps pmetric.NumberDataPointSlice, baseIdentity tracking.MetricIdentity) {
	n := dps.Len()
	if n == 0 {
		return
	}
	attrs := metricTypeAttrs[pmetric.MetricTypeSum]
	var outBuf [4]tracking.SumDelta

	// Fast path for single-datapoint slices (common in streaming OTLP batches).
	if n == 1 {
		dp := dps.At(0)
		if dp.Flags().NoRecordedValue() {
			dps.RemoveIf(func(pmetric.NumberDataPoint) bool { return true })
			return
		}
		if dp.ValueType() == pmetric.NumberDataPointValueTypeDouble && math.IsNaN(dp.DoubleValue()) {
			return
		}
		id := baseIdentity
		id.StartTimestamp = dp.StartTimestamp()
		id.Attributes = dp.Attributes()
		id.MetricValueType = dp.ValueType()
		point := tracking.ValuePoint{ObservedTimestamp: dp.Timestamp()}
		if id.IsFloatVal() {
			point.FloatValue = dp.DoubleValue()
		} else {
			point.IntValue = dp.IntValue()
		}
		deltas, outcome, reason := ctdp.deltaCalculator.ConvertSum(tracking.MetricPoint{Identity: id, Value: point}, outBuf[:0])
		switch outcome {
		case tracking.OutcomeDropped:
			ctdp.recordDropped(ctx, id, reason)
			dps.RemoveIf(func(pmetric.NumberDataPoint) bool { return true })
			return
		case tracking.OutcomeAbsorbed:
			dps.RemoveIf(func(pmetric.NumberDataPoint) bool { return true })
			return
		case tracking.OutcomeEmitted:
			if len(deltas) == 0 {
				dps.RemoveIf(func(pmetric.NumberDataPoint) bool { return true })
				return
			}
			ctdp.telemetry.CumulativetodeltaDatapoints.Add(ctx, int64(len(deltas)), attrs)
			if len(deltas) == 1 {
				applySumDelta(dp, deltas[0])
				if deltas[0].Synthetic {
					clearExemplars(dp)
				}
				return
			}
			// Append copies of metadata BEFORE overwriting dps.At(0) so exemplars and attributes stay intact.
			dps.EnsureCapacity(len(deltas))
			for k := 1; k < len(deltas); k++ {
				src := dps.At(0)
				dst := dps.AppendEmpty()
				src.Attributes().CopyTo(dst.Attributes())
				dst.SetFlags(src.Flags())
				if !deltas[k].Synthetic && src.Exemplars().Len() > 0 {
					src.Exemplars().CopyTo(dst.Exemplars())
				}
				applySumDelta(dst, deltas[k])
			}
			src := dps.At(0)
			applySumDelta(src, deltas[0])
			if deltas[0].Synthetic {
				clearExemplars(src)
			}
		}
		return
	}

	// Multi-datapoint batch: mutate in-place while every point yields exactly 1 delta;
	// lazily switch to a rebuilt slice on the first drop, absorption, or multi-point emission
	// to preserve strict causal ordering across the batch.
	var rebuilt pmetric.NumberDataPointSlice
	hasRebuilt := false

	startRebuild := func(upTo, extra int) {
		rebuilt = pmetric.NewNumberDataPointSlice()
		rebuilt.EnsureCapacity(n + extra)
		for j := range upTo {
			dps.At(j).MoveTo(rebuilt.AppendEmpty())
		}
		hasRebuilt = true
	}

	for i := range n {
		dp := dps.At(i)
		if dp.Flags().NoRecordedValue() {
			if !hasRebuilt {
				startRebuild(i, 0)
			}
			continue
		}
		if dp.ValueType() == pmetric.NumberDataPointValueTypeDouble && math.IsNaN(dp.DoubleValue()) {
			if hasRebuilt {
				dp.MoveTo(rebuilt.AppendEmpty())
			}
			continue
		}

		id := baseIdentity
		id.StartTimestamp = dp.StartTimestamp()
		id.Attributes = dp.Attributes()
		id.MetricValueType = dp.ValueType()
		point := tracking.ValuePoint{ObservedTimestamp: dp.Timestamp()}
		if id.IsFloatVal() {
			point.FloatValue = dp.DoubleValue()
		} else {
			point.IntValue = dp.IntValue()
		}

		deltas, outcome, reason := ctdp.deltaCalculator.ConvertSum(tracking.MetricPoint{Identity: id, Value: point}, outBuf[:0])
		if outcome == tracking.OutcomeDropped {
			ctdp.recordDropped(ctx, id, reason)
			if !hasRebuilt {
				startRebuild(i, 0)
			}
			continue
		}
		if outcome == tracking.OutcomeAbsorbed || len(deltas) == 0 {
			if !hasRebuilt {
				startRebuild(i, 0)
			}
			continue
		}

		ctdp.telemetry.CumulativetodeltaDatapoints.Add(ctx, int64(len(deltas)), attrs)
		if len(deltas) == 1 {
			applySumDelta(dp, deltas[0])
			if deltas[0].Synthetic {
				clearExemplars(dp)
			}
			if hasRebuilt {
				dp.MoveTo(rebuilt.AppendEmpty())
			}
			continue
		}

		if !hasRebuilt {
			startRebuild(i, len(deltas)-1)
		}
		for _, d := range deltas {
			dst := rebuilt.AppendEmpty()
			dp.Attributes().CopyTo(dst.Attributes())
			dst.SetFlags(dp.Flags())
			if !d.Synthetic && dp.Exemplars().Len() > 0 {
				dp.Exemplars().CopyTo(dst.Exemplars())
			}
			applySumDelta(dst, d)
		}
	}

	if hasRebuilt {
		dps.RemoveIf(func(pmetric.NumberDataPoint) bool { return true })
		rebuilt.MoveAndAppendTo(dps)
	}
}

func (ctdp *cumulativeToDeltaProcessor) convertHistogramDataPoints(ctx context.Context, dps pmetric.HistogramDataPointSlice, baseIdentity tracking.MetricIdentity) {
	attrs := metricTypeAttrs[pmetric.MetricTypeHistogram]
	dps.RemoveIf(func(dp pmetric.HistogramDataPoint) bool {
		id := baseIdentity
		id.StartTimestamp = dp.StartTimestamp()
		id.Attributes = dp.Attributes()

		if dp.Flags().NoRecordedValue() {
			// drop points with no value
			return true
		}

		point := tracking.ValuePoint{
			ObservedTimestamp: dp.Timestamp(),
			HistogramValue: &tracking.HistogramPoint{
				Count:        dp.Count(),
				Sum:          dp.Sum(),
				BucketBounds: dp.ExplicitBounds().AsRaw(),
				BucketCounts: dp.BucketCounts().AsRaw(),
			},
		}

		trackingPoint := tracking.MetricPoint{
			Identity: id,
			Value:    point,
		}
		delta, valid, reason := ctdp.deltaCalculator.Convert(trackingPoint)

		if valid {
			ctdp.telemetry.CumulativetodeltaDatapoints.Add(ctx, 1, attrs)
			dp.SetStartTimestamp(delta.StartTimestamp)
			dp.SetCount(delta.HistogramValue.Count)
			if dp.HasSum() && !math.IsNaN(dp.Sum()) {
				dp.SetSum(delta.HistogramValue.Sum)
			}
			dp.ExplicitBounds().FromRaw(delta.HistogramValue.BucketBounds)
			dp.BucketCounts().FromRaw(delta.HistogramValue.BucketCounts)
			dp.RemoveMin()
			dp.RemoveMax()
			return false
		}

		ctdp.recordDropped(ctx, id, reason)
		return !valid
	})
}

func (ctdp *cumulativeToDeltaProcessor) convertExponentialHistogramDataPoints(ctx context.Context, dps pmetric.ExponentialHistogramDataPointSlice, baseIdentity tracking.MetricIdentity) {
	attrs := metricTypeAttrs[pmetric.MetricTypeExponentialHistogram]
	dps.RemoveIf(func(dp pmetric.ExponentialHistogramDataPoint) bool {
		id := baseIdentity
		id.StartTimestamp = dp.StartTimestamp()
		id.Attributes = dp.Attributes()

		if dp.Flags().NoRecordedValue() {
			// drop points with no value
			return true
		}

		point := tracking.ValuePoint{
			ObservedTimestamp: dp.Timestamp(),
			ExponentialHistogramValue: &tracking.ExponentialHistogramPoint{
				Count:         dp.Count(),
				Sum:           dp.Sum(),
				Scale:         dp.Scale(),
				ZeroCount:     dp.ZeroCount(),
				ZeroThreshold: dp.ZeroThreshold(),
				Positive: tracking.ExponentialBuckets{
					Offset:       dp.Positive().Offset(),
					BucketCounts: dp.Positive().BucketCounts().AsRaw(),
				},
				Negative: tracking.ExponentialBuckets{
					Offset:       dp.Negative().Offset(),
					BucketCounts: dp.Negative().BucketCounts().AsRaw(),
				},
			},
		}
		trackingPoint := tracking.MetricPoint{
			Identity: id,
			Value:    point,
		}
		delta, valid, reason := ctdp.deltaCalculator.Convert(trackingPoint)

		if valid {
			ctdp.telemetry.CumulativetodeltaDatapoints.Add(ctx, 1, attrs)
			dp.SetStartTimestamp(delta.StartTimestamp)
			dp.SetCount(delta.ExponentialHistogramPoint.Count)
			if dp.HasSum() && !math.IsNaN(dp.Sum()) {
				dp.SetSum(delta.ExponentialHistogramPoint.Sum)
			}
			// Scale and ZeroThreshold are unchanged
			dp.SetZeroCount(delta.ExponentialHistogramPoint.ZeroCount)
			dp.Positive().SetOffset(delta.ExponentialHistogramPoint.Positive.Offset)
			if len(delta.ExponentialHistogramPoint.Positive.BucketCounts) == 0 {
				pcommon.NewUInt64Slice()
				dp.Positive().BucketCounts()
			}
			dp.Positive().BucketCounts().FromRaw(delta.ExponentialHistogramPoint.Positive.BucketCounts)
			dp.Negative().SetOffset(delta.ExponentialHistogramPoint.Negative.Offset)
			dp.Negative().BucketCounts().FromRaw(delta.ExponentialHistogramPoint.Negative.BucketCounts)
			// Cannot consistently compute min/max
			dp.RemoveMin()
			dp.RemoveMax()
			return false
		}

		ctdp.recordDropped(ctx, id, reason)
		return !valid
	})
}
