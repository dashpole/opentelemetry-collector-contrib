// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package subtractinitial // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/metricstarttimeprocessor/internal/subtractinitial"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/metricstarttimeprocessor/internal/datapointstorage"
)

// Type is the value users can use to configure the subtract initial point adjuster.
// The subtract initial point adjuster sets the start time of all points in a series by:
//   - Dropping the initial point, and recording its value and timestamp.
//   - Subtracting the initial point from all subsequent points, and using the timestamp of the initial point as the start timestamp.
//
// Note that when a reset is detected (eg: value of a counter is decreasing) - the strategy will set the
// start time of the reset point as point timestamp - 1ms.
const Type = "subtract_initial_point"

type Adjuster struct {
	referenceCache *datapointstorage.Cache
	set            component.TelemetrySettings
}

// NewAdjuster returns a new Adjuster which adjust metrics' start times based on the initial received points.
func NewAdjuster(set component.TelemetrySettings, gcInterval time.Duration) *Adjuster {
	return &Adjuster{
		referenceCache: datapointstorage.NewCache(gcInterval),
		set:            set,
	}
}

// AdjustMetrics adjusts the start time of metrics based on the initial received
// points.
//
// It uses two caches: referenceCache to store the initial point of each
// timeseries, and previousValueCache to store the previous value of each
// timeseries for reset detection.
//
// If a point has not been seen before, it will be dropped and cached as the
// reference point. For each subsequent point, it will normalize against the
// reference cached point reporting the delta. If a reset is detected, the
// current point will be reported as is, and the reference point will be
// updated. The function returns a new pmetric.Metrics containing the adjusted
// metrics.
func (a *Adjuster) AdjustMetrics(_ context.Context, metrics pmetric.Metrics) (pmetric.Metrics, error) {
	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		rm := metrics.ResourceMetrics().At(i)
		attrHash := pdatautil.MapHash(rm.Resource().Attributes())
		previousValueTsm, _ := a.referenceCache.Get(attrHash)

		// The lock on the relevant timeseriesMap is held throughout the adjustment process to ensure that
		// nothing else can modify the data used for adjustment.
		previousValueTsm.Lock()
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ilm := rm.ScopeMetrics().At(j)
			for k := range ilm.Metrics().Len() {
				metric := ilm.Metrics().At(k)
				switch dataType := metric.Type(); dataType {
				case pmetric.MetricTypeHistogram:
					adjustMetricHistogram(previousValueTsm, metric)

				case pmetric.MetricTypeSummary:
					adjustMetricSummary(previousValueTsm, metric)

				case pmetric.MetricTypeSum:
					adjustMetricSum(previousValueTsm, metric)

				case pmetric.MetricTypeExponentialHistogram:
					adjustMetricExponentialHistogram(previousValueTsm, metric)
				}
			}
		}
		previousValueTsm.Unlock()
	}

	return metrics, nil
}

func adjustMetricHistogram(referenceValueTsm *datapointstorage.TimeseriesMap, metric pmetric.Metric) {
	histogram := metric.Histogram()
	if histogram.AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
		// Only dealing with CumulativeDistributions.
		return
	}

	histogram.DataPoints().RemoveIf(func(currentDist pmetric.HistogramDataPoint) bool {
		pointStartTime := currentDist.StartTimestamp()
		if pointStartTime != 0 && pointStartTime != currentDist.Timestamp() {
			// Report point as is.
			return false
		}

		refTsi, found := referenceValueTsm.Get(metric, currentDist.Attributes())
		if !found {
			// First time we see this point. Skip it and use as a reference point for the next points.
			refTsi.Histogram = datapointstorage.HistogramInfo{
				RefCount:             currentDist.Count(),
				RefSum:               currentDist.Sum(),
				RefBucketCounts:      currentDist.BucketCounts().AsRaw(),
				PreviousCount:        currentDist.Count(),
				PreviousSum:          currentDist.Sum(),
				PreviousBucketCounts: currentDist.BucketCounts().AsRaw(),
				StartTime:            currentDist.Timestamp(),
				ExplicitBounds:       currentDist.ExplicitBounds().AsRaw(),
			}
			return true
		}

		// Adjust the datapoint based on the reference value.
		currentDist.SetStartTimestamp(refTsi.Histogram.StartTime)
		if currentDist.Flags().NoRecordedValue() {
			return false
		}

		if refTsi.IsResetHistogram(currentDist) {
			// reset re-initialize everything and use the non adjusted points start time.
			resetStartTimeStamp := pcommon.NewTimestampFromTime(currentDist.Timestamp().AsTime().Add(-1 * time.Millisecond))
			currentDist.SetStartTimestamp(resetStartTimeStamp)

			// Update the reference value with the metric point.
			refTsi.Histogram.RefCount, refTsi.Histogram.RefSum = 0, 0
			refTsi.Histogram.StartTime = resetStartTimeStamp
			refTsi.Histogram.RefBucketCounts = make([]uint64, currentDist.BucketCounts().Len())
			refTsi.Histogram.ExplicitBounds = currentDist.ExplicitBounds().AsRaw()
			refTsi.Histogram.PreviousBucketCounts = currentDist.BucketCounts().AsRaw()
			refTsi.Histogram.PreviousCount, refTsi.Histogram.PreviousSum = currentDist.Count(), currentDist.Sum()
		} else {
			refTsi.Histogram.PreviousBucketCounts = currentDist.BucketCounts().AsRaw()
			refTsi.Histogram.PreviousCount, refTsi.Histogram.PreviousSum = currentDist.Count(), currentDist.Sum()
			subtractHistogramDataPoint(currentDist, refTsi.Histogram)
		}
		return false
	})
}

func adjustMetricExponentialHistogram(referenceValueTsm *datapointstorage.TimeseriesMap, metric pmetric.Metric) {
	histogram := metric.ExponentialHistogram()
	if histogram.AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
		// Only dealing with CumulativeDistributions.
		return
	}

	histogram.DataPoints().RemoveIf(func(currentDist pmetric.ExponentialHistogramDataPoint) bool {
		pointStartTime := currentDist.StartTimestamp()
		if pointStartTime != 0 && pointStartTime != currentDist.Timestamp() {
			// Report point as is.
			return false
		}

		refTsi, found := referenceValueTsm.Get(metric, currentDist.Attributes())
		if !found {
			// First time we see this point. Skip it and use as a reference point for the next points.
			refTsi.ExponentialHistogram = datapointstorage.ExponentialHistogramInfo{
				RefCount: currentDist.Count(), RefSum: currentDist.Sum(), RefZeroCount: currentDist.ZeroCount(),
				RefPositive:   datapointstorage.NewExponentialHistogramBucketInfo(currentDist.Positive()),
				RefNegative:   datapointstorage.NewExponentialHistogramBucketInfo(currentDist.Negative()),
				PreviousCount: currentDist.Count(), PreviousSum: currentDist.Sum(), PreviousZeroCount: currentDist.ZeroCount(),
				PreviousPositive: datapointstorage.NewExponentialHistogramBucketInfo(currentDist.Positive()),
				PreviousNegative: datapointstorage.NewExponentialHistogramBucketInfo(currentDist.Negative()),
				Scale:            currentDist.Scale(),
				StartTime:        currentDist.Timestamp(),
			}
			return true
		}

		// Adjust the datapoint based on the reference value.
		currentDist.SetStartTimestamp(refTsi.ExponentialHistogram.StartTime)
		if currentDist.Flags().NoRecordedValue() {
			return false
		}

		if refTsi.IsResetExponentialHistogram(currentDist) {
			// reset re-initialize everything and use the non adjusted points start time.
			resetStartTimeStamp := pcommon.NewTimestampFromTime(currentDist.Timestamp().AsTime().Add(-1 * time.Millisecond))
			currentDist.SetStartTimestamp(resetStartTimeStamp)

			refTsi.ExponentialHistogram.RefCount, refTsi.ExponentialHistogram.RefSum, refTsi.ExponentialHistogram.RefZeroCount, refTsi.ExponentialHistogram.Scale = 0, 0, 0, currentDist.Scale()
			refTsi.ExponentialHistogram.StartTime = resetStartTimeStamp
			refTsi.ExponentialHistogram.RefPositive = datapointstorage.ExponentialHistogramBucketInfo{
				Offset:       currentDist.Positive().Offset(),
				BucketCounts: make([]uint64, currentDist.Positive().BucketCounts().Len()),
			}
			refTsi.ExponentialHistogram.RefNegative = datapointstorage.ExponentialHistogramBucketInfo{
				Offset:       currentDist.Negative().Offset(),
				BucketCounts: make([]uint64, currentDist.Negative().BucketCounts().Len()),
			}
			refTsi.ExponentialHistogram.PreviousPositive = datapointstorage.NewExponentialHistogramBucketInfo(currentDist.Positive())
			refTsi.ExponentialHistogram.PreviousNegative = datapointstorage.NewExponentialHistogramBucketInfo(currentDist.Negative())
			refTsi.ExponentialHistogram.PreviousCount, refTsi.ExponentialHistogram.PreviousSum, refTsi.ExponentialHistogram.PreviousZeroCount = currentDist.Count(), currentDist.Sum(), currentDist.ZeroCount()
		} else {
			refTsi.ExponentialHistogram.PreviousPositive = datapointstorage.NewExponentialHistogramBucketInfo(currentDist.Positive())
			refTsi.ExponentialHistogram.PreviousNegative = datapointstorage.NewExponentialHistogramBucketInfo(currentDist.Negative())
			refTsi.ExponentialHistogram.PreviousCount, refTsi.ExponentialHistogram.PreviousSum, refTsi.ExponentialHistogram.PreviousZeroCount = currentDist.Count(), currentDist.Sum(), currentDist.ZeroCount()
			subtractExponentialHistogramDataPoint(currentDist, refTsi.ExponentialHistogram)
		}
		return false
	})
}

func adjustMetricSum(referenceValueTsm *datapointstorage.TimeseriesMap, metric pmetric.Metric) {
	sum := metric.Sum()
	if sum.AggregationTemporality() != pmetric.AggregationTemporalityCumulative {
		// Only handle cumulative temporality sums
		return
	}

	sum.DataPoints().RemoveIf(func(currentSum pmetric.NumberDataPoint) bool {
		pointStartTime := currentSum.StartTimestamp()
		if pointStartTime != 0 && pointStartTime != currentSum.Timestamp() {
			// Report point as is.
			return false
		}

		refTsi, found := referenceValueTsm.Get(metric, currentSum.Attributes())
		if !found {
			// First time we see this point. Skip it and use as a reference point for the next points.
			refTsi.Number = datapointstorage.NumberInfo{
				PreviousDoubleValue: currentSum.DoubleValue(),
				PreviousIntValue:    currentSum.IntValue(),
				RefDoubleValue:      currentSum.DoubleValue(),
				RefIntValue:         currentSum.IntValue(),
				StartTime:           currentSum.Timestamp(),
			}
			return true
		}

		// Adjust the datapoint based on the reference value.
		currentSum.SetStartTimestamp(refTsi.Number.StartTime)
		if currentSum.Flags().NoRecordedValue() {
			return false
		}

		if refTsi.IsResetSum(currentSum) {
			// reset re-initialize everything and use the non adjusted points start time.
			resetStartTimeStamp := pcommon.NewTimestampFromTime(currentSum.Timestamp().AsTime().Add(-1 * time.Millisecond))
			currentSum.SetStartTimestamp(resetStartTimeStamp)

			refTsi.Number.StartTime = resetStartTimeStamp
			refTsi.Number.RefDoubleValue = 0
			refTsi.Number.RefIntValue = 0
			refTsi.Number.PreviousDoubleValue = currentSum.DoubleValue()
			refTsi.Number.PreviousIntValue = currentSum.IntValue()
		} else {
			refTsi.Number.PreviousDoubleValue = currentSum.DoubleValue()
			refTsi.Number.PreviousIntValue = currentSum.IntValue()

			// Update the currentSum appropriately based on the original value type.
			if currentSum.ValueType() == pmetric.NumberDataPointValueTypeDouble {
				currentSum.SetDoubleValue(currentSum.DoubleValue() - refTsi.Number.RefDoubleValue)
			} else {
				currentSum.SetIntValue(currentSum.IntValue() - refTsi.Number.RefIntValue)
			}
		}
		return false
	})
}

func adjustMetricSummary(referenceValueTsm *datapointstorage.TimeseriesMap, metric pmetric.Metric) {
	metric.Summary().DataPoints().RemoveIf(func(currentSummary pmetric.SummaryDataPoint) bool {
		pointStartTime := currentSummary.StartTimestamp()
		if pointStartTime != 0 && pointStartTime != currentSummary.Timestamp() {
			// Report point as is.
			return false
		}

		refTsi, found := referenceValueTsm.Get(metric, currentSummary.Attributes())
		if !found {
			// First time we see this point. Skip it and use as a reference point for the next points.
			refTsi.Summary = datapointstorage.SummaryInfo{
				RefCount: currentSummary.Count(), RefSum: currentSummary.Sum(),
				PreviousCount: currentSummary.Count(), PreviousSum: currentSummary.Sum(),
				StartTime: currentSummary.Timestamp(),
			}
			return true
		}

		// Adjust the datapoint based on the reference value.
		currentSummary.SetStartTimestamp(refTsi.Summary.StartTime)
		if currentSummary.Flags().NoRecordedValue() {
			return false
		}

		if refTsi.IsResetSummary(currentSummary) {
			// reset re-initialize everything and use the non adjusted points start time.
			resetStartTimeStamp := pcommon.NewTimestampFromTime(currentSummary.Timestamp().AsTime().Add(-1 * time.Millisecond))
			currentSummary.SetStartTimestamp(resetStartTimeStamp)

			refTsi.Summary.StartTime = resetStartTimeStamp
			refTsi.Summary.RefCount, refTsi.Summary.RefSum = 0, 0
			refTsi.Summary.PreviousCount, refTsi.Summary.PreviousSum = currentSummary.Count(), currentSummary.Sum()
		} else {
			refTsi.Summary.PreviousCount, refTsi.Summary.PreviousSum = currentSummary.Count(), currentSummary.Sum()
			if currentSummary.Count() >= refTsi.Summary.RefCount {
				currentSummary.SetCount(currentSummary.Count() - refTsi.Summary.RefCount)
			} else {
				currentSummary.SetCount(0)
			}
			if currentSummary.Sum() >= refTsi.Summary.RefSum {
				currentSummary.SetSum(currentSummary.Sum() - refTsi.Summary.RefSum)
			} else {
				currentSummary.SetSum(0)
			}
		}
		return false
	})
}

// subtractHistogramDataPoint subtracts b from a.
func subtractHistogramDataPoint(a pmetric.HistogramDataPoint, ref datapointstorage.HistogramInfo) {
	a.SetStartTimestamp(ref.StartTime)
	if a.Count() >= ref.RefCount {
		a.SetCount(a.Count() - ref.RefCount)
	} else {
		a.SetCount(0)
	}
	if a.Sum() >= ref.RefSum {
		a.SetSum(a.Sum() - ref.RefSum)
	} else {
		a.SetSum(0)
	}
	aBuckets := a.BucketCounts()
	bBuckets := ref.RefBucketCounts
	if len(bBuckets) != aBuckets.Len() {
		// Post reset, the reference histogram will have no buckets.
		return
	}
	newBuckets := make([]uint64, aBuckets.Len())
	for i := 0; i < aBuckets.Len(); i++ {
		if aBuckets.At(i) >= bBuckets[i] {
			newBuckets[i] = aBuckets.At(i) - bBuckets[i]
		} else {
			newBuckets[i] = 0
		}
	}
	a.BucketCounts().FromRaw(newBuckets)
}

// subtractExponentialHistogramDataPoint subtracts b from a.
func subtractExponentialHistogramDataPoint(a pmetric.ExponentialHistogramDataPoint, ref datapointstorage.ExponentialHistogramInfo) {
	a.SetStartTimestamp(ref.StartTime)
	if a.Count() >= ref.RefCount {
		a.SetCount(a.Count() - ref.RefCount)
	} else {
		a.SetCount(0)
	}
	if a.Sum() >= ref.RefSum {
		a.SetSum(a.Sum() - ref.RefSum)
	} else {
		a.SetSum(0)
	}
	if a.ZeroCount() >= ref.RefZeroCount {
		a.SetZeroCount(a.ZeroCount() - ref.RefZeroCount)
	} else {
		a.SetZeroCount(0)
	}
	if a.Positive().BucketCounts().Len() != len(ref.RefPositive.BucketCounts) ||
		a.Negative().BucketCounts().Len() != len(ref.RefNegative.BucketCounts) {
		// Post reset, the reference histogram will have no buckets.
		return
	}
	a.Positive().BucketCounts().FromRaw(subtractExponentialBuckets(a.Positive(), ref.RefPositive))
	a.Negative().BucketCounts().FromRaw(subtractExponentialBuckets(a.Negative(), ref.RefNegative))
}

// subtractExponentialBuckets subtracts b from a.
func subtractExponentialBuckets(a pmetric.ExponentialHistogramDataPointBuckets, b datapointstorage.ExponentialHistogramBucketInfo) []uint64 {
	newBuckets := make([]uint64, a.BucketCounts().Len())
	offsetDiff := int(a.Offset() - b.Offset)
	for i := 0; i < a.BucketCounts().Len(); i++ {
		bOffset := i + offsetDiff
		// if there is no corresponding bucket for the starting BucketCounts, don't normalize
		switch {
		case bOffset < 0 || bOffset >= len(b.BucketCounts):
			newBuckets[i] = a.BucketCounts().At(i)
		case a.BucketCounts().At(i) >= b.BucketCounts[bOffset]:
			newBuckets[i] = a.BucketCounts().At(i) - b.BucketCounts[bOffset]
		default:
			newBuckets[i] = 0
		}
	}
	return newBuckets
}
