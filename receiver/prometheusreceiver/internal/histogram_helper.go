package internal

import (
	"github.com/prometheus/prometheus/model/histogram"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func convertDeltaBuckets(spans []histogram.Span, deltas []int64, buckets pcommon.UInt64Slice) {
	totalBuckets := len(deltas)
	for spanIdx, span := range spans {
		if spanIdx > 0 && span.Offset > 0 {
			totalBuckets += int(span.Offset)
		}
	}
	buckets.EnsureCapacity(totalBuckets)
	bucketIdx := 0
	bucketCount := int64(0)
	for spanIdx, span := range spans {
		if spanIdx > 0 {
			for i := int32(0); i < span.Offset; i++ {
				buckets.Append(uint64(0))
			}
		}
		for i := uint32(0); i < span.Length; i++ {
			bucketCount += deltas[bucketIdx]
			bucketIdx++
			buckets.Append(uint64(bucketCount))
		}
	}
}

func convertAbsoluteBuckets(spans []histogram.Span, counts []float64, buckets pcommon.UInt64Slice) {
	totalBuckets := len(counts)
	for spanIdx, span := range spans {
		if spanIdx > 0 && span.Offset > 0 {
			totalBuckets += int(span.Offset)
		}
	}
	buckets.EnsureCapacity(totalBuckets)
	bucketIdx := 0
	for spanIdx, span := range spans {
		if spanIdx > 0 {
			for i := int32(0); i < span.Offset; i++ {
				buckets.Append(uint64(0))
			}
		}
		for i := uint32(0); i < span.Length; i++ {
			buckets.Append(uint64(counts[bucketIdx]))
			bucketIdx++
		}
	}
}

// validateNHCB validates that an NHCB histogram has consistent count/sum, strictly increasing custom bounds,
// and non-negative bucket counts without allocating.
func validateNHCB(h *histogram.Histogram, fh *histogram.FloatHistogram) bool {
	if h != nil {
		if h.Count == 0 && h.Sum != 0 {
			return false
		}
		for i, v := range h.CustomValues {
			if i > 0 && v <= h.CustomValues[i-1] {
				return false
			}
		}
		totalBuckets := len(h.CustomValues) + 1
		bucketIdx := 0
		bucketCount := int64(0)
		deltaIdx := 0
		for _, span := range h.PositiveSpans {
			bucketIdx += int(span.Offset)
			for i := uint32(0); i < span.Length && bucketIdx < totalBuckets && deltaIdx < len(h.PositiveBuckets); i++ {
				bucketCount += h.PositiveBuckets[deltaIdx]
				deltaIdx++
				if bucketCount < 0 {
					return false
				}
				bucketIdx++
			}
		}
	}
	if fh != nil {
		if fh.Count == 0 && fh.Sum != 0 {
			return false
		}
		for i, v := range fh.CustomValues {
			if i > 0 && v <= fh.CustomValues[i-1] {
				return false
			}
		}
	}
	return true
}

func populateZeroBuckets(totalBuckets int, buckets pcommon.UInt64Slice) {
	buckets.EnsureCapacity(totalBuckets)
	for i := 0; i < totalBuckets; i++ {
		buckets.Append(0)
	}
}

func populateNHCBDeltaBuckets(h *histogram.Histogram, buckets pcommon.UInt64Slice) {
	totalBuckets := len(h.CustomValues) + 1
	populateZeroBuckets(totalBuckets, buckets)
	if len(h.PositiveSpans) == 0 {
		return
	}
	bucketIdx := 0
	bucketCount := int64(0)
	deltaIdx := 0
	for _, span := range h.PositiveSpans {
		bucketIdx += int(span.Offset)
		for i := uint32(0); i < span.Length && bucketIdx < totalBuckets && deltaIdx < len(h.PositiveBuckets); i++ {
			bucketCount += h.PositiveBuckets[deltaIdx]
			deltaIdx++
			if bucketIdx >= 0 && bucketIdx < totalBuckets {
				buckets.SetAt(bucketIdx, uint64(bucketCount))
			}
			bucketIdx++
		}
	}
}

func populateNHCBAbsoluteBuckets(fh *histogram.FloatHistogram, buckets pcommon.UInt64Slice) {
	totalBuckets := len(fh.CustomValues) + 1
	populateZeroBuckets(totalBuckets, buckets)
	if len(fh.PositiveSpans) == 0 {
		return
	}
	bucketIdx := 0
	valIdx := 0
	for _, span := range fh.PositiveSpans {
		bucketIdx += int(span.Offset)
		for i := uint32(0); i < span.Length && bucketIdx < totalBuckets && valIdx < len(fh.PositiveBuckets); i++ {
			if bucketIdx >= 0 && bucketIdx < totalBuckets {
				buckets.SetAt(bucketIdx, uint64(fh.PositiveBuckets[valIdx]))
			}
			valIdx++
			bucketIdx++
		}
	}
}
