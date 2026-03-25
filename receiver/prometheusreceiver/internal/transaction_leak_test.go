// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver/internal/metadata"
)

// BenchmarkScrapeLoop simulates multiple successive scrapes to check for memory leaks.
func BenchmarkScrapeLoop(b *testing.B) {
	sink := new(consumertest.MetricsSink)
	settings := receivertest.NewNopSettings(metadata.Type)

	app, err := NewAppendable(sink, settings, false, labels.EmptyLabels(), false)
	assert.NoError(b, err)

	// Vary number of series to simulate high core count (more CPU metrics)
	const seriesCount = 10000
	labelSets := generateLabelSets(seriesCount, 10)
	timestamp := int64(1234567890)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		appender := app.Appender(benchCtx)
		for j, ls := range labelSets {
			_, err := appender.Append(0, ls, timestamp, float64(j))
			assert.NoError(b, err)
		}
		err := appender.Commit()
		assert.NoError(b, err)
	}
}

// BenchmarkScrapeLoopWithSTZero simulates scrapes with start timestamp zero ingestion.
func BenchmarkScrapeLoopWithSTZero(b *testing.B) {
	sink := new(consumertest.MetricsSink)
	settings := receivertest.NewNopSettings(metadata.Type)

	app, err := NewAppendable(sink, settings, false, labels.EmptyLabels(), false)
	assert.NoError(b, err)

	const seriesCount = 10000
	labelSets := generateLabelSets(seriesCount, 10)
	timestamp := int64(1234567890)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		appender := app.Appender(benchCtx)
		for j, ls := range labelSets {
			// Simulate STZero append if it's counting or if we want to test both.
			// Here we just test STZero append for all to see if it leaks.
			if l, ok := appender.(interface {
				AppendSTZeroSample(storage.SeriesRef, labels.Labels, int64, int64) (storage.SeriesRef, error)
			}); ok {
				_, err := l.AppendSTZeroSample(0, ls, timestamp, timestamp-10000)
				assert.NoError(b, err)
			}
			_, err := appender.Append(0, ls, timestamp, float64(j))
			assert.NoError(b, err)
		}
		err := appender.Commit()
		assert.NoError(b, err)
	}
}

func TestScrapeLoopLeak(t *testing.T) {
	sink := consumertest.NewNop()
	settings := receivertest.NewNopSettings(metadata.Type)

	app, err := NewAppendable(sink, settings, false, labels.EmptyLabels(), false)
	assert.NoError(t, err)

	const seriesCount = 10000
	labelSets := generateLabelSets(seriesCount, 10)
	timestamp := int64(1234567890)

	// Run once to warm up
	appender := app.Appender(benchCtx)
	for j, ls := range labelSets {
		appender.Append(0, ls, timestamp, float64(j))
	}
	appender.Commit()

	runtime.GC() // Force GC
	var msRun1 runtime.MemStats
	runtime.ReadMemStats(&msRun1)
	alloc1 := msRun1.Alloc

	// Run 100 times
	for i := 0; i < 100; i++ {
		appender := app.Appender(benchCtx)
		for j, ls := range labelSets {
			if l, ok := appender.(interface {
				AppendSTZeroSample(storage.SeriesRef, labels.Labels, int64, int64) (storage.SeriesRef, error)
			}); ok {
				l.AppendSTZeroSample(0, ls, timestamp, timestamp-10000)
			}
			appender.Append(0, ls, timestamp, float64(j))
		}
		appender.Commit()
	}

	runtime.GC() // Force GC
	var msRun100 runtime.MemStats
	runtime.ReadMemStats(&msRun100)
	alloc100 := msRun100.Alloc

	fmt.Printf("Memory after 1 run: %d, after 100 runs: %d, delta: %d\n", alloc1, alloc100, alloc100-alloc1)
	if alloc100 > alloc1+10*1024*1024 { // 10Mib threshold
		t.Errorf("Potential leak! Memory grew from %d to %d (delta %d)", alloc1, alloc100, alloc100-alloc1)
	}
}

func TestScrapeLoopChurnLeak(t *testing.T) {
	sink := consumertest.NewNop()
	settings := receivertest.NewNopSettings(metadata.Type)

	app, err := NewAppendable(sink, settings, false, labels.EmptyLabels(), false)
	assert.NoError(t, err)

	const seriesCount = 1000 // Use fewer series to be fast
	timestamp := int64(1234567890)

	// Run once to warm up
	appender := app.Appender(benchCtx)
	labelSets := generateLabelSets(seriesCount, 10)
	for j, ls := range labelSets {
		appender.Append(0, ls, timestamp, float64(j))
	}
	appender.Commit()

	runtime.GC()
	var msRun1 runtime.MemStats
	runtime.ReadMemStats(&msRun1)
	alloc1 := msRun1.Alloc

	// Run 100 times with churn
	for i := 0; i < 100; i++ {
		appender := app.Appender(benchCtx)
		churnedLabelSets := generateLabelSetsWithSuffix(seriesCount, 10, i)
		for j, ls := range churnedLabelSets {
			appender.Append(0, ls, timestamp, float64(j))
		}
		appender.Commit()
	}

	runtime.GC()
	var msRun100 runtime.MemStats
	runtime.ReadMemStats(&msRun100)
	alloc100 := msRun100.Alloc

	fmt.Printf("Churn Leak Test - Memory after 1 run: %d, after 100 runs (with churn): %d, delta: %d\n", alloc1, alloc100, int64(alloc100)-int64(alloc1))
	if alloc100 > alloc1+10*1024*1024 { // 10Mib threshold
		t.Errorf("Potential churn leak! Memory grew from %d to %d (delta %d)", alloc1, alloc100, int64(alloc100)-int64(alloc1))
	}
}

func generateLabelSetsWithSuffix(count int, labelsCount int, runID int) []labels.Labels {
	sets := generateLabelSets(count, labelsCount)
	for i := range sets {
		b := labels.NewBuilder(sets[i])
		b.Set("run", fmt.Sprintf("%d", runID))
		sets[i] = b.Labels()
	}
	return sets
}
