// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"

	types "github.com/gogo/protobuf/types"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/model/textparse"
	dto "github.com/prometheus/prometheus/prompb/io/prometheus/client"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheus"
	mdata "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver/internal/metadata"
)

const (
	numSeries = 10000
)

var (
	benchTarget = scrape.NewTarget(
		labels.FromMap(map[string]string{
			model.InstanceLabel: "localhost:8080",
			model.JobLabel:      "benchmark",
		}),
		&config.ScrapeConfig{},
		map[model.LabelName]model.LabelValue{
			model.AddressLabel: "localhost:8080",
			model.SchemeLabel:  "http",
		},
		nil,
	)

	benchCtx = scrape.ContextWithTarget(context.Background(), benchTarget)
)

// BenchmarkAppend benchmarks the Append method of the transaction.
// It tests the performance of appending classic metric types (counters, gauges, summaries, histograms).
func BenchmarkAppend(b *testing.B) {
	benchmarkAppend(b)
}

func benchmarkAppend(b *testing.B) {
	labelSets := generateLabelSets(numSeries, 50)
	timestamp := int64(1234567890)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tx := newBenchmarkTransaction(b)
		b.StartTimer()

		for j, ls := range labelSets {
			value := float64(j)
			_, err := tx.Append(0, ls, 0, timestamp, value, nil, nil, storage.AOptions{})
			assert.NoError(b, err)
		}
	}
}

// BenchmarkAppendHistogram benchmarks the AppendHistogram method of the transaction.
// It tests the performance of appending native histogram metrics.
func BenchmarkAppendHistogram(b *testing.B) {
	benchmarkAppendHistogram(b)
}

func benchmarkAppendHistogram(b *testing.B) {
	labelSets := generateLabelSets(numSeries, 50)
	histograms := generateNativeHistograms(numSeries)
	timestamp := int64(1234567890)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tx := newBenchmarkTransaction(b)
		b.StartTimer()

		for j := range labelSets {
			_, err := tx.Append(0, labelSets[j], 0, timestamp, 0, histograms[j], nil, storage.AOptions{})
			assert.NoError(b, err)
		}
	}
}

// BenchmarkCommit benchmarks the Commit method which converts accumulated metrics to pmetrics format
// and delivers them to the consumer. This is separate from Append/AppendHistogram to measure the
// conversion and delivery overhead independently.
// Note: The presence of target_info and otel_scope_info metrics affects the performance of the Commit method,
// so they are benchmarked in sub-benchmarks.
func BenchmarkCommit(b *testing.B) {
	b.Run("ClassicMetrics", func(b *testing.B) {
		b.Run("Baseline", func(b *testing.B) {
			benchmarkCommit(b, false, false, false)
		})

		b.Run("WithTargetInfo", func(b *testing.B) {
			benchmarkCommit(b, false, true, false)
		})

		b.Run("WithScopeInfo", func(b *testing.B) {
			benchmarkCommit(b, false, false, true)
		})
	})

	b.Run("NativeHistogram", func(b *testing.B) {
		b.Run("Baseline", func(b *testing.B) {
			benchmarkCommit(b, true, false, false)
		})

		b.Run("WithTargetInfo", func(b *testing.B) {
			benchmarkCommit(b, true, true, false)
		})

		b.Run("WithScopeInfo", func(b *testing.B) {
			benchmarkCommit(b, true, false, true)
		})
	})
}

func benchmarkCommit(b *testing.B, useNativeHistograms, withTargetInfo, withScopeInfo bool) {
	labelSets := generateLabelSets(numSeries, 50)
	var histograms []*histogram.Histogram
	if useNativeHistograms {
		histograms = generateNativeHistograms(numSeries)
	}
	timestamp := int64(1234567890)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Setup: Create transaction and append all data (not timed)
		b.StopTimer()
		tx := newBenchmarkTransaction(b)

		if withTargetInfo {
			targetInfoLabels := createTargetInfoLabels()
			_, _ = tx.Append(0, targetInfoLabels, 0, timestamp, 1, nil, nil, storage.AOptions{})
		}

		if withScopeInfo {
			scopeInfoLabels := createScopeInfoLabels()
			_, _ = tx.Append(0, scopeInfoLabels, 0, timestamp, 1, nil, nil, storage.AOptions{})
		}

		if useNativeHistograms {
			for j := range labelSets {
				_, _ = tx.Append(0, labelSets[j], 0, timestamp, 0, histograms[j], nil, storage.AOptions{})
			}
		} else {
			for j, ls := range labelSets {
				_, _ = tx.Append(0, ls, 0, timestamp, float64(j), nil, nil, storage.AOptions{})
			}
		}
		b.StartTimer()

		// Benchmark: Only measure Commit
		err := tx.Commit()
		assert.NoError(b, err)
	}
}

// BenchmarkE2ETransaction benchmarks the entire lifecycle of a transaction (new, Append, Commit)
// without stopping the timer, providing a fair end-to-end comparison of memory and CPU performance.
func BenchmarkE2ETransaction(b *testing.B) {
	b.Run("ClassicMetrics", func(b *testing.B) {
		benchmarkE2E(b, false, 0)
	})
	b.Run("ClassicMetrics/MultiSeries", func(b *testing.B) {
		benchmarkE2E(b, false, 100)
	})
	b.Run("NativeHistogram", func(b *testing.B) {
		benchmarkE2E(b, true, 0)
	})
}

func benchmarkE2E(b *testing.B, useNativeHistograms bool, numFamilies int) {
	var labelSets []labels.Labels
	if numFamilies > 0 {
		labelSets = generateMultiSeriesLabelSets(numSeries, 50, numFamilies)
	} else {
		labelSets = generateLabelSets(numSeries, 50)
	}
	var histograms []*histogram.Histogram
	if useNativeHistograms {
		histograms = generateNativeHistograms(numSeries)
	}
	timestamp := int64(1234567890)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tx := newBenchmarkTransaction(b)
		if useNativeHistograms {
			for j := range labelSets {
				_, err := tx.Append(0, labelSets[j], 0, timestamp, 0, histograms[j], nil, storage.AOptions{})
				assert.NoError(b, err)
			}
		} else {
			for j, ls := range labelSets {
				_, err := tx.Append(0, ls, 0, timestamp, float64(j), nil, nil, storage.AOptions{})
				assert.NoError(b, err)
			}
		}
		err := tx.Commit()
		assert.NoError(b, err)
	}
}

func generateMultiSeriesLabelSets(seriesCount, cardinality, numFamilies int) []labels.Labels {
	result := make([]labels.Labels, seriesCount)

	for i := range seriesCount {
		lbls := labels.NewBuilder(labels.EmptyLabels())
		lbls.Set(model.MetricNameLabel, fmt.Sprintf("metric_%d", i%numFamilies))

		for j := range cardinality {
			lbls.Set(fmt.Sprintf("label_%d", j), fmt.Sprintf("value_%d_%d", i, j))
		}

		result[i] = lbls.Labels()
	}

	return result
}


// newBenchmarkTransaction creates a new transaction configured for benchmarking.
// It uses a no-op consumer and minimal configuration to isolate transaction performance.
func newBenchmarkTransaction(b *testing.B) *transaction {
	b.Helper()

	sink := new(consumertest.MetricsSink)
	settings := receivertest.NewNopSettings(mdata.Type)
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             component.MustNewID("prometheus"),
		Transport:              "http",
		ReceiverCreateSettings: settings,
	})
	if err != nil {
		b.Fatalf("Failed to create ObsReport: %v", err)
	}

	tx := newTransaction(
		benchCtx,
		sink,
		labels.EmptyLabels(), // no external labels
		settings,
		obsrecv,
		false, // trimSuffixes
		false, // useMetadata
	)

	// Set a mock MetricMetadataStore to avoid nil pointer issues
	tx.mc = &mockMetadataStore{}

	return tx
}

// generateLabelSets creates label sets for benchmarking with the specified cardinality.
func generateLabelSets(seriesCount, cardinality int) []labels.Labels {
	result := make([]labels.Labels, seriesCount)

	for i := range seriesCount {
		lbls := labels.NewBuilder(labels.EmptyLabels())
		lbls.Set(model.MetricNameLabel, fmt.Sprintf("metric_%d", i))

		for j := range cardinality {
			lbls.Set(fmt.Sprintf("label_%d", j), fmt.Sprintf("value_%d_%d", i, j))
		}

		result[i] = lbls.Labels()
	}

	return result
}

// generateNativeHistograms creates native histogram instances for benchmarking.
// Uses Prometheus's test histogram generator for realistic histogram structures.
func generateNativeHistograms(count int) []*histogram.Histogram {
	result := make([]*histogram.Histogram, count)

	for i := range count {
		// Use tsdbutil.GenerateTestHistogram to create realistic native histograms
		// The parameter controls the histogram ID, which varies the bucket counts slightly
		result[i] = tsdbutil.GenerateTestHistogram(int64(i))
	}

	return result
}

// createTargetInfoLabels creates labels for a target_info metric.
// The target_info metric is used to add resource attributes to metrics.
func createTargetInfoLabels() labels.Labels {
	return labels.FromMap(map[string]string{
		model.MetricNameLabel: prometheus.TargetInfoMetricName,
		model.JobLabel:        "benchmark",
		model.InstanceLabel:   "localhost:8080",
		"environment":         "test",
		"region":              "us-west-2",
		"cluster":             "benchmark-cluster",
	})
}

// createScopeInfoLabels creates labels for an otel_scope_info metric.
// The otel_scope_info metric is used to add scope-level attributes.
func createScopeInfoLabels() labels.Labels {
	return labels.FromMap(map[string]string{
		model.MetricNameLabel:           prometheus.ScopeInfoMetricName,
		model.JobLabel:                  "benchmark",
		model.InstanceLabel:             "localhost:8080",
		prometheus.ScopeNameLabelKey:    "benchmark.scope",
		prometheus.ScopeVersionLabelKey: "1.0.0",
		"scope_attribute":               "test_value",
	})
}

// mockMetadataStore is a minimal implementation of scrape.MetricMetadataStore for testing
type mockMetadataStore struct{}

func (*mockMetadataStore) ListMetadata() []scrape.MetricMetadata {
	return nil
}

func (*mockMetadataStore) GetMetadata(_ string) (scrape.MetricMetadata, bool) {
	return scrape.MetricMetadata{}, false
}

func (*mockMetadataStore) SizeMetadata() int {
	return 0
}

func (*mockMetadataStore) LengthMetadata() int {
	return 0
}

type benchPayload struct {
	openMetricsBytes []byte
	protoBytes       []byte
}

// BenchmarkScrapePayload benchmarks the full CPU and memory usage of scraping and committing
// a 1,000-series metrics payload without network calls. It includes the Prometheus parser
// (textparse.New with ConvertClassicHistogramsToNHCB: true in AppenderV2) and tests all metric types
// with exemplars on all metrics, including classic (multi-line) histograms, without legacy _created series.
func BenchmarkScrapePayload(b *testing.B) {
	b.Run("Counter", func(b *testing.B) {
		// 1,000 counters * 1 line (_total) = 1,000 series lines
		p := benchPayload{openMetricsBytes: generateOpenMetricsCounterPayload(1000)}
		runScrapePayloadBenchmarkV2(b, p)
	})
	b.Run("Gauge", func(b *testing.B) {
		// 1,000 gauges * 1 line = 1,000 series lines
		p := benchPayload{openMetricsBytes: generateOpenMetricsGaugePayload(1000)}
		runScrapePayloadBenchmarkV2(b, p)
	})
	b.Run("ClassicHistogram", func(b *testing.B) {
		// 100 multi-line classic histograms * 10 lines (8 buckets + sum + count) = 1,000 series lines
		p := benchPayload{openMetricsBytes: generateOpenMetricsClassicHistogramPayload(100)}
		runScrapePayloadBenchmarkV2(b, p)
	})
	b.Run("Summary", func(b *testing.B) {
		// 200 summaries * 5 lines (3 quantiles + sum + count) = 1,000 series lines
		p := benchPayload{openMetricsBytes: generateOpenMetricsSummaryPayload(200)}
		runScrapePayloadBenchmarkV2(b, p)
	})
	b.Run("NativeHistogram", func(b *testing.B) {
		p := benchPayload{protoBytes: generateProtobufNativeHistogramPayload(1000)}
		runScrapePayloadBenchmarkV2(b, p)
	})
	b.Run("MixedPayload", func(b *testing.B) {
		// 200 Counters (200 lines) + 200 Gauges (200 lines) + 20 ClassicHistograms (200 lines) + 40 Summaries (200 lines) + 200 NativeHistograms = 1,000 series
		var omBuf bytes.Buffer
		omBuf.Write(generateOpenMetricsCounterPayloadNoEOF(200))
		omBuf.Write(generateOpenMetricsGaugePayloadNoEOF(200))
		omBuf.Write(generateOpenMetricsClassicHistogramPayloadNoEOF(20))
		omBuf.Write(generateOpenMetricsSummaryPayloadNoEOF(40))
		omBuf.WriteString("# EOF\n")
		p := benchPayload{
			openMetricsBytes: omBuf.Bytes(),
			protoBytes:       generateProtobufNativeHistogramPayload(200),
		}
		runScrapePayloadBenchmarkV2(b, p)
	})
}

func runScrapePayloadBenchmarkV2(b *testing.B, payload benchPayload) {
	settings := receivertest.NewNopSettings(mdata.Type)
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             component.MustNewID("prometheus"),
		Transport:              "http",
		ReceiverCreateSettings: settings,
	})
	if err != nil {
		b.Fatalf("Failed to create ObsReport: %v", err)
	}
	fallbackExemplarLabels := labels.FromStrings("trace_id", "0102030405060708090a0b0c0d0e0f10", "span_id", "0102030405060708")
	symbolTable := labels.NewSymbolTable()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		metaMap := make(testMetadataStore)
		ctx := scrape.ContextWithMetricMetadataStore(benchCtx, metaMap)
		tx := newTransaction(
			ctx,
			consumertest.NewNop(),
			labels.EmptyLabels(),
			settings,
			obsrecv,
			false,
			true,
		)
		tx.mc = metaMap

		if len(payload.openMetricsBytes) > 0 {
			parseAndAppendV2(b, tx, metaMap, payload.openMetricsBytes, "application/openmetrics-text; version=1.0.0", symbolTable, fallbackExemplarLabels)
		}
		if len(payload.protoBytes) > 0 {
			parseAndAppendV2(b, tx, metaMap, payload.protoBytes, "application/vnd.google.protobuf; proto=io.prometheus.client.MetricFamily; encoding=delimited", symbolTable, fallbackExemplarLabels)
		}
		if err := tx.Commit(); err != nil {
			b.Fatalf("Commit failed: %v", err)
		}
	}
}

func parseAndAppendV2(
	b *testing.B,
	tx *transaction,
	metaMap testMetadataStore,
	data []byte,
	contentType string,
	st *labels.SymbolTable,
	fallbackExemplarLabels labels.Labels,
) {
	p, err := textparse.New(data, contentType, st, textparse.ParserOptions{
		ConvertClassicHistogramsToNHCB: true,
		OpenMetricsSkipSTSeries:        false,
	})
	if err != nil && p == nil {
		b.Fatalf("Failed to create parser: %v", err)
	}
	var lset labels.Labels
	var currMFName string
	var currMeta metadata.Metadata

	for {
		et, err := p.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			b.Fatalf("Parse error: %v", err)
		}
		switch et {
		case textparse.EntryType:
			mName, mType := p.Type()
			currMFName = string(mName)
			currMeta.Type = mType
			md := scrape.MetricMetadata{
				MetricFamily: currMFName,
				Type:         mType,
				Help:         currMeta.Help,
				Unit:         currMeta.Unit,
			}
			metaMap[currMFName] = md
			switch mType {
			case model.MetricTypeCounter:
				metaMap[currMFName+"_total"] = md
			case model.MetricTypeHistogram:
				metaMap[currMFName+"_bucket"] = md
				metaMap[currMFName+"_sum"] = md
				metaMap[currMFName+"_count"] = md
			case model.MetricTypeSummary:
				metaMap[currMFName+"_sum"] = md
				metaMap[currMFName+"_count"] = md
			}
		case textparse.EntryHelp:
			mName, mHelp := p.Help()
			currMFName = string(mName)
			currMeta.Help = string(mHelp)
			md := metaMap[currMFName]
			md.MetricFamily = currMFName
			md.Help = currMeta.Help
			metaMap[currMFName] = md
		case textparse.EntryUnit:
			mName, mUnit := p.Unit()
			currMFName = string(mName)
			currMeta.Unit = string(mUnit)
			md := metaMap[currMFName]
			md.MetricFamily = currMFName
			md.Unit = currMeta.Unit
			metaMap[currMFName] = md
		case textparse.EntrySeries:
			_, tsPtr, val := p.Series()
			p.Labels(&lset)
			ts := int64(1700000000000)
			if tsPtr != nil {
				ts = *tsPtr
			}
			var exs []exemplar.Exemplar
			var ex exemplar.Exemplar
			for p.Exemplar(&ex) {
				exs = append(exs, ex)
			}
			// Ensure every metric has an exemplar even if OpenMetrics text syntax doesn't support inline exemplars on gauge/summary
			if len(exs) == 0 {
				exs = []exemplar.Exemplar{{
					Labels: fallbackExemplarLabels,
					Value:  val,
					Ts:     ts,
				}}
			}
			if _, err := tx.Append(0, lset, 0, ts, val, nil, nil, storage.AOptions{
				MetricFamilyName: currMFName,
				Metadata:         currMeta,
				Exemplars:        exs,
			}); err != nil {
				b.Fatalf("Append error: %v", err)
			}
		case textparse.EntryHistogram:
			_, tsPtr, h, fh := p.Histogram()
			p.Labels(&lset)
			ts := int64(1700000000000)
			if tsPtr != nil {
				ts = *tsPtr
			}
			var exs []exemplar.Exemplar
			var ex exemplar.Exemplar
			for p.Exemplar(&ex) {
				exs = append(exs, ex)
			}
			if len(exs) == 0 {
				exs = []exemplar.Exemplar{{
					Labels: fallbackExemplarLabels,
					Value:  1.0,
					Ts:     ts,
				}}
			}
			if _, err := tx.Append(0, lset, 0, ts, 0, h, fh, storage.AOptions{
				MetricFamilyName: currMFName,
				Metadata:         currMeta,
				Exemplars:        exs,
			}); err != nil {
				b.Fatalf("Append histogram error: %v", err)
			}
		}
	}
}

func generateOpenMetricsCounterPayload(count int) []byte {
	var buf bytes.Buffer
	buf.Write(generateOpenMetricsCounterPayloadNoEOF(count))
	buf.WriteString("# EOF\n")
	return buf.Bytes()
}

func generateOpenMetricsCounterPayloadNoEOF(count int) []byte {
	var buf bytes.Buffer
	numFamilies := 10
	if count < numFamilies {
		numFamilies = count
	}
	perFamily := count / numFamilies
	for f := 0; f < numFamilies; f++ {
		mf := fmt.Sprintf("bench_counter_%d", f)
		fmt.Fprintf(&buf, "# TYPE %s counter\n", mf)
		fmt.Fprintf(&buf, "# HELP %s Benchmark counter metric family %d\n", mf, f)
		for i := 0; i < perFamily; i++ {
			baseLabels := fmt.Sprintf("job=\"benchmark\",instance=\"localhost:8080\",service=\"svc_%d\",env=\"prod\",region=\"us-east-1\",pod=\"pod_%d\",endpoint=\"/api/v1/items\",method=\"GET\",status=\"200\"", f, i)
			fmt.Fprintf(&buf, "%s_total{%s} %d.0 1700000000.000 # {trace_id=\"0102030405060708090a0b0c0d0e0f10\",span_id=\"0102030405060708\"} 1.0 1700000000.000\n",
				mf, baseLabels, i+1)
		}
	}
	return buf.Bytes()
}

func generateOpenMetricsGaugePayload(count int) []byte {
	var buf bytes.Buffer
	buf.Write(generateOpenMetricsGaugePayloadNoEOF(count))
	buf.WriteString("# EOF\n")
	return buf.Bytes()
}

func generateOpenMetricsGaugePayloadNoEOF(count int) []byte {
	var buf bytes.Buffer
	numFamilies := 10
	perFamily := count / numFamilies
	for f := 0; f < numFamilies; f++ {
		mf := fmt.Sprintf("bench_gauge_%d", f)
		fmt.Fprintf(&buf, "# TYPE %s gauge\n", mf)
		fmt.Fprintf(&buf, "# HELP %s Benchmark gauge metric family %d\n", mf, f)
		for i := 0; i < perFamily; i++ {
			fmt.Fprintf(&buf, "%s{job=\"benchmark\",instance=\"localhost:8080\",service=\"svc_%d\",env=\"prod\",region=\"us-east-1\",pod=\"pod_%d\",endpoint=\"/api/v1/items\",method=\"GET\",status=\"200\"} %d.5 1700000000.000\n",
				mf, f, i, i+1)
		}
	}
	return buf.Bytes()
}

func generateOpenMetricsClassicHistogramPayload(numHistograms int) []byte {
	var buf bytes.Buffer
	buf.Write(generateOpenMetricsClassicHistogramPayloadNoEOF(numHistograms))
	buf.WriteString("# EOF\n")
	return buf.Bytes()
}

func generateOpenMetricsClassicHistogramPayloadNoEOF(numHistograms int) []byte {
	var buf bytes.Buffer
	// Use 1 histogram per family (or distinct metric family header per histogram) so OpenMetrics 1.0 parser
	// does not scan across unrelated histograms when checking for legacy _created lines.
	bounds := []string{"0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "+Inf"}
	for hIdx := 0; hIdx < numHistograms; hIdx++ {
		mf := fmt.Sprintf("bench_classic_hist_%d", hIdx)
		fmt.Fprintf(&buf, "# TYPE %s histogram\n", mf)
		fmt.Fprintf(&buf, "# HELP %s Benchmark classic histogram family %d\n", mf, hIdx)
		baseLabels := fmt.Sprintf("job=\"benchmark\",instance=\"localhost:8080\",service=\"svc_%d\",env=\"prod\",region=\"us-east-1\",pod=\"pod_%d\"", hIdx%10, hIdx)
		for bIdx, le := range bounds {
			cumCount := (bIdx + 1) * 10
			fmt.Fprintf(&buf, "%s_bucket{%s,le=\"%s\"} %d 1700000000.000 # {trace_id=\"0102030405060708090a0b0c0d0e0f10\",span_id=\"0102030405060708\"} 0.042 1700000000.000\n",
				mf, baseLabels, le, cumCount)
		}
		fmt.Fprintf(&buf, "%s_sum{%s} 45.67 1700000000.000\n", mf, baseLabels)
		fmt.Fprintf(&buf, "%s_count{%s} 80 1700000000.000\n", mf, baseLabels)
	}
	return buf.Bytes()
}

func generateOpenMetricsSummaryPayload(numSummaries int) []byte {
	var buf bytes.Buffer
	buf.Write(generateOpenMetricsSummaryPayloadNoEOF(numSummaries))
	buf.WriteString("# EOF\n")
	return buf.Bytes()
}

func generateOpenMetricsSummaryPayloadNoEOF(numSummaries int) []byte {
	var buf bytes.Buffer
	numFamilies := 10
	if numSummaries < numFamilies {
		numFamilies = numSummaries
	}
	perFamily := numSummaries / numFamilies
	quantiles := []struct {
		q string
		v float64
	}{
		{"0.5", 0.12},
		{"0.9", 0.45},
		{"0.99", 0.89},
	}
	for f := 0; f < numFamilies; f++ {
		mf := fmt.Sprintf("bench_summary_%d", f)
		fmt.Fprintf(&buf, "# TYPE %s summary\n", mf)
		fmt.Fprintf(&buf, "# HELP %s Benchmark summary family %d\n", mf, f)
		for i := 0; i < perFamily; i++ {
			baseLabels := fmt.Sprintf("job=\"benchmark\",instance=\"localhost:8080\",service=\"svc_%d\",env=\"prod\",region=\"us-east-1\",pod=\"pod_%d\"", f, i)
			for _, q := range quantiles {
				fmt.Fprintf(&buf, "%s{%s,quantile=\"%s\"} %.2f 1700000000.000\n", mf, baseLabels, q.q, q.v)
			}
			fmt.Fprintf(&buf, "%s_sum{%s} 23.45 1700000000.000\n", mf, baseLabels)
			fmt.Fprintf(&buf, "%s_count{%s} 50 1700000000.000\n", mf, baseLabels)
		}
	}
	return buf.Bytes()
}

func generateProtobufNativeHistogramPayload(count int) []byte {
	var buf bytes.Buffer
	numFamilies := 10
	if count < numFamilies {
		numFamilies = count
	}
	perFamily := count / numFamilies
	tsProto := &types.Timestamp{Seconds: 1700000000, Nanos: 0}
	for f := 0; f < numFamilies; f++ {
		mfName := fmt.Sprintf("bench_native_hist_%d", f)
		help := fmt.Sprintf("Benchmark native histogram family %d", f)
		mfType := dto.MetricType_HISTOGRAM
		mf := &dto.MetricFamily{
			Name:   mfName,
			Help:   help,
			Type:   mfType,
			Metric: make([]dto.Metric, 0, perFamily),
		}
		for i := 0; i < perFamily; i++ {
			svcVal := fmt.Sprintf("svc_%d", f)
			podVal := fmt.Sprintf("pod_%d", i)
			m := dto.Metric{
				Label: []dto.LabelPair{
					{Name: "job", Value: "benchmark"},
					{Name: "instance", Value: "localhost:8080"},
					{Name: "service", Value: svcVal},
					{Name: "env", Value: "prod"},
					{Name: "region", Value: "us-east-1"},
					{Name: "pod", Value: podVal},
				},
				TimestampMs: 1700000000000,
				Histogram: &dto.Histogram{
					SampleCount:   66,
					SampleSum:     1004.78,
					Schema:        3,
					ZeroThreshold: 0.001,
					ZeroCount:     2,
					PositiveSpan: []dto.BucketSpan{
						{Offset: 0, Length: 4},
					},
					PositiveDelta: []int64{10, 5, -3, 2},
					Exemplars: []*dto.Exemplar{
						{
							Value:     0.42,
							Timestamp: tsProto,
							Label: []dto.LabelPair{
								{Name: "trace_id", Value: "0102030405060708090a0b0c0d0e0f10"},
								{Name: "span_id", Value: "0102030405060708"},
							},
						},
					},
				},
			}
			mf.Metric = append(mf.Metric, m)
		}
		data, err := mf.Marshal()
		if err != nil {
			panic(err)
		}
		var varintBuf [binary.MaxVarintLen32]byte
		n := binary.PutUvarint(varintBuf[:], uint64(len(data)))
		buf.Write(varintBuf[:n])
		buf.Write(data)
	}
	return buf.Bytes()
}
