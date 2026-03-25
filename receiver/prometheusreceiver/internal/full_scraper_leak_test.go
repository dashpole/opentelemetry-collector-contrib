// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/featuregate"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/batchprocessor"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/service/telemetry"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/prometheusremotewriteexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver"
)

func TestFullScrapeLoopLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("This test can take a long time")
	}

	// Enable the feature gate for start timestamp zero ingestion to test the inconsistent SeriesRef behavior!
	err := featuregate.GlobalRegistry().Set("receiver.prometheusreceiver.EnableCreatedTimestampZeroIngestion", true)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// 1. Setup the server that sends metrics
	scrapeServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(rw, `
# HELP jvm_memory_bytes_used Used bytes of a given JVM memory area.
# TYPE jvm_memory_bytes_used gauge
jvm_memory_bytes_used{area="heap"} 1000
jvm_memory_bytes_used{area="non-heap"} 2000
`)
	}))
	defer scrapeServer.Close()

	serverURL, err := url.Parse(scrapeServer.URL)
	require.NoError(t, err)

	// 2. Set up the Prometheus RemoteWrite endpoint to drain data
	prweServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		payload, _ := io.ReadAll(req.Body)
		decoded, _ := snappy.Decode(nil, payload)
		writeReq := new(prompb.WriteRequest)
		proto.Unmarshal(decoded, writeReq)
		w.WriteHeader(http.StatusOK)
	}))
	defer prweServer.Close()

	// 3. Set the OpenTelemetry Prometheus receiver.
	cfg := fmt.Sprintf(`
receivers:
  prometheus:
    config:
      scrape_configs:
        - job_name: 'test'
          scrape_interval: 10ms
          static_configs:
            - targets: [%q]

exporters:
  prometheusremotewrite:
    endpoint: %q
    tls:
      insecure: true

service:
  pipelines:
    metrics:
      receivers: [prometheus]
      exporters: [prometheusremotewrite]`, serverURL.Host, prweServer.URL)

	confFile, err := os.CreateTemp(os.TempDir(), "conf-")
	require.NoError(t, err)
	defer os.Remove(confFile.Name())
	_, err = confFile.WriteString(cfg)
	require.NoError(t, err)

	// 4. Run the OpenTelemetry Collector.
	receivers, err := otelcol.MakeFactoryMap[receiver.Factory](prometheusreceiver.NewFactory())
	require.NoError(t, err)
	exporters, err := otelcol.MakeFactoryMap[exporter.Factory](prometheusremotewriteexporter.NewFactory())
	require.NoError(t, err)
	processors, err := otelcol.MakeFactoryMap[processor.Factory](batchprocessor.NewFactory())
	require.NoError(t, err)

	factories := otelcol.Factories{
		Receivers:  receivers,
		Exporters:  exporters,
		Processors: processors,
		Telemetry: telemetry.NewFactory(func() component.Config { return struct{}{} }),
	}

	appSettings := otelcol.CollectorSettings{
		Factories: func() (otelcol.Factories, error) { return factories, nil },
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs:              []string{confFile.Name()},
				ProviderFactories: []confmap.ProviderFactory{fileprovider.NewFactory()},
			},
		},
		BuildInfo: component.BuildInfo{Version: "tests"},
		LoggingOptions: []zap.Option{zap.WrapCore(func(zapcore.Core) zapcore.Core { return zapcore.NewNopCore() })},
	}

	app, err := otelcol.NewCollector(appSettings)
	require.NoError(t, err)

	go func() {
		app.Run(ctx)
	}()
	defer app.Shutdown()

	require.Eventually(t, func() bool {
		return app.GetState() == otelcol.StateRunning
	}, 10*time.Second, 100*time.Millisecond, "collector did not start")

	// Warmup
	time.Sleep(1 * time.Second)

	runtime.GC()
	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)
	allocBefore := msBefore.Alloc

	// Run for 10 seconds (approx 1000 scrapes)
	time.Sleep(10 * time.Second)

	runtime.GC()
	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)
	allocAfter := msAfter.Alloc

	fmt.Printf("Full Scraper Test (10s run @ 10ms) - Memory Before: %d, After: %d, Delta: %d\n", allocBefore, allocAfter, int64(allocAfter)-int64(allocBefore))
	
	// Threshold 20Mib for full collector overhead
	if allocAfter > allocBefore+20*1024*1024 {
		t.Errorf("Potential leak in full scraper! Memory grew from %d to %d (delta %d)", allocBefore, allocAfter, int64(allocAfter)-int64(allocBefore))
	}
}
