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
	"path/filepath"
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

func TestTargetChurnLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("This test can take a long time")
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Create temp dir for file_sd_config
	tempDir, err := os.MkdirTemp("", "file_sd_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	targetFile := filepath.Join(tempDir, "targets.json")
	err = os.WriteFile(targetFile, []byte("[]"), 0644)
	require.NoError(t, err)

	// Set up the Prometheus RemoteWrite endpoint to drain data
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
          scrape_interval: 100ms
          file_sd_configs:
            - files: [%q]

exporters:
  prometheusremotewrite:
    endpoint: %q
    tls:
      insecure: true

service:
  pipelines:
    metrics:
      receivers: [prometheus]
      exporters: [prometheusremotewrite]`, targetFile, prweServer.URL)

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

	// We will run a loop where we add a target, wait for a scrape, update the file with a NEW target (removing the old one), and shut down the old one.
	// 50 iterations of target churn.
	const iterations = 50
	var lastServer *httptest.Server

	for i := 0; i < iterations; i++ {
		// 1. Create a new ephemeral target.
		newServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
			fmt.Fprintf(rw, `# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter
http_requests_total 100
http_requests_total_created 1234567890
`)
		}))

		serverURL, _ := url.Parse(newServer.URL)

		// 2. Update the file_sd_config file with the new target and remove any old one.
		targetJSON := fmt.Sprintf(`[{"targets": [%q]}]`, serverURL.Host)
		err = os.WriteFile(targetFile, []byte(targetJSON), 0644)
		require.NoError(t, err)

		// 3. Wait for the scrape to happen.
		time.Sleep(200 * time.Millisecond)

		// 4. Shut down the old server (if any).
		if lastServer != nil {
			lastServer.Close()
		}
		lastServer = newServer
	}

	if lastServer != nil {
		lastServer.Close()
	}

	runtime.GC()
	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)
	allocAfter := msAfter.Alloc

	fmt.Printf("Target Churn Test (50 iterations) - Memory Before: %d, After: %d, Delta: %d\n", allocBefore, allocAfter, int64(allocAfter)-int64(allocBefore))

	if allocAfter > allocBefore+10*1024*1024 { // 10MiB threshold
		t.Errorf("Potential target churn leak! Memory grew from %d to %d (delta %d)", allocBefore, allocAfter, int64(allocAfter)-int64(allocBefore))
	}
}
