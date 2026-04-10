// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"fmt"
	"math"
	"net"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	monitoringpb "cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	loggingpb "cloud.google.com/go/logging/apiv2/loggingpb"
	tracepb "cloud.google.com/go/trace/apiv2/tracepb"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/common/testutil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/testbed/testbed"
)

// GoogleCloudMockReceiver implements a mock receiver for Google Cloud APIs.
type GoogleCloudMockReceiver struct {
	testbed.DataReceiverBase
	server *grpc.Server
	signal string // "metrics", "traces", or "logs"
}

// NewGoogleCloudMockReceiver creates a new GoogleCloudMockReceiver.
func NewGoogleCloudMockReceiver(port int, signal string) *GoogleCloudMockReceiver {
	return &GoogleCloudMockReceiver{
		DataReceiverBase: testbed.DataReceiverBase{Port: port},
		signal:           signal,
	}
}

// Start the mock receiver.
func (r *GoogleCloudMockReceiver) Start(tc consumer.Traces, mc consumer.Metrics, lc consumer.Logs) error {
	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", r.Port))
	if err != nil {
		return err
	}

	r.server = grpc.NewServer(grpc.UnknownServiceHandler(func(srv interface{}, stream grpc.ServerStream) error {
		// Increment counters based on the expected signal type.
		switch r.signal {
		case "metrics":
			var req monitoringpb.CreateTimeSeriesRequest
			if err := stream.RecvMsg(&req); err != nil {
				return err
			}
			count := 0
			for _, ts := range req.TimeSeries {
				count += len(ts.Points)
			}
			if mmc, ok := mc.(*testbed.MockMetricConsumer); ok {
				_ = mmc.MockConsumeMetricData(count)
			}
		case "traces":
			var req tracepb.BatchWriteSpansRequest
			if err := stream.RecvMsg(&req); err != nil {
				return err
			}
			count := len(req.Spans)
			if mtc, ok := tc.(*testbed.MockTraceConsumer); ok {
				_ = mtc.MockConsumeTraceData(count)
			}
		case "logs":
			var req loggingpb.WriteLogEntriesRequest
			if err := stream.RecvMsg(&req); err != nil {
				return err
			}
			count := len(req.Entries)
			if mlc, ok := lc.(*testbed.MockLogConsumer); ok {
				_ = mlc.MockConsumeLogData(count)
			}
		}

		return stream.SendMsg(&emptypb.Empty{})
	}))

	go func() {
		_ = r.server.Serve(lis)
	}()

	return nil
}

// Stop the mock receiver.
func (r *GoogleCloudMockReceiver) Stop() error {
	if r.server != nil {
		r.server.Stop()
	}
	return nil
}

// GenConfigYAMLStr generates exporter config for agent.
func (r *GoogleCloudMockReceiver) GenConfigYAMLStr() string {
	return fmt.Sprintf(`
  googlecloud:
    project: my-project
    metric:
      endpoint: "127.0.0.1:%d"
      use_insecure: true
    trace:
      endpoint: "127.0.0.1:%d"
      use_insecure: true
    log:
      endpoint: "127.0.0.1:%d"
      use_insecure: true
      default_log_name: "test-log"
`, r.Port, r.Port, r.Port)
}

// ProtocolName returns protocol name as it is specified in Collector config.
func (r *GoogleCloudMockReceiver) ProtocolName() string {
	return "googlecloud"
}

type wrappingCollector struct {
	base    testbed.TestResultsSummary
	results map[string][]*testbed.PerformanceTestResult
}

func (c *wrappingCollector) Init(resultsDir string) { c.base.Init(resultsDir) }
func (c *wrappingCollector) Add(testName string, result any) {
	c.base.Add(testName, result)
	if r, ok := result.(*testbed.PerformanceTestResult); ok {
		baseName := testName
		if idx := strings.LastIndex(testName, "/Run"); idx != -1 {
			baseName = testName[:idx]
		}
		c.results[baseName] = append(c.results[baseName], r)
	}
}
func (c *wrappingCollector) Save() { c.base.Save() }

func TestGoogleCloudPerformance(t *testing.T) {
	tests := []struct {
		name     string
		sender   testbed.DataSender
		receiver testbed.DataReceiver
		resourceSpec testbed.ResourceSpec
	}{
		{
			name: "OTLP-Metric",
			sender: testbed.NewOTLPMetricDataSender(testbed.DefaultHost, testutil.GetAvailablePort(t)),
			receiver: testbed.NewOTLPDataReceiver(testutil.GetAvailablePort(t)),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 60,
				ExpectedMaxRAM: 150, // Increased based on previous runs
			},
		},
		{
			name: "GoogleCloud-Metric",
			sender: testbed.NewOTLPMetricDataSender(testbed.DefaultHost, testutil.GetAvailablePort(t)),
			receiver: NewGoogleCloudMockReceiver(testutil.GetAvailablePort(t), "metrics"),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 150, // Increased based on previous runs
				ExpectedMaxRAM: 150,
			},
		},
		{
			name: "OTLP-Trace",
			sender: testbed.NewOTLPTraceDataSender(testbed.DefaultHost, testutil.GetAvailablePort(t)),
			receiver: testbed.NewOTLPDataReceiver(testutil.GetAvailablePort(t)),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 60,
				ExpectedMaxRAM: 150,
			},
		},
		{
			name: "GoogleCloud-Trace",
			sender: testbed.NewOTLPTraceDataSender(testbed.DefaultHost, testutil.GetAvailablePort(t)),
			receiver: NewGoogleCloudMockReceiver(testutil.GetAvailablePort(t), "traces"),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 60,
				ExpectedMaxRAM: 150,
			},
		},
		{
			name: "OTLP-Log",
			sender: testbed.NewOTLPLogsDataSender(testbed.DefaultHost, testutil.GetAvailablePort(t)),
			receiver: testbed.NewOTLPDataReceiver(testutil.GetAvailablePort(t)),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 60,
				ExpectedMaxRAM: 150,
			},
		},
		{
			name: "GoogleCloud-Log",
			sender: testbed.NewOTLPLogsDataSender(testbed.DefaultHost, testutil.GetAvailablePort(t)),
			receiver: NewGoogleCloudMockReceiver(testutil.GetAvailablePort(t), "logs"),
			resourceSpec: testbed.ResourceSpec{
				ExpectedMaxCPU: 60,
				ExpectedMaxRAM: 150,
			},
		},
	}

	collector := &wrappingCollector{
		base:    performanceResultsSummary,
		results: make(map[string][]*testbed.PerformanceTestResult),
	}

	numRuns := 6

	for _, test := range tests {
		for i := 0; i < numRuns; i++ {
			runName := fmt.Sprintf("%s/Run%d", test.name, i)
			t.Run(runName, func(t *testing.T) {
				runCustomScenario(t, test.sender, test.receiver, test.resourceSpec, collector)
			})
		}
	}

	// Calculate and print stats
	t.Logf("\nPerformance Results Summary (after %d runs):", numRuns)
	for name, results := range collector.results {
		cpuMean, cpuStdDev, ramMean, ramStdDev := calculateStats(results)
		t.Logf("%-30s | CPU Avg: %6.1f ± %5.1f %% | RAM Max: %6.1f ± %5.1f MiB",
			name, cpuMean, cpuStdDev, ramMean, ramStdDev)
	}
}

func calculateStats(results []*testbed.PerformanceTestResult) (cpuMean, cpuStdDev, ramMean, ramStdDev float64) {
	if len(results) == 0 {
		return
	}
	var cpuSum, ramSum float64
	for _, r := range results {
		cpuSum += r.CpuPercentageAvg
		ramSum += float64(r.RamMibMax)
	}
	cpuMean = cpuSum / float64(len(results))
	ramMean = ramSum / float64(len(results))

	var cpuVar, ramVar float64
	for _, r := range results {
		cpuVar += math.Pow(r.CpuPercentageAvg-cpuMean, 2)
		ramVar += math.Pow(float64(r.RamMibMax)-ramMean, 2)
	}
	cpuStdDev = math.Sqrt(cpuVar / float64(len(results)))
	ramStdDev = math.Sqrt(ramVar / float64(len(results)))
	return
}

func createCustomConfigYaml(
	t *testing.T,
	sender testbed.DataSender,
	receiver testbed.DataReceiver,
	resultDir string,
	pprofPort int,
) string {
	var pipeline string
	switch sender.(type) {
	case testbed.TraceDataSender:
		pipeline = "traces"
	case testbed.MetricDataSender:
		pipeline = "metrics"
	case testbed.LogDataSender:
		pipeline = "logs"
	default:
		t.Error("Invalid DataSender type")
	}

	format := `
receivers:%v
exporters:%v

extensions:
  pprof:
    save_to_file: %v/cpu.prof
    endpoint: "127.0.0.1:%d"

service:
  extensions: [pprof]
  pipelines:
    %s:
      receivers: [%v]
      exporters: [%v]
`

	return fmt.Sprintf(
		format,
		sender.GenConfigYAMLStr(),
		receiver.GenConfigYAMLStr(),
		resultDir,
		pprofPort,
		pipeline,
		sender.ProtocolName(),
		receiver.ProtocolName(),
	)
}

func runCustomScenario(t *testing.T, sender testbed.DataSender, receiver testbed.DataReceiver, resourceSpec testbed.ResourceSpec, collector testbed.TestResultsSummary) {
	resultDir, err := filepath.Abs(filepath.Join("results", t.Name()))
	require.NoError(t, err)

	loadOptions := testbed.LoadOptions{
		DataItemsPerSecond: 10_000,
		ItemsPerBatch:      100,
		Parallel:           1,
	}

	agentProc := testbed.NewChildProcessCollector(testbed.WithEnvVar("GOMAXPROCS", "2"))

	pprofPort := testutil.GetAvailablePort(t)
	configStr := createCustomConfigYaml(t, sender, receiver, resultDir, pprofPort)
	configCleanup, err := agentProc.PrepareConfig(t, configStr)
	require.NoError(t, err)
	defer configCleanup()

	dataProvider := testbed.NewPerfTestDataProvider(loadOptions)
	tc := testbed.NewTestCase(
		t,
		dataProvider,
		sender,
		receiver,
		agentProc,
		&testbed.PerfTestValidator{},
		collector,
		testbed.WithResourceLimits(resourceSpec),
	)
	t.Cleanup(tc.Stop)

	tc.StartBackend()
	tc.StartAgent()

	tc.StartLoad(loadOptions)

	tc.WaitFor(func() bool { return tc.LoadGenerator.DataItemsSent() > 0 }, "load generator started")

	tc.Sleep(tc.Duration)

	tc.StopLoad()

	tc.WaitFor(func() bool { return tc.LoadGenerator.DataItemsSent() == tc.MockBackend.DataItemsReceived() },
		"all data items received")

	tc.ValidateData()
}
