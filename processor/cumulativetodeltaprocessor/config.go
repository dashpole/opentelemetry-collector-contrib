// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package cumulativetodeltaprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor"

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/filterset"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/cumulativetodeltaprocessor/internal/tracking"
)

var validMetricTypes = map[string]bool{
	strings.ToLower(pmetric.MetricTypeSum.String()):                  true,
	strings.ToLower(pmetric.MetricTypeHistogram.String()):            true,
	strings.ToLower(pmetric.MetricTypeExponentialHistogram.String()): true,
}

var validMetricTypeList = []string{"exponentialhistogram", "histogram", "sum"}

// Config defines the configuration for the processor.
type Config struct {
	// MaxStaleness is the total time a state entry will live past the time it was last seen. Set to 0 to retain state indefinitely.
	MaxStaleness time.Duration `mapstructure:"max_staleness"`

	// InitialValue determines how to handle the first datapoint for a given metric. Valid values:
	//
	//   - auto: (default) send the first point iff the startime is set AND the starttime happens after the component started AND the starttime is different from the timestamp
	//   - keep: always send the first point
	//   - drop: don't send the first point, but store it for subsequent delta calculations
	InitialValue tracking.InitialValue `mapstructure:"initial_value"`

	// Include specifies a filter on the metrics that should be converted.
	// Exclude specifies a filter on the metrics that should not be converted.
	// If neither `include` nor `exclude` are set, all metrics will be converted.
	// Cannot be used with deprecated Metrics config option.
	Include MatchMetrics `mapstructure:"include"`
	Exclude MatchMetrics `mapstructure:"exclude"`

	// RetractionMode controls out-of-order and reset handling for monotonic sums.
	// Valid values:
	//   - off: (default) legacy single-point behavior
	//   - zset: emit full DBSP algebraic retraction triples on out-of-order/revised points
	//   - zset_compact: collapse same-endpoint retraction/insertion pairs into one net adjustment
	//   - non_negative: keep exact global cumulative conservation without emitting negative deltas
	RetractionMode tracking.RetractionMode `mapstructure:"retraction_mode"`

	// ReorderWindow is the lookback window retained per sum stream when RetractionMode != off.
	// A zero value applies the runtime default (30s) when retraction mode is enabled.
	ReorderWindow time.Duration `mapstructure:"reorder_window"`

	// MaxHistoryPoints bounds the maximum number of timestamp-ordered observations retained
	// per sum stream when RetractionMode != off (must be 0 for default 64, or >= 2).
	MaxHistoryPoints int `mapstructure:"max_history_points"`

	// PreserveMonotonic keeps IsMonotonic=true on converted sum metrics in zset/zset_compact modes.
	// When false (default), converted sums emitted in zset/zset_compact modes are marked
	// non-monotonic (IsMonotonic=false) to conform to the OTel data model when negative
	// retractions may be emitted. No-op in off and non_negative modes.
	PreserveMonotonic bool `mapstructure:"preserve_monotonic"`
}

type MatchMetrics struct {
	Config filterset.Config `mapstructure:",squash"`

	Metrics []string `mapstructure:"metrics"`

	MetricTypes []string `mapstructure:"metric_types"`
}

var _ component.Config = (*Config)(nil)

// Validate checks whether the input configuration has all of the required fields for the processor.
// An error is returned if there are any invalid inputs.
func (config *Config) Validate() error {
	if (len(config.Include.Metrics) > 0 && len(config.Include.Config.MatchType) == 0) ||
		(len(config.Exclude.Metrics) > 0 && len(config.Exclude.Config.MatchType) == 0) {
		return errors.New("match_type must be set if metrics are supplied")
	}
	if (len(config.Include.Config.MatchType) > 0 && len(config.Include.Metrics) == 0) ||
		(len(config.Exclude.Config.MatchType) > 0 && len(config.Exclude.Metrics) == 0) {
		return errors.New("metrics must be supplied if match_type is set")
	}

	for _, metricType := range config.Exclude.MetricTypes {
		if valid := validMetricTypes[strings.ToLower(metricType)]; !valid {
			return fmt.Errorf(
				"found invalid metric type in exclude.metric_types: %s. Valid values are %s",
				metricType,
				validMetricTypeList,
			)
		}
	}
	for _, metricType := range config.Include.MetricTypes {
		if valid := validMetricTypes[strings.ToLower(metricType)]; !valid {
			return fmt.Errorf(
				"found invalid metric type in include.metric_types: %s. Valid values are %s",
				metricType,
				validMetricTypeList,
			)
		}
	}
	if !config.RetractionMode.IsValid() {
		return fmt.Errorf("invalid retraction_mode: %s", config.RetractionMode.String())
	}
	if config.ReorderWindow < 0 {
		return fmt.Errorf("reorder_window must be non-negative, got %s", config.ReorderWindow)
	}
	if config.MaxHistoryPoints < 0 || config.MaxHistoryPoints == 1 {
		return fmt.Errorf("max_history_points must be 0 (default) or >= 2, got %d", config.MaxHistoryPoints)
	}
	return nil
}
