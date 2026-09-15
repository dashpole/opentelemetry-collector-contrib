// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package internal // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver/internal"

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/prometheus"
	mdata "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver/internal/metadata"
)

type resourceKey struct {
	job      string
	instance string
}

type scopeID struct {
	name      string
	version   string
	schemaURL string
	attrsHash [16]byte
}

type metricKey struct {
	rKey       resourceKey
	scope      scopeID
	metricType pmetric.MetricType
	metricName string
}

type dataPointKey struct {
	rKey     resourceKey
	baseName string
	hash     uint64
}

type knownMetricTypeKey struct {
	rKey       resourceKey
	metricName string
}

var emptyScopeID scopeID

type transaction struct {
	ctx                   context.Context
	sink                  consumer.Metrics
	externalLabels        labels.Labels
	logger                *zap.Logger
	buildInfo             component.BuildInfo
	obsrecv               *receiverhelper.ObsReport
	trimSuffixes          bool
	useMetadata           bool
	ignoreScopeInfoMetric bool
	mc                    scrape.MetricMetadataStore
	knownMetricTypes      *sync.Map

	md pmetric.Metrics

	resources map[resourceKey]pmetric.ResourceMetrics
	scopes    map[resourceKey]map[scopeID]pmetric.ScopeMetrics
	metrics   map[metricKey]pmetric.Metric

	nodeResources   map[resourceKey]pcommon.Resource
	scopeAttributes map[resourceKey]map[scopeID]pcommon.Map

	summaryAccumulator *summaryAccumulator

	createdTimestamps     map[dataPointKey]pcommon.Timestamp
	classicHistFamilies   map[string]bool
	classicHistTimestamps map[uint64]int64

	bufBytes           []byte
	lastDpExemplars    pmetric.ExemplarSlice
	lastLabels         labels.Labels
	lastMType          pmetric.MetricType
	hasLastDpExemplars bool
	exemplarMap        map[uint64]pmetric.ExemplarSlice
}

func newTransaction(
	ctx context.Context,
	sink consumer.Metrics,
	externalLabels labels.Labels,
	settings receiver.Settings,
	obsrecv *receiverhelper.ObsReport,
	trimSuffixes bool,
	useMetadata bool,
	knownTypes ...*sync.Map,
) *transaction {
	var knownMetricTypes *sync.Map
	if len(knownTypes) > 0 && knownTypes[0] != nil {
		knownMetricTypes = knownTypes[0]
	} else {
		knownMetricTypes = new(sync.Map)
	}
	return &transaction{
		ctx:                   ctx,
		sink:                  sink,
		externalLabels:        externalLabels,
		logger:                settings.Logger,
		buildInfo:             settings.BuildInfo,
		obsrecv:               obsrecv,
		trimSuffixes:          trimSuffixes,
		useMetadata:           useMetadata,
		ignoreScopeInfoMetric: mdata.ReceiverPrometheusreceiverIgnoreScopeInfoMetricFeatureGate.IsEnabled(),
		knownMetricTypes:      knownMetricTypes,

		md: pmetric.NewMetrics(),

		resources: make(map[resourceKey]pmetric.ResourceMetrics),
		scopes:    make(map[resourceKey]map[scopeID]pmetric.ScopeMetrics),
		metrics:   make(map[metricKey]pmetric.Metric),

		nodeResources: make(map[resourceKey]pcommon.Resource),

		bufBytes: make([]byte, 0, 8192),
	}
}

type summaryAccumulator struct {
	groupHashes map[metricKey][]uint64
	summaries   map[metricKey]map[uint64]*summaryGroup
}

type summaryGroup struct {
	ls        labels.Labels
	atMs      int64
	stMs      int64
	hasSum    bool
	sum       float64
	hasCount  bool
	count     float64
	isStale   bool
	quantiles []quantileValue
}

type quantileValue struct {
	quantile float64
	value    float64
}

func getSeriesRefWithoutScopeLabels(bytes []byte, ls labels.Labels, mtype pmetric.MetricType) (uint64, []byte) {
	return ls.HashWithoutLabels(bytes, getSortedNotUsefulLabelsForSeries(mtype, ls)...)
}

func (t *transaction) getSeriesRef(ls labels.Labels, mtype pmetric.MetricType) uint64 {
	hash, buf := getSeriesRefWithoutScopeLabels(t.bufBytes[:0], ls, mtype)
	t.bufBytes = buf
	return hash
}

func populateExemplar(dt pmetric.Exemplar, ex exemplar.Exemplar) {
	dt.FilteredAttributes().EnsureCapacity(ex.Labels.Len())
	ex.Labels.Range(func(l labels.Label) {
		switch strings.ToLower(l.Name) {
		case prometheus.ExemplarTraceIDKey:
			if l.Value == "" {
				return
			}
			var tid [16]byte
			if len(l.Value) == hex.EncodedLen(len(tid)) {
				if b, err := hex.DecodeString(l.Value); err == nil {
					copy(tid[:], b)
					if traceID := pcommon.TraceID(tid); !traceID.IsEmpty() {
						dt.SetTraceID(traceID)
						return
					}
				}
			}
			dt.FilteredAttributes().PutStr(l.Name, l.Value)
		case prometheus.ExemplarSpanIDKey:
			if l.Value == "" {
				return
			}
			var sid [8]byte
			if len(l.Value) == hex.EncodedLen(len(sid)) {
				if b, err := hex.DecodeString(l.Value); err == nil {
					copy(sid[:], b)
					if spanID := pcommon.SpanID(sid); !spanID.IsEmpty() {
						dt.SetSpanID(spanID)
						return
					}
				}
			}
			dt.FilteredAttributes().PutStr(l.Name, l.Value)
		default:
			dt.FilteredAttributes().PutStr(l.Name, l.Value)
		}
	})
}

func attributesMatchLabels(attrs pcommon.Map, ls labels.Labels, mtype pmetric.MetricType) bool {
	names := getSortedNotUsefulLabels(mtype)
	j := 0
	matched := 0
	mismatch := false
	ls.Range(func(l labels.Label) {
		if mismatch {
			return
		}
		for j < len(names) && names[j] < l.Name {
			j++
		}
		if j < len(names) && l.Name == names[j] {
			return
		}
		if strings.HasPrefix(l.Name, prometheus.ScopeLabelPrefix) {
			return
		}
		if l.Value == "" {
			return
		}
		v, ok := attrs.Get(l.Name)
		if !ok || v.Str() != l.Value {
			mismatch = true
			return
		}
		matched++
	})
	return !mismatch && matched == attrs.Len()
}

func (t *transaction) populateAttributes(attrs pcommon.Map, ls labels.Labels, mtype pmetric.MetricType) {
	attrs.EnsureCapacity(ls.Len())
	names := getSortedNotUsefulLabels(mtype)
	j := 0
	ls.Range(func(l labels.Label) {
		for j < len(names) && names[j] < l.Name {
			j++
		}
		if j < len(names) && l.Name == names[j] {
			return
		}
		if strings.HasPrefix(l.Name, prometheus.ScopeLabelPrefix) {
			return
		}
		if l.Value == "" {
			return
		}
		attrs.PutStr(l.Name, l.Value)
	})
}

func getScopeID(ls labels.Labels) (scopeID, pcommon.Map) {
	var scope scopeID
	var attrs pcommon.Map
	hasAttrs := false
	ls.Range(func(lbl labels.Label) {
		switch lbl.Name {
		case prometheus.ScopeNameLabelKey:
			scope.name = lbl.Value
			return
		case prometheus.ScopeVersionLabelKey:
			scope.version = lbl.Value
			return
		case prometheus.ScopeSchemaURLLabelKey:
			scope.schemaURL = lbl.Value
			return
		}
		if strings.HasPrefix(lbl.Name, prometheus.ScopeLabelPrefix) {
			if !hasAttrs {
				attrs = pcommon.NewMap()
				hasAttrs = true
			}
			attrKey := strings.TrimPrefix(lbl.Name, prometheus.ScopeLabelPrefix)
			attrs.PutStr(attrKey, lbl.Value)
		}
	})
	if hasAttrs {
		scope.attrsHash = pdatautil.MapHash(attrs)
	}
	return scope, attrs
}

func (t *transaction) SetOptions(_ *storage.AppendOptions) {}

func (t *transaction) getJobAndInstance(labels labels.Labels) (resourceKey, error) {
	job, instance := labels.Get(model.JobLabel), labels.Get(model.InstanceLabel)
	if job != "" && instance != "" {
		return resourceKey{
			job:      job,
			instance: instance,
		}, nil
	}

	if target, ok := scrape.TargetFromContext(t.ctx); ok {
		if job == "" {
			job = target.GetValue(model.JobLabel)
		}
		if instance == "" {
			instance = target.GetValue(model.InstanceLabel)
		}
		if job != "" && instance != "" {
			return resourceKey{
				job:      job,
				instance: instance,
			}, nil
		}
	}
	return resourceKey{}, errNoJobInstance
}

var emptyMetadataStoreInstance = &emptyMetadataStore{}

func (t *transaction) Append(
	ref storage.SeriesRef,
	ls labels.Labels,
	stMs, atMs int64,
	val float64,
	h *histogram.Histogram,
	fh *histogram.FloatHistogram,
	opts storage.AppendV2Options,
) (storage.SeriesRef, error) {
	if t.ctx.Err() != nil {
		return 0, errTransactionAborted
	}

	if ref == 0 {
		ref = storage.SeriesRef(ls.Hash())
	}

	if t.externalLabels.Len() != 0 {
		b := labels.NewBuilder(ls)
		t.externalLabels.Range(func(l labels.Label) {
			b.Set(l.Name, l.Value)
		})
		ls = b.Labels()
	}

	t.hasLastDpExemplars = false
	rKey, err := t.getJobAndInstance(ls)
	if err != nil {
		return 0, err
	}

	if dupLabel, hasDup := ls.HasDuplicateLabelNames(); hasDup {
		return 0, fmt.Errorf("invalid sample: non-unique label names: %q", dupLabel)
	}

	rawName := ls.Get(model.MetricNameLabel)
	if rawName == "" {
		return 0, errMetricNameNotFound
	}

	if t.mc == nil {
		t.mc = emptyMetadataStoreInstance
		if t.useMetadata {
			if mc, ok := scrape.MetricMetadataStoreFromContext(t.ctx); ok {
				t.mc = mc
			}
		}
	}

	if rawName == "up" && val != 1.0 && !value.IsStaleNaN(val) {
		if val == 0.0 {
			var scrapeErr error
			if target, ok := scrape.TargetFromContext(t.ctx); ok {
				scrapeErr = target.LastError()
			}
			if scrapeErr != nil {
				t.logger.Warn("Failed to scrape Prometheus endpoint", zap.Error(scrapeErr), zap.Int64("scrape_timestamp", atMs), zap.Stringer("target_labels", ls))
			} else {
				t.logger.Warn("Failed to scrape Prometheus endpoint", zap.Int64("scrape_timestamp", atMs), zap.Stringer("target_labels", ls))
			}
		} else {
			t.logger.Warn("The 'up' metric contains invalid value", zap.Float64("value", val), zap.Int64("scrape_timestamp", atMs), zap.Stringer("target_labels", ls))
		}
	}

	if rawName == prometheus.TargetInfoMetricName {
		res, ok := t.nodeResources[rKey]
		if !ok {
			if target, tok := scrape.TargetFromContext(t.ctx); tok {
				res = CreateResource(rKey.job, rKey.instance, target.DiscoveredLabels(labels.NewBuilder(labels.EmptyLabels())))
			} else {
				res = CreateResource(rKey.job, rKey.instance, labels.EmptyLabels())
			}
			t.nodeResources[rKey] = res
		}
		attrs := res.Attributes()
		ls.Range(func(lbl labels.Label) {
			if lbl.Name == model.JobLabel || lbl.Name == model.InstanceLabel || lbl.Name == model.MetricNameLabel {
				return
			}
			attrs.PutStr(lbl.Name, lbl.Value)
		})
		if rm, ok := t.resources[rKey]; ok {
			res.CopyTo(rm.Resource())
		}
		return ref, nil
	}

	if rawName == prometheus.ScopeInfoMetricName && !t.ignoreScopeInfoMetric {
		scope := scopeID{}
		attrs := pcommon.NewMap()
		ls.Range(func(lbl labels.Label) {
			if lbl.Name == model.JobLabel || lbl.Name == model.InstanceLabel || lbl.Name == model.MetricNameLabel {
				return
			}
			switch lbl.Name {
			case prometheus.ScopeNameLabelKey:
				scope.name = lbl.Value
			case prometheus.ScopeVersionLabelKey:
				scope.version = lbl.Value
			case prometheus.ScopeSchemaURLLabelKey:
				scope.schemaURL = lbl.Value
			default:
				attrs.PutStr(lbl.Name, lbl.Value)
			}
		})

		if attrs.Len() > 0 {
			if t.scopeAttributes == nil {
				t.scopeAttributes = make(map[resourceKey]map[scopeID]pcommon.Map)
			}
			if _, ok := t.scopeAttributes[rKey]; !ok {
				t.scopeAttributes[rKey] = make(map[scopeID]pcommon.Map)
			}
			t.scopeAttributes[rKey][scope] = attrs
			if smMap, ok := t.scopes[rKey]; ok {
				if sm, ok2 := smMap[scope]; ok2 {
					attrs.CopyTo(sm.Scope().Attributes())
				}
			}
		}
		return ref, nil
	}

	isCreatedLine := false
	if strings.HasSuffix(rawName, metricSuffixCreated) && t.useMetadata {
		baseName := strings.TrimSuffix(rawName, metricSuffixCreated)
		if opts.MetricFamilyName != "" {
			mfBase := strings.TrimSuffix(strings.TrimSuffix(opts.MetricFamilyName, metricSuffixTotal), metricSuffixCreated)
			if baseName == mfBase {
				if opts.Metadata.Type == model.MetricTypeCounter || opts.Metadata.Type == model.MetricTypeHistogram || opts.Metadata.Type == model.MetricTypeSummary || t.classicHistFamilies[baseName] {
					isCreatedLine = true
				}
			}
		} else if opts.Metadata.Type == model.MetricTypeCounter || opts.Metadata.Type == model.MetricTypeHistogram || opts.Metadata.Type == model.MetricTypeSummary || t.classicHistFamilies[baseName] {
			isCreatedLine = true
		} else if t.mc != nil {
			if md, ok := t.mc.GetMetadata(baseName); ok && (md.Type == model.MetricTypeCounter || md.Type == model.MetricTypeHistogram || md.Type == model.MetricTypeSummary) {
				isCreatedLine = true
			} else if md, ok := t.mc.GetMetadata(baseName + metricSuffixTotal); ok && md.Type == model.MetricTypeCounter {
				isCreatedLine = true
			}
		}
	}
	if isCreatedLine {
		baseName := strings.TrimSuffix(rawName, metricSuffixCreated)
		hash := t.getSeriesRef(ls, pmetric.MetricTypeSum)
		key := dataPointKey{rKey: rKey, baseName: baseName, hash: hash}
		ts := timestampFromFloat64(val)
		if t.createdTimestamps == nil {
			t.createdTimestamps = make(map[dataPointKey]pcommon.Timestamp)
		}
		t.createdTimestamps[key] = ts
		for mKey, m := range t.metrics {
			if mKey.rKey != rKey {
				continue
			}
			if mKey.metricName != baseName && strings.TrimSuffix(mKey.metricName, "_total") != baseName {
				continue
			}
			switch mKey.metricType {
			case pmetric.MetricTypeSum:
				dps := m.Sum().DataPoints()
				for i := 0; i < dps.Len(); i++ {
					dp := dps.At(i)
					if dp.StartTimestamp() == 0 && attributesMatchLabels(dp.Attributes(), ls, mKey.metricType) {
						dp.SetStartTimestamp(ts)
					}
				}
			case pmetric.MetricTypeHistogram:
				dps := m.Histogram().DataPoints()
				for i := 0; i < dps.Len(); i++ {
					dp := dps.At(i)
					if dp.StartTimestamp() == 0 && attributesMatchLabels(dp.Attributes(), ls, mKey.metricType) {
						dp.SetStartTimestamp(ts)
					}
				}
			case pmetric.MetricTypeExponentialHistogram:
				dps := m.ExponentialHistogram().DataPoints()
				for i := 0; i < dps.Len(); i++ {
					dp := dps.At(i)
					if dp.StartTimestamp() == 0 && attributesMatchLabels(dp.Attributes(), ls, mKey.metricType) {
						dp.SetStartTimestamp(ts)
					}
				}
			}
		}
		return ref, nil
	}

	scope, attrs := getScopeID(ls)
	if attrs != (pcommon.Map{}) && attrs.Len() > 0 {
		if t.scopeAttributes == nil {
			t.scopeAttributes = make(map[resourceKey]map[scopeID]pcommon.Map)
		}
		if _, ok := t.scopeAttributes[rKey]; !ok {
			t.scopeAttributes[rKey] = make(map[scopeID]pcommon.Map)
		}
		if _, exists := t.scopeAttributes[rKey][scope]; !exists {
			copied := pcommon.NewMap()
			attrs.CopyTo(copied)
			t.scopeAttributes[rKey][scope] = copied
			if scopes, rOk := t.scopes[rKey]; rOk {
				if sm, sOk := scopes[scope]; sOk && sm.Scope().Attributes().Len() == 0 {
					copied.CopyTo(sm.Scope().Attributes())
				}
			}
		}
	}

	mType := model.MetricTypeUnknown
	mHelp := ""
	mUnit := ""
	if im, ok := internalMetricMetadata[rawName]; ok {
		mType = im.Type
		mHelp = im.Help
		mUnit = im.Unit
	} else if t.useMetadata && (opts.Metadata.Type != "" || opts.Metadata.Help != "" || opts.Metadata.Unit != "") {
		mType = opts.Metadata.Type
		mHelp = opts.Metadata.Help
		mUnit = opts.Metadata.Unit
	} else if t.useMetadata && t.mc != nil {
		md, _ := metadataForMetric(rawName, t.mc)
		mType = md.Type
		mHelp = md.Help
		mUnit = md.Unit
	}

	if mType == "" {
		mType = model.MetricTypeUnknown
	}

	mtype := pmetric.MetricTypeGauge
	isMonotonic := false
	if h != nil || fh != nil {
		mType = model.MetricTypeHistogram
		if (h != nil && h.Schema == -53) || (fh != nil && fh.Schema == -53) {
			mtype = pmetric.MetricTypeHistogram
			isMonotonic = true
			if opts.MetricFamilyName != "" && !t.classicHistFamilies[opts.MetricFamilyName] {
				if t.classicHistFamilies == nil {
					t.classicHistFamilies = make(map[string]bool)
				}
				t.classicHistFamilies[strings.Clone(opts.MetricFamilyName)] = true
			}
			normName := normalizeMetricName(rawName)
			if !t.classicHistFamilies[normName] {
				if t.classicHistFamilies == nil {
					t.classicHistFamilies = make(map[string]bool)
				}
				t.classicHistFamilies[normName] = true
			}
		} else {
			mtype = pmetric.MetricTypeExponentialHistogram
			isMonotonic = true
		}
	} else if mType != model.MetricTypeGaugeHistogram && (mType == model.MetricTypeHistogram || t.classicHistFamilies[normalizeMetricName(rawName)] || (opts.MetricFamilyName != "" && t.classicHistFamilies[opts.MetricFamilyName]) || strings.HasSuffix(rawName, "_bucket")) && h == nil && fh == nil {
		if strings.HasSuffix(rawName, "_bucket") {
			if t.classicHistFamilies == nil {
				t.classicHistFamilies = make(map[string]bool)
			}
			t.classicHistFamilies[strings.TrimSuffix(rawName, "_bucket")] = true
		}
		if value.IsStaleNaN(val) && !strings.HasSuffix(rawName, "_bucket") && !strings.HasSuffix(rawName, "_sum") && !strings.HasSuffix(rawName, "_count") {
			if kt, ok := t.knownMetricTypes.Load(knownMetricTypeKey{rKey: rKey, metricName: normalizeMetricName(rawName)}); ok {
				mtype = kt.(pmetric.MetricType)
			} else {
				mtype = pmetric.MetricTypeHistogram
				if target, tok := scrape.TargetFromContext(t.ctx); tok {
					if target.DiscoveredLabels(labels.NewBuilder(labels.EmptyLabels())).Get("__scrape_native_histograms__") == "true" {
						mtype = pmetric.MetricTypeExponentialHistogram
					}
				}
			}
			isMonotonic = true
		} else {
			if strings.HasSuffix(rawName, "_bucket") {
				if _, err := getBoundary(pmetric.MetricTypeHistogram, ls); err != nil {
					t.logger.Info("failed to add datapoint", zap.Error(err), zap.String("metric_name", rawName))
				}
			}
			if t.classicHistTimestamps == nil {
				t.classicHistTimestamps = make(map[uint64]int64)
			}
			hash := t.getSeriesRef(ls, mtype)
			if prevTs, ok := t.classicHistTimestamps[hash]; ok && prevTs != atMs {
				t.logger.Info("failed to add datapoint", zap.Error(errors.New("timestamps are different")), zap.String("metric_name", rawName))
			} else {
				t.classicHistTimestamps[hash] = atMs
			}
			// Classical histogram scalar component (_bucket, _sum, _count) or incomplete histogram.
			// When NHCB is active, the histogram is already emitted via NHCB, or invalid/incomplete.
			// Do not emit as a separate metric.
			return ref, nil
		}
	} else {
		mtype, isMonotonic = convToMetricType(mType, false)
		if mtype == pmetric.MetricTypeEmpty {
			mtype = pmetric.MetricTypeGauge
		}
	}

	if (h != nil && h.Schema == -53) || (fh != nil && fh.Schema == -53) {
		isStale := value.IsStaleNaN(val) || (h != nil && value.IsStaleNaN(h.Sum)) || (fh != nil && value.IsStaleNaN(fh.Sum))
		if !isStale && !validateNHCB(h, fh) {
			return ref, nil
		}
	}

	cleanName := rawName
	if t.trimSuffixes {
		if opts.MetricFamilyName != "" {
			mfName := opts.MetricFamilyName
			if idx := strings.IndexByte(mfName, 255); idx >= 0 {
				mfName = mfName[:idx]
			}
			cleanName = strings.Clone(prometheus.TrimPromSuffixes(mfName, mtype, mUnit))
		} else if t.mc != nil {
			if _, ok := t.mc.GetMetadata(rawName); !ok {
				if mtype == pmetric.MetricTypeSummary {
					cleanName = normalizeSummaryName(rawName)
				} else {
					cleanName = normalizeMetricName(rawName)
				}
			}
			cleanName = prometheus.TrimPromSuffixes(cleanName, mtype, mUnit)
		} else {
			if mtype == pmetric.MetricTypeSummary {
				cleanName = prometheus.TrimPromSuffixes(normalizeSummaryName(rawName), mtype, mUnit)
			} else {
				cleanName = prometheus.TrimPromSuffixes(normalizeMetricName(rawName), mtype, mUnit)
			}
		}
	} else {
		if mtype == pmetric.MetricTypeSummary {
			cleanName = normalizeSummaryName(rawName)
		} else {
			cleanName = rawName
		}
	}
	if mType == model.MetricTypeInfo {
		cleanName = strings.TrimSuffix(cleanName, "_info")
	}

	mKey := metricKey{rKey: rKey, scope: scope, metricType: mtype, metricName: cleanName}
	m, ok := t.metrics[mKey]
	if !ok {
		if h != nil || fh != nil {
			kKey := knownMetricTypeKey{rKey: rKey, metricName: normalizeMetricName(rawName)}
			if _, loaded := t.knownMetricTypes.Load(kKey); !loaded {
				t.knownMetricTypes.Store(kKey, mtype)
			}
		}
		if _, rok := t.resources[rKey]; !rok {
			rm := t.md.ResourceMetrics().AppendEmpty()
			res, hasRes := t.nodeResources[rKey]
			if !hasRes {
				if target, tok := scrape.TargetFromContext(t.ctx); tok {
					res = CreateResource(rKey.job, rKey.instance, target.DiscoveredLabels(labels.NewBuilder(labels.EmptyLabels())))
				} else {
					res = CreateResource(rKey.job, rKey.instance, labels.EmptyLabels())
				}
				t.nodeResources[rKey] = res
			}
			res.CopyTo(rm.Resource())
			t.resources[rKey] = rm
		}
		if _, sok := t.scopes[rKey]; !sok {
			t.scopes[rKey] = make(map[scopeID]pmetric.ScopeMetrics)
		}
		sm, sok := t.scopes[rKey][scope]
		if !sok {
			rm := t.resources[rKey]
			sm = rm.ScopeMetrics().AppendEmpty()
			if scope == emptyScopeID {
				sm.Scope().SetName(mdata.ScopeName)
				sm.Scope().SetVersion(t.buildInfo.Version)
			} else {
				sm.Scope().SetName(scope.name)
				sm.Scope().SetVersion(scope.version)
				if scope.schemaURL != "" {
					sm.SetSchemaUrl(scope.schemaURL)
				}
				if scopeAttrs, exists := t.scopeAttributes[rKey]; exists {
					if sattrs, ok2 := scopeAttrs[scope]; ok2 {
						sattrs.CopyTo(sm.Scope().Attributes())
					}
				}
			}
			t.scopes[rKey][scope] = sm
		}
		m = sm.Metrics().AppendEmpty()
		m.SetName(cleanName)
		m.SetDescription(mHelp)
		m.SetUnit(prometheus.UnitWordToUCUM(mUnit))
		m.Metadata().PutStr(prometheus.MetricMetadataTypeKey, string(mType))

		switch mtype {
		case pmetric.MetricTypeSum:
			sum := m.SetEmptySum()
			sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			sum.SetIsMonotonic(isMonotonic)
		case pmetric.MetricTypeGauge:
			m.SetEmptyGauge()
		case pmetric.MetricTypeHistogram:
			hist := m.SetEmptyHistogram()
			hist.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		case pmetric.MetricTypeExponentialHistogram:
			eh := m.SetEmptyExponentialHistogram()
			eh.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		case pmetric.MetricTypeSummary:
			// No logic
		}
		t.metrics[mKey] = m
	}

	if mtype == pmetric.MetricTypeSummary {
		if t.summaryAccumulator == nil {
			t.summaryAccumulator = &summaryAccumulator{
				summaries:   make(map[metricKey]map[uint64]*summaryGroup),
				groupHashes: make(map[metricKey][]uint64),
			}
		}
		if _, ok := t.summaryAccumulator.summaries[mKey]; !ok {
			t.summaryAccumulator.summaries[mKey] = make(map[uint64]*summaryGroup)
			t.summaryAccumulator.groupHashes[mKey] = nil
		}
		hash := t.getSeriesRef(ls, mtype)
		sg, ok := t.summaryAccumulator.summaries[mKey][hash]
		if !ok {
			sg = &summaryGroup{
				ls:   ls,
				atMs: atMs,
				stMs: stMs,
			}
			t.summaryAccumulator.summaries[mKey][hash] = sg
			t.summaryAccumulator.groupHashes[mKey] = append(t.summaryAccumulator.groupHashes[mKey], hash)
		} else if sg.atMs != atMs {
			t.logger.Info("failed to add datapoint", zap.Error(errors.New("timestamps are different")), zap.String("metric_name", rawName))
			return ref, nil
		}
		if rawName == cleanName+"_sum" {
			sg.hasSum = true
			if value.IsStaleNaN(val) {
				sg.isStale = true
			} else {
				sg.sum = val
			}
		} else if rawName == cleanName+"_count" {
			sg.hasCount = true
			if value.IsStaleNaN(val) {
				sg.isStale = true
			} else {
				sg.count = val
			}
		} else {
			q, err := getBoundary(mtype, ls)
			if err != nil {
				t.logger.Info("failed to add datapoint", zap.Error(err), zap.String("metric_name", rawName))
				return ref, nil
			}
			if value.IsStaleNaN(val) {
				sg.isStale = true
			}
			sg.quantiles = append(sg.quantiles, quantileValue{quantile: q, value: val})
		}
		t.lastLabels = ls
		t.lastMType = pmetric.MetricTypeSummary
		t.hasLastDpExemplars = true
		return ref, nil
	}

	var dpExemplars pmetric.ExemplarSlice
	if mtype == pmetric.MetricTypeSum {
		dp := m.Sum().DataPoints().AppendEmpty()
		dp.SetTimestamp(timestampFromMs(atMs))
		if stMs != 0 {
			dp.SetStartTimestamp(timestampFromMs(stMs))
		} else if len(t.createdTimestamps) > 0 {
			baseName := strings.TrimSuffix(cleanName, "_total")
			hash := t.getSeriesRef(ls, mtype)
			if ts, ok := t.createdTimestamps[dataPointKey{rKey: rKey, baseName: baseName, hash: hash}]; ok {
				dp.SetStartTimestamp(ts)
			}
		}
		if value.IsStaleNaN(val) {
			dp.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))
		} else {
			dp.SetDoubleValue(val)
		}
		t.populateAttributes(dp.Attributes(), ls, mtype)
		dpExemplars = dp.Exemplars()
	} else if mtype == pmetric.MetricTypeGauge {
		dp := m.Gauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(timestampFromMs(atMs))
		if stMs != 0 {
			dp.SetStartTimestamp(timestampFromMs(stMs))
		}
		if value.IsStaleNaN(val) {
			dp.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))
		} else {
			dp.SetDoubleValue(val)
		}
		t.populateAttributes(dp.Attributes(), ls, mtype)
		dpExemplars = dp.Exemplars()
	} else if mtype == pmetric.MetricTypeHistogram {
		dp := m.Histogram().DataPoints().AppendEmpty()
		dp.SetTimestamp(timestampFromMs(atMs))
		if stMs != 0 {
			dp.SetStartTimestamp(timestampFromMs(stMs))
		} else if len(t.createdTimestamps) > 0 {
			hash := t.getSeriesRef(ls, mtype)
			if ts, ok := t.createdTimestamps[dataPointKey{rKey: rKey, baseName: cleanName, hash: hash}]; ok {
				dp.SetStartTimestamp(ts)
			}
		}
		isStale := value.IsStaleNaN(val) || (h != nil && value.IsStaleNaN(h.Sum)) || (fh != nil && value.IsStaleNaN(fh.Sum))
		if isStale {
			dp.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))
			if h != nil {
				if len(h.CustomValues) > 0 {
					dp.ExplicitBounds().FromRaw(h.CustomValues)
				}
				populateZeroBuckets(len(h.CustomValues)+1, dp.BucketCounts())
			} else if fh != nil {
				if len(fh.CustomValues) > 0 {
					dp.ExplicitBounds().FromRaw(fh.CustomValues)
				}
				populateZeroBuckets(len(fh.CustomValues)+1, dp.BucketCounts())
			}
		} else {
			if h != nil {
				dp.SetCount(h.Count)
				if !math.IsNaN(h.Sum) {
					dp.SetSum(h.Sum)
				}
				if len(h.CustomValues) > 0 {
					dp.ExplicitBounds().FromRaw(h.CustomValues)
				}
				populateNHCBDeltaBuckets(h, dp.BucketCounts())
			} else if fh != nil {
				dp.SetCount(uint64(fh.Count))
				if !math.IsNaN(fh.Sum) {
					dp.SetSum(fh.Sum)
				}
				if len(fh.CustomValues) > 0 {
					dp.ExplicitBounds().FromRaw(fh.CustomValues)
				}
				populateNHCBAbsoluteBuckets(fh, dp.BucketCounts())
			}
		}
		t.populateAttributes(dp.Attributes(), ls, mtype)
		dpExemplars = dp.Exemplars()
	} else if mtype == pmetric.MetricTypeExponentialHistogram {
		dp := m.ExponentialHistogram().DataPoints().AppendEmpty()
		dp.SetTimestamp(timestampFromMs(atMs))
		if stMs != 0 {
			dp.SetStartTimestamp(timestampFromMs(stMs))
		} else if len(t.createdTimestamps) > 0 {
			hash := t.getSeriesRef(ls, mtype)
			if ts, ok := t.createdTimestamps[dataPointKey{rKey: rKey, baseName: cleanName, hash: hash}]; ok {
				dp.SetStartTimestamp(ts)
			}
		}
		isStale := value.IsStaleNaN(val) || (h != nil && value.IsStaleNaN(h.Sum)) || (fh != nil && value.IsStaleNaN(fh.Sum))
		if isStale {
			dp.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))
		} else {
			if h != nil {
				dp.SetCount(h.Count)
				if !math.IsNaN(h.Sum) {
					dp.SetSum(h.Sum)
				}
				dp.SetZeroThreshold(h.ZeroThreshold)
				dp.SetZeroCount(h.ZeroCount)
				dp.SetScale(h.Schema)

				if len(h.PositiveSpans) > 0 {
					dp.Positive().SetOffset(h.PositiveSpans[0].Offset - 1)
					convertDeltaBuckets(h.PositiveSpans, h.PositiveBuckets, dp.Positive().BucketCounts())
				}
				if len(h.NegativeSpans) > 0 {
					dp.Negative().SetOffset(h.NegativeSpans[0].Offset - 1)
					convertDeltaBuckets(h.NegativeSpans, h.NegativeBuckets, dp.Negative().BucketCounts())
				}
			} else if fh != nil {
				dp.SetCount(uint64(fh.Count))
				if !math.IsNaN(fh.Sum) {
					dp.SetSum(fh.Sum)
				}
				dp.SetZeroThreshold(fh.ZeroThreshold)
				dp.SetZeroCount(uint64(fh.ZeroCount))
				dp.SetScale(fh.Schema)

				if len(fh.PositiveSpans) > 0 {
					dp.Positive().SetOffset(fh.PositiveSpans[0].Offset - 1)
					convertAbsoluteBuckets(fh.PositiveSpans, fh.PositiveBuckets, dp.Positive().BucketCounts())
				}
				if len(fh.NegativeSpans) > 0 {
					dp.Negative().SetOffset(fh.NegativeSpans[0].Offset - 1)
					convertAbsoluteBuckets(fh.NegativeSpans, fh.NegativeBuckets, dp.Negative().BucketCounts())
				}
			}
		}
		t.populateAttributes(dp.Attributes(), ls, mtype)
		dpExemplars = dp.Exemplars()
	}

	t.lastDpExemplars = dpExemplars
	t.lastLabels = ls
	t.lastMType = mtype
	t.hasLastDpExemplars = true

	for _, ex := range opts.Exemplars {
		dt := dpExemplars.AppendEmpty()
		dt.SetTimestamp(timestampFromMs(ex.Ts))
		dt.SetDoubleValue(ex.Value)
		populateExemplar(dt, ex)
	}

	return ref, nil
}

func (t *transaction) AppendExemplar(ref storage.SeriesRef, l labels.Labels, ex exemplar.Exemplar) (storage.SeriesRef, error) {
	var dpExemplars pmetric.ExemplarSlice
	if t.hasLastDpExemplars && labels.Equal(l, t.lastLabels) {
		if t.lastMType == pmetric.MetricTypeSummary {
			return ref, nil
		}
		dpExemplars = t.lastDpExemplars
	} else {
		var ok bool
		dpExemplars, ok = t.findExemplarSliceSlow(l)
		if !ok {
			return ref, nil
		}
	}
	dt := dpExemplars.AppendEmpty()
	dt.SetTimestamp(timestampFromMs(ex.Ts))
	dt.SetDoubleValue(ex.Value)
	populateExemplar(dt, ex)
	return ref, nil
}

func (t *transaction) findExemplarSliceSlow(l labels.Labels) (pmetric.ExemplarSlice, bool) {
	hash := t.getSeriesRef(l, pmetric.MetricTypeEmpty)
	if t.exemplarMap != nil {
		if slice, ok := t.exemplarMap[hash]; ok {
			return slice, true
		}
	}
	rKey, err := t.getJobAndInstance(l)
	if err != nil {
		return pmetric.ExemplarSlice{}, false
	}
	scope, _ := getScopeID(l)
	rawName := l.Get(model.MetricNameLabel)
	for mKey, m := range t.metrics {
		if mKey.rKey != rKey || mKey.scope != scope {
			continue
		}
		if mKey.metricName != rawName && mKey.metricName != normalizeMetricName(rawName) && mKey.metricName != strings.TrimSuffix(rawName, "_total") {
			continue
		}
		mtype := m.Type()
		var matchedSlice pmetric.ExemplarSlice
		var found bool
		switch mtype {
		case pmetric.MetricTypeSum:
			dps := m.Sum().DataPoints()
			for i := 0; i < dps.Len(); i++ {
				dp := dps.At(i)
				if attributesMatchLabels(dp.Attributes(), l, mtype) {
					matchedSlice = dp.Exemplars()
					found = true
					break
				}
			}
		case pmetric.MetricTypeGauge:
			dps := m.Gauge().DataPoints()
			for i := 0; i < dps.Len(); i++ {
				dp := dps.At(i)
				if attributesMatchLabels(dp.Attributes(), l, mtype) {
					matchedSlice = dp.Exemplars()
					found = true
					break
				}
			}
		case pmetric.MetricTypeHistogram:
			dps := m.Histogram().DataPoints()
			for i := 0; i < dps.Len(); i++ {
				dp := dps.At(i)
				if attributesMatchLabels(dp.Attributes(), l, mtype) {
					matchedSlice = dp.Exemplars()
					found = true
					break
				}
			}
		case pmetric.MetricTypeExponentialHistogram:
			dps := m.ExponentialHistogram().DataPoints()
			for i := 0; i < dps.Len(); i++ {
				dp := dps.At(i)
				if attributesMatchLabels(dp.Attributes(), l, mtype) {
					matchedSlice = dp.Exemplars()
					found = true
					break
				}
			}
		}
		if found {
			if t.exemplarMap == nil {
				t.exemplarMap = make(map[uint64]pmetric.ExemplarSlice, 64)
			}
			t.exemplarMap[hash] = matchedSlice
			return matchedSlice, true
		}
	}
	return pmetric.ExemplarSlice{}, false
}

func (t *transaction) Commit() error {
	if t.summaryAccumulator != nil {
		for mKey, sumGroupMap := range t.summaryAccumulator.summaries {
			m, ok := t.metrics[mKey]
			if !ok {
				continue
			}
			sum := m.SetEmptySummary()
			for _, sgHash := range t.summaryAccumulator.groupHashes[mKey] {
				sg := sumGroupMap[sgHash]
				if !sg.hasCount {
					continue
				}
				dp := sum.DataPoints().AppendEmpty()
				dp.SetTimestamp(timestampFromMs(sg.atMs))
				if sg.stMs != 0 {
					dp.SetStartTimestamp(timestampFromMs(sg.stMs))
				} else if len(t.createdTimestamps) > 0 {
					key := dataPointKey{rKey: mKey.rKey, baseName: mKey.metricName, hash: sgHash}
					if ts, ok := t.createdTimestamps[key]; ok {
						dp.SetStartTimestamp(ts)
					}
				}
				if sg.isStale {
					dp.SetFlags(pmetric.DefaultDataPointFlags.WithNoRecordedValue(true))
				} else {
					dp.SetCount(uint64(sg.count))
					if sg.hasSum {
						dp.SetSum(sg.sum)
					}
				}
				t.populateAttributes(dp.Attributes(), sg.ls, pmetric.MetricTypeSummary)

				qVals := dp.QuantileValues()
				sort.Slice(sg.quantiles, func(i, j int) bool { return sg.quantiles[i].quantile < sg.quantiles[j].quantile })
				qVals.EnsureCapacity(len(sg.quantiles))
				for _, qv := range sg.quantiles {
					qDp := qVals.AppendEmpty()
					qDp.SetQuantile(qv.quantile)
					if !sg.isStale {
						qDp.SetValue(qv.value)
					}
				}
			}
		}
	}
	if len(t.classicHistFamilies) > 0 || t.summaryAccumulator != nil {
		for _, rm := range t.resources {
			sms := rm.ScopeMetrics()
			for i := 0; i < sms.Len(); i++ {
				sm := sms.At(i)
				sm.Metrics().RemoveIf(func(m pmetric.Metric) bool {
					if len(t.classicHistFamilies) > 0 && (m.Type() == pmetric.MetricTypeGauge || m.Type() == pmetric.MetricTypeSum) {
						if strings.HasSuffix(m.Name(), "_count") && t.classicHistFamilies[strings.TrimSuffix(m.Name(), "_count")] {
							return true
						}
						if strings.HasSuffix(m.Name(), "_sum") && t.classicHistFamilies[strings.TrimSuffix(m.Name(), "_sum")] {
							return true
						}
						if strings.HasSuffix(m.Name(), "_bucket") && t.classicHistFamilies[strings.TrimSuffix(m.Name(), "_bucket")] {
							return true
						}
					}
					switch m.Type() {
					case pmetric.MetricTypeGauge:
						return m.Gauge().DataPoints().Len() == 0
					case pmetric.MetricTypeSum:
						return m.Sum().DataPoints().Len() == 0
					case pmetric.MetricTypeHistogram:
						return m.Histogram().DataPoints().Len() == 0
					case pmetric.MetricTypeExponentialHistogram:
						return m.ExponentialHistogram().DataPoints().Len() == 0
					case pmetric.MetricTypeSummary:
						return m.Summary().DataPoints().Len() == 0
					default:
						return true
					}
				})
			}
		}

		t.md.ResourceMetrics().RemoveIf(func(metrics pmetric.ResourceMetrics) bool {
			if metrics.ScopeMetrics().Len() == 0 {
				return true
			}
			remove := true
			for i := 0; i < metrics.ScopeMetrics().Len(); i++ {
				if metrics.ScopeMetrics().At(i).Metrics().Len() > 0 {
					remove = false
					break
				}
			}
			return remove
		})
	}

	numPoints := t.md.DataPointCount()
	if numPoints == 0 {
		return nil
	}

	ctx := t.obsrecv.StartMetricsOp(t.ctx)
	err := t.sink.ConsumeMetrics(ctx, t.md)
	t.obsrecv.EndMetricsOp(ctx, dataformat, numPoints, err)
	return err
}

func (t *transaction) Rollback() error {
	t.md = pmetric.NewMetrics()
	t.resources = make(map[resourceKey]pmetric.ResourceMetrics)
	t.scopes = make(map[resourceKey]map[scopeID]pmetric.ScopeMetrics)
	t.metrics = make(map[metricKey]pmetric.Metric)
	t.nodeResources = make(map[resourceKey]pcommon.Resource)
	t.scopeAttributes = nil
	t.summaryAccumulator = nil
	t.createdTimestamps = nil
	t.classicHistFamilies = nil
	t.classicHistTimestamps = nil
	return nil
}

