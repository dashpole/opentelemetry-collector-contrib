// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tailsamplingprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"math/big"
	"regexp"
	"slices"
	"time"

	"github.com/golang/groupcache/lru"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	tracesdk "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/filter/filterottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspan"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottlspanevent"
	pkgsampling "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/sampling"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/sampling"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/pkg/samplingpolicy"
)

// ivmState is the 5-state incremental Kleene lattice value for each circuit node.
type ivmState uint8

const (
	ivmStateUnset    ivmState = 0
	ivmStateDefTrue  ivmState = 1 // Permanently true for all future trace extensions.
	ivmStateDefFalse ivmState = 2 // Permanently false for all future trace extensions.
	ivmStateDefError ivmState = 3 // Evaluation errored on a span or span event.
	ivmStateMayTrue  ivmState = 4 // Currently true if window closed now; may still change.
	ivmStateMayFalse ivmState = 5 // Currently false if window closed now; may still change.
)

func (s ivmState) isDefinite() bool {
	return s == ivmStateDefTrue || s == ivmStateDefFalse || s == ivmStateDefError
}

type ivmNodeKind uint8

const (
	ivmKindAlwaysSample ivmNodeKind = iota
	ivmKindStatusCode
	ivmKindStringAttr
	ivmKindNumericAttr
	ivmKindBoolAttr
	ivmKindLatency
	ivmKindSpanCount
	ivmKindProbabilistic
	ivmKindTraceState
	ivmKindTraceFlags
	ivmKindOTTL
	ivmKindNot
	ivmKindAnd
	ivmKindDrop
	ivmKindOpaque // rate_limiting, bytes_limiting, composite, or custom extension
)

const (
	ivmDefaultRegexCacheSize = 128
	ivmDefaultHashSalt       = "default-hash-seed"
	ivmNoSlot                = uint32(math.MaxUint32)

	ivmTSFlagHasSamplingInfo uint8 = 1 << 0
	ivmTSFlagHasRandomness   uint8 = 1 << 1
)

// ivmNode is a compiled node inside an ivmCircuit. Nodes are stored in post-order
// (every child index is strictly less than its parent index) so propagation is a
// single flat loop with zero recursion.
type ivmNode struct {
	kind          ivmNodeKind
	policyIdx     int // Index in tsp.policies (-1 for non-top-level nodes)
	hasSideEffect bool
	canError      bool
	isExtension   bool
	invertMatch   bool
	minThreshold  pkgsampling.Threshold

	// Combinator wiring.
	child    int   // Used by ivmKindNot.
	children []int // Used by ivmKindAnd and ivmKindDrop.

	// Leaf parameters.
	statusCodes      []ptrace.StatusCode
	attrKey          string
	strExactValues   map[string]struct{}
	strRegexes       []*regexp.Regexp
	strRegexCache    *lru.Cache
	strRegexEnabled  bool
	numMin           int64
	numMax           int64
	boolVal          bool
	latThresholdMs   int64
	latUpperMs       int64
	spanMin          int64
	spanMax          int64
	probHashSalt     string
	probFnvThreshold uint64
	probOtelTh       pkgsampling.Threshold
	probPct          float64
	tsValues         map[string]struct{}
	ottlSpanExpr     *ottl.ConditionSequence[*ottlspan.TransformContext]
	ottlEventExpr    *ottl.ConditionSequence[*ottlspanevent.TransformContext]
	opaqueEval       samplingpolicy.ThresholdEvaluator
}

// ivmCircuit is the immutable compiled representation of a shard's policy list.
type ivmCircuit struct {
	version uint32
	nodes   []ivmNode

	topDropNodes   []int // Node indices for tsp.policies[:numDropPolicies], in config order.
	topSampleNodes []int // Node indices for tsp.policies[numDropPolicies:], in config order.
	numDrop        int

	// Grouped leaf indices for fast, minimal hot-path iteration.
	resourceNodes  []int // string/numeric/bool attr nodes evaluated once per ResourceSpans.
	spanNodes      []int // status/string/numeric/bool/trace_state/trace_flags/ottl nodes.
	dropSpanNodes  []int // subset of spanNodes reachable from topDropNodes (for drop-only deferral).
	aggregateNodes []int // latency / span_count nodes updated from scalar counters.
	probNodes      []int // probabilistic nodes (traceID + optional tracestate).
	combinators    []int // not / and / drop nodes in post-order.

	// Precomputed recursive capability flags.
	useGlobalTraceState    bool
	needsTimestamps        bool
	needsTracestateScan    bool
	hasAnyBatchEvaluator   bool
	hasAnyExtension        bool
	hasAnyStatefulDrop     bool
	hasAnyStatefulPositive bool
	hasAnyOpaque           bool
	hasDropPolicies        bool
	// positiveEvaluators is true when injected policies (withPolicies) have no PolicyCfgs.
	fallbackOnly bool

	// Reusable scratch mask for resource-level matches per ResourceSpans (length = len(nodes)).
	resMatchScratch []uint8
}

type ivmCompileError struct {
	policy string
	err    error
}

func (e *ivmCompileError) Error() string {
	return fmt.Sprintf("failed to compile IVM circuit for policy %q: %v", e.policy, e.err)
}

func compileIVMCircuit(
	settings component.TelemetrySettings,
	cfgs []PolicyCfg,
	policies []*policy,
	version uint32,
) (*ivmCircuit, error) {
	c := &ivmCircuit{
		version:             version,
		useGlobalTraceState: metadata.ProcessorTailsamplingprocessorUsetracestateFeatureGate.IsEnabled(),
	}

	// Fallback helper: if policies were injected directly via withPolicies(...)
	// (causing len(cfgs) == 0 or mismatch against policies), operate safely in
	// full evaluator fallback mode.
	setFallback := func() *ivmCircuit {
		c.fallbackOnly = true
		c.hasAnyOpaque = true
		c.numDrop = 0
		c.hasDropPolicies = false
		c.hasAnyStatefulDrop = false
		c.hasAnyStatefulPositive = false
		c.hasAnyBatchEvaluator = false
		for _, p := range policies {
			if p.isDrop {
				c.numDrop++
				c.hasDropPolicies = true
				c.hasAnyStatefulDrop = true
			} else {
				c.hasAnyStatefulPositive = true
			}
			if _, ok := p.evaluator.(sampling.BatchEvaluator); ok {
				c.hasAnyBatchEvaluator = true
			}
		}
		return c
	}

	if len(cfgs) != len(policies) {
		return setFallback(), nil
	}
	if len(cfgs) == 0 {
		return c, nil
	}

	// Build a lookup from policy name -> index in tsp.policies (which has drop
	// policies partitioned before positive policies while preserving relative order).
	policyIdxByName := make(map[string]int, len(policies))
	for i, p := range policies {
		policyIdxByName[p.name] = i
		if p.isDrop {
			c.numDrop++
			c.hasDropPolicies = true
		}
		if _, ok := p.evaluator.(sampling.BatchEvaluator); ok {
			c.hasAnyBatchEvaluator = true
		}
	}

	dropCfgs := make([]*PolicyCfg, 0, c.numDrop)
	sampleCfgs := make([]*PolicyCfg, 0, len(cfgs))
	for i := range cfgs {
		if _, ok := policyIdxByName[cfgs[i].Name]; !ok {
			return setFallback(), nil
		}
		if cfgs[i].Type == Drop {
			dropCfgs = append(dropCfgs, &cfgs[i])
		} else {
			sampleCfgs = append(sampleCfgs, &cfgs[i])
		}
	}

	for _, cfg := range dropCfgs {
		pIdx := policyIdxByName[cfg.Name]
		nodeIdx, err := c.compileTopPolicy(settings, cfg, policies[pIdx], pIdx)
		if err != nil {
			return nil, err
		}
		c.topDropNodes = append(c.topDropNodes, nodeIdx)
		if c.nodes[nodeIdx].hasSideEffect {
			c.hasAnyStatefulDrop = true
		}
	}
	for _, cfg := range sampleCfgs {
		pIdx := policyIdxByName[cfg.Name]
		nodeIdx, err := c.compileTopPolicy(settings, cfg, policies[pIdx], pIdx)
		if err != nil {
			return nil, err
		}
		c.topSampleNodes = append(c.topSampleNodes, nodeIdx)
		if c.nodes[nodeIdx].hasSideEffect {
			c.hasAnyStatefulPositive = true
		}
	}

	// Mark all nodes reachable from topDropNodes so deferred positive traces can
	// evaluate only drop-relevant span leaves when positive evaluation is settled.
	inDrop := make([]bool, len(c.nodes))
	var markDrop func(int)
	markDrop = func(idx int) {
		if idx < 0 || idx >= len(c.nodes) || inDrop[idx] {
			return
		}
		inDrop[idx] = true
		n := &c.nodes[idx]
		if n.kind == ivmKindNot {
			markDrop(n.child)
		}
		for _, ch := range n.children {
			markDrop(ch)
		}
	}
	for _, dIdx := range c.topDropNodes {
		markDrop(dIdx)
	}

	// Categorize nodes for tight hot-path loops.
	for i := range c.nodes {
		n := &c.nodes[i]
		switch n.kind {
		case ivmKindStringAttr, ivmKindNumericAttr, ivmKindBoolAttr:
			c.resourceNodes = append(c.resourceNodes, i)
			c.spanNodes = append(c.spanNodes, i)
			if inDrop[i] {
				c.dropSpanNodes = append(c.dropSpanNodes, i)
			}
		case ivmKindStatusCode, ivmKindTraceState, ivmKindTraceFlags, ivmKindOTTL:
			c.spanNodes = append(c.spanNodes, i)
			if inDrop[i] {
				c.dropSpanNodes = append(c.dropSpanNodes, i)
			}
		case ivmKindLatency:
			c.needsTimestamps = true
			c.aggregateNodes = append(c.aggregateNodes, i)
		case ivmKindSpanCount:
			c.aggregateNodes = append(c.aggregateNodes, i)
		case ivmKindProbabilistic:
			if c.useGlobalTraceState {
				c.needsTracestateScan = true
			}
			c.probNodes = append(c.probNodes, i)
		case ivmKindNot, ivmKindAnd, ivmKindDrop:
			c.combinators = append(c.combinators, i)
		case ivmKindOpaque:
			c.hasAnyOpaque = true
			if n.isExtension {
				c.hasAnyExtension = true
			}
		}
	}

	c.resMatchScratch = make([]uint8, len(c.nodes))
	return c, nil
}

func (c *ivmCircuit) addNode(n ivmNode) int {
	idx := len(c.nodes)
	c.nodes = append(c.nodes, n)
	return idx
}

func (c *ivmCircuit) compileTopPolicy(settings component.TelemetrySettings, cfg *PolicyCfg, pol *policy, policyIdx int) (int, error) {
	var idx int
	var err error
	switch cfg.Type {
	case Drop:
		idx, err = c.compileDrop(settings, &cfg.DropCfg, pol.evaluator)
	case And:
		idx, err = c.compileAnd(settings, &cfg.AndCfg, pol.evaluator)
	case Not:
		idx, err = c.compileNot(settings, &cfg.NotCfg)
	case Composite:
		hasExt := containsExtensionInComposite(&cfg.CompositeCfg)
		idx = c.addNode(ivmNode{
			kind:          ivmKindOpaque,
			policyIdx:     policyIdx,
			hasSideEffect: true,
			isExtension:   hasExt,
			minThreshold:  pkgsampling.AlwaysSampleThreshold,
			opaqueEval:    pol.evaluator,
		})
	default:
		idx, err = c.compileShared(settings, &cfg.sharedPolicyCfg, pol.evaluator)
	}
	if err != nil {
		return -1, &ivmCompileError{policy: cfg.Name, err: err}
	}
	c.nodes[idx].policyIdx = policyIdx
	if c.nodes[idx].kind == ivmKindOpaque && c.nodes[idx].opaqueEval == nil {
		c.nodes[idx].opaqueEval = pol.evaluator
	}
	return idx, nil
}

func containsExtensionInComposite(cfg *CompositeCfg) bool {
	for i := range cfg.SubPolicyCfg {
		sub := &cfg.SubPolicyCfg[i]
		if sub.Type == And {
			for j := range sub.AndCfg.SubPolicyCfg {
				if isExtensionType(sub.AndCfg.SubPolicyCfg[j].Type) {
					return true
				}
				if sub.AndCfg.SubPolicyCfg[j].Type == Not && isExtensionType(sub.AndCfg.SubPolicyCfg[j].NotCfg.SubPolicy.Type) {
					return true
				}
			}
		} else if isExtensionType(sub.Type) {
			return true
		}
	}
	return false
}

func isExtensionType(t PolicyType) bool {
	switch t {
	case AlwaysSample, Latency, NumericAttribute, Probabilistic, StatusCode,
		StringAttribute, RateLimiting, BytesLimiting, Composite, And, Not, Drop,
		SpanCount, TraceState, BooleanAttribute, OTTLCondition, TraceFlags:
		return false
	default:
		return true
	}
}

func (c *ivmCircuit) compileDrop(settings component.TelemetrySettings, cfg *DropCfg, topEval samplingpolicy.ThresholdEvaluator) (int, error) {
	startLen := len(c.nodes)
	children := make([]int, 0, len(cfg.SubPolicyCfg))
	hasSideEffect := false
	canError := false
	for i := range cfg.SubPolicyCfg {
		sub := &cfg.SubPolicyCfg[i]
		var childIdx int
		var err error
		if sub.Type == Not {
			childIdx, err = c.compileNot(settings, &sub.NotCfg)
		} else {
			childIdx, err = c.compileShared(settings, &sub.sharedPolicyCfg, nil)
		}
		if err != nil {
			return -1, err
		}
		if c.nodes[childIdx].hasSideEffect {
			hasSideEffect = true
		}
		if c.nodes[childIdx].canError {
			canError = true
		}
		children = append(children, childIdx)
	}
	if hasSideEffect {
		isExt := anyChildExtension(c.nodes, children)
		c.nodes = c.nodes[:startLen]
		return c.addNode(ivmNode{
			kind:          ivmKindOpaque,
			policyIdx:     -1,
			hasSideEffect: true,
			canError:      canError,
			isExtension:   isExt,
			minThreshold:  pkgsampling.AlwaysSampleThreshold,
			opaqueEval:    topEval,
		}), nil
	}
	return c.addNode(ivmNode{
		kind:         ivmKindDrop,
		policyIdx:    -1,
		canError:     canError,
		children:     children,
		minThreshold: pkgsampling.AlwaysSampleThreshold,
	}), nil
}

func (c *ivmCircuit) compileAnd(settings component.TelemetrySettings, cfg *AndCfg, topEval samplingpolicy.ThresholdEvaluator) (int, error) {
	startLen := len(c.nodes)
	children := make([]int, 0, len(cfg.SubPolicyCfg))
	hasSideEffect := false
	canError := false
	minTh := pkgsampling.AlwaysSampleThreshold
	for i := range cfg.SubPolicyCfg {
		sub := &cfg.SubPolicyCfg[i]
		var childIdx int
		var err error
		if sub.Type == Not {
			childIdx, err = c.compileNot(settings, &sub.NotCfg)
		} else {
			childIdx, err = c.compileShared(settings, &sub.sharedPolicyCfg, nil)
		}
		if err != nil {
			return -1, err
		}
		if c.nodes[childIdx].hasSideEffect {
			hasSideEffect = true
		}
		if c.nodes[childIdx].canError {
			canError = true
		}
		if pkgsampling.ThresholdGreater(c.nodes[childIdx].minThreshold, minTh) {
			minTh = c.nodes[childIdx].minThreshold
		}
		children = append(children, childIdx)
	}
	if hasSideEffect {
		isExt := anyChildExtension(c.nodes, children)
		c.nodes = c.nodes[:startLen]
		return c.addNode(ivmNode{
			kind:          ivmKindOpaque,
			policyIdx:     -1,
			hasSideEffect: true,
			canError:      canError,
			isExtension:   isExt,
			minThreshold:  minTh,
			opaqueEval:    topEval,
		}), nil
	}
	return c.addNode(ivmNode{
		kind:         ivmKindAnd,
		policyIdx:    -1,
		canError:     canError,
		children:     children,
		minThreshold: minTh,
	}), nil
}

func anyChildExtension(nodes []ivmNode, children []int) bool {
	for _, idx := range children {
		if nodes[idx].isExtension {
			return true
		}
	}
	return false
}

func (c *ivmCircuit) compileNot(settings component.TelemetrySettings, cfg *NotCfg) (int, error) {
	startLen := len(c.nodes)
	childIdx, err := c.compileShared(settings, &cfg.SubPolicy.sharedPolicyCfg, nil)
	if err != nil {
		return -1, err
	}
	if c.nodes[childIdx].hasSideEffect {
		canErr := c.nodes[childIdx].canError
		isExt := c.nodes[childIdx].isExtension
		c.nodes = c.nodes[:startLen]
		return c.addNode(ivmNode{
			kind:          ivmKindOpaque,
			policyIdx:     -1,
			hasSideEffect: true,
			canError:      canErr,
			isExtension:   isExt,
			minThreshold:  pkgsampling.AlwaysSampleThreshold,
		}), nil
	}
	return c.addNode(ivmNode{
		kind:         ivmKindNot,
		policyIdx:    -1,
		canError:     c.nodes[childIdx].canError,
		child:        childIdx,
		minThreshold: pkgsampling.AlwaysSampleThreshold,
	}), nil
}

func (c *ivmCircuit) compileShared(settings component.TelemetrySettings, cfg *sharedPolicyCfg, topEval samplingpolicy.ThresholdEvaluator) (int, error) {
	switch cfg.Type {
	case AlwaysSample:
		return c.addNode(ivmNode{
			kind:         ivmKindAlwaysSample,
			policyIdx:    -1,
			minThreshold: pkgsampling.AlwaysSampleThreshold,
		}), nil

	case StatusCode:
		sc := cfg.StatusCodeCfg.StatusCodes
		if len(sc) == 0 {
			return -1, errors.New("expected at least one status code to filter on")
		}
		codes := make([]ptrace.StatusCode, len(sc))
		for i, s := range sc {
			switch s {
			case "OK":
				codes[i] = ptrace.StatusCodeOk
			case "ERROR":
				codes[i] = ptrace.StatusCodeError
			case "UNSET":
				codes[i] = ptrace.StatusCodeUnset
			default:
				return -1, fmt.Errorf("unknown status code %q, supported: OK, ERROR, UNSET", s)
			}
		}
		return c.addNode(ivmNode{
			kind:         ivmKindStatusCode,
			policyIdx:    -1,
			statusCodes:  codes,
			minThreshold: pkgsampling.AlwaysSampleThreshold,
		}), nil

	case StringAttribute:
		sc := cfg.StringAttributeCfg
		n := ivmNode{
			kind:            ivmKindStringAttr,
			policyIdx:       -1,
			attrKey:         sc.Key,
			invertMatch:     sc.InvertMatch,
			strRegexEnabled: sc.EnabledRegexMatching,
			minThreshold:    pkgsampling.AlwaysSampleThreshold,
		}
		if sc.EnabledRegexMatching {
			evictSize := sc.CacheMaxSize
			if evictSize <= 0 {
				evictSize = ivmDefaultRegexCacheSize
			}
			n.strRegexCache = lru.New(evictSize)
			n.strRegexes = make([]*regexp.Regexp, 0, len(sc.Values))
			for _, expr := range sc.Values {
				r, err := regexp.Compile(expr)
				if err != nil {
					return -1, fmt.Errorf("invalid regex `%s`: %w", expr, err)
				}
				n.strRegexes = append(n.strRegexes, r)
			}
		} else {
			n.strExactValues = make(map[string]struct{}, len(sc.Values))
			for _, v := range sc.Values {
				// Matches string_tag_filter.go:81 exact-mode handling of empty strings.
				if v != "" {
					n.strExactValues[v] = struct{}{}
				}
			}
		}
		return c.addNode(n), nil

	case NumericAttribute:
		nc := cfg.NumericAttributeCfg
		// Matches processor.go:389-396 & numeric_tag_filter.go:32-35.
		if nc.MinValue == 0 && nc.MaxValue == 0 {
			return -1, errors.New("at least one of min_value or max_value must be non-zero")
		}
		minVal := int64(math.MinInt64)
		if nc.MinValue != 0 {
			minVal = nc.MinValue
		}
		maxVal := int64(math.MaxInt64)
		if nc.MaxValue != 0 {
			maxVal = nc.MaxValue
		}
		return c.addNode(ivmNode{
			kind:         ivmKindNumericAttr,
			policyIdx:    -1,
			attrKey:      nc.Key,
			numMin:       minVal,
			numMax:       maxVal,
			invertMatch:  nc.InvertMatch,
			minThreshold: pkgsampling.AlwaysSampleThreshold,
		}), nil

	case BooleanAttribute:
		bc := cfg.BooleanAttributeCfg
		return c.addNode(ivmNode{
			kind:         ivmKindBoolAttr,
			policyIdx:    -1,
			attrKey:      bc.Key,
			boolVal:      bc.Value,
			invertMatch:  bc.InvertMatch,
			minThreshold: pkgsampling.AlwaysSampleThreshold,
		}), nil

	case Latency:
		lc := cfg.LatencyCfg
		return c.addNode(ivmNode{
			kind:           ivmKindLatency,
			policyIdx:      -1,
			latThresholdMs: lc.ThresholdMs,
			latUpperMs:     lc.UpperThresholdMs,
			minThreshold:   pkgsampling.AlwaysSampleThreshold,
		}), nil

	case SpanCount:
		sc := cfg.SpanCountCfg
		return c.addNode(ivmNode{
			kind:         ivmKindSpanCount,
			policyIdx:    -1,
			spanMin:      int64(sc.MinSpans),
			spanMax:      int64(sc.MaxSpans),
			minThreshold: pkgsampling.AlwaysSampleThreshold,
		}), nil

	case Probabilistic:
		pc := cfg.ProbabilisticCfg
		salt := pc.HashSalt
		if salt == "" {
			salt = ivmDefaultHashSalt
		}
		ratio := pc.SamplingPercentage / 100
		otelTh, err := pkgsampling.ProbabilityToThreshold(ratio)
		if err != nil {
			if ratio >= 1 {
				otelTh = pkgsampling.AlwaysSampleThreshold
			} else {
				otelTh = pkgsampling.NeverSampleThreshold
			}
		}
		return c.addNode(ivmNode{
			kind:             ivmKindProbabilistic,
			policyIdx:        -1,
			probHashSalt:     salt,
			probFnvThreshold: ivmCalculateThreshold(ratio),
			probOtelTh:       otelTh,
			probPct:          pc.SamplingPercentage,
			minThreshold:     otelTh,
		}), nil

	case TraceState:
		tc := cfg.TraceStateCfg
		vals := make(map[string]struct{}, len(tc.Values))
		for _, v := range tc.Values {
			// Matches trace_state_filter.go:33.
			if v != "" && len(tc.Key)+len(v) < 256 {
				vals[v] = struct{}{}
			}
		}
		return c.addNode(ivmNode{
			kind:         ivmKindTraceState,
			policyIdx:    -1,
			attrKey:      tc.Key,
			tsValues:     vals,
			minThreshold: pkgsampling.AlwaysSampleThreshold,
		}), nil

	case TraceFlags:
		return c.addNode(ivmNode{
			kind:         ivmKindTraceFlags,
			policyIdx:    -1,
			minThreshold: pkgsampling.AlwaysSampleThreshold,
		}), nil

	case OTTLCondition:
		oc := cfg.OTTLConditionCfg
		if len(oc.SpanConditions) == 0 && len(oc.SpanEventConditions) == 0 {
			return -1, errors.New("expected at least one OTTL condition to filter on")
		}
		n := ivmNode{
			kind:         ivmKindOTTL,
			policyIdx:    -1,
			canError:     oc.ErrorMode == ottl.PropagateError,
			minThreshold: pkgsampling.AlwaysSampleThreshold,
		}
		var err error
		if len(oc.SpanConditions) > 0 {
			n.ottlSpanExpr, err = filterottl.NewBoolExprForSpanWithPathContextNames(
				oc.SpanConditions, filterottl.StandardSpanFuncs(), oc.ErrorMode, settings,
			)
			if err != nil {
				return -1, err
			}
		}
		if len(oc.SpanEventConditions) > 0 {
			n.ottlEventExpr, err = filterottl.NewBoolExprForSpanEventWithPathContextNames(
				oc.SpanEventConditions, filterottl.StandardSpanEventFuncs(), oc.ErrorMode, settings,
			)
			if err != nil {
				return -1, err
			}
		}
		return c.addNode(n), nil

	case RateLimiting, BytesLimiting:
		return c.addNode(ivmNode{
			kind:          ivmKindOpaque,
			policyIdx:     -1,
			hasSideEffect: true,
			minThreshold:  pkgsampling.AlwaysSampleThreshold,
			opaqueEval:    topEval,
		}), nil

	default:
		// Custom samplingpolicy.Extension.
		return c.addNode(ivmNode{
			kind:          ivmKindOpaque,
			policyIdx:     -1,
			hasSideEffect: true,
			canError:      true,
			isExtension:   true,
			minThreshold:  pkgsampling.AlwaysSampleThreshold,
			opaqueEval:    topEval,
		}), nil
	}
}

func (n *ivmNode) matchString(s string) bool {
	if !n.strRegexEnabled {
		_, ok := n.strExactValues[s]
		return ok
	}
	if v, ok := n.strRegexCache.Get(s); ok {
		return v.(bool)
	}
	for _, r := range n.strRegexes {
		if r.MatchString(s) {
			n.strRegexCache.Add(s, true)
			return true
		}
	}
	n.strRegexCache.Add(s, false)
	return false
}

// evalResource evaluates all resource-sensitive leaf nodes ONCE for an incoming
// ResourceSpans, writing 1 into c.resMatchScratch[idx] if the resource matches
// the predicate (or violates the invert_match predicate), and 0 otherwise.
func (c *ivmCircuit) evalResource(res pcommon.Resource) {
	attrs := res.Attributes()
	for _, idx := range c.resourceNodes {
		n := &c.nodes[idx]
		matched := false
		if v, ok := attrs.Get(n.attrKey); ok {
			switch n.kind {
			case ivmKindStringAttr:
				// Matches string_tag_filter.go:109-114 & 133-138:
				// Resource attributes do NOT skip empty string "" before matcher!
				matched = n.matchString(v.Str())
			case ivmKindNumericAttr:
				val := v.Int()
				matched = val >= n.numMin && val <= n.numMax
			case ivmKindBoolAttr:
				matched = v.Bool() == n.boolVal
			}
		}
		if matched {
			c.resMatchScratch[idx] = 1
		} else {
			c.resMatchScratch[idx] = 0
		}
	}
}

// initTraceStates sets the initial 5-state values for a brand-new trace slot.
func (c *ivmCircuit) initTraceStates(traceID pcommon.TraceID, states []uint8) {
	for i := range c.nodes {
		n := &c.nodes[i]
		switch n.kind {
		case ivmKindAlwaysSample:
			states[i] = uint8(ivmStateDefTrue)
		case ivmKindStringAttr, ivmKindNumericAttr, ivmKindBoolAttr:
			if n.invertMatch {
				states[i] = uint8(ivmStateMayTrue)
			} else {
				states[i] = uint8(ivmStateMayFalse)
			}
		case ivmKindProbabilistic:
			fnvMatch := ivmHashTraceID(n.probHashSalt, traceID[:]) <= n.probFnvThreshold
			if !c.useGlobalTraceState {
				if fnvMatch {
					states[i] = uint8(ivmStateDefTrue)
				} else {
					states[i] = uint8(ivmStateDefFalse)
				}
			} else {
				// Global usetracestate feature gate is enabled.
				// At pct >= 100, both ShouldSample and FNV always sample.
				// At pct <= 0, if fnvMatch is false (all hashes != 0), neither can sample.
				switch {
				case n.probPct >= 100:
					states[i] = uint8(ivmStateDefTrue)
				case n.probPct <= 0 && !fnvMatch:
					states[i] = uint8(ivmStateDefFalse)
				case fnvMatch:
					states[i] = uint8(ivmStateMayTrue)
				default:
					states[i] = uint8(ivmStateMayFalse)
				}
			}
		case ivmKindSpanCount:
			// Guard impossible ranges upfront (span_count_sampler.go:39-44).
			if n.spanMax < 0 || (n.spanMax > 0 && n.spanMax < n.spanMin) {
				states[i] = uint8(ivmStateDefFalse)
			} else {
				states[i] = uint8(ivmStateMayFalse)
			}
		case ivmKindOpaque:
			states[i] = uint8(ivmStateMayFalse)
		default:
			states[i] = uint8(ivmStateMayFalse)
		}
	}
}

// applyResource applies the precomputed resource-level matches in c.resMatchScratch
// to a trace's node state slice.
func (c *ivmCircuit) applyResource(states []uint8) {
	for _, idx := range c.resourceNodes {
		if c.resMatchScratch[idx] == 0 {
			continue
		}
		if ivmState(states[idx]).isDefinite() {
			continue
		}
		if c.nodes[idx].invertMatch {
			states[idx] = uint8(ivmStateDefFalse)
		} else {
			states[idx] = uint8(ivmStateDefTrue)
		}
	}
}

// applySpan updates trace scalar metadata (timestamps, tracestate randomness) and
// leaf node states for a single incoming span. When dropOnly is true (positive side
// already settled and waiting only on pending drop policies), only drop-subtree
// leaves are evaluated.
func (c *ivmCircuit) applySpan(
	ctx context.Context,
	slot *ivmTraceSlot,
	states []uint8,
	rs ptrace.ResourceSpans,
	ss ptrace.ScopeSpans,
	span ptrace.Span,
	dropOnly bool,
	logger *zap.Logger,
) {
	// Update trace-wide timestamps for latency policies (explicit hasTimestamps
	// handles StartTimestamp == 0 without overwriting!).
	if c.needsTimestamps {
		st := uint64(span.StartTimestamp())
		et := uint64(span.EndTimestamp())
		if slot.hasTimestamps == 0 {
			slot.minStart = st
			slot.maxEnd = et
			slot.hasTimestamps = 1
		} else {
			if st < slot.minStart {
				slot.minStart = st
			}
			if et > slot.maxEnd {
				slot.maxEnd = et
			}
		}
	}

	// Scan W3C tracestate for OTel `rv`/`th` when probabilistic + usetracestate is active.
	if c.needsTracestateScan && (slot.tsFlags&ivmTSFlagHasRandomness) == 0 {
		if raw := span.TraceState().AsRaw(); raw != "" {
			if ts, err := pkgsampling.NewW3CTraceState(raw); err == nil {
				otts := ts.OTelValue()
				if rv, ok := otts.RValueRandomness(); ok {
					slot.tsRandomness = rv.Unsigned()
					slot.tsFlags |= ivmTSFlagHasRandomness | ivmTSFlagHasSamplingInfo
				} else if (slot.tsFlags & ivmTSFlagHasSamplingInfo) == 0 {
					if _, ok := otts.TValueThreshold(); ok {
						slot.tsFlags |= ivmTSFlagHasSamplingInfo
					}
				}
			}
		}
	}

	leafList := c.spanNodes
	if dropOnly {
		leafList = c.dropSpanNodes
	}

	// Update span-level leaf nodes (skipping any node already in a definite state!).
	for _, idx := range leafList {
		if ivmState(states[idx]).isDefinite() {
			continue
		}
		n := &c.nodes[idx]
		switch n.kind {
		case ivmKindStatusCode:
			if slices.Contains(n.statusCodes, span.Status().Code()) {
				states[idx] = uint8(ivmStateDefTrue)
			}
		case ivmKindStringAttr:
			if v, ok := span.Attributes().Get(n.attrKey); ok {
				// Matches string_tag_filter.go:118-119 & 142-143: span attrs skip "".
				if s := v.Str(); s != "" && n.matchString(s) {
					if n.invertMatch {
						states[idx] = uint8(ivmStateDefFalse)
					} else {
						states[idx] = uint8(ivmStateDefTrue)
					}
				}
			}
		case ivmKindNumericAttr:
			if v, ok := span.Attributes().Get(n.attrKey); ok {
				val := v.Int()
				if val >= n.numMin && val <= n.numMax {
					if n.invertMatch {
						states[idx] = uint8(ivmStateDefFalse)
					} else {
						states[idx] = uint8(ivmStateDefTrue)
					}
				}
			}
		case ivmKindBoolAttr:
			if v, ok := span.Attributes().Get(n.attrKey); ok {
				if v.Bool() == n.boolVal {
					if n.invertMatch {
						states[idx] = uint8(ivmStateDefFalse)
					} else {
						states[idx] = uint8(ivmStateDefTrue)
					}
				}
			}
		case ivmKindTraceState:
			if raw := span.TraceState().AsRaw(); raw != "" {
				if parsed, err := tracesdk.ParseTraceState(raw); err == nil {
					if _, ok := n.tsValues[parsed.Get(n.attrKey)]; ok {
						states[idx] = uint8(ivmStateDefTrue)
					}
				}
			}
		case ivmKindTraceFlags:
			if (byte(span.Flags()) & byte(tracesdk.FlagsSampled)) != 0 {
				states[idx] = uint8(ivmStateDefTrue)
			}
		case ivmKindOTTL:
			if n.ottlSpanExpr != nil {
				tCtx := ottlspan.NewTransformContext(rs, ss, span)
				ok, err := n.ottlSpanExpr.Eval(ctx, tCtx)
				tCtx.Close()
				if err != nil {
					states[idx] = uint8(ivmStateDefError)
					if logger != nil {
						logger.Debug("Sampling policy error", zap.Error(err))
					}
					continue
				}
				if ok {
					states[idx] = uint8(ivmStateDefTrue)
					continue
				}
			}
			if n.ottlEventExpr != nil {
				events := span.Events()
				for l := 0; l < events.Len(); l++ {
					tCtx := ottlspanevent.NewTransformContext(rs, ss, span, events.At(l))
					ok, err := n.ottlEventExpr.Eval(ctx, tCtx)
					tCtx.Close()
					if err != nil {
						states[idx] = uint8(ivmStateDefError)
						if logger != nil {
							logger.Debug("Sampling policy error", zap.Error(err))
						}
						break
					}
					if ok {
						states[idx] = uint8(ivmStateDefTrue)
						break
					}
				}
			}
		}
	}
}

// updateAggregatesAndPropagate recomputes latency, span_count, probabilistic
// (tracestate), and all boolean combinators (NOT, AND, DROP) in post-order.
func (c *ivmCircuit) updateAggregatesAndPropagate(slot *ivmTraceSlot, states []uint8) {
	// Probabilistic nodes under usetracestate.
	if c.useGlobalTraceState && len(c.probNodes) > 0 {
		for _, idx := range c.probNodes {
			if ivmState(states[idx]).isDefinite() {
				continue
			}
			n := &c.nodes[idx]
			if (slot.tsFlags & ivmTSFlagHasRandomness) != 0 {
				rnd, _ := pkgsampling.UnsignedToRandomness(slot.tsRandomness)
				if n.probOtelTh.ShouldSample(rnd) {
					states[idx] = uint8(ivmStateDefTrue)
				} else {
					states[idx] = uint8(ivmStateDefFalse)
				}
			} else if (slot.tsFlags & ivmTSFlagHasSamplingInfo) != 0 {
				rnd := pkgsampling.TraceIDToRandomness(slot.traceID)
				if n.probOtelTh.ShouldSample(rnd) {
					states[idx] = uint8(ivmStateMayTrue)
				} else {
					states[idx] = uint8(ivmStateMayFalse)
				}
			}
		}
	}

	// Latency and span_count aggregate nodes.
	for _, idx := range c.aggregateNodes {
		if ivmState(states[idx]).isDefinite() {
			continue
		}
		n := &c.nodes[idx]
		switch n.kind {
		case ivmKindLatency:
			var ms int64
			if slot.hasTimestamps != 0 && slot.maxEnd >= slot.minStart {
				ms = int64(slot.maxEnd-slot.minStart) / int64(time.Millisecond)
			}
			if n.latUpperMs == 0 {
				if ms > n.latThresholdMs {
					states[idx] = uint8(ivmStateDefTrue)
				} else {
					states[idx] = uint8(ivmStateMayFalse)
				}
			} else {
				switch {
				case ms > n.latUpperMs:
					states[idx] = uint8(ivmStateDefFalse)
				case ms > n.latThresholdMs:
					states[idx] = uint8(ivmStateMayTrue)
				default:
					states[idx] = uint8(ivmStateMayFalse)
				}
			}
		case ivmKindSpanCount:
			cnt := slot.spanCount
			if n.spanMax == 0 {
				if cnt >= n.spanMin {
					states[idx] = uint8(ivmStateDefTrue)
				} else {
					states[idx] = uint8(ivmStateMayFalse)
				}
			} else {
				switch {
				case cnt > n.spanMax:
					states[idx] = uint8(ivmStateDefFalse)
				case cnt >= n.spanMin:
					states[idx] = uint8(ivmStateMayTrue)
				default:
					states[idx] = uint8(ivmStateMayFalse)
				}
			}
		}
	}

	// Propagate combinators in post-order (children are always at smaller indices).
	for _, idx := range c.combinators {
		n := &c.nodes[idx]
		switch n.kind {
		case ivmKindNot:
			switch ivmState(states[n.child]) {
			case ivmStateDefTrue:
				states[idx] = uint8(ivmStateDefFalse)
			case ivmStateDefFalse:
				states[idx] = uint8(ivmStateDefTrue)
			case ivmStateDefError:
				states[idx] = uint8(ivmStateDefError)
			case ivmStateMayTrue:
				states[idx] = uint8(ivmStateMayFalse)
			case ivmStateMayFalse:
				states[idx] = uint8(ivmStateMayTrue)
			}
		case ivmKindAnd, ivmKindDrop:
			states[idx] = uint8(evalAndDropStates(c.nodes, n.children, states))
		}
	}
}

// evalAndDropStates computes the 5-state Kleene conjunction in strict left-to-right
// slice order, matching internal/sampling/and.go:62-75 and drop.go:36-46.
func evalAndDropStates(nodes []ivmNode, children []int, states []uint8) ivmState {
	if len(children) == 0 {
		// Vacuously true (and.go:75, drop.go:46).
		return ivmStateDefTrue
	}
	allDefTrue := true
	allCurTrue := true
	priorCanError := false
	for _, ch := range children {
		st := ivmState(states[ch])
		switch st {
		case ivmStateDefFalse:
			// An earlier definite false stops evaluation and suppresses later errors.
			// If an earlier non-definite node could still produce an error, keep
			// MayFalse so the error telemetry stays accurate if it fires; otherwise DefFalse.
			if !priorCanError {
				return ivmStateDefFalse
			}
			return ivmStateMayFalse
		case ivmStateDefError:
			if allDefTrue {
				// All preceding children are definitely true, so this error is
				// guaranteed to be reached in slice order.
				return ivmStateDefError
			}
			// Preceding children could still resolve to false and suppress this error.
			return ivmStateMayFalse
		case ivmStateDefTrue:
			// Continue scanning.
		case ivmStateMayTrue:
			allDefTrue = false
			if nodes[ch].canError {
				priorCanError = true
			}
		case ivmStateMayFalse:
			allDefTrue = false
			allCurTrue = false
			if nodes[ch].canError {
				priorCanError = true
			}
		}
	}
	if allDefTrue {
		return ivmStateDefTrue
	}
	if allCurTrue {
		return ivmStateMayTrue
	}
	return ivmStateMayFalse
}

// ivmDecisionResult represents an immediate short-circuit or tick-finalized decision.
type ivmDecisionResult struct {
	decision      samplingpolicy.Decision // Unspecified/Pending if not ready yet
	policyIdx     int
	threshold     pkgsampling.Threshold
	needsFallback bool
	evalErrors    int64
}

// tryShortCircuit evaluates the top-level Gate 1 (Dropped), Gate 2 (NotSampled),
// and Gate 3 (Sampled) rules. Returns decision == samplingpolicy.Pending when
// the trace must remain buffered until more spans or tick expiry.
func (c *ivmCircuit) tryShortCircuit(
	states []uint8,
	sampleOnFirstMatch bool,
	useTracestate bool,
	recordPolicy bool,
	hasSampledHooks bool,
	hasNonSampledHooks bool,
) ivmDecisionResult {
	if c.fallbackOnly {
		return ivmDecisionResult{decision: samplingpolicy.Pending}
	}

	// Gate 1: Check drop policies in config order.
	// Early drop on drop_i == DefTrue is safe only if drop_0..i have no side effects,
	// and when nonSampledHooks are present, no earlier drop_j (j < i) is pending.
	allDropsSettledNonDrop := true
	anyEarlierDropPending := false
	var dropErrs int64
	for _, nIdx := range c.topDropNodes {
		n := &c.nodes[nIdx]
		if n.hasSideEffect {
			// Cannot short-circuit past a side-effecting drop policy.
			allDropsSettledNonDrop = false
			return ivmDecisionResult{decision: samplingpolicy.Pending}
		}
		st := ivmState(states[nIdx])
		switch st {
		case ivmStateDefTrue:
			if hasNonSampledHooks && anyEarlierDropPending {
				return ivmDecisionResult{decision: samplingpolicy.Pending}
			}
			return ivmDecisionResult{
				decision:   samplingpolicy.Dropped,
				policyIdx:  n.policyIdx,
				threshold:  pkgsampling.AlwaysSampleThreshold,
				evalErrors: dropErrs,
			}
		case ivmStateDefError:
			dropErrs++
		case ivmStateDefFalse:
			// This drop policy can never drop the trace.
		default:
			// MayTrue or MayFalse: still pending.
			allDropsSettledNonDrop = false
			anyEarlierDropPending = true
		}
	}

	if !allDropsSettledNonDrop {
		return ivmDecisionResult{decision: samplingpolicy.Pending}
	}

	// All drop policies are settled (DefFalse or DefError) with no side effects!
	// If there are custom extensions or BatchEvaluators anywhere, disable early
	// Sampled and NotSampled short-circuits (Gate 2 / Gate 3 safety).
	if c.hasAnyExtension || c.hasAnyBatchEvaluator {
		return ivmDecisionResult{decision: samplingpolicy.Pending}
	}

	// Scan positive policies.
	needStrictOrderFirstMatch := sampleOnFirstMatch && (useTracestate || recordPolicy || hasSampledHooks)
	needWinnerParity := !sampleOnFirstMatch && (recordPolicy || hasSampledHooks)

	// Gate 3: If !sampleOnFirstMatch, any stateful positive policy anywhere disables early Sampled.
	// If sampleOnFirstMatch, only stateful policies at or before the latching policy disable it.
	if !sampleOnFirstMatch && c.hasAnyStatefulPositive {
		return ivmDecisionResult{decision: samplingpolicy.Pending}
	}

	allPosSettledFalse := true
	bestIdx := -1
	bestNodeIdx := -1
	bestTh := pkgsampling.Threshold{}
	haveBest := false
	seenStatefulBefore := false
	var totalPosErrs int64

	for i, nIdx := range c.topSampleNodes {
		n := &c.nodes[nIdx]
		if n.hasSideEffect {
			seenStatefulBefore = true
			allPosSettledFalse = false
			continue
		}
		st := ivmState(states[nIdx])
		switch st {
		case ivmStateDefTrue:
			allPosSettledFalse = false
			if sampleOnFirstMatch {
				if seenStatefulBefore {
					continue
				}
				if !needStrictOrderFirstMatch {
					return ivmDecisionResult{
						decision:   samplingpolicy.Sampled,
						policyIdx:  n.policyIdx,
						threshold:  n.minThreshold,
						evalErrors: dropErrs + totalPosErrs,
					}
				}
				// Ensure all earlier positive policies (j < i) are DefFalse/DefError.
				earlierSettled := true
				var earlierErrs int64
				for j := range i {
					ej := ivmState(states[c.topSampleNodes[j]])
					if c.nodes[c.topSampleNodes[j]].hasSideEffect || (ej != ivmStateDefFalse && ej != ivmStateDefError) {
						earlierSettled = false
						break
					}
					if ej == ivmStateDefError {
						earlierErrs++
					}
				}
				if earlierSettled {
					return ivmDecisionResult{
						decision:   samplingpolicy.Sampled,
						policyIdx:  n.policyIdx,
						threshold:  n.minThreshold,
						evalErrors: dropErrs + earlierErrs,
					}
				}
				continue
			}

			// sampleOnFirstMatch == false: track best (smallest threshold; first on tie).
			if !haveBest || pkgsampling.ThresholdLessThan(n.minThreshold, bestTh) {
				bestIdx = i
				bestNodeIdx = nIdx
				bestTh = n.minThreshold
				haveBest = true
			}
		case ivmStateDefError:
			totalPosErrs++
		case ivmStateDefFalse:
			// Cannot match.
		default:
			allPosSettledFalse = false
		}
	}

	if haveBest {
		// Can we latch bestIdx immediately under sampleOnFirstMatch == false?
		if !needWinnerParity && !useTracestate {
			return ivmDecisionResult{
				decision:   samplingpolicy.Sampled,
				policyIdx:  c.nodes[bestNodeIdx].policyIdx,
				threshold:  bestTh,
				evalErrors: dropErrs + totalPosErrs,
			}
		}
		canBeat := false
		for j, nIdx := range c.topSampleNodes {
			if j == bestIdx {
				continue
			}
			st := ivmState(states[nIdx])
			if st == ivmStateDefFalse || st == ivmStateDefError || st == ivmStateDefTrue {
				continue
			}
			otherTh := c.nodes[nIdx].minThreshold
			if needWinnerParity {
				if j < bestIdx && !pkgsampling.ThresholdGreater(otherTh, bestTh) {
					// Earlier pending policy has minPossibleThreshold <= bestTh (wins tie or beats).
					canBeat = true
					break
				}
				if j > bestIdx && pkgsampling.ThresholdLessThan(otherTh, bestTh) {
					// Later pending policy has strictly smaller minPossibleThreshold (overwrites winner!).
					canBeat = true
					break
				}
			} else if useTracestate {
				if pkgsampling.ThresholdLessThan(otherTh, bestTh) {
					canBeat = true
					break
				}
			}
		}
		if !canBeat {
			return ivmDecisionResult{
				decision:   samplingpolicy.Sampled,
				policyIdx:  c.nodes[bestNodeIdx].policyIdx,
				threshold:  bestTh,
				evalErrors: dropErrs + totalPosErrs,
			}
		}
	}

	// Gate 2: All drop AND positive policies are settled DefFalse/DefError (no stateful policies).
	if allPosSettledFalse && !c.hasAnyStatefulPositive {
		return ivmDecisionResult{
			decision:   samplingpolicy.NotSampled,
			policyIdx:  -1,
			evalErrors: dropErrs + totalPosErrs,
		}
	}

	return ivmDecisionResult{decision: samplingpolicy.Pending}
}

// resolveAtTick resolves a pending trace at window close in O(numNodes) WITHOUT
// unmarshaling its payload, whenever no opaque/stateful policy needs execution.
func (c *ivmCircuit) resolveAtTick(states []uint8, sampleOnFirstMatch bool) ivmDecisionResult {
	if c.fallbackOnly || c.hasAnyOpaque {
		return ivmDecisionResult{needsFallback: true}
	}

	var evalErrs int64

	// Evaluate drop policies in config order.
	for _, nIdx := range c.topDropNodes {
		res := c.resolveFinalNode(nIdx, states)
		switch res {
		case finalTrue:
			return ivmDecisionResult{
				decision:   samplingpolicy.Dropped,
				policyIdx:  c.nodes[nIdx].policyIdx,
				threshold:  pkgsampling.AlwaysSampleThreshold,
				evalErrors: evalErrs,
			}
		case finalError:
			evalErrs++
		}
	}

	// Evaluate positive policies in config order (matching processor.go:912-952).
	bestPolicyIdx := -1
	bestTh := pkgsampling.Threshold{}
	haveBest := false

	for _, nIdx := range c.topSampleNodes {
		res := c.resolveFinalNode(nIdx, states)
		switch res {
		case finalError:
			evalErrs++
		case finalTrue:
			n := &c.nodes[nIdx]
			if !haveBest || pkgsampling.ThresholdLessThan(n.minThreshold, bestTh) {
				bestTh = n.minThreshold
				bestPolicyIdx = n.policyIdx
				haveBest = true
			}
			if sampleOnFirstMatch {
				return ivmDecisionResult{
					decision:   samplingpolicy.Sampled,
					policyIdx:  bestPolicyIdx,
					threshold:  bestTh,
					evalErrors: evalErrs,
				}
			}
		}
	}

	if haveBest {
		return ivmDecisionResult{
			decision:   samplingpolicy.Sampled,
			policyIdx:  bestPolicyIdx,
			threshold:  bestTh,
			evalErrors: evalErrs,
		}
	}

	return ivmDecisionResult{
		decision:   samplingpolicy.NotSampled,
		policyIdx:  -1,
		evalErrors: evalErrs,
	}
}

type finalVal uint8

const (
	finalFalse finalVal = iota
	finalTrue
	finalError
)

func (c *ivmCircuit) resolveFinalNode(idx int, states []uint8) finalVal {
	n := &c.nodes[idx]
	switch n.kind {
	case ivmKindNot:
		switch c.resolveFinalNode(n.child, states) {
		case finalTrue:
			return finalFalse
		case finalFalse:
			return finalTrue
		default:
			return finalError
		}
	case ivmKindAnd, ivmKindDrop:
		for _, ch := range n.children {
			switch c.resolveFinalNode(ch, states) {
			case finalFalse:
				return finalFalse
			case finalError:
				return finalError
			}
		}
		return finalTrue
	default:
		switch ivmState(states[idx]) {
		case ivmStateDefTrue, ivmStateMayTrue:
			return finalTrue
		case ivmStateDefError:
			return finalError
		default:
			return finalFalse
		}
	}
}

// resolveDropAtTick evaluates ONLY the pure drop policy prefix when
// !fallbackOnly && !hasAnyStatefulDrop. Returns droppedPolicyIdx >= 0 if dropped.
func (c *ivmCircuit) resolveDropAtTick(states []uint8) (droppedPolicyIdx int, evalErrs int64) {
	for _, nIdx := range c.topDropNodes {
		res := c.resolveFinalNode(nIdx, states)
		switch res {
		case finalTrue:
			return c.nodes[nIdx].policyIdx, evalErrs
		case finalError:
			evalErrs++
		}
	}
	return -1, evalErrs
}

// positiveLatched reports whether at least one positive policy is already DefTrue
// and no other positive policy can override the winner/threshold, so future spans
// need only evaluate drop-subtree leaves while waiting on pending drop policies.
func (c *ivmCircuit) positiveLatched(states []uint8, sampleOnFirstMatch, useTracestate, recordPolicy, hasSampledHooks bool) bool {
	if c.fallbackOnly || c.hasAnyOpaque || c.hasAnyBatchEvaluator || c.hasAnyExtension || !c.hasDropPolicies {
		return false
	}
	needStrictOrderFirstMatch := sampleOnFirstMatch && (useTracestate || recordPolicy || hasSampledHooks)
	needWinnerParity := !sampleOnFirstMatch && (recordPolicy || hasSampledHooks)

	bestIdx := -1
	bestTh := pkgsampling.Threshold{}
	haveBest := false
	for i, nIdx := range c.topSampleNodes {
		if ivmState(states[nIdx]) != ivmStateDefTrue {
			continue
		}
		n := &c.nodes[nIdx]
		if sampleOnFirstMatch {
			if !needStrictOrderFirstMatch {
				return true
			}
			earlierSettled := true
			for j := range i {
				ej := ivmState(states[c.topSampleNodes[j]])
				if ej != ivmStateDefFalse && ej != ivmStateDefError {
					earlierSettled = false
					break
				}
			}
			if earlierSettled {
				return true
			}
			continue
		}
		if !haveBest || pkgsampling.ThresholdLessThan(n.minThreshold, bestTh) {
			bestIdx = i
			bestTh = n.minThreshold
			haveBest = true
		}
	}
	if !haveBest {
		return false
	}
	if !needWinnerParity && !useTracestate {
		return true
	}
	for j, nIdx := range c.topSampleNodes {
		if j == bestIdx {
			continue
		}
		st := ivmState(states[nIdx])
		if st == ivmStateDefFalse || st == ivmStateDefError || st == ivmStateDefTrue {
			continue
		}
		otherTh := c.nodes[nIdx].minThreshold
		if needWinnerParity {
			if j < bestIdx && !pkgsampling.ThresholdGreater(otherTh, bestTh) {
				return false
			}
			if j > bestIdx && pkgsampling.ThresholdLessThan(otherTh, bestTh) {
				return false
			}
		} else if useTracestate {
			if pkgsampling.ThresholdLessThan(otherTh, bestTh) {
				return false
			}
		}
	}
	return true
}

// ivmTraceSlot is strictly pointer-free (Go runtime noscan!) scalar metadata for one trace.
type ivmTraceSlot struct {
	traceID        pcommon.TraceID // [16]byte
	arrivalNano    int64
	decisionNano   int64
	spanCount      int64
	sizeBytes      uint64
	batchID        uint64
	minStart       uint64
	maxEnd         uint64
	tsRandomness   uint64
	finalThreshold uint64
	circuitVer     uint32
	policyIdx      int32  // Index into policyNamesByVer[circuitVer], or -1.
	finalDecision  int32  // samplingpolicy.Decision
	prevFIFO       uint32 // Intrusive FIFO queue prev slot index, or ivmNoSlot.
	nextFIFO       uint32 // Intrusive FIFO queue next slot index, or ivmNoSlot.
	hasTimestamps  uint8
	tsFlags        uint8
	inFIFO         uint8
	inUse          uint8
}

// ivmTraceTable stores all active traces in flat, pointer-free (noscan) slices
// with an intrusive FIFO eviction queue (zero *list.Element pointers!).
type ivmTraceTable struct {
	index       map[pcommon.TraceID]uint32
	slots       []ivmTraceSlot
	freeSlots   []uint32
	nodeStates  []uint8 // stride = stateStride per slot
	stateStride int

	fifoHead uint32
	fifoTail uint32
	peakSize int
}

func newIVMTraceTable(numNodes int) *ivmTraceTable {
	stride := max(1, numNodes)
	return &ivmTraceTable{
		index:       make(map[pcommon.TraceID]uint32),
		stateStride: stride,
		fifoHead:    ivmNoSlot,
		fifoTail:    ivmNoSlot,
	}
}

func (t *ivmTraceTable) len() int {
	return len(t.index)
}

func (t *ivmTraceTable) lookup(id pcommon.TraceID) (uint32, *ivmTraceSlot, bool) {
	idx, ok := t.index[id]
	if !ok {
		return ivmNoSlot, nil, false
	}
	return idx, &t.slots[idx], true
}

func (t *ivmTraceTable) ensureStride(numNodes int) {
	newStride := max(1, numNodes)
	if newStride <= t.stateStride {
		return
	}
	// Resize nodeStates preserving existing slots.
	numSlots := len(t.slots)
	next := make([]uint8, numSlots*newStride)
	for i := range numSlots {
		if t.slots[i].inUse != 0 {
			copy(next[i*newStride:i*newStride+t.stateStride], t.nodeStates[i*t.stateStride:(i+1)*t.stateStride])
		}
	}
	t.nodeStates = next
	t.stateStride = newStride
}

func (t *ivmTraceTable) allocSlot(id pcommon.TraceID, pushFIFO bool) (uint32, *ivmTraceSlot) {
	var idx uint32
	if n := len(t.freeSlots); n > 0 {
		idx = t.freeSlots[n-1]
		t.freeSlots = t.freeSlots[:n-1]
		t.slots[idx] = ivmTraceSlot{
			traceID:   id,
			policyIdx: -1,
			prevFIFO:  ivmNoSlot,
			nextFIFO:  ivmNoSlot,
			inUse:     1,
		}
		clear(t.nodeStates[int(idx)*t.stateStride : (int(idx)+1)*t.stateStride])
	} else {
		idx = uint32(len(t.slots))
		t.slots = append(t.slots, ivmTraceSlot{
			traceID:   id,
			policyIdx: -1,
			prevFIFO:  ivmNoSlot,
			nextFIFO:  ivmNoSlot,
			inUse:     1,
		})
		base := len(t.nodeStates)
		t.nodeStates = slices.Grow(t.nodeStates, t.stateStride)[:base+t.stateStride]
		clear(t.nodeStates[base:])
	}
	t.index[id] = idx
	if len(t.index) > t.peakSize {
		t.peakSize = len(t.index)
	}
	if pushFIFO {
		t.fifoPushBack(idx)
	}
	return idx, &t.slots[idx]
}

func (t *ivmTraceTable) statesFor(idx uint32) []uint8 {
	start := int(idx) * t.stateStride
	return t.nodeStates[start : start+t.stateStride]
}

func (t *ivmTraceTable) fifoPushBack(idx uint32) {
	s := &t.slots[idx]
	s.inFIFO = 1
	s.prevFIFO = t.fifoTail
	s.nextFIFO = ivmNoSlot
	if t.fifoTail != ivmNoSlot {
		t.slots[t.fifoTail].nextFIFO = idx
	} else {
		t.fifoHead = idx
	}
	t.fifoTail = idx
}

func (t *ivmTraceTable) fifoRemove(idx uint32) {
	s := &t.slots[idx]
	if s.inFIFO == 0 {
		return
	}
	prev := s.prevFIFO
	next := s.nextFIFO
	if prev != ivmNoSlot {
		t.slots[prev].nextFIFO = next
	} else {
		t.fifoHead = next
	}
	if next != ivmNoSlot {
		t.slots[next].prevFIFO = prev
	} else {
		t.fifoTail = prev
	}
	s.prevFIFO = ivmNoSlot
	s.nextFIFO = ivmNoSlot
	s.inFIFO = 0
}

func (t *ivmTraceTable) freeSlot(id pcommon.TraceID) (ivmTraceSlot, bool) {
	idx, ok := t.index[id]
	if !ok {
		return ivmTraceSlot{}, false
	}
	s := t.slots[idx]
	t.fifoRemove(idx)
	delete(t.index, id)
	t.slots[idx].inUse = 0
	t.freeSlots = append(t.freeSlots, idx)

	// Compact map buckets after traffic bursts (resetting peakSize to prevent O(N^2) churn).
	if len(t.index) == 0 {
		if t.peakSize >= 1024 {
			t.index = make(map[pcommon.TraceID]uint32)
			t.peakSize = 0
		}
	} else if t.peakSize >= 4096 && len(t.index) < t.peakSize/4 {
		compacted := make(map[pcommon.TraceID]uint32, len(t.index))
		for k, v := range t.index {
			compacted[k] = v
		}
		t.index = compacted
		t.peakSize = len(t.index)
	}
	return s, true
}

func ivmCalculateThreshold(ratio float64) uint64 {
	boundary := new(big.Float).SetInt(new(big.Int).SetUint64(math.MaxUint64))
	res, _ := boundary.Mul(boundary, big.NewFloat(ratio)).Uint64()
	return res
}

func ivmHashTraceID(salt string, b []byte) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(salt))
	_, _ = hasher.Write(b)
	return hasher.Sum64()
}
