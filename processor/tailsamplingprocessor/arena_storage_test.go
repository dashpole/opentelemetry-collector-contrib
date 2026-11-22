// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tailsamplingprocessor

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/tailstorageextension"
)

// assertTracesDeepEqual performs a canonical lossless deep comparison of two ptrace.Traces
// without requiring external test packages (zero go.mod changes).
func assertTracesDeepEqual(t *testing.T, want, got ptrace.Traces) {
	t.Helper()
	var m ptrace.ProtoMarshaler
	require.Equal(t, want.ResourceSpans().Len(), got.ResourceSpans().Len(), "ResourceSpans length mismatch")
	for i := 0; i < want.ResourceSpans().Len(); i++ {
		wRS := want.ResourceSpans().At(i)
		gRS := got.ResourceSpans().At(i)
		require.Equal(t, wRS.SchemaUrl(), gRS.SchemaUrl(), "ResourceSpans[%d].SchemaUrl", i)
		require.Equal(t, wRS.Resource().DroppedAttributesCount(), gRS.Resource().DroppedAttributesCount(), "ResourceSpans[%d].DroppedAttributesCount", i)
		assertMapDeepEqual(t, fmt.Sprintf("RS[%d].Resource", i), wRS.Resource().Attributes(), gRS.Resource().Attributes())

		require.Equal(t, wRS.ScopeSpans().Len(), gRS.ScopeSpans().Len(), "RS[%d].ScopeSpans len", i)
		for j := 0; j < wRS.ScopeSpans().Len(); j++ {
			wSS := wRS.ScopeSpans().At(j)
			gSS := gRS.ScopeSpans().At(j)
			require.Equal(t, wSS.SchemaUrl(), gSS.SchemaUrl(), "RS[%d].SS[%d].SchemaUrl", i, j)
			require.Equal(t, wSS.Scope().Name(), gSS.Scope().Name(), "RS[%d].SS[%d].Scope.Name", i, j)
			require.Equal(t, wSS.Scope().Version(), gSS.Scope().Version(), "RS[%d].SS[%d].Scope.Version", i, j)
			require.Equal(t, wSS.Scope().DroppedAttributesCount(), gSS.Scope().DroppedAttributesCount(), "RS[%d].SS[%d].Scope.Dropped", i, j)
			assertMapDeepEqual(t, fmt.Sprintf("RS[%d].SS[%d].Scope", i, j), wSS.Scope().Attributes(), gSS.Scope().Attributes())

			require.Equal(t, wSS.Spans().Len(), gSS.Spans().Len(), "RS[%d].SS[%d].Spans len", i, j)
			for k := 0; k < wSS.Spans().Len(); k++ {
				assertSpanDeepEqual(t, fmt.Sprintf("RS[%d].SS[%d].Sp[%d]", i, j, k), wSS.Spans().At(k), gSS.Spans().At(k))
			}
		}
	}
	require.Equal(t, m.TracesSize(want), m.TracesSize(got), "TracesSize parity mismatch")
}

func assertSpanDeepEqual(t *testing.T, path string, want, got ptrace.Span) {
	t.Helper()
	require.Equal(t, want.TraceID(), got.TraceID(), "%s.TraceID", path)
	require.Equal(t, want.SpanID(), got.SpanID(), "%s.SpanID", path)
	require.Equal(t, want.ParentSpanID(), got.ParentSpanID(), "%s.ParentSpanID", path)
	require.Equal(t, want.TraceState().AsRaw(), got.TraceState().AsRaw(), "%s.TraceState", path)
	require.Equal(t, want.Flags(), got.Flags(), "%s.Flags", path)
	require.Equal(t, want.Name(), got.Name(), "%s.Name", path)
	require.Equal(t, want.Kind(), got.Kind(), "%s.Kind", path)
	require.Equal(t, want.StartTimestamp(), got.StartTimestamp(), "%s.StartTimestamp", path)
	require.Equal(t, want.EndTimestamp(), got.EndTimestamp(), "%s.EndTimestamp", path)
	require.Equal(t, want.DroppedAttributesCount(), got.DroppedAttributesCount(), "%s.DroppedAttributesCount", path)
	require.Equal(t, want.DroppedEventsCount(), got.DroppedEventsCount(), "%s.DroppedEventsCount", path)
	require.Equal(t, want.DroppedLinksCount(), got.DroppedLinksCount(), "%s.DroppedLinksCount", path)
	require.Equal(t, want.Status().Code(), got.Status().Code(), "%s.Status.Code", path)
	require.Equal(t, want.Status().Message(), got.Status().Message(), "%s.Status.Message", path)
	assertMapDeepEqual(t, path+".Attrs", want.Attributes(), got.Attributes())

	require.Equal(t, want.Events().Len(), got.Events().Len(), "%s.Events len", path)
	for i := 0; i < want.Events().Len(); i++ {
		we, ge := want.Events().At(i), got.Events().At(i)
		require.Equal(t, we.Timestamp(), ge.Timestamp(), "%s.Events[%d].Timestamp", path, i)
		require.Equal(t, we.Name(), ge.Name(), "%s.Events[%d].Name", path, i)
		require.Equal(t, we.DroppedAttributesCount(), ge.DroppedAttributesCount(), "%s.Events[%d].Dropped", path, i)
		assertMapDeepEqual(t, fmt.Sprintf("%s.Events[%d]", path, i), we.Attributes(), ge.Attributes())
	}

	require.Equal(t, want.Links().Len(), got.Links().Len(), "%s.Links len", path)
	for i := 0; i < want.Links().Len(); i++ {
		wl, gl := want.Links().At(i), got.Links().At(i)
		require.Equal(t, wl.TraceID(), gl.TraceID(), "%s.Links[%d].TraceID", path, i)
		require.Equal(t, wl.SpanID(), gl.SpanID(), "%s.Links[%d].SpanID", path, i)
		require.Equal(t, wl.TraceState().AsRaw(), gl.TraceState().AsRaw(), "%s.Links[%d].TraceState", path, i)
		require.Equal(t, wl.Flags(), gl.Flags(), "%s.Links[%d].Flags", path, i)
		require.Equal(t, wl.DroppedAttributesCount(), gl.DroppedAttributesCount(), "%s.Links[%d].Dropped", path, i)
		assertMapDeepEqual(t, fmt.Sprintf("%s.Links[%d]", path, i), wl.Attributes(), gl.Attributes())
	}
}

func assertMapDeepEqual(t *testing.T, path string, want, got pcommon.Map) {
	t.Helper()
	require.Equal(t, want.Len(), got.Len(), "%s map len", path)
	for k, wv := range want.All() {
		gv, ok := got.Get(k)
		require.True(t, ok, "%s missing key %q", path, k)
		assertValueDeepEqual(t, fmt.Sprintf("%s[%q]", path, k), wv, gv)
	}
}

func assertValueDeepEqual(t *testing.T, path string, want, got pcommon.Value) {
	t.Helper()
	require.Equal(t, want.Type(), got.Type(), "%s type mismatch", path)
	switch want.Type() {
	case pcommon.ValueTypeEmpty:
	case pcommon.ValueTypeStr:
		require.Equal(t, want.Str(), got.Str(), "%s str", path)
	case pcommon.ValueTypeBool:
		require.Equal(t, want.Bool(), got.Bool(), "%s bool", path)
	case pcommon.ValueTypeInt:
		require.Equal(t, want.Int(), got.Int(), "%s int", path)
	case pcommon.ValueTypeDouble:
		if math.IsNaN(want.Double()) {
			require.True(t, math.IsNaN(got.Double()), "%s NaN", path)
		} else {
			require.Equal(t, math.Float64bits(want.Double()), math.Float64bits(got.Double()), "%s double bits", path)
		}
	case pcommon.ValueTypeBytes:
		require.Equal(t, want.Bytes().AsRaw(), got.Bytes().AsRaw(), "%s bytes", path)
	case pcommon.ValueTypeSlice:
		ws, gs := want.Slice(), got.Slice()
		require.Equal(t, ws.Len(), gs.Len(), "%s slice len", path)
		for i := 0; i < ws.Len(); i++ {
			assertValueDeepEqual(t, fmt.Sprintf("%s[%d]", path, i), ws.At(i), gs.At(i))
		}
	case pcommon.ValueTypeMap:
		assertMapDeepEqual(t, path, want.Map(), got.Map())
	}
}

func buildRichTrace(tid pcommon.TraceID) ptrace.Traces {
	td := ptrace.NewTraces()

	// ResourceSpans 0: rich resource with schema_url, dropped count, 2 scopes, all ValueTypes (including zero values).
	rs := td.ResourceSpans().AppendEmpty()
	rs.SetSchemaUrl("https://opentelemetry.io/schemas/1.28.0")
	rs.Resource().SetDroppedAttributesCount(7)
	ra := rs.Resource().Attributes()
	ra.PutStr("service.name", "checkout")
	ra.PutStr("empty.str", "")
	ra.PutBool("bool.true", true)
	ra.PutBool("bool.false", false)
	ra.PutInt("int.zero", 0)
	ra.PutInt("int.pos", 42)
	ra.PutInt("int.neg", -9223372036854775808)
	ra.PutDouble("dbl.zero", 0.0)
	ra.PutDouble("dbl.negzero", math.Copysign(0, -1))
	ra.PutDouble("dbl.pi", math.Pi)
	ra.PutDouble("dbl.inf", math.Inf(1))
	ra.PutDouble("dbl.nan", math.NaN())
	ra.PutEmpty("empty.val")
	ra.PutEmptyBytes("bytes.empty")
	ra.PutEmptyBytes("bytes.nonempty").FromRaw([]byte{0x00, 0xde, 0xad, 0xbe, 0xef, 0xff})
	ra.PutStr("large.attr", string(bytes.Repeat([]byte("chunk-payload-"), 40))) // > 500 bytes forcing multi-byte varint shift

	ra.PutEmptySlice("slice.empty")
	sl := ra.PutEmptySlice("slice.nested")
	sl.AppendEmpty() // ValueTypeEmpty element
	sl.AppendEmpty().SetStr("")
	sl.AppendEmpty().SetStr("hello")
	sl.AppendEmpty().SetBool(false)
	sl.AppendEmpty().SetInt(-1234567890123)
	sl.AppendEmpty().SetDouble(-3.5)
	sl.AppendEmpty().SetEmptyBytes().FromRaw([]byte{1, 2, 3})
	subMap := sl.AppendEmpty().SetEmptyMap()
	subMap.PutStr("k", "v")
	subMap.PutEmptySlice("inner").AppendEmpty().SetInt(9)

	ra.PutEmptyMap("map.empty")
	mp := ra.PutEmptyMap("map.nested")
	mp.PutStr("nested.str", "abc")
	mp.PutBool("nested.bool", true)
	mp.PutEmpty("nested.empty")
	mp.PutStr("", "empty-key-value")

	// Scope 0: rich scope with schema_url, attributes, dropped count, 2 spans (1 rich, 1 zero-TraceID/minimal).
	ss0 := rs.ScopeSpans().AppendEmpty()
	ss0.SetSchemaUrl("https://opentelemetry.io/schemas/scope/1.0")
	ss0.Scope().SetName("net/http")
	ss0.Scope().SetVersion("1.2.3")
	ss0.Scope().SetDroppedAttributesCount(2)
	ss0.Scope().Attributes().PutStr("scope.attr", "val")
	ss0.Scope().Attributes().PutInt("scope.num", -7)

	sp0 := ss0.Spans().AppendEmpty()
	sp0.SetTraceID(tid)
	sp0.SetSpanID(uInt64ToSpanID(101))
	sp0.SetParentSpanID(uInt64ToSpanID(100))
	sp0.TraceState().FromRaw("ot=th:8;rv:abcdef01234567,vendor=foo")
	sp0.SetFlags(0x101)
	sp0.SetName("POST /orders/checkout")
	sp0.SetKind(ptrace.SpanKindServer)
	sp0.SetStartTimestamp(1700000000123456789)
	sp0.SetEndTimestamp(1700000001987654321)
	sp0.SetDroppedAttributesCount(3)
	sp0.SetDroppedEventsCount(1)
	sp0.SetDroppedLinksCount(2)
	sp0.Status().SetCode(ptrace.StatusCodeError)
	sp0.Status().SetMessage("upstream timeout after 1.8s")
	sp0.Attributes().PutStr("http.method", "POST")
	sp0.Attributes().PutInt("http.status", 504)
	sp0.Attributes().PutEmpty("nil.attr")
	sp0.Attributes().PutStr("long.payload", string(bytes.Repeat([]byte("span-attr-data-0123456789-"), 15)))

	ev := sp0.Events().AppendEmpty()
	ev.SetTimestamp(1700000000500000000)
	ev.SetName("exception")
	ev.SetDroppedAttributesCount(4)
	ev.Attributes().PutStr("exception.type", "TimeoutError")
	ev.Attributes().PutBool("exception.escaped", true)

	lk := sp0.Links().AppendEmpty()
	lk.SetTraceID(uInt64ToTraceID(999))
	lk.SetSpanID(uInt64ToSpanID(888))
	lk.TraceState().FromRaw("k=v")
	lk.SetFlags(0x200)
	lk.SetDroppedAttributesCount(5)
	lk.Attributes().PutStr("link.kind", "follows_from")

	lkEmpty := sp0.Links().AppendEmpty()
	_ = lkEmpty // zero TraceID/SpanID link

	// Scope 1: empty Scope metadata, 1 minimal span.
	ss1 := rs.ScopeSpans().AppendEmpty()
	sp1 := ss1.Spans().AppendEmpty()
	sp1.SetTraceID(tid)
	sp1.SetSpanID(uInt64ToSpanID(102))
	sp1.SetName("SELECT * FROM orders")

	return td
}

func TestWireCodec_LosslessPdataAndExactProtoSize(t *testing.T) {
	for _, tid := range []pcommon.TraceID{uInt64ToTraceID(42), {}} {
		td := buildRichTrace(tid)
		rs := td.ResourceSpans().At(0)
		scopeIndices := []int{0, 1}
		spanIndices := [][]int{{0}, {0}}

		rawRS, err := appendRawResourceSpansGroup(nil, rs, nil, scopeIndices, spanIndices)
		require.NoError(t, err)

		var m ptrace.ProtoMarshaler
		require.Equal(t, m.ResourceSpansSize(rs), len(rawRS), "appendRawResourceSpansGroup must match pdata ProtoMarshaler exact wire byte size")

		wire := appendLenTagBytes(nil, 0x0a, rawRS)
		var u ptrace.ProtoUnmarshaler
		got, err := u.UnmarshalTraces(wire)
		require.NoError(t, err)
		assertTracesDeepEqual(t, td, got)
	}
}

func TestWireCodec_RawWireSplitAndUnknownFields(t *testing.T) {
	// Helper to append synthetic unknown fields across wire types 0, 1, 2, 3/4 (nested group), and 5.
	appendUnknownFields := func(dst []byte, baseField uint64) []byte {
		// Wire type 0: Varint
		dst = binary.AppendUvarint(dst, (baseField<<3)|0)
		dst = binary.AppendUvarint(dst, 123456789)
		// Wire type 1: I64
		dst = binary.AppendUvarint(dst, ((baseField+1)<<3)|1)
		dst = binary.LittleEndian.AppendUint64(dst, 0x0123456789abcdef)
		// Wire type 2: LEN
		dst = binary.AppendUvarint(dst, ((baseField+2)<<3)|2)
		dst = binary.AppendUvarint(dst, 5)
		dst = append(dst, "hello"...)
		// Wire type 3/4: SGROUP ... nested SGROUP/EGROUP ... EGROUP
		gOuter := baseField + 3
		gInner := baseField + 4
		dst = binary.AppendUvarint(dst, (gOuter<<3)|3)
		dst = binary.AppendUvarint(dst, (901<<3)|0)
		dst = binary.AppendUvarint(dst, 42)
		dst = binary.AppendUvarint(dst, (gInner<<3)|3)
		dst = binary.AppendUvarint(dst, (902<<3)|5)
		dst = binary.LittleEndian.AppendUint32(dst, 0xdeadbeef)
		dst = binary.AppendUvarint(dst, (gInner<<3)|4)
		dst = binary.AppendUvarint(dst, (gOuter<<3)|4)
		// Wire type 5: I32
		dst = binary.AppendUvarint(dst, ((baseField+5)<<3)|5)
		dst = binary.LittleEndian.AppendUint32(dst, 0xcafebabe)
		return dst
	}

	tid1 := uInt64ToTraceID(1001)
	tid2 := uInt64ToTraceID(2002)
	var zeroTID pcommon.TraceID

	// Build Span A (tid1) in reverse field order with duplicate TraceID (last-wins) + unknown fields 0..5.
	var spanA []byte
	spanA = appendUnknownFields(spanA, 50)
	spanA = appendLenTagString(spanA, 0x2a, "span-a")
	spanA = appendLenTagBytes(spanA, 0x0a, tid2[:]) // overridden by second occurrence!
	spanA = appendLenTagBytes(spanA, 0x0a, tid1[:]) // last-wins -> tid1
	sid1 := uInt64ToSpanID(1)
	spanA = appendLenTagBytes(spanA, 0x12, sid1[:])

	// Build Span B (omitted proto3 zero TraceID!) with unknown fields 0..5.
	var spanB []byte
	spanB = appendLenTagString(spanB, 0x2a, "span-zero-tid")
	spanB = appendUnknownFields(spanB, 60)

	// Build ScopeSpans in REVERSE order: Spans BEFORE Scope + unknown fields + SchemaUrl.
	var ssRaw []byte
	ssUnknown := appendUnknownFields(nil, 70)
	ssRaw = appendLenTagBytes(ssRaw, 0x12, spanA)
	ssRaw = appendLenTagBytes(ssRaw, 0x12, spanB)
	ssRaw = append(ssRaw, ssUnknown...)
	ssRaw = appendLenTagString(ssRaw, 0x1a, "https://scope.schema/v1")
	scopeBody := appendLenTagString(nil, 0x0a, "custom-scope")
	ssRaw = appendLenTagBytes(ssRaw, 0x0a, scopeBody)

	// Build ResourceSpans in REVERSE order: SchemaUrl, ScopeSpans, DeprecatedScopeSpans (1000), unknown fields, Resource LAST!
	var rsRaw []byte
	rsUnknown := appendUnknownFields(nil, 80)
	rsRaw = appendLenTagString(rsRaw, 0x1a, "https://res.schema/v1")
	rsRaw = appendLenTagBytes(rsRaw, 0x12, ssRaw)
	// Deprecated field 1000 carrying Span C (tid2).
	var spanC []byte
	spanC = appendLenTagBytes(spanC, 0x0a, tid2[:])
	spanC = appendLenTagString(spanC, 0x2a, "span-deprecated")
	var depSS []byte
	depSS = appendLenTagBytes(depSS, 0x12, spanC)
	rsRaw = append(rsRaw, 0xc2, 0x3e)
	rsRaw = binary.AppendUvarint(rsRaw, uint64(len(depSS)))
	rsRaw = append(rsRaw, depSS...)
	rsRaw = append(rsRaw, rsUnknown...)
	resBody := appendLenTagBytes(nil, 0x0a, appendLenTagString(nil, 0x0a, "svc"))
	rsRaw = appendLenTagBytes(rsRaw, 0x0a, resBody)

	// Top-level TracesData with unknown fields across wire types 0..5.
	topUnknown := appendUnknownFields(nil, 90)
	var tracesRaw []byte
	tracesRaw = append(tracesRaw, topUnknown...)
	tracesRaw = appendLenTagBytes(tracesRaw, 0x0a, rsRaw)

	split, err := splitRawWire(tracesRaw)
	require.NoError(t, err)
	require.Equal(t, topUnknown, split.topUnknownRaw, "top-level unknown fields (wire types 0..5) must round-trip verbatim")
	require.Len(t, split.groups, 3, "expected 3 distinct trace groups: tid1, zeroTID, tid2(deprecated)")

	byTID := make(map[pcommon.TraceID]rawWireTraceGroup)
	for _, g := range split.groups {
		byTID[g.traceID] = g
	}
	require.Contains(t, byTID, tid1)
	require.Contains(t, byTID, zeroTID)
	require.Contains(t, byTID, tid2)
	require.True(t, byTID[tid2].scopes[0].isDeprecated)
	require.Equal(t, spanA, byTID[tid1].scopes[0].spans[0], "raw spanA bytes (including unknown fields) must match verbatim")
	require.Equal(t, spanB, byTID[zeroTID].scopes[0].spans[0], "raw spanB bytes (omitted zero TraceID + unknown fields) must match verbatim")
	require.Contains(t, string(byTID[tid1].resHeader), string(rsUnknown), "ResourceSpans unknown fields preserved")
	require.Contains(t, string(byTID[tid1].scopes[0].scopeHeader), string(ssUnknown), "ScopeSpans unknown fields preserved")

	// Reassemble & re-split -> idempotent preservation of ALL unknown fields and spans!
	reassembled := reassembleRawWire(split.groups, split.topUnknownRaw)
	split2, err := splitRawWire(reassembled)
	require.NoError(t, err)
	require.Equal(t, split.topUnknownRaw, split2.topUnknownRaw)
	require.Len(t, split2.groups, 3)
	for i := range split.groups {
		require.Equal(t, split.groups[i].traceID, split2.groups[i].traceID)
		require.Equal(t, split.groups[i].resHeader, split2.groups[i].resHeader)
		require.Equal(t, split.groups[i].scopes, split2.groups[i].scopes)
	}

	// F10: Same TraceID across BOTH regular field 2 (ScopeSpans) AND deprecated field 1000
	// inside ONE ResourceSpans must stay merged in ONE rawWireTraceGroup (zero Resource duplication!).
	var mixedRS []byte
	var spReg []byte
	spReg = appendLenTagBytes(spReg, 0x0a, tid1[:])
	spReg = appendLenTagString(spReg, 0x2a, "regular")
	var spDep []byte
	spDep = appendLenTagBytes(spDep, 0x0a, tid1[:])
	spDep = appendLenTagString(spDep, 0x2a, "deprecated")
	mixedRS = appendLenTagBytes(mixedRS, 0x0a, resBody)
	mixedRS = appendLenTagBytes(mixedRS, 0x12, appendLenTagBytes(nil, 0x12, spReg))
	depScope := appendLenTagBytes(nil, 0x12, spDep)
	mixedRS = append(mixedRS, 0xc2, 0x3e)
	mixedRS = binary.AppendUvarint(mixedRS, uint64(len(depScope)))
	mixedRS = append(mixedRS, depScope...)
	mixedSplit, err := splitRawWire(appendLenTagBytes(nil, 0x0a, mixedRS))
	require.NoError(t, err)
	require.Len(t, mixedSplit.groups, 1, "same TraceID in field 2 and field 1000 must remain 1 ResourceSpans group")
	require.Len(t, mixedSplit.groups[0].scopes, 2)
	require.False(t, mixedSplit.groups[0].scopes[0].isDeprecated)
	require.True(t, mixedSplit.groups[0].scopes[1].isDeprecated)
	mixedReasm := reassembleRawWire(mixedSplit.groups, nil)
	mixedSplit2, err := splitRawWire(mixedReasm)
	require.NoError(t, err)
	require.Len(t, mixedSplit2.groups, 1)
	require.Equal(t, mixedSplit.groups[0].scopes, mixedSplit2.groups[0].scopes)

	// Explicit 0-length field 1 (trace_id) overriding an earlier non-zero trace_id -> zero TraceID!
	var overrideZero []byte
	overrideZero = appendLenTagBytes(overrideZero, 0x0a, tid1[:])
	overrideZero = append(overrideZero, 0x0a, 0x00) // last-wins zero override
	gotZero, err := extractSpanTraceIDFromWire(overrideZero)
	require.NoError(t, err)
	require.Equal(t, pcommon.TraceID{}, gotZero)

	// Upstream pdata@v1.71.1 bug note: internal/proto/unmarshal.go ConsumeUnknown returns early
	// on the first Varint/I64/Len/I32 field inside a non-empty proto2 group (depth > 0), causing
	// ErrUnexpectedEndOfGroup even on valid protobuf groups. Verify both:
	// (a) pdata rejects the non-empty group above (documenting the upstream bug), AND
	// (b) pdata cleanly parses reassembled wire bytes carrying unknown wire types 0, 1, 2, 5, and empty 3/4 groups!
	var u ptrace.ProtoUnmarshaler
	_, pdataErr := u.UnmarshalTraces(reassembled)
	require.ErrorContains(t, pdataErr, "unexpected end of group", "documents upstream pdata@v1.71.1 ConsumeUnknown bug on non-empty proto2 groups")

	var pdataCompatSpan []byte
	pdataCompatSpan = binary.AppendUvarint(pdataCompatSpan, (50<<3)|0)
	pdataCompatSpan = binary.AppendUvarint(pdataCompatSpan, 12345)
	pdataCompatSpan = binary.AppendUvarint(pdataCompatSpan, (51<<3)|1)
	pdataCompatSpan = binary.LittleEndian.AppendUint64(pdataCompatSpan, 0x0123456789abcdef)
	pdataCompatSpan = binary.AppendUvarint(pdataCompatSpan, (52<<3)|2)
	pdataCompatSpan = binary.AppendUvarint(pdataCompatSpan, 3)
	pdataCompatSpan = append(pdataCompatSpan, "abc"...)
	pdataCompatSpan = binary.AppendUvarint(pdataCompatSpan, (53<<3)|3) // empty group
	pdataCompatSpan = binary.AppendUvarint(pdataCompatSpan, (53<<3)|4)
	pdataCompatSpan = binary.AppendUvarint(pdataCompatSpan, (54<<3)|5)
	pdataCompatSpan = binary.LittleEndian.AppendUint32(pdataCompatSpan, 0xdeadbeef)
	pdataCompatSpan = appendLenTagBytes(pdataCompatSpan, 0x0a, tid1[:])
	pdataCompatSpan = appendLenTagString(pdataCompatSpan, 0x2a, "compat-span")
	compatSS := appendLenTagBytes(nil, 0x12, pdataCompatSpan)
	compatRS := appendLenTagBytes(nil, 0x12, compatSS)
	compatWire := appendLenTagBytes(nil, 0x0a, compatRS)
	compatSplit, err := splitRawWire(compatWire)
	require.NoError(t, err)
	compatParsed, err := u.UnmarshalTraces(reassembleRawWire(compatSplit.groups, compatSplit.topUnknownRaw))
	require.NoError(t, err)
	require.Equal(t, 1, compatParsed.SpanCount())
	require.Equal(t, "compat-span", compatParsed.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name())

	// Malformed wire inputs must return errors cleanly without panicking.
	t.Run("MalformedWireErrors", func(t *testing.T) {
		// Invalid wire type 6.
		_, err := splitRawWire([]byte{(1 << 3) | 6})
		require.Error(t, err)
		// Unexpected top-level EGROUP.
		_, err = splitRawWire([]byte{(1 << 3) | 4})
		require.Error(t, err)
		// Mismatched group end tag.
		_, err = splitRawWire([]byte{(2 << 3) | 3, (3 << 3) | 4})
		require.Error(t, err)
		// Truncated length-delimited field.
		_, err = splitRawWire([]byte{0x0a, 50, 0x01, 0x02})
		require.Error(t, err)
		// Excessively nested group (> maxWireGroupDepth = 64).
		var deep []byte
		for range maxWireGroupDepth + 2 {
			deep = append(deep, (2<<3)|3)
		}
		for range maxWireGroupDepth + 2 {
			deep = append(deep, (2<<3)|4)
		}
		_, err = splitRawWire(deep)
		require.ErrorIs(t, err, errWireDepthExceeded)
		// Invalid TraceID length (10 bytes instead of 16).
		badSpan := appendLenTagBytes(nil, 0x0a, make([]byte, 10))
		_, err = extractSpanTraceIDFromWire(badSpan)
		require.Error(t, err)
		// F11: Field number > maxProtoFieldNum ((1<<29)-1) rejected cleanly.
		badTag := binary.AppendUvarint(nil, (uint64(1<<30)<<3)|0)
		badTag = append(badTag, 0x01)
		_, err = splitRawWire(badTag)
		require.ErrorIs(t, err, errWireInvalidTag)
		_, err = extractSpanTraceIDFromWire(badTag)
		require.ErrorIs(t, err, errWireInvalidTag)
	})
}

func TestLZ4_RoundTripAndCorruptSafety(t *testing.T) {
	var st lz4State

	cases := []struct {
		name     string
		data     []byte
		wantComp bool
	}{
		{name: "rle-1KB", data: bytes.Repeat([]byte("A"), 1024), wantComp: true},
		{name: "periodic-3B-2KB", data: bytes.Repeat([]byte("abc"), 700), wantComp: true},
		{name: "extended-match-and-literals", data: append(append(bytes.Repeat([]byte("X"), 600), bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7}, 90)...), bytes.Repeat([]byte("Y"), 600)...), wantComp: true},
		{name: "otlp-multi-span-chunk", data: func() []byte {
			batch := generateBenchBatchShaped(16, 1, func(int, int) int { return 0 })
			var m ptrace.ProtoMarshaler
			b, err := m.MarshalTraces(batch)
			require.NoError(t, err)
			return b
		}(), wantComp: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			comp, ok := st.compressBlock(tc.data, nil, (len(tc.data)*9)/10)
			require.Equal(t, tc.wantComp, ok)
			require.Less(t, len(comp), (len(tc.data)*9)/10)

			got, err := lz4DecompressBlock(comp, nil, len(tc.data))
			require.NoError(t, err)
			require.Equal(t, tc.data, got)
		})
	}

	// Random incompressible data must bail out cleanly (ok == false).
	rng := rand.New(rand.NewSource(999))
	incomp := make([]byte, 4096)
	rng.Read(incomp)
	_, ok := st.compressBlock(incomp, nil, (len(incomp)*9)/10)
	require.False(t, ok, "random incompressible data must not claim >=10% compression")

	// Tiny data (< 12 bytes).
	_, ok = st.compressBlock([]byte("short"), nil, 5)
	require.False(t, ok)

	// Hash table base wraparound across math.MaxUint32.
	st.base = math.MaxUint32 - 50
	payload := bytes.Repeat([]byte("opentelemetry-tail-sampling-"), 60)
	for range 5 {
		comp, compressed := st.compressBlock(payload, nil, len(payload))
		require.True(t, compressed)
		got, err := lz4DecompressBlock(comp, nil, len(payload))
		require.NoError(t, err)
		require.Equal(t, payload, got)
	}

	// Randomized mixed compressible/incompressible round-trips.
	var compBuf, decompBuf []byte
	for i := range 200 {
		sz := 16 + rng.Intn(4096)
		buf := make([]byte, sz)
		pattern := fmt.Sprintf("service-%d/http.route=/api/v1/items/%d;", i%5, i%13)
		for pos := 0; pos < sz; {
			if rng.Intn(3) == 0 {
				buf[pos] = byte(rng.Intn(256))
				pos++
			} else {
				n := copy(buf[pos:], pattern)
				pos += n
			}
		}
		var ok bool
		compBuf, ok = st.compressBlock(buf, compBuf[:0], len(buf))
		if ok {
			var err error
			decompBuf, err = lz4DecompressBlock(compBuf, decompBuf[:0], len(buf))
			require.NoError(t, err)
			require.Equal(t, buf, decompBuf)
		}
	}

	// Corrupt / malformed blocks must fail safely without panicking.
	badInputs := [][]byte{
		{0xf0},                         // truncated extended literal len
		{0x50, 1, 2},                   // truncated literals
		{0x11, 'a'},                    // non-zero low nibble on last sequence
		{0x10, 'a', 0x00},              // truncated offset
		{0x10, 'a', 0x00, 0x00},        // offset == 0
		{0x10, 'a', 0x05, 0x00},        // offset > len(dst)
		{0x1f, 'a', 0x01, 0x00},        // truncated extended match len
		{0x10, 'a', 0x01, 0x00, 0x10},  // size overflow / mismatch
		{0xf0, 0xff, 0xff, 0xff, 0x00}, // excessive litLen > origSize
	}
	for i, bad := range badInputs {
		_, err := lz4DecompressBlock(bad, nil, 64)
		require.Error(t, err, "bad input #%d should return error", i)
	}
	_, err := lz4DecompressBlock([]byte{0x01}, nil, 0)
	require.Error(t, err)
	_, err = lz4DecompressBlock(nil, nil, -1)
	require.Error(t, err)
}

func allArenaModes() []struct {
	name string
	mode arenaStorageMode
} {
	return []struct {
		name string
		mode arenaStorageMode
	}{
		{name: "rawProtoSlab", mode: arenaModeRawProtoSlab},
		{name: "internedHeap", mode: arenaModeInternedHeap},
		{name: "internedMmap", mode: arenaModeInternedMmap},
		{name: "lz4Heap", mode: arenaModeLZ4Heap},
		{name: "lz4Mmap", mode: arenaModeLZ4Mmap},
	}
}

func TestArena_LosslessRoundTripAll5Modes(t *testing.T) {
	for _, tc := range allArenaModes() {
		t.Run(tc.name, func(t *testing.T) {
			store := newArenaPayloadStore(tc.mode, 16*1024, 2)
			defer func() { require.NoError(t, store.Close()) }()

			// Lazy allocation check: 0 mmap bytes & 0 slabs before first Append!
			st0 := store.Stats()
			require.Equal(t, 0, st0.LiveSlabs)
			require.Equal(t, uint64(0), st0.MmapActiveBytes)

			// 1. Rich multi-scope trace (including zero TraceID and non-zero TraceID) via Append().
			for _, tid := range []pcommon.TraceID{uInt64ToTraceID(1), {}} {
				want := buildRichTrace(tid)
				require.NoError(t, store.Append(tid, want))
				got, err := store.Take(tid)
				require.NoError(t, err)
				assertTracesDeepEqual(t, want, got)
			}

			// 2. Multi-batch AppendGroup preserving exact ResourceSpans count and TracesSize.
			tidMulti := uInt64ToTraceID(77)
			b1 := buildRichTrace(tidMulti)
			b2 := buildRichTrace(tidMulti)
			b2.ResourceSpans().At(0).Resource().Attributes().PutStr("batch", "second")
			require.NoError(t, store.Append(tidMulti, b1))
			require.NoError(t, store.Append(tidMulti, b2))
			wantMerged := ptrace.NewTraces()
			b1.ResourceSpans().At(0).CopyTo(wantMerged.ResourceSpans().AppendEmpty())
			b2.ResourceSpans().At(0).CopyTo(wantMerged.ResourceSpans().AppendEmpty())
			gotMerged, err := store.Take(tidMulti)
			require.NoError(t, err)
			assertTracesDeepEqual(t, wantMerged, gotMerged)

			// 3. Batch-pinned AppendGroup (BeginResourceSpans / FinishResourceSpans) + TakeMany.
			batch := generateBenchBatchShaped(16, 1, func(int, int) int { return 0 })
			rs := batch.ResourceSpans().At(0)
			if bs, ok := store.(ivmBatchAwareStore); ok {
				bs.BeginResourceSpans(rs.ScopeSpans().Len())
			}
			var ids []pcommon.TraceID
			wantByID := make(map[pcommon.TraceID]ptrace.Traces)
			for i := range 16 {
				tid := uInt64ToTraceID(uint64(i))
				ids = append(ids, tid)
				var scopeIdxs []int
				var spanIdxs [][]int
				for sIdx := 0; sIdx < rs.ScopeSpans().Len(); sIdx++ {
					var idxs []int
					spans := rs.ScopeSpans().At(sIdx).Spans()
					for k := 0; k < spans.Len(); k++ {
						if spans.At(k).TraceID() == tid {
							idxs = append(idxs, k)
						}
					}
					if len(idxs) > 0 {
						scopeIdxs = append(scopeIdxs, sIdx)
						spanIdxs = append(spanIdxs, idxs)
					}
				}
				require.NoError(t, store.AppendGroup(tid, rs, scopeIdxs, spanIdxs))
				wantTd := ptrace.NewTraces()
				dstRS := wantTd.ResourceSpans().AppendEmpty()
				rs.Resource().CopyTo(dstRS.Resource())
				for pIdx, sIdx := range scopeIdxs {
					srcSS := rs.ScopeSpans().At(sIdx)
					dstSS := dstRS.ScopeSpans().AppendEmpty()
					srcSS.Scope().CopyTo(dstSS.Scope())
					for _, k := range spanIdxs[pIdx] {
						srcSS.Spans().At(k).CopyTo(dstSS.Spans().AppendEmpty())
					}
				}
				wantByID[tid] = wantTd
			}
			if bs, ok := store.(ivmBatchAwareStore); ok {
				bs.FinishResourceSpans()
			}
			gotSlice, errs := store.(tailstorageextension.BatchTailStorage).TakeMany(ids)
			for i, tid := range ids {
				require.NoError(t, errs[i])
				assertTracesDeepEqual(t, wantByID[tid], gotSlice[i])
			}

			// 4. Oversized chunk (> 16 KiB slabSize -> dedicated slab path).
			tidBig := uInt64ToTraceID(9999)
			bigTd := ptrace.NewTraces()
			bigRS := bigTd.ResourceSpans().AppendEmpty()
			bigRS.Resource().Attributes().PutStr("service.name", "big-svc")
			bigSp := bigRS.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
			bigSp.SetTraceID(tidBig)
			bigSp.SetSpanID(uInt64ToSpanID(1))
			bigSp.Attributes().PutStr("giant", string(bytes.Repeat([]byte("abcdefgh01234567"), 3000))) // 48 KiB
			require.NoError(t, store.Append(tidBig, bigTd))
			gotBig, err := store.Take(tidBig)
			require.NoError(t, err)
			assertTracesDeepEqual(t, bigTd, gotBig)

			// 5. Empty Take returns empty Traces, nil error.
			emptyTd, err := store.Take(uInt64ToTraceID(777777))
			require.NoError(t, err)
			require.Equal(t, 0, emptyTd.ResourceSpans().Len())

			// 6. All traces taken -> zero leaks even before Close()!
			store.OnTick()
			st := store.Stats()
			require.Equal(t, 0, st.LiveSlabs)
			require.Equal(t, uint64(0), st.LiveChunks)
			require.Equal(t, 0, st.ResourceInternEntries)
			require.Equal(t, 0, st.ScopeInternEntries)
			require.Equal(t, 0, st.ResourceInternPeak)
			require.Equal(t, 0, st.ScopeInternPeak)
			require.Equal(t, uint64(0), st.MmapActiveBytes)
		})
	}

	// Direct arenaInternTable empty slice + hash collision chaining tests.
	t.Run("InternTableEmptyAndCollisionChaining", func(t *testing.T) {
		tbl := newArenaInternTable(false, 4096, false, nil, 1)
		defer func() { _ = tbl.close() }()

		id0, err := tbl.intern([]byte{})
		require.NoError(t, err)
		id0Dup, err := tbl.intern([]byte{})
		require.NoError(t, err)
		require.Equal(t, id0, id0Dup)
		out, err := tbl.appendBytes(id0, nil)
		require.NoError(t, err)
		require.Empty(t, out)
		tbl.release(id0)
		require.Equal(t, 1, tbl.liveEntries)
		tbl.release(id0Dup)
		require.Equal(t, 0, tbl.liveEntries)

		// Force a synthetic hash collision inside buckets[42] to test intrusive nextInBucket chaining!
		idA, err := tbl.intern([]byte("alpha-entry"))
		require.NoError(t, err)
		idB, err := tbl.intern([]byte("beta-entry"))
		require.NoError(t, err)
		// Artificially re-bucket both under identical hash 42 to verify collision chain unlink.
		clear(tbl.buckets)
		tbl.entries[idA].hash = 42
		tbl.entries[idB].hash = 42
		tbl.entries[idA].nextInBucket = arenaNoIdx
		tbl.entries[idB].nextInBucket = idA
		tbl.buckets[42] = idB

		tbl.release(idA) // unlink tail of collision chain
		require.Equal(t, idB, tbl.buckets[42])
		require.Equal(t, arenaNoIdx, tbl.entries[idB].nextInBucket)
		tbl.release(idB) // unlink head & delete bucket
		require.Empty(t, tbl.buckets)
		require.Equal(t, 0, tbl.liveEntries)
	})

	// Corrupt chunk inside Take() must return error AND still release chunks/intern refs!
	t.Run("TakeCorruptChunkStillReleasesRefs", func(t *testing.T) {
		as := newArenaPayloadStore(arenaModeLZ4Mmap, 16*1024, 1).(*arenaPayloadStore)
		defer func() { require.NoError(t, as.Close()) }()

		tid := uInt64ToTraceID(555)
		td := buildRichTrace(tid)
		require.NoError(t, as.Append(tid, td))

		// Corrupt stored compressed bytes of chunk 0.
		list := as.traceChunks[tid]
		c := &as.chunks[list.head]
		prefixLen := int(c.scopeCount) * 4
		sl := &as.chunkPool.slabs[c.slabID]
		for i := prefixLen; i < int(c.storedLen); i++ {
			sl.buf[c.offset+uint32(i)] = 0xff
		}

		_, err := as.Take(tid)
		require.Error(t, err)

		st := as.Stats()
		require.Equal(t, uint64(0), st.LiveChunks)
		require.Equal(t, 0, st.ResourceInternEntries)
		require.Equal(t, 0, st.ScopeInternEntries)
		require.Equal(t, uint64(0), st.MmapActiveBytes)
	})
}

func TestArena_ZeroLeaksLifecycleAndShutdown(t *testing.T) {
	base := time.Unix(1700000000, 0)

	// 1. 50,000-trace stress lifecycle covering tick expiry, early drop, early keep, overflow,
	//    and 10,000 ephemeral unique k8s.pod.uid resources on lz4Mmap and internedHeap!
	for _, mode := range []arenaStorageMode{arenaModeInternedHeap, arenaModeLZ4Mmap} {
		t.Run(fmt.Sprintf("50kTraces_mode=%d", mode), func(t *testing.T) {
			store := newArenaPayloadStore(mode, 64*1024, 2)
			defer func() { require.NoError(t, store.Close()) }()

			template := ptrace.NewTraces()
			rs := template.ResourceSpans().AppendEmpty()
			rs.Resource().Attributes().PutStr("service.name", "shop")
			rs.Resource().Attributes().PutStr("k8s.cluster.name", "prod-us-east-1")
			ss := rs.ScopeSpans().AppendEmpty()
			ss.Scope().SetName("net/http")
			sp := ss.Spans().AppendEmpty()
			sp.SetName("GET /api/v1/items")
			sp.Attributes().PutStr("http.route", "/api/v1/items")
			sp.Attributes().PutStr("user_agent", "Mozilla/5.0 (Linux x86_64)")
			sp.Attributes().PutStr("server.address", "items.shop.svc.cluster.local")

			const total = 50_000
			scopeIdxs := []int{0}
			spanIdxs := [][]int{{0}}
			var liveQueue []pcommon.TraceID

			for i := range total {
				tid := uInt64ToTraceID(uint64(i + 1))
				sp.SetTraceID(tid)
				sp.SetSpanID(uInt64ToSpanID(uint64(i + 1)))
				// First 10,000 traces carry ephemeral unique k8s.pod.uid!
				if i < 10_000 {
					rs.Resource().Attributes().PutStr("k8s.pod.uid", fmt.Sprintf("pod-uid-%06d", i))
				} else {
					rs.Resource().Attributes().PutStr("k8s.pod.uid", fmt.Sprintf("shared-pod-%d", i%8))
				}
				require.NoError(t, store.AppendGroup(tid, rs, scopeIdxs, spanIdxs))
				liveQueue = append(liveQueue, tid)

				// Simulate mixed lifecycle: delete every 3rd trace early, take every 50th trace,
				// expire window every 250 traces.
				if i%3 == 0 {
					require.NoError(t, store.Delete(tid))
				} else if i%50 == 0 {
					got, err := store.Take(tid)
					require.NoError(t, err)
					require.Equal(t, 1, got.SpanCount())
				}
				if i%1000 == 999 {
					for _, old := range liveQueue {
						require.NoError(t, store.Delete(old))
					}
					liveQueue = liveQueue[:0]
					store.OnTick()
				}
			}
			for _, old := range liveQueue {
				require.NoError(t, store.Delete(old))
			}
			store.OnTick()

			st := store.Stats()
			require.Equal(t, 0, st.LiveSlabs, "no live slabs after draining 50,000 traces")
			require.Equal(t, uint64(0), st.LiveChunks, "no live chunks after draining 50,000 traces")
			require.Equal(t, 0, st.ResourceInternEntries)
			require.Equal(t, 0, st.ScopeInternEntries)
			require.Equal(t, 0, st.ResourceInternPeak, "peakEntries must reset to 0 when empty")
			require.Equal(t, 0, st.ScopeInternPeak)
			require.Equal(t, uint64(0), st.MmapActiveBytes, "mmapActiveBytes must return to 0")
			require.Equal(t, 0, st.PooledSlabs)
		})
	}

	// 2. Start() error path leaks 0 mmap bytes!
	t.Run("StartErrorPathZeroMmapLeak", func(t *testing.T) {
		cfg := Config{
			SamplingStrategy: samplingStrategyIncremental,
			DecisionWait:     defaultTestDecisionWait,
			NumTraces:        100,
			PolicyCfgs: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "bad-num", Type: NumericAttribute, NumericAttributeCfg: NumericAttributeCfg{Key: "k", MinValue: 0, MaxValue: 0}}},
			},
			Options: []Option{withArenaStorageMode(arenaModeLZ4Mmap)},
		}
		p, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), new(consumertest.TracesSink), cfg)
		require.NoError(t, err)
		require.Error(t, p.Start(t.Context(), componenttest.NewNopHost()))
		require.Nil(t, shard0(p).ivm, "failed initIncremental must leave ivm nil (0 mmap allocated)")
	})

	// 3. DropPendingTracesOnShutdown: true closes & munmaps ALL active slabs before Shutdown returns!
	t.Run("DropPendingTracesOnShutdown_MunmapsAll", func(t *testing.T) {
		controller := newTestTSPController()
		cfg := Config{
			SamplingStrategy:            samplingStrategyIncremental,
			DecisionWait:                10 * time.Second,
			NumTraces:                   1000,
			DropPendingTracesOnShutdown: true,
			PolicyCfgs: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
			},
			Options: []Option{withTestController(controller), withIDBatcher(), withArenaStorageMode(arenaModeLZ4Mmap)},
		}
		p, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), new(consumertest.TracesSink), cfg)
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))

		batch := generateBenchBatchShaped(64, 4, func(trace, span int) int { return (trace + span) % 4 })
		// Clear ERROR status so all traces stay buffered.
		for _, rs := range batch.ResourceSpans().All() {
			for _, ss := range rs.ScopeSpans().All() {
				for _, sp := range ss.Spans().All() {
					sp.Status().SetCode(ptrace.StatusCodeOk)
				}
			}
		}
		require.NoError(t, p.ConsumeTraces(t.Context(), batch))
		controller.waitForTick()
		require.NoError(t, p.Shutdown(t.Context()))

		st := shard0(p).ivm.payloadStore.Stats()
		require.Equal(t, 0, st.LiveSlabs)
		require.Equal(t, uint64(0), st.LiveChunks)
		require.Equal(t, uint64(0), st.MmapActiveBytes)
		require.Equal(t, uint64(0), st.UnmarshalCount, "0% sampled -> zero unmarshals!")
	})

	// 4. ctx.Done() cancels loop() and runs defer closeIVMStorage() BEFORE doneChan closes!
	t.Run("CtxCancelMunmapsBeforeShutdownReturns", func(t *testing.T) {
		runCtx, cancel := context.WithCancel(t.Context())
		controller := newTestTSPController()
		cfg := Config{
			SamplingStrategy: samplingStrategyIncremental,
			DecisionWait:     10 * time.Second,
			NumTraces:        100,
			PolicyCfgs: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
			},
			Options: []Option{withTestController(controller), withIDBatcher(), withArenaStorageMode(arenaModeInternedMmap)},
		}
		p, err := newTracesProcessor(runCtx, processortest.NewNopSettings(metadata.Type), new(consumertest.TracesSink), cfg)
		require.NoError(t, err)
		require.NoError(t, p.Start(runCtx, componenttest.NewNopHost()))
		require.NoError(t, p.ConsumeTraces(runCtx, makeSpanBatch(uInt64ToTraceID(1), 1, 0, "svc", base, base.Add(time.Millisecond), ptrace.StatusCodeOk, nil)))
		cancel()
		require.NoError(t, p.Shutdown(t.Context()))

		st := shard0(p).ivm.payloadStore.Stats()
		require.Equal(t, 0, st.LiveSlabs)
		require.Equal(t, uint64(0), st.LiveChunks)
		require.Equal(t, uint64(0), st.MmapActiveBytes)
	})

	// 5. Full processor tick-expiry zero-unmarshal proof on W3 (0% sampled) across ALL 5 arena modes.
	for _, tc := range allArenaModes() {
		t.Run("ZeroUnmarshalOnW3ExpiredTraces/"+tc.name, func(t *testing.T) {
			controller := newTestTSPController()
			sink := new(consumertest.TracesSink)
			cfg := Config{
				SamplingStrategy: samplingStrategyIncremental,
				DecisionWait:     defaultTestDecisionWait,
				NumTraces:        5000,
				PolicyCfgs: []PolicyCfg{
					{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
				},
				Options: []Option{withTestController(controller), withArenaStorageMode(tc.mode)},
			}
			p, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), sink, cfg)
			require.NoError(t, err)
			require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))

			batch := generateBenchBatchShaped(128, 4, func(trace, span int) int { return (trace + span) % 4 })
			for _, rs := range batch.ResourceSpans().All() {
				for _, ss := range rs.ScopeSpans().All() {
					for _, sp := range ss.Spans().All() {
						sp.Status().SetCode(ptrace.StatusCodeOk)
					}
				}
			}
			for i := range 5 {
				require.NoError(t, p.ConsumeTraces(t.Context(), cloneWithNewTraceIDs(batch, uint64(i+1))))
			}
			controller.waitForTick()
			controller.waitForTick()
			require.NoError(t, p.Shutdown(t.Context()))

			require.Equal(t, 0, sink.SpanCount())
			require.Equal(t, int64(640), shard0(p).ivm.stats.droppedWithoutUnmarshal.Load())
			st := shard0(p).ivm.payloadStore.Stats()
			require.Equal(t, uint64(0), st.UnmarshalCount, "W3 0% sampled must perform ZERO unmarshals")
			require.Equal(t, 0, st.LiveSlabs)
			require.Equal(t, uint64(0), st.MmapActiveBytes)
		})
	}
}

func TestArena_SteadyStatePinningAndCompaction(t *testing.T) {
	// Part A: Chunk slab pinning — 1% of traces remain pinned across ticks;
	// verify < 25% utilization compaction kicks in and keeps slab ratio <= 2.0 across
	// BOTH mmap AND heap arenas (F4: heap arenas must NOT pool dead slabs, so TotalAllocatedBytes / livePayloadBytes <= 2.0!).
	pinModes := []struct {
		name string
		mode arenaStorageMode
	}{
		{name: "internedMmap", mode: arenaModeInternedMmap},
		{name: "internedHeap", mode: arenaModeInternedHeap},
		{name: "lz4Heap", mode: arenaModeLZ4Heap},
	}
	for _, pm := range pinModes {
		t.Run("ChunkSlabPinningCompaction/"+pm.name, func(t *testing.T) {
			const slabSize = 32 * 1024
			as := newArenaPayloadStore(pm.mode, slabSize, 2).(*arenaPayloadStore)
			defer func() { require.NoError(t, as.Close()) }()

			rng := rand.New(rand.NewSource(4242))
			// Use high-entropy payload so even lz4Heap stores >64 KiB of live payload (3+ slabs).
			baseBuf := make([]byte, 1200)
			for i := range baseBuf {
				baseBuf[i] = byte('a' + rng.Intn(26))
			}
			td := ptrace.NewTraces()
			rs := td.ResourceSpans().AppendEmpty()
			rs.Resource().Attributes().PutStr("service.name", "pinned-svc")
			sp := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
			scopeIdxs := []int{0}
			spanIdxs := [][]int{{0}}

			var pinnedIDs []pcommon.TraceID
			var nextID uint64 = 1

			for tick := range 60 {
				var ephemeral []pcommon.TraceID
				for i := range 100 {
					tid := uInt64ToTraceID(nextID)
					nextID++
					sp.SetTraceID(tid)
					sp.SetSpanID(uInt64ToSpanID(nextID))
					// Vary tail bytes deterministically per span while keeping size ~1.2 KiB.
					sp.Attributes().PutStr("payload", fmt.Sprintf("%s-%d-%d", string(baseBuf), tick, i))
					require.NoError(t, as.AppendGroup(tid, rs, scopeIdxs, spanIdxs))
					if i == 0 {
						pinnedIDs = append(pinnedIDs, tid)
					} else {
						ephemeral = append(ephemeral, tid)
					}
				}
				for _, id := range ephemeral {
					require.NoError(t, as.Delete(id))
				}
				as.OnTick()
			}
			for range 3 {
				as.OnTick()
			}

			st := as.Stats()
			as.mu.Lock()
			var liveSlabBytes uint64
			for i := range as.chunkPool.slabs {
				if as.chunkPool.slabs[i].inUse {
					liveSlabBytes += uint64(len(as.chunkPool.slabs[i].buf))
				}
			}
			livePayload := as.liveBytes
			compactions := as.compactionCount
			as.mu.Unlock()

			require.Positive(t, compactions, "pinned 1% slabs MUST trigger compaction")
			require.Greater(t, livePayload, uint64(64*1024), "ensure >2 slabs of pinned payload so 1-slab floor doesn't dominate")
			liveRatio := float64(liveSlabBytes) / float64(livePayload)
			t.Logf("Chunk pinning [%s]: livePayload=%d B, liveSlabBytes=%d B, totalAlloc=%d B, pooled=%d, liveRatio=%.2fx, compactions=%d",
				pm.name, livePayload, liveSlabBytes, st.TotalAllocatedBytes, st.PooledSlabs, liveRatio, compactions)
			require.LessOrEqual(t, liveRatio, 2.0, "live slab bytes / livePayloadBytes must stay <= 2.0")

			if !as.useMmap {
				// F4: Heap arenas MUST NOT pool dead slabs, so TotalAllocatedBytes == liveSlabBytes!
				require.Equal(t, 0, st.PooledSlabs, "heap arenas must not retain pooled dead slabs")
				require.Equal(t, liveSlabBytes, st.TotalAllocatedBytes)
				totalRatio := float64(st.TotalAllocatedBytes) / float64(livePayload)
				require.LessOrEqual(t, totalRatio, 2.0, "heap TotalAllocatedBytes / livePayloadBytes must stay <= 2.0")
			}

			for _, id := range pinnedIDs {
				got, err := as.Take(id)
				require.NoError(t, err)
				require.Equal(t, 1, got.SpanCount())
			}
			as.OnTick()
			stAfter := as.Stats()
			require.Equal(t, 0, stAfter.LiveSlabs)
			require.Equal(t, uint64(0), stAfter.MmapActiveBytes)
			require.Equal(t, uint64(0), stAfter.TotalAllocatedBytes)
		})
	}

	// Part B: Intern table pinning — 1 pinned long-lived Resource + 2 shared Scopes
	// while churning 10,000 ephemeral unique Resources across 40 ticks!
	t.Run("InternSlabPinningCompaction_10kEphemeralResources", func(t *testing.T) {
		const slabSize = 32 * 1024
		as := newArenaPayloadStore(arenaModeInternedMmap, slabSize, 2).(*arenaPayloadStore)
		defer func() { require.NoError(t, as.Close()) }()

		makeRS := func(podUID string, tid uint64) ptrace.Traces {
			td := ptrace.NewTraces()
			rs := td.ResourceSpans().AppendEmpty()
			a := rs.Resource().Attributes()
			a.PutStr("service.name", "shop-api")
			a.PutStr("k8s.pod.uid", podUID)
			a.PutStr("k8s.cluster.name", "prod-eu-west-1")
			a.PutStr("host.name", "ip-10-0-0-1.eu-west-1.compute.internal")
			a.PutStr("telemetry.sdk.name", "opentelemetry")
			a.PutStr("telemetry.sdk.language", "go")
			a.PutStr("deployment.environment.name", "production")
			a.PutStr("extra.padding", string(bytes.Repeat([]byte("r"), 256)))

			for i, name := range []string{"net/http", "database/sql"} {
				ss := rs.ScopeSpans().AppendEmpty()
				ss.Scope().SetName(name)
				ss.Scope().SetVersion("0.63.0")
				sp := ss.Spans().AppendEmpty()
				sp.SetTraceID(uInt64ToTraceID(tid))
				sp.SetSpanID(uInt64ToSpanID(tid*10 + uint64(i+1)))
				sp.SetName("op")
			}
			return td
		}

		// Pin 1 long-lived trace with 1 long-lived Resource + 2 constant Scopes.
		pinnedID := uInt64ToTraceID(1)
		require.NoError(t, as.Append(pinnedID, makeRS("pinned-forever-pod", 1)))

		var nextID uint64 = 2
		// Churn 10,000 ephemeral unique Resources across 40 ticks (250/tick -> ~100 KB/tick, ~4 MB total).
		for tick := range 40 {
			var eph []pcommon.TraceID
			for i := range 250 {
				id := nextID
				nextID++
				tid := uInt64ToTraceID(id)
				require.NoError(t, as.Append(tid, makeRS(fmt.Sprintf("ephemeral-pod-%d-%d", tick, i), id)))
				eph = append(eph, tid)
			}
			as.OnTick()
			for _, tid := range eph {
				require.NoError(t, as.Delete(tid))
			}
		}
		for range 3 {
			as.OnTick()
		}

		st := as.Stats()
		t.Logf("Intern pinning after 10k churn: resEntries=%d, resPeak=%d, scopeEntries=%d, internAllocBytes=%d, compactions=%d",
			st.ResourceInternEntries, st.ResourceInternPeak, st.ScopeInternEntries, st.InternAllocatedBytes, st.CompactionCount)
		require.Equal(t, 1, st.ResourceInternEntries, "only the 1 pinned Resource remains")
		require.Equal(t, 2, st.ScopeInternEntries, "2 shared Scopes remain")
		require.Positive(t, as.resIntern.compactCount, "resourceIntern MUST compact slabs pinned by 1 long-lived Resource")
		// Without intern compaction, 40 ticks * 100 KB = ~4 MB would stay pinned; with compaction,
		// allocated intern bytes stay bounded (<= active + pooled slabs, well under 512 KiB).
		require.LessOrEqual(t, st.InternAllocatedBytes, uint64(512*1024), "intern slab memory must stay tightly bounded (no monotonic RSS leak)")

		got, err := as.Take(pinnedID)
		require.NoError(t, err)
		require.Equal(t, 2, got.SpanCount())
		as.OnTick()
		require.Equal(t, uint64(0), as.Stats().MmapActiveBytes)
	})
}

func makeAllPendingBatch(shape benchShape, ephemeralUniqueRes bool) ptrace.Traces {
	var base ptrace.Traces
	if ephemeralUniqueRes {
		base = generateBenchBatchShaped(benchTracesPerBatch, benchTracesPerBatch, func(trace, _ int) int { return trace })
	} else {
		base = shape.generate()
	}
	for _, rs := range base.ResourceSpans().All() {
		for _, ss := range rs.ScopeSpans().All() {
			for _, sp := range ss.Spans().All() {
				sp.Status().SetCode(ptrace.StatusCodeOk)
			}
		}
	}
	return base
}

func measureW3Retained(t *testing.T, strategy samplingStrategy, mode arenaStorageMode, shape benchShape, ephemeralUniqueRes bool) (heapBytes int64, heapObjs int64, unmarshalCount uint64, stats ArenaStats) {
	t.Helper()
	const numBatches = 30
	template := makeAllPendingBatch(shape, ephemeralUniqueRes)

	controller := newTestTSPController()
	opts := []Option{withTestController(controller), withIDBatcher()}
	if strategy == samplingStrategyIncremental && mode != arenaModeInMemory {
		opts = append(opts, withArenaStorageMode(mode))
	}
	cfg := Config{
		SamplingStrategy:            strategy,
		DecisionWait:                numBatches * time.Second,
		NumTraces:                   50000,
		PolicyCfgs:                  []PolicyCfg{{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}}},
		DropPendingTracesOnShutdown: false, // Flush & expire all 3,840 traces on Shutdown for non-tautological zero-unmarshal proof!
		Options:                     opts,
	}
	p, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), consumertest.NewNop(), cfg)
	require.NoError(t, err)
	require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))

	var before, after runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&before)

	for i := range numBatches {
		batch := cloneWithNewTraceIDs(template, uint64(i+1))
		if ephemeralUniqueRes {
			for rIdx := 0; rIdx < batch.ResourceSpans().Len(); rIdx++ {
				batch.ResourceSpans().At(rIdx).Resource().Attributes().PutStr("k8s.pod.uid", fmt.Sprintf("uid-%d-%d", i, rIdx))
			}
		}
		require.NoError(t, p.ConsumeTraces(t.Context(), batch))
		controller.waitForTick()
	}

	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)
	// Keep template and processor alive across both GC snapshots so neither HeapAlloc
	// nor HeapObjects is skewed by Go freeing the 1,662-span template mid-measurement.
	runtime.KeepAlive(template)
	runtime.KeepAlive(p)

	tsp := shard0(p)
	if strategy == samplingStrategyIncremental {
		require.Equal(t, numBatches*benchTracesPerBatch, tsp.ivm.table.len(), "all 3,840 traces must remain buffered pending before expiry")
		if tsp.ivm.payloadStore != nil {
			stats = tsp.ivm.payloadStore.Stats()
		}
		// F8: Non-tautological zero-unmarshal verification — actually tick-expire ALL 3,840 traces!
		for range numBatches + 2 {
			controller.waitForTick()
		}
		require.Equal(t, 0, tsp.ivm.table.len(), "all 3,840 traces must expire cleanly")
		require.Equal(t, int64(numBatches*benchTracesPerBatch), tsp.ivm.stats.droppedWithoutUnmarshal.Load())
		if tsp.ivm.payloadStore != nil {
			afterExpire := tsp.ivm.payloadStore.Stats()
			unmarshalCount = afterExpire.UnmarshalCount
			require.Equal(t, 0, afterExpire.LiveSlabs, "all slabs must drain after tick expiry")
			require.Equal(t, uint64(0), afterExpire.LiveChunks)
			require.Equal(t, uint64(0), afterExpire.MmapActiveBytes)
		}
	} else {
		require.Len(t, tsp.idToTrace, numBatches*benchTracesPerBatch)
	}

	require.NoError(t, p.Shutdown(t.Context()))
	heapBytes = int64(after.HeapAlloc) - int64(before.HeapAlloc)
	heapObjs = int64(after.HeapObjects) - int64(before.HeapObjects)
	return heapBytes, heapObjs, unmarshalCount, stats
}

func TestArena_W3WorstCaseMemoryReduction(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping W3 memory reduction verification in -short mode")
	}

	type variant struct {
		name      string
		shape     benchShape
		ephemeral bool
	}
	variants := make([]variant, 0, len(benchShapes)+1)
	for _, s := range benchShapes {
		variants = append(variants, variant{name: s.name, shape: s})
	}
	variants = append(variants, variant{name: "ephemeral-unique-res", shape: benchShapes[1], ephemeral: true})

	const totalSpans = 30 * 1662 // 49,860 spans across 3,840 traces

	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			baseBytes, baseObjs, _, _ := measureW3Retained(t, samplingStrategyTraceComplete, arenaModeInMemory, v.shape, v.ephemeral)
			intHeapBytes, intHeapObjs, intHeapUnm, intHeapSt := measureW3Retained(t, samplingStrategyIncremental, arenaModeInternedHeap, v.shape, v.ephemeral)
			lz4HeapBytes, lz4HeapObjs, lz4HeapUnm, lz4HeapSt := measureW3Retained(t, samplingStrategyIncremental, arenaModeLZ4Heap, v.shape, v.ephemeral)
			intMmapBytes, intMmapObjs, intMmapUnm, intMmapSt := measureW3Retained(t, samplingStrategyIncremental, arenaModeInternedMmap, v.shape, v.ephemeral)
			lz4MmapBytes, lz4MmapObjs, lz4MmapUnm, lz4MmapSt := measureW3Retained(t, samplingStrategyIncremental, arenaModeLZ4Mmap, v.shape, v.ephemeral)

			pct := func(cand, base int64) float64 {
				if base == 0 {
					return 0
				}
				return 100.0 * (1.0 - float64(cand)/float64(base))
			}

			t.Logf("W3 [%s] (49,860 spans, 3,840 pending traces):", v.name)
			t.Logf("  Baseline (trace-complete):  heap=%7.1f B/span (%6.2f MiB)  objs=%d",
				float64(baseBytes)/totalSpans, float64(baseBytes)/(1<<20), baseObjs)
			t.Logf("  internedHeap:               heap=%7.1f B/span (%5.1f%% red) objs=%d (%5.1f%% red) payload=%5.2f MiB",
				float64(intHeapBytes)/totalSpans, pct(intHeapBytes, baseBytes), intHeapObjs, pct(intHeapObjs, baseObjs), float64(intHeapSt.LivePayloadBytes+intHeapSt.ResourceInternBytes+intHeapSt.ScopeInternBytes)/(1<<20))
			t.Logf("  lz4Heap:                    heap=%7.1f B/span (%5.1f%% red) objs=%d (%5.1f%% red) payload=%5.2f MiB compChunks=%d/%d",
				float64(lz4HeapBytes)/totalSpans, pct(lz4HeapBytes, baseBytes), lz4HeapObjs, pct(lz4HeapObjs, baseObjs), float64(lz4HeapSt.LivePayloadBytes+lz4HeapSt.ResourceInternBytes+lz4HeapSt.ScopeInternBytes)/(1<<20), lz4HeapSt.CompressedChunks, lz4HeapSt.CompressedChunks+lz4HeapSt.UncompressedChunks)
			t.Logf("  internedMmap:               heap=%7.1f B/span (%5.1f%% red) objs=%d (%5.1f%% red) mmap=%5.2f MiB",
				float64(intMmapBytes)/totalSpans, pct(intMmapBytes, baseBytes), intMmapObjs, pct(intMmapObjs, baseObjs), float64(intMmapSt.MmapActiveBytes)/(1<<20))
			t.Logf("  lz4Mmap:                    heap=%7.1f B/span (%5.1f%% red) objs=%d (%5.1f%% red) mmap=%5.2f MiB",
				float64(lz4MmapBytes)/totalSpans, pct(lz4MmapBytes, baseBytes), lz4MmapObjs, pct(lz4MmapObjs, baseObjs), float64(lz4MmapSt.MmapActiveBytes)/(1<<20))

			// Zero-unmarshal proof!
			require.Zero(t, intHeapUnm)
			require.Zero(t, lz4HeapUnm)
			require.Zero(t, intMmapUnm)
			require.Zero(t, lz4MmapUnm)

			// M2 Exit Gate 4 assertions:
			if v.name == "mixed" {
				require.GreaterOrEqual(t, pct(intHeapBytes, baseBytes), 50.0, "internedHeap must achieve >= 50% HeapAlloc reduction on mixed")
				require.GreaterOrEqual(t, pct(intHeapObjs, baseObjs), 85.0, "internedHeap must achieve >= 85% HeapObjects reduction on mixed")
			}
			require.GreaterOrEqual(t, pct(lz4HeapBytes, baseBytes), 50.0, "lz4Heap must achieve >= 50% HeapAlloc reduction on %s", v.name)
			require.GreaterOrEqual(t, pct(lz4HeapObjs, baseObjs), 85.0, "lz4Heap must achieve >= 85% HeapObjects reduction on %s", v.name)
			if arenaMmapSupported {
				require.GreaterOrEqual(t, pct(intMmapBytes, baseBytes), 90.0, "internedMmap must achieve >= 90% HeapAlloc reduction on %s", v.name)
				require.GreaterOrEqual(t, pct(lz4MmapBytes, baseBytes), 90.0, "lz4Mmap must achieve >= 90% HeapAlloc reduction on %s", v.name)
			}
		})
	}
}

func collectSpanContentMap(sink *consumertest.TracesSink) map[pcommon.SpanID]string {
	out := make(map[pcommon.SpanID]string)
	for _, td := range sink.AllTraces() {
		for _, rs := range td.ResourceSpans().All() {
			svc, _ := rs.Resource().Attributes().Get("service.name")
			for _, ss := range rs.ScopeSpans().All() {
				for _, sp := range ss.Spans().All() {
					route, _ := sp.Attributes().Get("http.route")
					code, _ := sp.Attributes().Get("http.response.status_code")
					out[sp.SpanID()] = fmt.Sprintf("%s|%s|%s|%d|%d|%d|%s|%s|%s",
						sp.TraceID().String(),
						svc.AsString(),
						sp.Name(),
						sp.Status().Code(),
						sp.StartTimestamp(),
						sp.EndTimestamp(),
						sp.TraceState().AsRaw(),
						route.AsString(),
						code.AsString(),
					)
				}
			}
		}
	}
	return out
}

func TestArena_EndToEndIVMParity(t *testing.T) {
	suites := []struct {
		name     string
		pureIVM  bool
		policies []PolicyCfg
	}{
		{
			name:    "PureIVM_ZeroUnmarshalDropAndEarlySample",
			pureIVM: true,
			policies: []PolicyCfg{
				{
					sharedPolicyCfg: sharedPolicyCfg{Name: "drop-healthz", Type: Drop},
					DropCfg: DropCfg{SubPolicyCfg: []AndSubPolicyCfg{
						{sharedPolicyCfg: sharedPolicyCfg{Name: "h", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "http.route", Values: []string{"/healthz"}}}},
					}},
				},
				{
					sharedPolicyCfg: sharedPolicyCfg{Name: "and-cross", Type: And},
					AndCfg: AndCfg{SubPolicyCfg: []AndSubPolicyCfg{
						{sharedPolicyCfg: sharedPolicyCfg{Name: "a1", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "service.name", Values: []string{"service-1"}}}},
						{sharedPolicyCfg: sharedPolicyCfg{Name: "a2", Type: NumericAttribute, NumericAttributeCfg: NumericAttributeCfg{Key: "http.response.status_code", MinValue: 500, MaxValue: 599}}},
					}},
				},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "slow", Type: Latency, LatencyCfg: LatencyCfg{ThresholdMs: 500}}},
			},
		},
		{
			name:    "HybridWithBytesLimiting",
			pureIVM: false,
			policies: []PolicyCfg{
				{
					sharedPolicyCfg: sharedPolicyCfg{Name: "drop-healthz", Type: Drop},
					DropCfg: DropCfg{SubPolicyCfg: []AndSubPolicyCfg{
						{sharedPolicyCfg: sharedPolicyCfg{Name: "h", Type: StringAttribute, StringAttributeCfg: StringAttributeCfg{Key: "http.route", Values: []string{"/healthz"}}}},
					}},
				},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
				{sharedPolicyCfg: sharedPolicyCfg{Name: "bytes-lim", Type: BytesLimiting, BytesLimitingCfg: BytesLimitingCfg{BytesPerSecond: 10_000_000, BurstCapacity: 20_000_000}}},
			},
		},
	}

	for _, suite := range suites {
		t.Run(suite.name, func(t *testing.T) {
			for _, order := range []string{"in-order", "reversed", "shuffled"} {
				batches := generateRandomizedParityBatches(2026, 200, order, false)
				oracleSink, _ := runProcessorSync(t, Config{
					SamplingStrategy: samplingStrategyTraceComplete,
					DecisionWait:     defaultTestDecisionWait,
					NumTraces:        1000,
					PolicyCfgs:       suite.policies,
					DecisionCache:    DecisionCacheConfig{SampledCacheSize: 1024, NonSampledCacheSize: 1024},
				}, nil, batches)
				wantIDs := collectSampledTraceIDs(oracleSink)
				wantContent := collectSpanContentMap(oracleSink)

				for _, m := range allArenaModes() {
					t.Run(fmt.Sprintf("%s/%s", m.name, order), func(t *testing.T) {
						ivmSink, tsp := runProcessorSync(t, Config{
							SamplingStrategy: samplingStrategyIncremental,
							DecisionWait:     defaultTestDecisionWait,
							NumTraces:        1000,
							PolicyCfgs:       suite.policies,
							DecisionCache:    DecisionCacheConfig{SampledCacheSize: 1024, NonSampledCacheSize: 1024},
							Options:          []Option{withArenaStorageMode(m.mode)},
						}, nil, batches)
						require.Equal(t, wantIDs, collectSampledTraceIDs(ivmSink))
						require.Equal(t, wantContent, collectSpanContentMap(ivmSink), "emitted span content parity mismatch")
						if suite.pureIVM {
							require.Positive(t, tsp.ivm.stats.droppedWithoutUnmarshal.Load(), "pure IVM must drop non-sampled traces without unmarshaling")
						}
						require.Equal(t, uint64(0), tsp.ivm.payloadStore.Stats().MmapActiveBytes)
					})
				}
			}
		})
	}

	// NumShards=4 concurrent ingestion under -race with lz4Mmap and internedHeap arenas!
	t.Run("MultiShard4_ConcurrentRace", func(t *testing.T) {
		batches := generateRandomizedParityBatches(3030, 300, "shuffled", false)
		oracleSink := new(consumertest.TracesSink)
		oracleCfg := Config{
			SamplingStrategy: samplingStrategyTraceComplete,
			DecisionWait:     10 * time.Second,
			NumTraces:        2000,
			NumShards:        4,
			PolicyCfgs:       suites[0].policies,
			DecisionCache:    DecisionCacheConfig{SampledCacheSize: 4096, NonSampledCacheSize: 4096},
			Options:          []Option{withTickerFrequency(time.Hour)},
		}
		op, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), oracleSink, oracleCfg)
		require.NoError(t, err)
		require.NoError(t, op.Start(t.Context(), componenttest.NewNopHost()))
		for _, b := range cloneTracesSlice(batches) {
			require.NoError(t, op.ConsumeTraces(t.Context(), b))
		}
		require.NoError(t, op.Shutdown(t.Context()))
		wantIDs := collectSampledTraceIDs(oracleSink)
		wantContent := collectSpanContentMap(oracleSink)

		for _, mode := range []arenaStorageMode{arenaModeInternedHeap, arenaModeLZ4Mmap} {
			ivmSink := new(consumertest.TracesSink)
			ivmCfg := oracleCfg
			ivmCfg.SamplingStrategy = samplingStrategyIncremental
			ivmCfg.Options = []Option{withTickerFrequency(time.Hour), withArenaStorageMode(mode)}
			ip, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), ivmSink, ivmCfg)
			require.NoError(t, err)
			require.NoError(t, ip.Start(t.Context(), componenttest.NewNopHost()))

			var wg sync.WaitGroup
			clones := cloneTracesSlice(batches)
			for g := range 4 {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					for i := worker; i < len(clones); i += 4 {
						require.NoError(t, ip.ConsumeTraces(t.Context(), clones[i]))
					}
				}(g)
			}
			wg.Wait()
			require.NoError(t, ip.Shutdown(t.Context()))

			require.Equal(t, wantIDs, collectSampledTraceIDs(ivmSink))
			require.Equal(t, wantContent, collectSpanContentMap(ivmSink))
			sp := ip.(*shardedProcessor)
			for _, shard := range sp.shards {
				require.Equal(t, uint64(0), shard.ivm.payloadStore.Stats().MmapActiveBytes)
			}
		}
	})
}

func TestArena_Gate2Regressions(t *testing.T) {
	// F1: Exact recursion depth boundaries matching pdata@v1.71.1 ProtoUnmarshaler (proto.RecursionLimit = 100).
	t.Run("F1_ExactPdataRecursionDepthBoundaries", func(t *testing.T) {
		buildNestedSlice := func(levels int) ptrace.Traces {
			td := ptrace.NewTraces()
			rs := td.ResourceSpans().AppendEmpty()
			sp := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
			sp.SetTraceID(uInt64ToTraceID(1))
			sp.SetSpanID(uInt64ToSpanID(1))
			cur := sp.Attributes().PutEmptySlice("s")
			for range levels - 1 {
				cur = cur.AppendEmpty().SetEmptySlice()
			}
			cur.AppendEmpty().SetInt(1)
			return td
		}
		buildNestedMap := func(levels int, leafNonEmpty bool) ptrace.Traces {
			td := ptrace.NewTraces()
			rs := td.ResourceSpans().AppendEmpty()
			sp := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
			sp.SetTraceID(uInt64ToTraceID(2))
			sp.SetSpanID(uInt64ToSpanID(2))
			cur := sp.Attributes().PutEmptyMap("m")
			for range levels - 1 {
				cur = cur.PutEmptyMap("m")
			}
			if leafNonEmpty {
				cur.PutInt("leaf", 1)
			}
			return td
		}

		// Slice: 49 non-empty passes & round-trips through pdata; 50 fails at ingest matching pdata!
		td49 := buildNestedSlice(49)
		store := newArenaPayloadStore(arenaModeInternedHeap, 16*1024, 1)
		defer func() { require.NoError(t, store.Close()) }()
		require.NoError(t, store.Append(uInt64ToTraceID(1), td49))
		got49, err := store.Take(uInt64ToTraceID(1))
		require.NoError(t, err)
		assertTracesDeepEqual(t, td49, got49)

		td50 := buildNestedSlice(50)
		require.ErrorIs(t, store.Append(uInt64ToTraceID(1), td50), errWireDepthExceeded)

		// Map: 32 non-empty passes; 33 empty passes; 33 non-empty fails; 34 empty fails!
		tdMap32 := buildNestedMap(32, true)
		require.NoError(t, store.Append(uInt64ToTraceID(2), tdMap32))
		got32, err := store.Take(uInt64ToTraceID(2))
		require.NoError(t, err)
		assertTracesDeepEqual(t, tdMap32, got32)

		tdMap33Empty := buildNestedMap(33, false)
		require.NoError(t, store.Append(uInt64ToTraceID(2), tdMap33Empty))
		got33, err := store.Take(uInt64ToTraceID(2))
		require.NoError(t, err)
		assertTracesDeepEqual(t, tdMap33Empty, got33)

		require.ErrorIs(t, store.Append(uInt64ToTraceID(2), buildNestedMap(33, true)), errWireDepthExceeded)
		require.ErrorIs(t, store.Append(uInt64ToTraceID(2), buildNestedMap(34, false)), errWireDepthExceeded)
	})

	// F2: Repeated incompressible chunks and resources reuse both scratchComp backing arrays without reallocation.
	t.Run("F2_IncompressibleScratchCompReused", func(t *testing.T) {
		as := newArenaPayloadStore(arenaModeLZ4Heap, 256*1024, 2).(*arenaPayloadStore)
		defer func() { require.NoError(t, as.Close()) }()

		rng := rand.New(rand.NewSource(12345))
		randBytes := make([]byte, 1024)
		rng.Read(randBytes)
		resRand := make([]byte, 512)
		rng.Read(resRand)

		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutEmptyBytes("rand.res").FromRaw(resRand)
		rs.Resource().Attributes().PutStr("k8s.pod.uid", "pod-0000")
		sp := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		sp.SetSpanID(uInt64ToSpanID(1))
		sp.Attributes().PutEmptyBytes("rand.span").FromRaw(randBytes)

		scopeIdxs := []int{0}
		spanIdxs := [][]int{{0}}
		// Keep one pinned trace alive so slabs stay allocated and scratchComp stays initialized.
		tidPin := uInt64ToTraceID(888888)
		sp.SetTraceID(tidPin)
		require.NoError(t, as.AppendGroup(tidPin, rs, scopeIdxs, spanIdxs))
		require.NotNil(t, as.scratchComp, "scratchComp must be retained after incompressible bail-out")
		require.NotNil(t, as.resIntern.scratchComp, "resIntern.scratchComp must be retained after incompressible bail-out")
		require.Greater(t, cap(as.scratchComp), 0)
		require.Greater(t, cap(as.resIntern.scratchComp), 0)

		chunkPtr := &as.scratchComp[:1][0]
		prevChunkCap := cap(as.scratchComp)
		resPtr := &as.resIntern.scratchComp[:1][0]
		prevResCap := cap(as.resIntern.scratchComp)
		stBefore := as.Stats()

		const iters = 50
		for i := 1; i <= iters; i++ {
			tid := uInt64ToTraceID(uint64(i))
			sp.SetTraceID(tid)
			// Mutate a fixed-width attribute so len(resHdr) stays constant while missing
			// resIntern.buckets on every iteration, forcing LZ4 bail-out on both tables.
			rs.Resource().Attributes().PutStr("k8s.pod.uid", fmt.Sprintf("pod-%04d", i))
			require.NoError(t, as.AppendGroup(tid, rs, scopeIdxs, spanIdxs))
			require.Equal(t, 2, as.resIntern.liveEntries, "iter %d must miss resIntern cache and insert a new entry", i)
			require.NoError(t, as.Delete(tid))
			require.Equal(t, 1, as.resIntern.liveEntries)
		}

		stAfter := as.Stats()
		require.Equal(t, uint64(iters), stAfter.UncompressedChunks-stBefore.UncompressedChunks, "every iteration must hit LZ4 bail-out on chunk body")
		require.Equal(t, stBefore.CompressedChunks, stAfter.CompressedChunks, "incompressible chunks must not compress")
		require.Same(t, chunkPtr, &as.scratchComp[:1][0], "chunk scratchComp backing array must be reused without reallocation")
		require.Equal(t, prevChunkCap, cap(as.scratchComp), "chunk scratchComp capacity must remain unchanged")
		require.Same(t, resPtr, &as.resIntern.scratchComp[:1][0], "resIntern scratchComp backing array must be reused without reallocation")
		require.Equal(t, prevResCap, cap(as.resIntern.scratchComp), "resIntern scratchComp capacity must remain unchanged")
		require.NoError(t, as.Delete(tidPin))
	})

	// F3: High-cardinality intern table compaction (>1024 pinned, >4096 idle) & traceChunks compaction (>4096 pinned).
	t.Run("F3_MapCompactionAndPeakReset_InternAndTraceChunks", func(t *testing.T) {
		as := newArenaPayloadStore(arenaModeInternedHeap, 64*1024, 1).(*arenaPayloadStore)
		defer func() { require.NoError(t, as.Close()) }()

		// Keep 1 pinned trace so liveEntries/len(traceChunks) never drop to 0 during peak shrink!
		pinnedTID := uInt64ToTraceID(1)
		tdPin := makeSpanBatch(pinnedTID, 1, 0, "pinned-service", time.Unix(1700000000, 0), time.Unix(1700000001, 0), ptrace.StatusCodeOk, nil)
		require.NoError(t, as.Append(pinnedTID, tdPin))

		// Insert 4,200 traces with unique resources (driving both resIntern > 1024 AND traceChunks > 4096!).
		const burst = 4200
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		sp := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		scopeIdxs := []int{0}
		spanIdxs := [][]int{{0}}
		for i := range burst {
			tid := uInt64ToTraceID(uint64(i + 2))
			sp.SetTraceID(tid)
			sp.SetSpanID(uInt64ToSpanID(uint64(i + 2)))
			rs.Resource().Attributes().PutStr("k8s.pod.uid", fmt.Sprintf("burst-pod-%05d", i))
			require.NoError(t, as.AppendGroup(tid, rs, scopeIdxs, spanIdxs))
		}
		require.GreaterOrEqual(t, as.resIntern.peakEntries, burst)
		require.GreaterOrEqual(t, as.peakTraces, burst)

		// Delete all 4,200 burst traces leaving pinnedTID intact -> BOTH maps MUST compact and reset peaks!
		for i := range burst {
			require.NoError(t, as.Delete(uInt64ToTraceID(uint64(i+2))))
		}
		require.Equal(t, 1, as.resIntern.liveEntries)
		require.Less(t, as.resIntern.peakEntries, 1024, "resIntern peakEntries must reset after compaction")
		require.Less(t, as.peakTraces, 4096, "traceChunks peakTraces must reset after compaction")

		// Verify pinned trace is still intact & lossless after map compaction!
		got, err := as.Take(pinnedTID)
		require.NoError(t, err)
		assertTracesDeepEqual(t, tdPin, got)
		require.Equal(t, 0, as.resIntern.peakEntries)
		require.Equal(t, 0, as.peakTraces)
		require.Empty(t, as.chunks, "chunks backing slice must reset after >=4096 burst drains to idle")
		require.Empty(t, as.resIntern.entries, "entries backing slice must reset after >=4096 burst drains to idle")
	})

	// F5: Empty spanIndices[pos] skipped in rawProtoSlab, mismatch error, closed store returns errArenaClosed.
	t.Run("F5_EmptySpanIndicesMismatchAndClosedCheck", func(t *testing.T) {
		td := buildRichTrace(uInt64ToTraceID(1))
		rs := td.ResourceSpans().At(0)

		rawStore := newArenaPayloadStore(arenaModeRawProtoSlab, 16*1024, 1)
		intStore := newArenaPayloadStore(arenaModeInternedHeap, 16*1024, 1)
		defer func() {
			require.NoError(t, rawStore.Close())
			require.NoError(t, intStore.Close())
		}()

		tid := uInt64ToTraceID(1)
		// Scope 0 has empty spanIndices, Scope 1 has span 0 -> both modes MUST skip Scope 0!
		require.NoError(t, rawStore.AppendGroup(tid, rs, []int{0, 1}, [][]int{{}, {0}}))
		require.NoError(t, intStore.AppendGroup(tid, rs, []int{0, 1}, [][]int{{}, {0}}))
		gotRaw, err := rawStore.Take(tid)
		require.NoError(t, err)
		gotInt, err := intStore.Take(tid)
		require.NoError(t, err)
		require.Equal(t, 1, gotRaw.ResourceSpans().At(0).ScopeSpans().Len(), "rawProtoSlab must skip empty spanIndices[pos]")
		assertTracesDeepEqual(t, gotInt, gotRaw)

		// Mismatched lengths return errArenaMismatchedIndices.
		require.ErrorIs(t, rawStore.AppendGroup(tid, rs, []int{0, 1}, [][]int{{0}}), errArenaMismatchedIndices)

		// Closed store returns errArenaClosed even on empty scopeIndices!
		require.NoError(t, rawStore.Close())
		require.ErrorIs(t, rawStore.AppendGroup(tid, rs, nil, nil), errArenaClosed)
	})

	// F6: Bounds, liveness, and overflow safety on corrupted chunk metadata / mmap sizes / pool clamping.
	t.Run("F6_CorruptScopeCountScopeIDAndSizeGuards", func(t *testing.T) {
		as := newArenaPayloadStore(arenaModeInternedHeap, 16*1024, 1).(*arenaPayloadStore)
		defer func() { require.NoError(t, as.Close()) }()

		// Corrupt scopeCount > storedLen/4 -> Take returns error & defer freeChunkChainLocked must NOT panic!
		tid1 := uInt64ToTraceID(1)
		require.NoError(t, as.Append(tid1, buildRichTrace(tid1)))
		as.chunks[as.traceChunks[tid1].head].scopeCount = 999999
		_, err := as.Take(tid1)
		require.ErrorIs(t, err, errArenaCorruptBody)

		// Corrupt scopeID inside prefix -> appendBytes returns error instead of panicking!
		tid2 := uInt64ToTraceID(2)
		require.NoError(t, as.Append(tid2, buildRichTrace(tid2)))
		c := &as.chunks[as.traceChunks[tid2].head]
		binary.LittleEndian.PutUint32(as.chunkPool.slabs[c.slabID].buf[c.offset:], 0xdeadbeef)
		_, err = as.Take(tid2)
		require.ErrorIs(t, err, errArenaCorruptBody)

		// Clamp huge defaultSize & guard arenaAlignMmapSize <= 0 / overflow!
		hugePool := newArenaSlabPool(false, maxArenaSlabSize*4)
		require.Equal(t, maxArenaSlabSize, hugePool.defaultSize)
		_, err = arenaAlignMmapSize(0)
		require.Error(t, err)
		_, err = arenaAlignMmapSize(-10)
		require.Error(t, err)
		if arenaMmapSupported {
			_, err = arenaAlignMmapSize(math.MaxInt)
			require.Error(t, err)
		}
	})

	// F7: ENOMEM during slab rotation keeps active slab unsealed so smaller chunks still fit!
	t.Run("F7_ENOMEMDoesNotSealPartiallyFilledActiveSlab", func(t *testing.T) {
		pool := newArenaSlabPool(false, 4096)
		defer func() { _ = pool.closeAll() }()

		failNext := false
		pool.testAllocHook = func(size int) ([]byte, error) {
			if failNext {
				return nil, errors.New("simulated ENOMEM")
			}
			return make([]byte, size), nil
		}

		// Allocate 2048 B in slab 0 (2048 B remaining!).
		s0, off0, err := pool.alloc(2048, 1)
		require.NoError(t, err)
		require.Equal(t, uint32(0), off0)
		pool.slabs[s0].liveCount = 1
		pool.slabs[s0].liveBytes = 2048

		// Try to allocate 3000 B (triggers rotation -> ENOMEM!).
		failNext = true
		_, _, err = pool.alloc(3000, 1)
		require.Error(t, err)
		require.Equal(t, s0, pool.activeSlabID, "ENOMEM must NOT detach active slab")
		require.False(t, pool.slabs[s0].sealed, "ENOMEM must NOT prematurely seal active slab")

		// Smaller 1024 B chunk STILL fits inside s0's remaining 2048 B even while ENOMEM persists!
		s1, off1, err := pool.alloc(1024, 1)
		require.NoError(t, err)
		require.Equal(t, s0, s1)
		require.Equal(t, uint32(2048), off1)
	})

	// F8 extra coverage: NumTraces overflow (FIFO + block_on_overflow) exercising BeginResourceSpans,
	// compactSlabID re-sealing, rollback under batchActive, 3-node collision chain, LZ4 extended literals & errLZ4Size, empty SpanID!
	t.Run("F8_NumTracesOverflow_FIFOAndBlockOnOverflow_WithArena", func(t *testing.T) {
		batch := generateBenchBatchShaped(32, 1, func(int, int) int { return 0 }) // 32 traces sharing 1 ResourceSpans -> exercises BeginResourceSpans!
		for _, rs := range batch.ResourceSpans().All() {
			for _, ss := range rs.ScopeSpans().All() {
				for _, sp := range ss.Spans().All() {
					sp.Status().SetCode(ptrace.StatusCodeOk)
				}
			}
		}

		// Case A: Non-blocking FIFO eviction with NumTraces = 1 and NumTraces = 8!
		for _, limit := range []uint64{1, 8} {
			for _, mode := range []arenaStorageMode{arenaModeInternedHeap, arenaModeLZ4Mmap} {
				controller := newTestTSPController()
				cfg := Config{
					SamplingStrategy: samplingStrategyIncremental,
					DecisionWait:     defaultTestDecisionWait,
					NumTraces:        limit,
					BlockOnOverflow:  false,
					PolicyCfgs: []PolicyCfg{
						{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
					},
					Options: []Option{withTestController(controller), withArenaStorageMode(mode)},
				}
				p, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), new(consumertest.TracesSink), cfg)
				require.NoError(t, err)
				require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))

				cp := ptrace.NewTraces()
				batch.CopyTo(cp)
				require.NoError(t, p.ConsumeTraces(t.Context(), cp))
				controller.waitForTick()
				controller.waitForTick()
				require.NoError(t, p.Shutdown(t.Context()))

				st := shard0(p).ivm.payloadStore.Stats()
				require.Equal(t, 0, st.LiveSlabs)
				require.Equal(t, uint64(0), st.LiveChunks)
				require.Equal(t, 0, st.ResourceInternEntries)
				require.Equal(t, 0, st.ScopeInternEntries)
				require.Equal(t, uint64(0), st.MmapActiveBytes)
			}
		}

		// Case B: block_on_overflow = true with 32 traces in ONE ResourceSpans & NumTraces = 8!
		controller := newTestTSPController()
		cfg := Config{
			SamplingStrategy: samplingStrategyIncremental,
			DecisionWait:     defaultTestDecisionWait,
			NumTraces:        8,
			BlockOnOverflow:  true,
			PolicyCfgs: []PolicyCfg{
				{sharedPolicyCfg: sharedPolicyCfg{Name: "errors", Type: StatusCode, StatusCodeCfg: StatusCodeCfg{StatusCodes: []string{"ERROR"}}}},
			},
			Options: []Option{withTestController(controller), withArenaStorageMode(arenaModeLZ4Mmap)},
		}
		p, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), new(consumertest.TracesSink), cfg)
		require.NoError(t, err)
		require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))

		// Deliver all 32 traces in ONE ResourceSpans batch (unbuffered rawWorkChan returns
		// as soon as loop() accepts the batch, before ivmWaitForSpace blocks on trace 9).
		cp := ptrace.NewTraces()
		batch.CopyTo(cp)
		require.NoError(t, p.ConsumeTraces(t.Context(), cp))
		// Drive enough ticks (32 / 8 = 4 windows + drain ticks) to unblock every ivmWaitForSpace
		// pause inside processIncrementalRawTraces while BeginResourceSpans pin is active!
		for range 8 {
			controller.waitForTick()
		}
		require.NoError(t, p.Shutdown(t.Context()))
		st := shard0(p).ivm.payloadStore.Stats()
		require.Equal(t, 0, st.LiveSlabs)
		require.Equal(t, 0, st.ResourceInternEntries)
		require.Equal(t, uint64(0), st.MmapActiveBytes)
	})

	t.Run("F8_CompactSlabReseal_RollbackBatchActive_Collision3Node_LZ4AndEmptySpanID", func(t *testing.T) {
		// 1. Mid-append rollback with batchActive == true (depth error in scope 1 after scope 0 & resource interned).
		as := newArenaPayloadStore(arenaModeInternedHeap, 16*1024, 1).(*arenaPayloadStore)
		defer func() { require.NoError(t, as.Close()) }()
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", "rollback-svc")
		ss0 := rs.ScopeSpans().AppendEmpty()
		ss0.Scope().SetName("ok-scope")
		sp0 := ss0.Spans().AppendEmpty()
		sp0.SetTraceID(uInt64ToTraceID(1))
		// Empty SpanID coverage (wire_codec.go:147-148)!
		ss1 := rs.ScopeSpans().AppendEmpty()
		ss1.Scope().SetName("bad-scope")
		sp1 := ss1.Spans().AppendEmpty()
		sp1.SetTraceID(uInt64ToTraceID(1))
		cur := sp1.Attributes().PutEmptySlice("deep")
		for range 50 {
			cur = cur.AppendEmpty().SetEmptySlice()
		}

		as.BeginResourceSpans(2)
		err := as.AppendGroup(uInt64ToTraceID(1), rs, []int{0, 1}, [][]int{{0}, {0}})
		require.ErrorIs(t, err, errWireDepthExceeded)
		as.FinishResourceSpans()
		st := as.Stats()
		require.Equal(t, 0, st.ResourceInternEntries, "rollback + FinishResourceSpans must leak 0 Resource refs")
		require.Equal(t, 0, st.ScopeInternEntries, "rollback + FinishResourceSpans must leak 0 Scope refs")
		require.Equal(t, uint64(0), st.LiveChunks)

		// Empty SpanID round-trip cleanly!
		require.NoError(t, as.AppendGroup(uInt64ToTraceID(1), rs, []int{0}, [][]int{{0}}))
		gotEmptySID, err := as.Take(uInt64ToTraceID(1))
		require.NoError(t, err)
		require.True(t, gotEmptySID.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).SpanID().IsEmpty())

		// 2. compactSlabID re-sealing after dropping < 25% live past compactAgeTicks.
		pool := newArenaSlabPool(false, 4096)
		defer func() { _ = pool.closeAll() }()
		cid, _, err := pool.allocCompact(1000, 1)
		require.NoError(t, err)
		pool.slabs[cid].liveCount = 1
		pool.slabs[cid].liveBytes = 100 // 100*4 < 1000 (<25% of used)
		pool.sealActive(5, 2)           // age = 4 >= 2 -> compactSlabID seals!
		require.True(t, pool.slabs[cid].sealed)
		require.Equal(t, arenaNoIdx, pool.compactSlabID)
		// Empty compactSlabID released immediately!
		cid2, _, err := pool.allocCompact(100, 5)
		require.NoError(t, err)
		pool.slabs[cid2].liveCount = 0
		pool.sealActive(6, 2)
		require.Equal(t, arenaNoIdx, pool.compactSlabID)

		// 3. 3-node collision chain: unlink middle, head, tail cleanly!
		tbl := newArenaInternTable(false, 4096, false, nil, 1)
		defer func() { _ = tbl.close() }()
		id1, _ := tbl.intern([]byte("n1"))
		id2, _ := tbl.intern([]byte("n2"))
		id3, _ := tbl.intern([]byte("n3"))
		clear(tbl.buckets)
		tbl.entries[id1].hash = 7
		tbl.entries[id2].hash = 7
		tbl.entries[id3].hash = 7
		tbl.entries[id3].nextInBucket = id2
		tbl.entries[id2].nextInBucket = id1
		tbl.entries[id1].nextInBucket = arenaNoIdx
		tbl.buckets[7] = id3
		tbl.release(id2) // middle unlink
		require.Equal(t, id1, tbl.entries[id3].nextInBucket)
		tbl.release(id3) // head unlink
		require.Equal(t, id1, tbl.buckets[7])
		tbl.release(id1) // tail unlink -> bucket deleted
		require.Empty(t, tbl.buckets)

		// 4. LZ4 >= 270-byte literal run + mid-loop bail-out + errLZ4Size + origSize == 0!
		var lz lz4State
		rng := rand.New(rand.NewSource(777))
		bigLit := make([]byte, 1200)
		rng.Read(bigLit[:400]) // > 270 random bytes forcing >= 255 extended literal length byte
		copy(bigLit[400:], bytes.Repeat([]byte("MATCH-BLOCK-1234"), 50))
		comp, ok := lz.compressBlock(bigLit, nil, len(bigLit))
		require.True(t, ok)
		decomp, err := lz4DecompressBlock(comp, nil, len(bigLit))
		require.NoError(t, err)
		require.Equal(t, bigLit, decomp)

		// errLZ4Size: valid compressed stream whose decoded size != requested origSize.
		_, err = lz4DecompressBlock(comp, nil, len(bigLit)+10)
		require.ErrorIs(t, err, errLZ4Size)
		// origSize == 0 with empty input succeeds cleanly.
		emptyOut, err := lz4DecompressBlock(nil, nil, 0)
		require.NoError(t, err)
		require.Empty(t, emptyOut)
		// Mid-loop bail-out when maxOut is tiny.
		_, ok = lz.compressBlock(bigLit, nil, 10)
		require.False(t, ok)
	})

	// F9: Lossless Resource.EntityRefs (field 3 on Resource, wire tag 0x1a) across ALL 5 arena modes!
	t.Run("F9_LosslessEntityRefsAll5Modes", func(t *testing.T) {
		// Construct a valid OTLP Resource containing field 3 (EntityRef: schema_url, type, id_keys, description_keys).
		// EntityRef proto: field 1 schema_url (string), field 2 type (string), field 3 id_keys (repeated string).
		var entityRef []byte
		entityRef = appendLenTagString(entityRef, 0x0a, "https://opentelemetry.io/schemas/1.28.0")
		entityRef = appendLenTagString(entityRef, 0x12, "k8s.pod")
		entityRef = appendLenTagString(entityRef, 0x1a, "k8s.pod.uid")

		var resBody []byte
		kv := appendLenTagString(nil, 0x0a, "k8s.pod.uid")
		kv = appendLenTagBytes(kv, 0x12, appendLenTagString(nil, 0x0a, "pod-xyz-123"))
		resBody = appendLenTagBytes(resBody, 0x0a, kv)
		resBody = appendLenTagBytes(resBody, 0x1a, entityRef) // Resource.EntityRefs field 3!

		tid := uInt64ToTraceID(4242)
		sid := uInt64ToSpanID(99)
		var spBody []byte
		spBody = appendLenTagBytes(spBody, 0x0a, tid[:])
		spBody = appendLenTagBytes(spBody, 0x12, sid[:])
		spBody = appendLenTagString(spBody, 0x2a, "entity-span")
		spBody = append(spBody, 0x7a, 0x00)

		var ssBody []byte
		ssBody = append(ssBody, 0x0a, 0x00)
		ssBody = appendLenTagBytes(ssBody, 0x12, spBody)

		var rsBody []byte
		rsBody = appendLenTagBytes(rsBody, 0x0a, resBody)
		rsBody = appendLenTagBytes(rsBody, 0x12, ssBody)
		rsBody = appendLenTagString(rsBody, 0x1a, "https://res.schema/entity")

		wireIn := appendLenTagBytes(nil, 0x0a, rsBody)
		var u ptrace.ProtoUnmarshaler
		wantTd, err := u.UnmarshalTraces(wireIn)
		require.NoError(t, err)

		var m ptrace.ProtoMarshaler
		wantMarshaled, err := m.MarshalTraces(wantTd)
		require.NoError(t, err)
		require.Contains(t, string(wantMarshaled), "k8s.pod", "sanity check: pdata retains EntityRef in Resource")

		for _, tc := range allArenaModes() {
			t.Run(tc.name, func(t *testing.T) {
				store := newArenaPayloadStore(tc.mode, 16*1024, 1)
				defer func() { require.NoError(t, store.Close()) }()

				require.NoError(t, store.Append(tid, wantTd))
				got, err := store.Take(tid)
				require.NoError(t, err)
				assertTracesDeepEqual(t, wantTd, got)

				gotMarshaled, err := m.MarshalTraces(got)
				require.NoError(t, err)
				require.Equal(t, wantMarshaled, gotMarshaled, "EntityRefs bytes must survive round-trip identically in %s", tc.name)
			})
		}
	})

	// F12: Slab header slice resets after >= 256 slab burst drains to idle.
	t.Run("F12_SlabHeaderSliceResetOnIdle", func(t *testing.T) {
		pool := newArenaSlabPool(false, 4096)
		defer func() { _ = pool.closeAll() }()

		ids := make([]uint32, 0, 270)
		for i := range 270 {
			id, _, err := pool.alloc(4096, uint64(i))
			require.NoError(t, err)
			pool.slabs[id].liveCount = 1
			ids = append(ids, id)
		}
		require.GreaterOrEqual(t, len(pool.slabs), 270)
		for _, id := range ids {
			pool.slabs[id].liveCount = 0
			pool.releaseSlab(id)
		}
		require.Equal(t, 0, pool.liveSlabCount)
		require.NoError(t, pool.drainPooled())
		require.Empty(t, pool.slabs, "slabs header slice must reset once >=256 slabs drain to idle")
		require.Empty(t, pool.freeSlabIDs)
	})
}
