// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tailsamplingprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const (
	// maxProtoValueDepth mirrors pdata's proto.RecursionLimit (100) for nested AnyValue/Map/Slice.
	maxProtoValueDepth = 100
	// maxWireGroupDepth bounds legacy proto2 group (wire type 3/4) nesting during raw wire parsing.
	maxWireGroupDepth = 64
	// maxProtoFieldNum is the maximum valid protobuf field number ((1<<29) - 1 = 536,870,911).
	maxProtoFieldNum = (1 << 29) - 1
)

var (
	errWireTruncated     = errors.New("wire codec: truncated protobuf input")
	errWireInvalidTag    = errors.New("wire codec: invalid field tag or wire type")
	errWireDepthExceeded = errors.New("wire codec: recursion depth limit exceeded")
	errWireInvalidGroup  = errors.New("wire codec: unexpected or mismatched end-group tag")
	errWireInvalidLen    = errors.New("wire codec: invalid length-delimited field length")
)

// patchLenPrefix patches a reserved 1-byte length prefix written right before payloadStart.
// For lengths < 128 (the vast majority of KeyValue, AnyValue, Status, and small fields),
// this writes a single byte with zero copies and zero allocations.
// For lengths >= 128, it shifts payload bytes forward inside dst with zero heap allocations
// (assuming dst has capacity; otherwise Go grows the reusable scratch buffer once).
func patchLenPrefix(dst []byte, payloadStart int) []byte {
	n := len(dst) - payloadStart
	if n < 0x80 {
		dst[payloadStart-1] = byte(n)
		return dst
	}
	vl := uvarintLen(uint64(n))
	extra := vl - 1
	var pad [binary.MaxVarintLen64]byte
	dst = append(dst, pad[:extra]...)
	copy(dst[payloadStart+extra:], dst[payloadStart:payloadStart+n])
	binary.PutUvarint(dst[payloadStart-1:], uint64(n))
	return dst
}

func appendLenTagBytes(dst []byte, tag byte, b []byte) []byte {
	dst = append(dst, tag)
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

func appendLenTagString(dst []byte, tag byte, s string) []byte {
	dst = append(dst, tag)
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

// appendResourceHeader appends canonical OTLP ResourceSpans header fields (field 1 Resource,
// always emitted even when empty matching pdata, and field 3 SchemaUrl if non-empty) directly
// into dst. When rs.Resource() carries internal EntityRefs (field 3 on Resource, not exposed
// by public pcommon.Resource), it detects the exact wire size delta via ProtoMarshaler and
// falls back to marshaling ONLY rs.Resource() + SchemaUrl (0 spans copied, zero go.mod edits)
// so EntityRefs round-trip losslessly across all 5 arena modes.
func appendResourceHeader(dst []byte, rs ptrace.ResourceSpans) ([]byte, error) {
	start := len(dst)
	dst = append(dst, 0x0a, 0)
	resStart := len(dst)
	var err error
	dst, err = appendResourceBody(dst, rs.Resource())
	if err != nil {
		return nil, err
	}
	dst = patchLenPrefix(dst, resStart)
	if su := rs.SchemaUrl(); len(su) > 0 {
		dst = appendLenTagString(dst, 0x1a, su)
	}

	// Zero-alloc fast check for hidden fields such as Resource.EntityRefs:
	// ResourceSpansSize(rs) - sum(ScopeSpans wire sizes) equals len(resHeader) iff
	// no EntityRefs (or DeprecatedScopeSpans) are present.
	var m ptrace.ProtoMarshaler
	scopes := rs.ScopeSpans()
	var scopesWireSize int
	for i := 0; i < scopes.Len(); i++ {
		ssLen := m.ScopeSpansSize(scopes.At(i))
		scopesWireSize += 1 + uvarintLen(uint64(ssLen)) + ssLen
	}
	if len(dst)-start != m.ResourceSpansSize(rs)-scopesWireSize {
		tmp := ptrace.NewTraces()
		tmpRS := tmp.ResourceSpans().AppendEmpty()
		rs.Resource().CopyTo(tmpRS.Resource())
		tmpRS.SetSchemaUrl(rs.SchemaUrl())
		wire, mErr := m.MarshalTraces(tmp)
		if mErr != nil {
			return nil, mErr
		}
		if len(wire) >= 2 && wire[0] == 0x0a {
			rsLen, n := binary.Uvarint(wire[1:])
			if n > 0 && uint64(1+n)+rsLen == uint64(len(wire)) {
				return append(dst[:start], wire[1+n:]...), nil
			}
		}
	}
	return dst, nil
}

func appendResourceBody(dst []byte, r pcommon.Resource) ([]byte, error) {
	var err error
	dst, err = appendAttributes(dst, 0x0a, r.Attributes(), 0)
	if err != nil {
		return nil, err
	}
	if dc := r.DroppedAttributesCount(); dc != 0 {
		dst = append(dst, 0x10)
		dst = binary.AppendUvarint(dst, uint64(dc))
	}
	return dst, nil
}

// appendScopeHeader appends canonical OTLP ScopeSpans header fields (field 1 InstrumentationScope,
// always emitted even when empty matching pdata, and field 3 SchemaUrl if non-empty) directly
// into dst.
func appendScopeHeader(dst []byte, ss ptrace.ScopeSpans) ([]byte, error) {
	dst = append(dst, 0x0a, 0)
	scStart := len(dst)
	var err error
	dst, err = appendScopeBody(dst, ss.Scope())
	if err != nil {
		return nil, err
	}
	dst = patchLenPrefix(dst, scStart)
	if su := ss.SchemaUrl(); len(su) > 0 {
		dst = appendLenTagString(dst, 0x1a, su)
	}
	return dst, nil
}

func appendScopeBody(dst []byte, s pcommon.InstrumentationScope) ([]byte, error) {
	if name := s.Name(); len(name) > 0 {
		dst = appendLenTagString(dst, 0x0a, name)
	}
	if ver := s.Version(); len(ver) > 0 {
		dst = appendLenTagString(dst, 0x12, ver)
	}
	var err error
	dst, err = appendAttributes(dst, 0x1a, s.Attributes(), 0)
	if err != nil {
		return nil, err
	}
	if dc := s.DroppedAttributesCount(); dc != 0 {
		dst = append(dst, 0x20)
		dst = binary.AppendUvarint(dst, uint64(dc))
	}
	return dst, nil
}

// appendSpanBody appends the OTLP protobuf wire encoding of all 16 Span fields into dst
// (without the outer field 2 tag/length of ScopeSpans). Matches pdata@v1.71.1 wire layout.
func appendSpanBody(dst []byte, sp ptrace.Span) ([]byte, error) {
	// Field 1: trace_id (bytes) — pdata always emits tag 0x0a even when empty (0x0a 0x00).
	tid := sp.TraceID()
	if tid.IsEmpty() {
		dst = append(dst, 0x0a, 0x00)
	} else {
		dst = append(dst, 0x0a, 16)
		dst = append(dst, tid[:]...)
	}

	// Field 2: span_id (bytes) — always emits tag 0x12 even when empty.
	sid := sp.SpanID()
	if sid.IsEmpty() {
		dst = append(dst, 0x12, 0x00)
	} else {
		dst = append(dst, 0x12, 8)
		dst = append(dst, sid[:]...)
	}

	// Field 3: trace_state (string)
	if ts := sp.TraceState().AsRaw(); len(ts) > 0 {
		dst = appendLenTagString(dst, 0x1a, ts)
	}

	// Field 4: parent_span_id (bytes) — always emits tag 0x22 even when empty.
	pid := sp.ParentSpanID()
	if pid.IsEmpty() {
		dst = append(dst, 0x22, 0x00)
	} else {
		dst = append(dst, 0x22, 8)
		dst = append(dst, pid[:]...)
	}

	// Field 16: flags (fixed32) — tag (16<<3)|5 = 133 -> varint [0x85, 0x01].
	if flags := sp.Flags(); flags != 0 {
		dst = append(dst, 0x85, 0x01)
		dst = binary.LittleEndian.AppendUint32(dst, flags)
	}

	// Field 5: name (string)
	if name := sp.Name(); len(name) > 0 {
		dst = appendLenTagString(dst, 0x2a, name)
	}

	// Field 6: kind (enum / varint)
	if kind := sp.Kind(); kind != 0 {
		dst = append(dst, 0x30)
		dst = binary.AppendUvarint(dst, uint64(kind))
	}

	// Field 7: start_time_unix_nano (fixed64)
	if st := sp.StartTimestamp(); st != 0 {
		dst = append(dst, 0x39)
		dst = binary.LittleEndian.AppendUint64(dst, uint64(st))
	}

	// Field 8: end_time_unix_nano (fixed64)
	if et := sp.EndTimestamp(); et != 0 {
		dst = append(dst, 0x41)
		dst = binary.LittleEndian.AppendUint64(dst, uint64(et))
	}

	// Field 9: attributes (repeated KeyValue)
	var err error
	dst, err = appendAttributes(dst, 0x4a, sp.Attributes(), 0)
	if err != nil {
		return nil, err
	}

	// Field 10: dropped_attributes_count (uint32)
	if dac := sp.DroppedAttributesCount(); dac != 0 {
		dst = append(dst, 0x50)
		dst = binary.AppendUvarint(dst, uint64(dac))
	}

	// Field 11: events (repeated SpanEvent)
	events := sp.Events()
	for i := 0; i < events.Len(); i++ {
		dst = append(dst, 0x5a, 0)
		evStart := len(dst)
		dst, err = appendSpanEventBody(dst, events.At(i))
		if err != nil {
			return nil, err
		}
		dst = patchLenPrefix(dst, evStart)
	}

	// Field 12: dropped_events_count (uint32)
	if dec := sp.DroppedEventsCount(); dec != 0 {
		dst = append(dst, 0x60)
		dst = binary.AppendUvarint(dst, uint64(dec))
	}

	// Field 13: links (repeated SpanLink)
	links := sp.Links()
	for i := 0; i < links.Len(); i++ {
		dst = append(dst, 0x6a, 0)
		lkStart := len(dst)
		dst, err = appendSpanLinkBody(dst, links.At(i))
		if err != nil {
			return nil, err
		}
		dst = patchLenPrefix(dst, lkStart)
	}

	// Field 14: dropped_links_count (uint32)
	if dlc := sp.DroppedLinksCount(); dlc != 0 {
		dst = append(dst, 0x70)
		dst = binary.AppendUvarint(dst, uint64(dlc))
	}

	// Field 15: status (Status) — pdata always emits tag 0x7a even when empty.
	dst = append(dst, 0x7a, 0)
	stStart := len(dst)
	status := sp.Status()
	if msg := status.Message(); len(msg) > 0 {
		dst = appendLenTagString(dst, 0x12, msg)
	}
	if code := status.Code(); code != 0 {
		dst = append(dst, 0x18)
		dst = binary.AppendUvarint(dst, uint64(code))
	}
	dst = patchLenPrefix(dst, stStart)

	return dst, nil
}

func appendSpanEventBody(dst []byte, ev ptrace.SpanEvent) ([]byte, error) {
	// Field 1: time_unix_nano (fixed64)
	if ts := ev.Timestamp(); ts != 0 {
		dst = append(dst, 0x09)
		dst = binary.LittleEndian.AppendUint64(dst, uint64(ts))
	}
	// Field 2: name (string)
	if name := ev.Name(); len(name) > 0 {
		dst = appendLenTagString(dst, 0x12, name)
	}
	// Field 3: attributes (repeated KeyValue)
	var err error
	dst, err = appendAttributes(dst, 0x1a, ev.Attributes(), 0)
	if err != nil {
		return nil, err
	}
	// Field 4: dropped_attributes_count (uint32)
	if dac := ev.DroppedAttributesCount(); dac != 0 {
		dst = append(dst, 0x20)
		dst = binary.AppendUvarint(dst, uint64(dac))
	}
	return dst, nil
}

func appendSpanLinkBody(dst []byte, lk ptrace.SpanLink) ([]byte, error) {
	// Field 1: trace_id (bytes)
	tid := lk.TraceID()
	if tid.IsEmpty() {
		dst = append(dst, 0x0a, 0x00)
	} else {
		dst = append(dst, 0x0a, 16)
		dst = append(dst, tid[:]...)
	}
	// Field 2: span_id (bytes)
	sid := lk.SpanID()
	if sid.IsEmpty() {
		dst = append(dst, 0x12, 0x00)
	} else {
		dst = append(dst, 0x12, 8)
		dst = append(dst, sid[:]...)
	}
	// Field 3: trace_state (string)
	if ts := lk.TraceState().AsRaw(); len(ts) > 0 {
		dst = appendLenTagString(dst, 0x1a, ts)
	}
	// Field 4: attributes (repeated KeyValue)
	var err error
	dst, err = appendAttributes(dst, 0x22, lk.Attributes(), 0)
	if err != nil {
		return nil, err
	}
	// Field 5: dropped_attributes_count (uint32)
	if dac := lk.DroppedAttributesCount(); dac != 0 {
		dst = append(dst, 0x28)
		dst = binary.AppendUvarint(dst, uint64(dac))
	}
	// Field 6: flags (fixed32)
	if flags := lk.Flags(); flags != 0 {
		dst = append(dst, 0x35)
		dst = binary.LittleEndian.AppendUint32(dst, flags)
	}
	return dst, nil
}

func appendAttributes(dst []byte, tag byte, m pcommon.Map, depth int) ([]byte, error) {
	if m.Len() == 0 {
		return dst, nil
	}
	var retErr error
	for k, v := range m.All() {
		dst = append(dst, tag, 0)
		kvStart := len(dst)
		dst, retErr = appendKeyValueBody(dst, k, v, depth)
		if retErr != nil {
			return nil, retErr
		}
		dst = patchLenPrefix(dst, kvStart)
	}
	return dst, nil
}

func appendKeyValueBody(dst []byte, key string, v pcommon.Value, depth int) ([]byte, error) {
	if depth >= maxProtoValueDepth {
		return nil, errWireDepthExceeded
	}
	if len(key) > 0 {
		dst = appendLenTagString(dst, 0x0a, key)
	}
	// Field 2: value (AnyValue) — pdata always emits tag 0x12 even when ValueTypeEmpty (0x12 0x00).
	dst = append(dst, 0x12, 0)
	valStart := len(dst)
	var err error
	dst, err = appendAnyValueBody(dst, v, depth+1)
	if err != nil {
		return nil, err
	}
	dst = patchLenPrefix(dst, valStart)
	return dst, nil
}

func appendAnyValueBody(dst []byte, v pcommon.Value, depth int) ([]byte, error) {
	if depth >= maxProtoValueDepth {
		return nil, errWireDepthExceeded
	}
	// In OTLP proto3 AnyValue oneof, every non-empty variant MUST emit its tag even
	// for zero values ("", false, 0, 0.0, [], {}, []byte{}) to preserve exact ValueType.
	switch v.Type() {
	case pcommon.ValueTypeEmpty:
		// 0 bytes inside AnyValue.
	case pcommon.ValueTypeStr:
		dst = appendLenTagString(dst, 0x0a, v.Str())
	case pcommon.ValueTypeBool:
		if v.Bool() {
			dst = append(dst, 0x10, 1)
		} else {
			dst = append(dst, 0x10, 0)
		}
	case pcommon.ValueTypeInt:
		dst = append(dst, 0x18)
		dst = binary.AppendUvarint(dst, uint64(v.Int()))
	case pcommon.ValueTypeDouble:
		dst = append(dst, 0x21)
		dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(v.Double()))
	case pcommon.ValueTypeSlice:
		// ArrayValue occupies an intermediate message boundary matching pdata's
		// AnyValue(depth) -> ArrayValue(depth+1) -> AnyValue(depth+2).
		if depth+1 >= maxProtoValueDepth {
			return nil, errWireDepthExceeded
		}
		dst = append(dst, 0x2a, 0)
		arrStart := len(dst)
		sl := v.Slice()
		for i := 0; i < sl.Len(); i++ {
			dst = append(dst, 0x0a, 0)
			elemStart := len(dst)
			var err error
			dst, err = appendAnyValueBody(dst, sl.At(i), depth+2)
			if err != nil {
				return nil, err
			}
			dst = patchLenPrefix(dst, elemStart)
		}
		dst = patchLenPrefix(dst, arrStart)
	case pcommon.ValueTypeMap:
		// KeyValueList occupies an intermediate message boundary matching pdata's
		// AnyValue(depth) -> KeyValueList(depth+1) -> KeyValue(depth+2) -> AnyValue(depth+3).
		if depth+1 >= maxProtoValueDepth {
			return nil, errWireDepthExceeded
		}
		dst = append(dst, 0x32, 0)
		mapStart := len(dst)
		var err error
		dst, err = appendAttributes(dst, 0x0a, v.Map(), depth+2)
		if err != nil {
			return nil, err
		}
		dst = patchLenPrefix(dst, mapStart)
	case pcommon.ValueTypeBytes:
		dst = appendLenTagBytes(dst, 0x3a, v.Bytes().AsRaw())
	}
	return dst, nil
}

// appendRawResourceSpansGroup appends a complete OTLP ResourceSpans wire body (without outer
// TracesData field 1 tag/len) directly from pdata for rawProtoSlabArena (no interning).
// When cachedResHeader is non-empty it is copied directly instead of re-encoding rs.Resource().
func appendRawResourceSpansGroup(dst []byte, rs ptrace.ResourceSpans, cachedResHeader []byte, scopeIndices []int, spanIndices [][]int) ([]byte, error) {
	var err error
	if len(cachedResHeader) > 0 {
		dst = append(dst, cachedResHeader...)
	} else {
		dst, err = appendResourceHeader(dst, rs)
		if err != nil {
			return nil, err
		}
	}
	scopes := rs.ScopeSpans()
	for pos, sIdx := range scopeIndices {
		spIdxs := spanIndices[pos]
		if len(spIdxs) == 0 {
			continue
		}
		ss := scopes.At(sIdx)
		dst = append(dst, 0x12, 0)
		ssStart := len(dst)
		dst, err = appendScopeHeader(dst, ss)
		if err != nil {
			return nil, err
		}
		spans := ss.Spans()
		for _, k := range spIdxs {
			dst = append(dst, 0x12, 0)
			spStart := len(dst)
			dst, err = appendSpanBody(dst, spans.At(k))
			if err != nil {
				return nil, err
			}
			dst = patchLenPrefix(dst, spStart)
		}
		dst = patchLenPrefix(dst, ssStart)
	}
	return dst, nil
}

// ---------------------------------------------------------------------------
// Wire-level OTLP frame splitter & reassembler (preserves unknown fields across
// protobuf wire types 0..5, reverse field order, schema_url, and zero TraceID).
// ---------------------------------------------------------------------------

// rawWireTraceGroup represents one (TraceID, incoming ResourceSpans) group extracted
// directly from raw OTLP TracesData wire bytes without pdata unmarshaling.
type rawWireTraceGroup struct {
	traceID     pcommon.TraceID
	resHeader   []byte // field 1 Resource + field 3 SchemaUrl + any unknown ResourceSpans fields
	scopes      []rawWireScopeGroup
	origRSIndex int
}

type rawWireScopeGroup struct {
	scopeHeader  []byte   // field 1 Scope + field 3 SchemaUrl + any unknown ScopeSpans fields
	spans        [][]byte // raw Span message wire bodies (preserving all unknown fields inside Span)
	isDeprecated bool     // true if extracted from deprecated field 1000
}

type rawWireSplitResult struct {
	groups        []rawWireTraceGroup
	topUnknownRaw []byte // any unknown fields present at top-level TracesData
}

func parseWireTag(buf []byte, pos int) (fieldNum uint64, wireType uint8, nextPos int, err error) {
	tag, n := binary.Uvarint(buf[pos:])
	if n <= 0 {
		return 0, 0, 0, errWireTruncated
	}
	fieldNum = tag >> 3
	wireType = uint8(tag & 0x7)
	if fieldNum == 0 || fieldNum > maxProtoFieldNum {
		return 0, 0, 0, fmt.Errorf("%w: fieldNum=%d", errWireInvalidTag, fieldNum)
	}
	return fieldNum, wireType, pos + n, nil
}

// consumeWireField skips a single protobuf field value starting at pos (immediately after
// the tag varint) for wireType 0..5 with bounded group recursion depth <= maxWireGroupDepth.
// Returns the new offset immediately after the field value.
func consumeWireField(buf []byte, pos int, fieldNum uint64, wireType uint8, depth int) (int, error) {
	switch wireType {
	case 0: // Varint
		_, n := binary.Uvarint(buf[pos:])
		if n <= 0 {
			return 0, errWireTruncated
		}
		return pos + n, nil
	case 1: // I64
		if len(buf)-pos < 8 {
			return 0, errWireTruncated
		}
		return pos + 8, nil
	case 2: // LEN
		length, n := binary.Uvarint(buf[pos:])
		if n <= 0 {
			return 0, errWireTruncated
		}
		pos += n
		if length > uint64(len(buf)-pos) {
			return 0, errWireInvalidLen
		}
		return pos + int(length), nil
	case 3: // SGROUP
		if depth >= maxWireGroupDepth {
			return 0, errWireDepthExceeded
		}
		for pos < len(buf) {
			subNum, subType, next, err := parseWireTag(buf, pos)
			if err != nil {
				return 0, err
			}
			pos = next
			if subType == 4 {
				if subNum != fieldNum {
					return 0, errWireInvalidGroup
				}
				return pos, nil
			}
			pos, err = consumeWireField(buf, pos, subNum, subType, depth+1)
			if err != nil {
				return 0, err
			}
		}
		return 0, errWireTruncated
	case 4: // EGROUP without matching SGROUP
		return 0, errWireInvalidGroup
	case 5: // I32
		if len(buf)-pos < 4 {
			return 0, errWireTruncated
		}
		return pos + 4, nil
	default:
		return 0, fmt.Errorf("%w: wireType=%d", errWireInvalidTag, wireType)
	}
}

// extractSpanTraceIDFromWire scans raw Span wire bytes for field 1 (trace_id).
// Per proto3 rules:
// - If field 1 is omitted or has length 0, returns zero TraceID ([16]byte{}).
// - If field 1 appears multiple times, last occurrence wins.
// - Skips all other known and unknown fields across wire types 0..5.
func extractSpanTraceIDFromWire(spanBuf []byte) (pcommon.TraceID, error) {
	var tid pcommon.TraceID
	pos := 0
	for pos < len(spanBuf) {
		fieldNum, wireType, next, err := parseWireTag(spanBuf, pos)
		if err != nil {
			return tid, err
		}
		pos = next
		if fieldNum == 1 && wireType == 2 {
			length, ln := binary.Uvarint(spanBuf[pos:])
			if ln <= 0 {
				return tid, errWireTruncated
			}
			pos += ln
			if length > uint64(len(spanBuf)-pos) {
				return tid, errWireInvalidLen
			}
			val := spanBuf[pos : pos+int(length)]
			pos += int(length)
			if len(val) == 0 {
				tid = pcommon.TraceID{}
			} else if len(val) == 16 {
				copy(tid[:], val)
			} else {
				return tid, fmt.Errorf("%w: invalid trace_id length %d", errWireInvalidLen, len(val))
			}
			continue
		}
		pos, err = consumeWireField(spanBuf, pos, fieldNum, wireType, 0)
		if err != nil {
			return tid, err
		}
	}
	return tid, nil
}

// splitRawWire parses raw OTLP TracesData protobuf bytes into per-(TraceID, ResourceSpans)
// groups while preserving unknown fields (wire types 0..5), reverse field order, schema_url,
// and omitted proto3 zero TraceIDs.
func splitRawWire(raw []byte) (rawWireSplitResult, error) {
	var res rawWireSplitResult
	pos := 0
	rsIdx := 0
	for pos < len(raw) {
		fieldStart := pos
		fieldNum, wireType, next, err := parseWireTag(raw, pos)
		if err != nil {
			return res, err
		}
		pos = next
		if fieldNum == 1 && wireType == 2 {
			length, ln := binary.Uvarint(raw[pos:])
			if ln <= 0 {
				return res, errWireTruncated
			}
			pos += ln
			if length > uint64(len(raw)-pos) {
				return res, errWireInvalidLen
			}
			rsBuf := raw[pos : pos+int(length)]
			pos += int(length)
			if err := splitRawResourceSpans(rsBuf, rsIdx, &res); err != nil {
				return res, err
			}
			rsIdx++
			continue
		}
		end, err := consumeWireField(raw, pos, fieldNum, wireType, 0)
		if err != nil {
			return res, err
		}
		res.topUnknownRaw = append(res.topUnknownRaw, raw[fieldStart:end]...)
		pos = end
	}
	return res, nil
}

func splitRawResourceSpans(rsBuf []byte, rsIdx int, out *rawWireSplitResult) error {
	var resHeader []byte
	type rawScopeItem struct {
		header     []byte
		spans      [][]byte
		deprecated bool
	}
	var scopes []rawScopeItem

	pos := 0
	for pos < len(rsBuf) {
		fieldStart := pos
		fieldNum, wireType, next, err := parseWireTag(rsBuf, pos)
		if err != nil {
			return err
		}
		pos = next

		if (fieldNum == 2 || fieldNum == 1000) && wireType == 2 {
			length, ln := binary.Uvarint(rsBuf[pos:])
			if ln <= 0 {
				return errWireTruncated
			}
			pos += ln
			if length > uint64(len(rsBuf)-pos) {
				return errWireInvalidLen
			}
			ssBuf := rsBuf[pos : pos+int(length)]
			pos += int(length)
			scHeader, scSpans, err := splitRawScopeSpans(ssBuf)
			if err != nil {
				return err
			}
			scopes = append(scopes, rawScopeItem{
				header:     scHeader,
				spans:      scSpans,
				deprecated: fieldNum == 1000,
			})
			continue
		}

		// Field 1 (Resource), Field 3 (SchemaUrl), or ANY unknown field on ResourceSpans
		// (wire types 0..5) are preserved verbatim in resHeader!
		end, err := consumeWireField(rsBuf, pos, fieldNum, wireType, 0)
		if err != nil {
			return err
		}
		resHeader = append(resHeader, rsBuf[fieldStart:end]...)
		pos = end
	}

	// Group spans by TraceID within this ResourceSpans, preserving regular (field 2)
	// and deprecated (field 1000) scopes inside the same ResourceSpans group.
	idxByTrace := make(map[pcommon.TraceID]int)
	var localGroups []rawWireTraceGroup

	for _, sc := range scopes {
		scopeAdded := make(map[int]int) // localGroupIdx -> scopePos in localGroups[g].scopes
		for _, spRaw := range sc.spans {
			tid, err := extractSpanTraceIDFromWire(spRaw)
			if err != nil {
				return err
			}
			gIdx, ok := idxByTrace[tid]
			if !ok {
				gIdx = len(localGroups)
				idxByTrace[tid] = gIdx
				localGroups = append(localGroups, rawWireTraceGroup{
					traceID:     tid,
					resHeader:   resHeader,
					origRSIndex: rsIdx,
				})
			}
			sPos, hasScope := scopeAdded[gIdx]
			if !hasScope {
				sPos = len(localGroups[gIdx].scopes)
				scopeAdded[gIdx] = sPos
				localGroups[gIdx].scopes = append(localGroups[gIdx].scopes, rawWireScopeGroup{
					scopeHeader:  sc.header,
					isDeprecated: sc.deprecated,
				})
			}
			localGroups[gIdx].scopes[sPos].spans = append(localGroups[gIdx].scopes[sPos].spans, spRaw)
		}
	}

	out.groups = append(out.groups, localGroups...)
	return nil
}

func splitRawScopeSpans(ssBuf []byte) ([]byte, [][]byte, error) {
	var scopeHeader []byte
	var spans [][]byte

	pos := 0
	for pos < len(ssBuf) {
		fieldStart := pos
		fieldNum, wireType, next, err := parseWireTag(ssBuf, pos)
		if err != nil {
			return nil, nil, err
		}
		pos = next

		if fieldNum == 2 && wireType == 2 {
			length, ln := binary.Uvarint(ssBuf[pos:])
			if ln <= 0 {
				return nil, nil, errWireTruncated
			}
			pos += ln
			if length > uint64(len(ssBuf)-pos) {
				return nil, nil, errWireInvalidLen
			}
			spans = append(spans, ssBuf[pos:pos+int(length)])
			pos += int(length)
			continue
		}

		// Field 1 (Scope), Field 3 (SchemaUrl), or ANY unknown field on ScopeSpans (0..5)
		// are preserved verbatim in scopeHeader!
		end, err := consumeWireField(ssBuf, pos, fieldNum, wireType, 0)
		if err != nil {
			return nil, nil, err
		}
		scopeHeader = append(scopeHeader, ssBuf[fieldStart:end]...)
		pos = end
	}
	return scopeHeader, spans, nil
}

// reassembleRawWire reconstructs a valid OTLP TracesData wire message from split groups
// plus top-level unknown fields.
func reassembleRawWire(groups []rawWireTraceGroup, topUnknownRaw []byte) []byte {
	var dst []byte
	for _, g := range groups {
		dst = append(dst, 0x0a, 0)
		rsStart := len(dst)
		dst = append(dst, g.resHeader...)
		for _, sc := range g.scopes {
			if sc.isDeprecated {
				// Tag for field 1000 LEN: (1000<<3)|2 = 8002 -> varint [0xc2, 0x3e].
				dst = append(dst, 0xc2, 0x3e, 0)
			} else {
				dst = append(dst, 0x12, 0)
			}
			ssStart := len(dst)
			dst = append(dst, sc.scopeHeader...)
			for _, sp := range sc.spans {
				dst = appendLenTagBytes(dst, 0x12, sp)
			}
			dst = patchLenPrefix(dst, ssStart)
		}
		dst = patchLenPrefix(dst, rsStart)
	}
	dst = append(dst, topUnknownRaw...)
	return dst
}
