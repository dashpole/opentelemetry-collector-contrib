// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tailsamplingprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/tailstorageextension"
)

const (
	arenaNoIdx = ^uint32(0)

	defaultArenaChunkSlabSize  = 256 * 1024 // 256 KiB default chunk slab (no 16 MiB floor across shards)
	defaultArenaInternSlabSize = 64 * 1024  // 64 KiB max default intern slab
	maxArenaSlabSize           = 1 << 29    // 512 MiB upper clamp preventing uint32 overflow on slab offsets
	maxArenaChunkSize          = math.MaxInt32
	maxArenaScratchCompCap     = 512 * 1024 // cap retained scratchComp buffer to avoid pinning giant outliers
	arenaMaxPooledSlabs        = 4          // bounded recycled mmap slabs (MADV_DONTNEED on Linux; drained when idle)

	chunkFlagCompressed uint8 = 1 << 0
	chunkFlagLive       uint8 = 1 << 1

	internFlagCompressed uint8 = 1 << 0
	internFlagLive       uint8 = 1 << 1

	// FNV-1a 64-bit constants (inlined for zero-alloc hash computation).
	fnvOffset64 uint64 = 14695981039346656037
	fnvPrime64  uint64 = 1099511628211
)

var (
	errArenaClosed            = errors.New("arena payload store is closed")
	errArenaCorruptBody       = errors.New("arena payload store: corrupt chunk body")
	errArenaOversizedPayload  = errors.New("arena payload store: payload exceeds maximum chunk size")
	errArenaMismatchedIndices = errors.New("arena payload store: scopeIndices and spanIndices length mismatch")
)

// ivmBatchAwareStore is an optional optimization interface implemented by arena stores
// to pin and reuse Resource/Scope intern IDs across all traces sharing one incoming ResourceSpans.
type ivmBatchAwareStore interface {
	BeginResourceSpans(numScopes int)
	FinishResourceSpans()
}

func fnv1a64(b []byte) uint64 {
	h := fnvOffset64
	for _, c := range b {
		h ^= uint64(c)
		h *= fnvPrime64
	}
	return h
}

// ---------------------------------------------------------------------------
// Generic slab pool (supports noscan Go heap []byte slabs OR off-heap mmap slabs).
// ---------------------------------------------------------------------------

type arenaSlab struct {
	buf            []byte
	used           uint32
	liveBytes      uint32
	liveCount      uint32
	liveHead       uint32
	liveTail       uint32
	firstAllocTick uint64
	sealedTick     uint64
	sealed         bool
	dedicated      bool
	mmapped        bool
	inUse          bool
}

type arenaSlabPool struct {
	useMmap         bool
	defaultSize     int
	slabs           []arenaSlab
	freeSlabIDs     []uint32
	activeSlabID    uint32
	compactSlabID   uint32 // generational old-slab target for relocated aged items (zero mixing with ephemeral ingest)
	liveSlabCount   int
	pooledSlabs     [][]byte
	totalAllocBytes uint64
	mmapActiveBytes uint64
	// testAllocHook optionally intercepts backing allocations in unit tests (e.g. ENOMEM injection).
	testAllocHook func(size int) ([]byte, error)
}

func newArenaSlabPool(useMmap bool, defaultSize int) arenaSlabPool {
	if defaultSize < 4096 {
		defaultSize = 4096
	}
	if defaultSize > maxArenaSlabSize {
		defaultSize = maxArenaSlabSize
	}
	return arenaSlabPool{
		useMmap:       useMmap && arenaMmapSupported,
		defaultSize:   defaultSize,
		activeSlabID:  arenaNoIdx,
		compactSlabID: arenaNoIdx,
	}
}

func (p *arenaSlabPool) allocBacking(size int) ([]byte, bool, error) {
	if size <= 0 || size > maxArenaChunkSize {
		return nil, false, errArenaOversizedPayload
	}
	if p.testAllocHook != nil {
		b, err := p.testAllocHook(size)
		if err != nil {
			return nil, false, err
		}
		p.totalAllocBytes += uint64(len(b))
		return b, false, nil
	}
	if p.useMmap {
		b, err := arenaMmapAlloc(size)
		if err != nil {
			return nil, false, err
		}
		p.mmapActiveBytes += uint64(len(b))
		p.totalAllocBytes += uint64(len(b))
		return b, true, nil
	}
	b := make([]byte, size)
	p.totalAllocBytes += uint64(len(b))
	return b, false, nil
}

func (p *arenaSlabPool) acquireSlab(minSize int) (uint32, error) {
	dedicated := minSize > p.defaultSize
	var buf []byte
	var mmapped bool
	var err error

	if !dedicated && len(p.pooledSlabs) > 0 {
		last := len(p.pooledSlabs) - 1
		buf = p.pooledSlabs[last]
		p.pooledSlabs[last] = nil
		p.pooledSlabs = p.pooledSlabs[:last]
		mmapped = p.useMmap
	} else {
		allocSize := p.defaultSize
		if dedicated {
			allocSize = minSize
		}
		buf, mmapped, err = p.allocBacking(allocSize)
		if err != nil {
			return arenaNoIdx, err
		}
	}

	var id uint32
	if len(p.freeSlabIDs) > 0 {
		last := len(p.freeSlabIDs) - 1
		id = p.freeSlabIDs[last]
		p.freeSlabIDs = p.freeSlabIDs[:last]
	} else {
		id = uint32(len(p.slabs))
		p.slabs = append(p.slabs, arenaSlab{})
	}
	p.slabs[id] = arenaSlab{
		buf:       buf,
		liveHead:  arenaNoIdx,
		liveTail:  arenaNoIdx,
		dedicated: dedicated,
		sealed:    dedicated, // oversized dedicated slabs seal immediately
		mmapped:   mmapped,
		inUse:     true,
	}
	p.liveSlabCount++
	return id, nil
}

// allocFrom allocates n bytes from the designated unsealed target slab (*targetID),
// rotating to a new slab when full. For oversized items (n > defaultSize), allocates
// a dedicated immediately-sealed slab. On acquireSlab failure (e.g. ENOMEM), the prior
// active slab remains unsealed so smaller chunks can still fit into its remaining space.
func (p *arenaSlabPool) allocFrom(targetID *uint32, n uint32, curTick uint64) (slabID uint32, offset uint32, err error) {
	if uint64(n) > uint64(maxArenaChunkSize) {
		return arenaNoIdx, 0, errArenaOversizedPayload
	}
	if uint64(n) > uint64(p.defaultSize) {
		id, err := p.acquireSlab(int(n))
		if err != nil {
			return arenaNoIdx, 0, err
		}
		sl := &p.slabs[id]
		sl.used = n
		sl.firstAllocTick = curTick
		sl.sealedTick = curTick
		return id, 0, nil
	}

	prevID := *targetID
	if prevID != arenaNoIdx {
		sl := &p.slabs[prevID]
		if uint64(sl.used)+uint64(n) <= uint64(len(sl.buf)) {
			off := sl.used
			if sl.liveCount == 0 {
				sl.firstAllocTick = curTick
			}
			sl.used += n
			return prevID, off, nil
		}
		if sl.liveCount == 0 {
			sl.used = n
			sl.firstAllocTick = curTick
			return prevID, 0, nil
		}
	}

	// Acquire the replacement slab BEFORE sealing prevID so ENOMEM never detaches a
	// partially-filled slab that still has room for smaller chunks.
	id, err := p.acquireSlab(p.defaultSize)
	if err != nil {
		return arenaNoIdx, 0, err
	}
	if prevID != arenaNoIdx {
		prev := &p.slabs[prevID]
		prev.sealed = true
		prev.sealedTick = curTick
	}
	*targetID = id
	p.slabs[id].used = n
	p.slabs[id].firstAllocTick = curTick
	return id, 0, nil
}

// alloc reserves n contiguous bytes from the young active slab (or dedicated slab if oversized).
func (p *arenaSlabPool) alloc(n uint32, curTick uint64) (slabID uint32, offset uint32, err error) {
	return p.allocFrom(&p.activeSlabID, n, curTick)
}

// allocCompact reserves n contiguous bytes from the generational compaction target slab
// so aged surviving items pack tightly without mixing with churning ephemeral ingest.
func (p *arenaSlabPool) allocCompact(n uint32, curTick uint64) (slabID uint32, offset uint32, err error) {
	return p.allocFrom(&p.compactSlabID, n, curTick)
}

// sealActive seals young/compact slabs only when empty (released immediately),
// well-utilized (>= 75% full for tight tick alignment at high volume), or fragmented
// after aging past compactAgeTicks (so low-volume slabs don't waste 80% tail slack!).
func (p *arenaSlabPool) sealActive(curTick, compactAgeTicks uint64) {
	if p.activeSlabID != arenaNoIdx {
		sl := &p.slabs[p.activeSlabID]
		if sl.liveCount == 0 {
			id := p.activeSlabID
			p.activeSlabID = arenaNoIdx
			p.releaseSlab(id)
		} else if uint64(sl.used)*4 >= uint64(len(sl.buf))*3 ||
			(curTick-sl.firstAllocTick >= compactAgeTicks && uint64(sl.liveBytes)*2 < uint64(sl.used)) {
			sl.sealed = true
			sl.sealedTick = curTick
			p.activeSlabID = arenaNoIdx
		}
	}
	if p.compactSlabID != arenaNoIdx {
		sl := &p.slabs[p.compactSlabID]
		if sl.liveCount == 0 {
			id := p.compactSlabID
			p.compactSlabID = arenaNoIdx
			p.releaseSlab(id)
		} else if curTick-sl.firstAllocTick >= compactAgeTicks && uint64(sl.liveBytes)*4 < uint64(sl.used) {
			sl.sealed = true
			sl.sealedTick = curTick
			p.compactSlabID = arenaNoIdx
		}
	}
}

func (p *arenaSlabPool) releaseSlab(id uint32) {
	sl := &p.slabs[id]
	if !sl.inUse {
		return
	}
	buf := sl.buf
	mmapped := sl.mmapped
	dedicated := sl.dedicated
	sl.buf = nil
	sl.inUse = false
	sl.used = 0
	sl.liveBytes = 0
	sl.liveCount = 0
	sl.liveHead = arenaNoIdx
	sl.liveTail = arenaNoIdx
	p.liveSlabCount--
	if p.activeSlabID == id {
		p.activeSlabID = arenaNoIdx
	}
	if p.compactSlabID == id {
		p.compactSlabID = arenaNoIdx
	}
	p.freeSlabIDs = append(p.freeSlabIDs, id)

	if mmapped {
		// On Linux, MADV_DONTNEED returns physical pages immediately so pooling up to 4
		// slabs avoids TLB shootdowns without retaining RSS. On non-Linux (e.g. Darwin),
		// unmap immediately to avoid holding RSS pages.
		if !dedicated && arenaSupportsMadvDontNeed && len(p.pooledSlabs) < arenaMaxPooledSlabs {
			arenaMmapDontNeed(buf)
			p.pooledSlabs = append(p.pooledSlabs, buf)
			return
		}
		p.mmapActiveBytes -= uint64(len(buf))
		p.totalAllocBytes -= uint64(len(buf))
		_ = arenaMmapFree(buf)
		return
	}

	// For Go-heap (!mmapped) slabs, never retain in pooledSlabs: Go's mheap natively
	// reuses and scavenges large noscan spans without syscalls, whereas pooling pins
	// dead HeapAlloc/VmRSS bytes permanently whenever >= 1 trace remains live.
	p.totalAllocBytes -= uint64(len(buf))
}

func (p *arenaSlabPool) drainPooled() error {
	var firstErr error
	for i, buf := range p.pooledSlabs {
		if p.useMmap {
			p.mmapActiveBytes -= uint64(len(buf))
			p.totalAllocBytes -= uint64(len(buf))
			if err := arenaMmapFree(buf); err != nil && firstErr == nil {
				firstErr = err
			}
		} else {
			p.totalAllocBytes -= uint64(len(buf))
		}
		p.pooledSlabs[i] = nil
	}
	p.pooledSlabs = p.pooledSlabs[:0]
	if p.liveSlabCount == 0 && len(p.slabs) >= 256 {
		p.slabs = nil
		p.freeSlabIDs = nil
	}
	return firstErr
}

func (p *arenaSlabPool) closeAll() error {
	var firstErr error
	for i := range p.slabs {
		sl := &p.slabs[i]
		if !sl.inUse {
			continue
		}
		buf := sl.buf
		mmapped := sl.mmapped
		sl.buf = nil
		sl.inUse = false
		sl.used = 0
		sl.liveBytes = 0
		sl.liveCount = 0
		sl.liveHead = arenaNoIdx
		sl.liveTail = arenaNoIdx
		if mmapped {
			p.mmapActiveBytes -= uint64(len(buf))
			p.totalAllocBytes -= uint64(len(buf))
			if err := arenaMmapFree(buf); err != nil && firstErr == nil {
				firstErr = err
			}
		} else {
			p.totalAllocBytes -= uint64(len(buf))
		}
	}
	p.liveSlabCount = 0
	p.activeSlabID = arenaNoIdx
	p.compactSlabID = arenaNoIdx
	p.slabs = nil
	p.freeSlabIDs = nil
	if err := p.drainPooled(); err != nil && firstErr == nil {
		firstErr = err
	}
	p.pooledSlabs = nil
	return firstErr
}

// ---------------------------------------------------------------------------
// Intrusive noscan refcounted intern table backed by tick-sealed compacted slabs.
// Separate instances are used for Resources and Scopes so constant Scopes never
// pin slabs holding churning ephemeral Resources.
// ---------------------------------------------------------------------------

// internEntry contains strictly scalar primitives (0 Go pointers -> noscan).
type internEntry struct {
	hash         uint64
	refCount     uint64
	nextInBucket uint32
	slabID       uint32
	offset       uint32
	storedLen    uint32
	rawLen       uint32
	prevInSlab   uint32
	nextInSlab   uint32
	flags        uint8
}

type arenaInternTable struct {
	pool            arenaSlabPool
	buckets         map[uint64]uint32 // 0 Go pointers -> noscan
	entries         []internEntry     // 0 Go pointers -> noscan
	freeIDs         []uint32          // 0 Go pointers -> noscan
	liveEntries     int
	peakEntries     int
	liveRawBytes    uint64
	liveStoredBytes uint64
	useLZ4          bool
	lz4             *lz4State
	scratchComp     []byte
	scratchDecomp   []byte
	compactCount    uint64
	curTick         uint64
	compactAgeTicks uint64
}

func newArenaInternTable(useMmap bool, slabSize int, useLZ4 bool, lz4 *lz4State, compactAgeTicks uint64) *arenaInternTable {
	if compactAgeTicks == 0 {
		compactAgeTicks = 1
	}
	return &arenaInternTable{
		pool:            newArenaSlabPool(useMmap, slabSize),
		buckets:         make(map[uint64]uint32),
		useLZ4:          useLZ4,
		lz4:             lz4,
		compactAgeTicks: compactAgeTicks,
	}
}

func (t *arenaInternTable) entryMatches(e *internEntry, raw []byte) bool {
	if e.rawLen == 0 {
		return len(raw) == 0
	}
	stored := t.pool.slabs[e.slabID].buf[e.offset : e.offset+e.storedLen]
	if e.flags&internFlagCompressed != 0 {
		decomp, err := lz4DecompressBlock(stored, t.scratchDecomp[:0], int(e.rawLen))
		if err != nil {
			return false
		}
		t.scratchDecomp = decomp
		return bytes.Equal(decomp, raw)
	}
	return bytes.Equal(stored, raw)
}

func (t *arenaInternTable) intern(raw []byte) (uint32, error) {
	if len(raw) > maxArenaChunkSize {
		return arenaNoIdx, errArenaOversizedPayload
	}
	h := fnv1a64(raw)
	if head, ok := t.buckets[h]; ok {
		for id := head; id != arenaNoIdx; {
			e := &t.entries[id]
			if e.hash == h && int(e.rawLen) == len(raw) && t.entryMatches(e, raw) {
				e.refCount++
				return id, nil
			}
			id = e.nextInBucket
		}
	}

	stored := raw
	flags := internFlagLive
	if t.useLZ4 && t.lz4 != nil && len(raw) >= lz4MinCompressSize {
		cutoff := (len(raw) * 9) / 10
		comp, ok := t.lz4.compressBlock(raw, t.scratchComp[:0], cutoff)
		// Retain allocated scratchComp even on bail-out (capped to avoid pinning outliers).
		if cap(comp) <= maxArenaScratchCompCap {
			t.scratchComp = comp[:0]
		}
		if ok {
			stored = comp
			flags |= internFlagCompressed
		}
	}

	var slabID, offset uint32
	storedLen := uint32(len(stored))
	if storedLen > 0 {
		var err error
		slabID, offset, err = t.pool.alloc(storedLen, t.curTick)
		if err != nil {
			return arenaNoIdx, err
		}
		copy(t.pool.slabs[slabID].buf[offset:offset+storedLen], stored)
	} else {
		slabID = arenaNoIdx
	}

	var id uint32
	if len(t.freeIDs) > 0 {
		last := len(t.freeIDs) - 1
		id = t.freeIDs[last]
		t.freeIDs = t.freeIDs[:last]
	} else {
		id = uint32(len(t.entries))
		t.entries = append(t.entries, internEntry{})
	}

	prevHead := arenaNoIdx
	if existing, ok := t.buckets[h]; ok {
		prevHead = existing
	}
	t.buckets[h] = id

	t.entries[id] = internEntry{
		hash:         h,
		refCount:     1,
		nextInBucket: prevHead,
		slabID:       slabID,
		offset:       offset,
		storedLen:    storedLen,
		rawLen:       uint32(len(raw)),
		prevInSlab:   arenaNoIdx,
		nextInSlab:   arenaNoIdx,
		flags:        flags,
	}

	if slabID != arenaNoIdx {
		t.linkIntoSlab(id, slabID)
	}

	t.liveEntries++
	if t.liveEntries > t.peakEntries {
		t.peakEntries = t.liveEntries
	}
	t.liveRawBytes += uint64(len(raw))
	t.liveStoredBytes += uint64(storedLen)
	return id, nil
}

func (t *arenaInternTable) addRef(id uint32) {
	t.entries[id].refCount++
}

func (t *arenaInternTable) appendBytes(id uint32, dst []byte) ([]byte, error) {
	if id == arenaNoIdx || int(id) >= len(t.entries) {
		return nil, errArenaCorruptBody
	}
	e := &t.entries[id]
	if e.flags&internFlagLive == 0 {
		return nil, errArenaCorruptBody
	}
	if e.rawLen == 0 {
		return dst, nil
	}
	if e.slabID == arenaNoIdx || int(e.slabID) >= len(t.pool.slabs) {
		return nil, errArenaCorruptBody
	}
	sl := &t.pool.slabs[e.slabID]
	if !sl.inUse || uint64(e.offset)+uint64(e.storedLen) > uint64(len(sl.buf)) {
		return nil, errArenaCorruptBody
	}
	stored := sl.buf[e.offset : e.offset+e.storedLen]
	if e.flags&internFlagCompressed != 0 {
		decomp, err := lz4DecompressBlock(stored, t.scratchDecomp[:0], int(e.rawLen))
		if err != nil {
			return nil, err
		}
		t.scratchDecomp = decomp
		return append(dst, decomp...), nil
	}
	return append(dst, stored...), nil
}

func (t *arenaInternTable) linkIntoSlab(id uint32, slabID uint32) {
	sl := &t.pool.slabs[slabID]
	e := &t.entries[id]
	e.prevInSlab = sl.liveTail
	e.nextInSlab = arenaNoIdx
	if sl.liveTail != arenaNoIdx {
		t.entries[sl.liveTail].nextInSlab = id
	} else {
		sl.liveHead = id
	}
	sl.liveTail = id
	sl.liveCount++
	sl.liveBytes += e.storedLen
}

func (t *arenaInternTable) unlinkFromSlab(id uint32, slabID uint32) {
	sl := &t.pool.slabs[slabID]
	e := &t.entries[id]
	if e.prevInSlab != arenaNoIdx {
		t.entries[e.prevInSlab].nextInSlab = e.nextInSlab
	} else {
		sl.liveHead = e.nextInSlab
	}
	if e.nextInSlab != arenaNoIdx {
		t.entries[e.nextInSlab].prevInSlab = e.prevInSlab
	} else {
		sl.liveTail = e.prevInSlab
	}
	e.prevInSlab = arenaNoIdx
	e.nextInSlab = arenaNoIdx
	sl.liveCount--
	sl.liveBytes -= e.storedLen
}

func (t *arenaInternTable) release(id uint32) {
	if id == arenaNoIdx || int(id) >= len(t.entries) {
		return
	}
	e := &t.entries[id]
	if e.flags&internFlagLive == 0 {
		return
	}
	e.refCount--
	if e.refCount > 0 {
		return
	}

	// Unlink from hash bucket collision chain.
	h := e.hash
	if head, ok := t.buckets[h]; ok {
		if head == id {
			if e.nextInBucket == arenaNoIdx {
				delete(t.buckets, h)
			} else {
				t.buckets[h] = e.nextInBucket
			}
		} else {
			prev := head
			for prev != arenaNoIdx {
				cur := t.entries[prev].nextInBucket
				if cur == id {
					t.entries[prev].nextInBucket = e.nextInBucket
					break
				}
				prev = cur
			}
		}
	}

	slabID := e.slabID
	if slabID != arenaNoIdx {
		t.unlinkFromSlab(id, slabID)
		sl := &t.pool.slabs[slabID]
		if sl.liveCount == 0 {
			if sl.sealed || sl.dedicated {
				t.pool.releaseSlab(slabID)
			} else {
				// Empty unsealed active slab: rewind for instant reuse.
				sl.used = 0
			}
		}
	}

	t.liveRawBytes -= uint64(e.rawLen)
	t.liveStoredBytes -= uint64(e.storedLen)
	t.liveEntries--
	e.flags = 0
	e.slabID = arenaNoIdx
	e.nextInBucket = arenaNoIdx
	t.freeIDs = append(t.freeIDs, id)

	// Map compaction with peakEntries reset (prevents O(N^2) rebuild thrashing).
	if t.liveEntries == 0 {
		if t.peakEntries >= 64 {
			t.buckets = make(map[uint64]uint32)
		} else {
			clear(t.buckets)
		}
		t.peakEntries = 0
		if len(t.entries) >= 4096 {
			t.entries = nil
			t.freeIDs = nil
		}
		// Release empty active/compact slabs & pooled slabs when completely idle.
		if t.pool.activeSlabID != arenaNoIdx && t.pool.slabs[t.pool.activeSlabID].liveCount == 0 {
			t.pool.releaseSlab(t.pool.activeSlabID)
		}
		if t.pool.compactSlabID != arenaNoIdx && t.pool.slabs[t.pool.compactSlabID].liveCount == 0 {
			t.pool.releaseSlab(t.pool.compactSlabID)
		}
		if t.pool.liveSlabCount == 0 {
			_ = t.pool.drainPooled()
		}
	} else if t.peakEntries >= 1024 && t.liveEntries < t.peakEntries/4 {
		// Existing nextInBucket collision chains remain valid; copy bucket heads directly
		// in O(len(t.buckets)) instead of scanning historical dead slots in t.entries.
		compacted := make(map[uint64]uint32, len(t.buckets))
		for h, head := range t.buckets {
			compacted[h] = head
		}
		t.buckets = compacted
		t.peakEntries = t.liveEntries
	}
}

func (t *arenaInternTable) onTick() {
	t.curTick++
	t.pool.sealActive(t.curTick, t.compactAgeTicks)

	// Compact sealed slabs aged >= compactAgeTicks with < 25% byte utilization.
	numSlabs := len(t.pool.slabs)
	for sIdx := range numSlabs {
		sl := &t.pool.slabs[sIdx]
		if !sl.inUse || !sl.sealed || sl.dedicated {
			continue
		}
		if sl.liveCount == 0 {
			t.pool.releaseSlab(uint32(sIdx))
			continue
		}
		if t.curTick-sl.sealedTick < t.compactAgeTicks {
			continue
		}
		if uint64(sl.liveBytes)*4 >= uint64(len(sl.buf)) {
			continue
		}

		// Relocate surviving entries verbatim into generational compactSlabID (zero LZ4 decompress/recompress).
		t.compactCount++
		oldBuf := sl.buf
		for id := sl.liveHead; id != arenaNoIdx; {
			e := &t.entries[id]
			nextID := e.nextInSlab
			newSlabID, newOff, err := t.pool.allocCompact(e.storedLen, t.curTick)
			if err != nil {
				// Out of memory; stop compaction safely, leave remaining entries in old slab.
				break
			}
			copy(t.pool.slabs[newSlabID].buf[newOff:newOff+e.storedLen], oldBuf[e.offset:e.offset+e.storedLen])
			t.unlinkFromSlab(id, uint32(sIdx))
			e.slabID = newSlabID
			e.offset = newOff
			t.linkIntoSlab(id, newSlabID)
			id = nextID
		}
		if t.pool.slabs[sIdx].liveCount == 0 {
			t.pool.releaseSlab(uint32(sIdx))
		}
	}

	if t.liveEntries == 0 && t.pool.liveSlabCount == 0 {
		_ = t.pool.drainPooled()
	}
}

func (t *arenaInternTable) close() error {
	t.buckets = nil
	t.entries = nil
	t.freeIDs = nil
	t.liveEntries = 0
	t.peakEntries = 0
	t.liveRawBytes = 0
	t.liveStoredBytes = 0
	t.scratchComp = nil
	t.scratchDecomp = nil
	return t.pool.closeAll()
}

// ---------------------------------------------------------------------------
// arenaPayloadStore: implements ivmPayloadStore + BatchTailStorage + ivmBatchAwareStore
// for all 5 arena modes (rawProtoSlab, internedHeap, internedMmap, lz4Heap, lz4Mmap).
// ---------------------------------------------------------------------------

// arenaChunkMeta holds strictly scalar primitives (0 Go pointers -> noscan, 40 bytes).
type arenaChunkMeta struct {
	slabID     uint32
	offset     uint32
	storedLen  uint32
	rawBodyLen uint32
	nextChunk  uint32
	prevInSlab uint32
	nextInSlab uint32
	resID      uint32
	scopeCount uint32
	flags      uint8
}

type traceChunkList struct {
	head uint32
	tail uint32
}

type arenaPayloadStore struct {
	mu sync.Mutex

	mode            arenaStorageMode
	useIntern       bool
	useMmap         bool
	useLZ4          bool
	closed          bool
	compactAgeTicks uint64
	curTick         uint64

	chunkPool arenaSlabPool
	// Strictly noscan maps/slices (0 Go pointers!).
	traceChunks map[pcommon.TraceID]traceChunkList
	peakTraces  int
	chunks      []arenaChunkMeta
	freeChunks  []uint32
	liveChunks  uint64
	liveBytes   uint64

	resIntern   *arenaInternTable
	scopeIntern *arenaInternTable

	lz4           lz4State
	unmarshaler   ptrace.ProtoUnmarshaler
	scratchHdr    []byte
	scratchSpan   []byte
	scratchBody   []byte
	scratchComp   []byte
	scratchDecomp []byte
	scratchReasm  []byte
	scratchScopes []uint32

	// Lazy per-ResourceSpans pin cache (avoids re-encoding/hashing identical Resource/Scope
	// 128x across traces within the same incoming ResourceSpans batch, and caches raw
	// Resource header bytes once per ResourceSpans for rawProtoSlab mode too).
	batchActive    bool
	batchResID     uint32
	batchScopeIDs  []uint32
	batchRawResHdr []byte
	batchRawValid  bool

	// Diagnostic counters.
	rawBytesIn            uint64
	compressedBytesStored uint64
	compressedChunks      uint64
	uncompressedChunks    uint64
	compactionCount       uint64
	unmarshalCount        uint64
}

var (
	_ ivmPayloadStore                       = (*arenaPayloadStore)(nil)
	_ tailstorageextension.BatchTailStorage = (*arenaPayloadStore)(nil)
	_ ivmBatchAwareStore                    = (*arenaPayloadStore)(nil)
)

func newArenaPayloadStore(mode arenaStorageMode, slabSize int, decisionWaitTicks int) ivmPayloadStore {
	if slabSize <= 0 {
		slabSize = defaultArenaChunkSlabSize
	}
	if decisionWaitTicks <= 0 {
		decisionWaitTicks = 1
	}
	internSlabSize := min(slabSize, defaultArenaInternSlabSize)

	var useIntern, useMmap, useLZ4 bool
	switch mode {
	case arenaModeRawProtoSlab:
		// Raw wire ResourceSpans in noscan heap slabs, no interning, no compression.
	case arenaModeInternedHeap:
		useIntern = true
	case arenaModeInternedMmap:
		useIntern = true
		useMmap = true
	case arenaModeLZ4Heap:
		useIntern = true
		useLZ4 = true
	case arenaModeLZ4Mmap:
		useIntern = true
		useMmap = true
		useLZ4 = true
	default:
		useIntern = true
	}

	s := &arenaPayloadStore{
		mode:            mode,
		useIntern:       useIntern,
		useMmap:         useMmap,
		useLZ4:          useLZ4,
		compactAgeTicks: uint64(decisionWaitTicks),
		chunkPool:       newArenaSlabPool(useMmap, slabSize),
		traceChunks:     make(map[pcommon.TraceID]traceChunkList),
		batchResID:      arenaNoIdx,
	}
	if useIntern {
		s.resIntern = newArenaInternTable(useMmap, internSlabSize, useLZ4, &s.lz4, uint64(decisionWaitTicks))
		s.scopeIntern = newArenaInternTable(useMmap, internSlabSize, false, nil, uint64(decisionWaitTicks))
	}
	return s
}

func (s *arenaPayloadStore) BeginResourceSpans(numScopes int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.releaseBatchPinsLocked()
	s.batchActive = true
	s.batchResID = arenaNoIdx
	s.batchRawValid = false
	s.batchRawResHdr = s.batchRawResHdr[:0]
	if !s.useIntern {
		return
	}
	if cap(s.batchScopeIDs) < numScopes {
		s.batchScopeIDs = make([]uint32, numScopes)
	} else {
		s.batchScopeIDs = s.batchScopeIDs[:numScopes]
	}
	for i := range s.batchScopeIDs {
		s.batchScopeIDs[i] = arenaNoIdx
	}
}

func (s *arenaPayloadStore) FinishResourceSpans() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseBatchPinsLocked()
}

func (s *arenaPayloadStore) releaseBatchPinsLocked() {
	if !s.batchActive {
		return
	}
	s.batchActive = false
	s.batchRawValid = false
	s.batchRawResHdr = s.batchRawResHdr[:0]
	if s.batchResID != arenaNoIdx && s.resIntern != nil {
		s.resIntern.release(s.batchResID)
		s.batchResID = arenaNoIdx
	}
	if s.scopeIntern != nil {
		for i, sid := range s.batchScopeIDs {
			if sid != arenaNoIdx {
				s.scopeIntern.release(sid)
				s.batchScopeIDs[i] = arenaNoIdx
			}
		}
	}
}

// Append implements tailstorageextension.TailStorage for compatibility with non-bypass callers.
func (s *arenaPayloadStore) Append(traceID pcommon.TraceID, td ptrace.Traces) error {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		scopes := rs.ScopeSpans()
		var scopeIndices []int
		var spanIndices [][]int
		for j := 0; j < scopes.Len(); j++ {
			spLen := scopes.At(j).Spans().Len()
			if spLen == 0 {
				continue
			}
			idxs := make([]int, spLen)
			for k := range spLen {
				idxs[k] = k
			}
			scopeIndices = append(scopeIndices, j)
			spanIndices = append(spanIndices, idxs)
		}
		if len(scopeIndices) == 0 {
			continue
		}
		if err := s.AppendGroup(traceID, rs, scopeIndices, spanIndices); err != nil {
			return err
		}
	}
	return nil
}

// AppendGroup encodes and stores all spans for traceID from one incoming ResourceSpans,
// preserving multi-scope grouping so TracesSize and lossless round-trip match inMemoryTailStorage.
func (s *arenaPayloadStore) AppendGroup(traceID pcommon.TraceID, rs ptrace.ResourceSpans, scopeIndices []int, spanIndices [][]int) error {
	if len(scopeIndices) != len(spanIndices) {
		return errArenaMismatchedIndices
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errArenaClosed
	}
	if len(scopeIndices) == 0 {
		return nil
	}
	hasAnySpan := false
	for _, idxs := range spanIndices {
		if len(idxs) > 0 {
			hasAnySpan = true
			break
		}
	}
	if !hasAnySpan {
		return nil
	}

	if !s.useIntern {
		return s.appendGroupRawLocked(traceID, rs, scopeIndices, spanIndices)
	}
	return s.appendGroupInternedLocked(traceID, rs, scopeIndices, spanIndices)
}

func (s *arenaPayloadStore) appendGroupRawLocked(traceID pcommon.TraceID, rs ptrace.ResourceSpans, scopeIndices []int, spanIndices [][]int) error {
	var cachedRes []byte
	if s.batchActive {
		if !s.batchRawValid {
			var err error
			s.batchRawResHdr, err = appendResourceHeader(s.batchRawResHdr[:0], rs)
			if err != nil {
				return err
			}
			s.batchRawValid = true
		}
		cachedRes = s.batchRawResHdr
	}
	raw, err := appendRawResourceSpansGroup(s.scratchBody[:0], rs, cachedRes, scopeIndices, spanIndices)
	if err != nil {
		return err
	}
	s.scratchBody = raw
	if len(raw) == 0 {
		return nil
	}
	if len(raw) > maxArenaChunkSize {
		return errArenaOversizedPayload
	}

	storedLen := uint32(len(raw))
	slabID, offset, err := s.chunkPool.alloc(storedLen, s.curTick)
	if err != nil {
		return err
	}
	copy(s.chunkPool.slabs[slabID].buf[offset:offset+storedLen], raw)

	cIdx := s.allocChunkMetaLocked(arenaChunkMeta{
		slabID:     slabID,
		offset:     offset,
		storedLen:  storedLen,
		rawBodyLen: storedLen,
		nextChunk:  arenaNoIdx,
		prevInSlab: arenaNoIdx,
		nextInSlab: arenaNoIdx,
		resID:      arenaNoIdx,
		scopeCount: 0,
		flags:      chunkFlagLive,
	})
	s.linkChunkIntoSlabLocked(cIdx, slabID)
	s.appendChunkToTraceLocked(traceID, cIdx)

	s.rawBytesIn += uint64(storedLen)
	s.compressedBytesStored += uint64(storedLen)
	s.uncompressedChunks++
	return nil
}

func (s *arenaPayloadStore) appendGroupInternedLocked(traceID pcommon.TraceID, rs ptrace.ResourceSpans, scopeIndices []int, spanIndices [][]int) error {
	// 1. Intern (or reuse pinned batch) Resource header.
	var resID uint32
	if s.batchActive && s.batchResID != arenaNoIdx {
		resID = s.batchResID
		s.resIntern.addRef(resID)
	} else {
		resHdr, err := appendResourceHeader(s.scratchHdr[:0], rs)
		if err != nil {
			return err
		}
		s.scratchHdr = resHdr
		resID, err = s.resIntern.intern(resHdr)
		if err != nil {
			return err
		}
		if s.batchActive {
			// Hold +1 batch pin released in FinishResourceSpans().
			s.batchResID = resID
			s.resIntern.addRef(resID)
		}
	}

	// Rollback helper ensures zero leaked intern refcounts on any mid-append error.
	scopeIDs := s.scratchScopes[:0]
	rollback := func() {
		s.resIntern.release(resID)
		for _, sid := range scopeIDs {
			s.scopeIntern.release(sid)
		}
	}

	// 2. Encode each non-empty scope's header (interned) and span wire bodies.
	body := s.scratchBody[:0]
	scopes := rs.ScopeSpans()
	for pos, sIdx := range scopeIndices {
		spIdxs := spanIndices[pos]
		if len(spIdxs) == 0 {
			continue
		}
		ss := scopes.At(sIdx)
		var sid uint32
		if s.batchActive && sIdx < len(s.batchScopeIDs) && s.batchScopeIDs[sIdx] != arenaNoIdx {
			sid = s.batchScopeIDs[sIdx]
			s.scopeIntern.addRef(sid)
		} else {
			scHdr, err := appendScopeHeader(s.scratchHdr[:0], ss)
			if err != nil {
				rollback()
				return err
			}
			s.scratchHdr = scHdr
			sid, err = s.scopeIntern.intern(scHdr)
			if err != nil {
				rollback()
				return err
			}
			if s.batchActive && sIdx < len(s.batchScopeIDs) {
				s.batchScopeIDs[sIdx] = sid
				s.scopeIntern.addRef(sid)
			}
		}
		scopeIDs = append(scopeIDs, sid)

		body = binary.AppendUvarint(body, uint64(len(spIdxs)))
		spans := ss.Spans()
		for _, k := range spIdxs {
			spBytes, err := appendSpanBody(s.scratchSpan[:0], spans.At(k))
			if err != nil {
				rollback()
				return err
			}
			s.scratchSpan = spBytes
			body = binary.AppendUvarint(body, uint64(len(spBytes)))
			body = append(body, spBytes...)
		}
	}
	s.scratchScopes = scopeIDs
	s.scratchBody = body

	if len(scopeIDs) == 0 {
		rollback()
		return nil
	}

	// 3. Optional zero-dep LZ4 block compression of the span body (prefix scopeIDs stay
	//    uncompressed outside the LZ4 block so Delete(id) releases refs in O(k) without
	//    LZ4 decompression!).
	storedBody := body
	flags := chunkFlagLive
	if s.useLZ4 && len(body) >= lz4MinCompressSize {
		cutoff := (len(body) * 9) / 10
		comp, ok := s.lz4.compressBlock(body, s.scratchComp[:0], cutoff)
		// Retain allocated scratchComp even on bail-out (capped to avoid pinning outliers).
		if cap(comp) <= maxArenaScratchCompCap {
			s.scratchComp = comp[:0]
		}
		if ok {
			storedBody = comp
			flags |= chunkFlagCompressed
		}
	}

	if len(body) > maxArenaChunkSize || len(scopeIDs) > maxArenaChunkSize/4 {
		rollback()
		return errArenaOversizedPayload
	}
	prefixBytes := uint64(len(scopeIDs)) * 4
	totalStoredU64 := prefixBytes + uint64(len(storedBody))
	if totalStoredU64 > uint64(maxArenaChunkSize) {
		rollback()
		return errArenaOversizedPayload
	}
	totalStored := uint32(totalStoredU64)
	slabID, offset, err := s.chunkPool.alloc(totalStored, s.curTick)
	if err != nil {
		rollback()
		return err
	}

	dst := s.chunkPool.slabs[slabID].buf[offset : offset+totalStored]
	for i, sid := range scopeIDs {
		binary.LittleEndian.PutUint32(dst[i*4:], sid)
	}
	copy(dst[int(prefixBytes):], storedBody)

	cIdx := s.allocChunkMetaLocked(arenaChunkMeta{
		slabID:     slabID,
		offset:     offset,
		storedLen:  totalStored,
		rawBodyLen: uint32(len(body)),
		nextChunk:  arenaNoIdx,
		prevInSlab: arenaNoIdx,
		nextInSlab: arenaNoIdx,
		resID:      resID,
		scopeCount: uint32(len(scopeIDs)),
		flags:      flags,
	})
	s.linkChunkIntoSlabLocked(cIdx, slabID)
	s.appendChunkToTraceLocked(traceID, cIdx)

	s.rawBytesIn += prefixBytes + uint64(len(body))
	s.compressedBytesStored += uint64(totalStored)
	if flags&chunkFlagCompressed != 0 {
		s.compressedChunks++
	} else {
		s.uncompressedChunks++
	}
	return nil
}

func (s *arenaPayloadStore) allocChunkMetaLocked(meta arenaChunkMeta) uint32 {
	var idx uint32
	if len(s.freeChunks) > 0 {
		last := len(s.freeChunks) - 1
		idx = s.freeChunks[last]
		s.freeChunks = s.freeChunks[:last]
		s.chunks[idx] = meta
	} else {
		idx = uint32(len(s.chunks))
		s.chunks = append(s.chunks, meta)
	}
	s.liveChunks++
	s.liveBytes += uint64(meta.storedLen)
	return idx
}

func (s *arenaPayloadStore) linkChunkIntoSlabLocked(cIdx, slabID uint32) {
	sl := &s.chunkPool.slabs[slabID]
	c := &s.chunks[cIdx]
	c.prevInSlab = sl.liveTail
	c.nextInSlab = arenaNoIdx
	if sl.liveTail != arenaNoIdx {
		s.chunks[sl.liveTail].nextInSlab = cIdx
	} else {
		sl.liveHead = cIdx
	}
	sl.liveTail = cIdx
	sl.liveCount++
	sl.liveBytes += c.storedLen
}

func (s *arenaPayloadStore) unlinkChunkFromSlabLocked(cIdx, slabID uint32) {
	sl := &s.chunkPool.slabs[slabID]
	c := &s.chunks[cIdx]
	if c.prevInSlab != arenaNoIdx {
		s.chunks[c.prevInSlab].nextInSlab = c.nextInSlab
	} else {
		sl.liveHead = c.nextInSlab
	}
	if c.nextInSlab != arenaNoIdx {
		s.chunks[c.nextInSlab].prevInSlab = c.prevInSlab
	} else {
		sl.liveTail = c.prevInSlab
	}
	c.prevInSlab = arenaNoIdx
	c.nextInSlab = arenaNoIdx
	sl.liveCount--
	sl.liveBytes -= c.storedLen
}

func (s *arenaPayloadStore) appendChunkToTraceLocked(id pcommon.TraceID, cIdx uint32) {
	if list, ok := s.traceChunks[id]; ok {
		s.chunks[list.tail].nextChunk = cIdx
		list.tail = cIdx
		s.traceChunks[id] = list
		return
	}
	s.traceChunks[id] = traceChunkList{head: cIdx, tail: cIdx}
	if len(s.traceChunks) > s.peakTraces {
		s.peakTraces = len(s.traceChunks)
	}
}

func (s *arenaPayloadStore) compactTraceMapLocked() {
	n := len(s.traceChunks)
	if n == 0 {
		if s.peakTraces >= 1024 {
			s.traceChunks = make(map[pcommon.TraceID]traceChunkList)
		}
		s.peakTraces = 0
		if s.liveChunks == 0 && len(s.chunks) >= 4096 {
			s.chunks = nil
			s.freeChunks = nil
		}
		if s.chunkPool.activeSlabID != arenaNoIdx && s.chunkPool.slabs[s.chunkPool.activeSlabID].liveCount == 0 {
			s.chunkPool.releaseSlab(s.chunkPool.activeSlabID)
		}
		if s.chunkPool.compactSlabID != arenaNoIdx && s.chunkPool.slabs[s.chunkPool.compactSlabID].liveCount == 0 {
			s.chunkPool.releaseSlab(s.chunkPool.compactSlabID)
		}
		if s.chunkPool.liveSlabCount == 0 {
			_ = s.chunkPool.drainPooled()
		}
		return
	}
	if s.peakTraces >= 4096 && n < s.peakTraces/4 {
		compacted := make(map[pcommon.TraceID]traceChunkList, n)
		for k, v := range s.traceChunks {
			compacted[k] = v
		}
		s.traceChunks = compacted
		s.peakTraces = n
	}
}

func (s *arenaPayloadStore) freeChunkChainLocked(head uint32) {
	for cIdx := head; cIdx != arenaNoIdx; {
		c := &s.chunks[cIdx]
		next := c.nextChunk
		if c.flags&chunkFlagLive == 0 {
			cIdx = next
			continue
		}
		sl := &s.chunkPool.slabs[c.slabID]
		if s.useIntern {
			// Release Resource refcount + uncompressed Scope IDs prefix WITHOUT LZ4 decompression!
			if c.resID != arenaNoIdx {
				s.resIntern.release(c.resID)
			}
			prefixBytes := uint64(c.scopeCount) * 4
			if prefixBytes <= uint64(c.storedLen) && uint64(c.offset)+prefixBytes <= uint64(len(sl.buf)) {
				prefix := sl.buf[c.offset : uint64(c.offset)+prefixBytes]
				for i := range int(c.scopeCount) {
					sid := binary.LittleEndian.Uint32(prefix[i*4:])
					s.scopeIntern.release(sid)
				}
			}
		}
		slabID := c.slabID
		s.unlinkChunkFromSlabLocked(cIdx, slabID)
		if sl.liveCount == 0 {
			if sl.sealed || sl.dedicated {
				s.chunkPool.releaseSlab(slabID)
			} else {
				sl.used = 0
			}
		}
		s.liveChunks--
		s.liveBytes -= uint64(c.storedLen)
		c.flags = 0
		c.nextChunk = arenaNoIdx
		s.freeChunks = append(s.freeChunks, cIdx)
		cIdx = next
	}
}

// Delete drops traceID in O(chunks * scopes) with ZERO LZ4 decompression and ZERO unmarshaling.
func (s *arenaPayloadStore) Delete(traceID pcommon.TraceID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	list, ok := s.traceChunks[traceID]
	if !ok {
		return nil
	}
	delete(s.traceChunks, traceID)
	s.freeChunkChainLocked(list.head)
	s.compactTraceMapLocked()
	return nil
}

// Take reconstructs valid OTLP TracesData wire bytes for traceID, unmarshals into ptrace.Traces,
// and releases all slabs and intern references (even if decompression or unmarshaling fails).
func (s *arenaPayloadStore) Take(traceID pcommon.TraceID) (ptrace.Traces, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.takeLocked(traceID)
}

func (s *arenaPayloadStore) TakeMany(traceIDs []pcommon.TraceID) ([]ptrace.Traces, []error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ptrace.Traces, len(traceIDs))
	errs := make([]error, len(traceIDs))
	for i, id := range traceIDs {
		out[i], errs[i] = s.takeLocked(id)
	}
	return out, errs
}

func (s *arenaPayloadStore) takeLocked(traceID pcommon.TraceID) (ptrace.Traces, error) {
	if s.closed {
		return ptrace.NewTraces(), errArenaClosed
	}
	list, ok := s.traceChunks[traceID]
	if !ok {
		return ptrace.NewTraces(), nil
	}
	delete(s.traceChunks, traceID)
	// Always free chunks & release intern refcounts, even on corrupt/unmarshal error!
	defer func() {
		s.freeChunkChainLocked(list.head)
		s.compactTraceMapLocked()
	}()

	buf := s.scratchReasm[:0]
	for cIdx := list.head; cIdx != arenaNoIdx; cIdx = s.chunks[cIdx].nextChunk {
		c := &s.chunks[cIdx]
		rawChunk := s.chunkPool.slabs[c.slabID].buf[c.offset : c.offset+c.storedLen]

		if !s.useIntern {
			buf = appendLenTagBytes(buf, 0x0a, rawChunk)
			continue
		}

		// Reassemble ResourceSpans (TracesData field 1 = 0x0a).
		buf = append(buf, 0x0a, 0)
		rsStart := len(buf)
		var err error
		buf, err = s.resIntern.appendBytes(c.resID, buf)
		if err != nil {
			return ptrace.NewTraces(), err
		}

		k := uint64(c.scopeCount)
		prefixLen := k * 4
		if prefixLen > uint64(len(rawChunk)) {
			return ptrace.NewTraces(), errArenaCorruptBody
		}
		prefix := rawChunk[:int(prefixLen)]
		bodyRaw := rawChunk[int(prefixLen):]

		if c.flags&chunkFlagCompressed != 0 {
			bodyRaw, err = lz4DecompressBlock(bodyRaw, s.scratchDecomp[:0], int(c.rawBodyLen))
			if err != nil {
				return ptrace.NewTraces(), err
			}
			s.scratchDecomp = bodyRaw
		}

		pos := 0
		for i := range int(k) {
			sid := binary.LittleEndian.Uint32(prefix[i*4:])
			spanCount, n := binary.Uvarint(bodyRaw[pos:])
			if n <= 0 {
				return ptrace.NewTraces(), errArenaCorruptBody
			}
			pos += n

			buf = append(buf, 0x12, 0)
			ssStart := len(buf)
			buf, err = s.scopeIntern.appendBytes(sid, buf)
			if err != nil {
				return ptrace.NewTraces(), err
			}
			for range spanCount {
				spLen, ln := binary.Uvarint(bodyRaw[pos:])
				if ln <= 0 {
					return ptrace.NewTraces(), errArenaCorruptBody
				}
				pos += ln
				if spLen > uint64(len(bodyRaw)-pos) {
					return ptrace.NewTraces(), fmt.Errorf("%w: span length %d exceeds remaining %d", errArenaCorruptBody, spLen, len(bodyRaw)-pos)
				}
				buf = appendLenTagBytes(buf, 0x12, bodyRaw[pos:pos+int(spLen)])
				pos += int(spLen)
			}
			buf = patchLenPrefix(buf, ssStart)
		}
		if pos != len(bodyRaw) {
			return ptrace.NewTraces(), errArenaCorruptBody
		}
		buf = patchLenPrefix(buf, rsStart)
	}
	s.scratchReasm = buf

	td, err := s.unmarshaler.UnmarshalTraces(buf)
	if err != nil {
		return ptrace.NewTraces(), err
	}
	s.unmarshalCount++
	return td, nil
}

// OnTick seals active chunk and intern slabs and compacts pinned low-utilization (< 25%)
// slabs aged >= decisionWaitTicks.
func (s *arenaPayloadStore) OnTick() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}

	s.curTick++
	s.chunkPool.sealActive(s.curTick, s.compactAgeTicks)

	numSlabs := len(s.chunkPool.slabs)
	for sIdx := range numSlabs {
		sl := &s.chunkPool.slabs[sIdx]
		if !sl.inUse || !sl.sealed || sl.dedicated {
			continue
		}
		if sl.liveCount == 0 {
			s.chunkPool.releaseSlab(uint32(sIdx))
			continue
		}
		if s.curTick-sl.sealedTick < s.compactAgeTicks {
			continue
		}
		if uint64(sl.liveBytes)*4 >= uint64(len(sl.buf)) {
			continue
		}

		// Relocate surviving chunks verbatim into generational compactSlabID (zero LZ4 decompress/recompress).
		s.compactionCount++
		oldBuf := sl.buf
		for cIdx := sl.liveHead; cIdx != arenaNoIdx; {
			c := &s.chunks[cIdx]
			nextIdx := c.nextInSlab
			newSlabID, newOff, err := s.chunkPool.allocCompact(c.storedLen, s.curTick)
			if err != nil {
				break
			}
			copy(s.chunkPool.slabs[newSlabID].buf[newOff:newOff+c.storedLen], oldBuf[c.offset:c.offset+c.storedLen])
			s.unlinkChunkFromSlabLocked(cIdx, uint32(sIdx))
			c.slabID = newSlabID
			c.offset = newOff
			s.linkChunkIntoSlabLocked(cIdx, newSlabID)
			cIdx = nextIdx
		}
		if s.chunkPool.slabs[sIdx].liveCount == 0 {
			s.chunkPool.releaseSlab(uint32(sIdx))
		}
	}

	if s.liveChunks == 0 && s.chunkPool.liveSlabCount == 0 {
		_ = s.chunkPool.drainPooled()
	}

	if s.useIntern {
		s.resIntern.onTick()
		s.scopeIntern.onTick()
	}
}

func (s *arenaPayloadStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.releaseBatchPinsLocked()
	s.closed = true

	var firstErr error
	if err := s.chunkPool.closeAll(); err != nil && firstErr == nil {
		firstErr = err
	}
	if s.resIntern != nil {
		if err := s.resIntern.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.scopeIntern != nil {
		if err := s.scopeIntern.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.traceChunks = nil
	s.peakTraces = 0
	s.chunks = nil
	s.freeChunks = nil
	s.liveChunks = 0
	s.liveBytes = 0
	s.scratchHdr = nil
	s.scratchSpan = nil
	s.scratchBody = nil
	s.scratchComp = nil
	s.scratchDecomp = nil
	s.scratchReasm = nil
	s.scratchScopes = nil
	s.batchScopeIDs = nil
	s.batchRawResHdr = nil
	s.batchRawValid = false
	return firstErr
}

func (s *arenaPayloadStore) Stats() ArenaStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := ArenaStats{
		LiveSlabs:             s.chunkPool.liveSlabCount,
		PooledSlabs:           len(s.chunkPool.pooledSlabs),
		TotalAllocatedBytes:   s.chunkPool.totalAllocBytes,
		LivePayloadBytes:      s.liveBytes,
		LiveChunks:            s.liveChunks,
		MmapActiveBytes:       s.chunkPool.mmapActiveBytes,
		RawBytesIn:            s.rawBytesIn,
		CompressedBytesStored: s.compressedBytesStored,
		CompressedChunks:      s.compressedChunks,
		UncompressedChunks:    s.uncompressedChunks,
		CompactionCount:       s.compactionCount,
		UnmarshalCount:        s.unmarshalCount,
	}
	if s.resIntern != nil {
		st.ResourceInternEntries = s.resIntern.liveEntries
		st.ResourceInternPeak = s.resIntern.peakEntries
		st.ResourceInternBytes = s.resIntern.liveStoredBytes
		st.InternAllocatedBytes += s.resIntern.pool.totalAllocBytes
		st.MmapActiveBytes += s.resIntern.pool.mmapActiveBytes
		st.LiveSlabs += s.resIntern.pool.liveSlabCount
		st.PooledSlabs += len(s.resIntern.pool.pooledSlabs)
		st.CompactionCount += s.resIntern.compactCount
	}
	if s.scopeIntern != nil {
		st.ScopeInternEntries = s.scopeIntern.liveEntries
		st.ScopeInternPeak = s.scopeIntern.peakEntries
		st.ScopeInternBytes = s.scopeIntern.liveStoredBytes
		st.InternAllocatedBytes += s.scopeIntern.pool.totalAllocBytes
		st.MmapActiveBytes += s.scopeIntern.pool.mmapActiveBytes
		st.LiveSlabs += s.scopeIntern.pool.liveSlabCount
		st.PooledSlabs += len(s.scopeIntern.pool.pooledSlabs)
		st.CompactionCount += s.scopeIntern.compactCount
	}
	return st
}
