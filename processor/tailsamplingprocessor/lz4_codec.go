// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tailsamplingprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"

import (
	"encoding/binary"
	"errors"
	"math"
)

const (
	lz4MinMatch     = 4
	lz4LastLiterals = 5
	lz4MFLimit      = 12
	lz4MaxDist      = 65535
	lz4HashLog      = 12
	lz4HashSize     = 1 << lz4HashLog
	lz4HashShift    = 32 - lz4HashLog
	lz4HashPrime    = 2654435761

	// lz4MinCompressSize avoids compression overhead on tiny payloads.
	lz4MinCompressSize = 128
)

var (
	errLZ4Corrupt = errors.New("lz4: corrupt or truncated block")
	errLZ4Size    = errors.New("lz4: decompressed length mismatch")
)

// lz4State holds a reusable, pointer-free (noscan) generation-stamped hash table for
// zero-allocation LZ4 block compression across repeated small trace chunks.
// Incrementing base avoids clearing the 16 KiB hash table on every chunk!
type lz4State struct {
	table [lz4HashSize]uint32
	base  uint32
}

func (s *lz4State) initBase(srcLen int) uint32 {
	need := uint32(srcLen) + 1
	if s.base == 0 || math.MaxUint32-s.base <= need {
		clear(s.table[:])
		s.base = 1
	}
	b := s.base
	s.base += need
	return b
}

// compressBlock compresses src in standard LZ4 block format into dst[:0], reusing dst's
// backing array. Returns (compressed, true) ONLY if compressed length < maxOut.
// When maxOut <= 0, no savings cutoff is enforced (up to len(src)+1).
func (s *lz4State) compressBlock(src, dst []byte, maxOut int) ([]byte, bool) {
	n := len(src)
	if n < lz4MFLimit || n > math.MaxInt32 {
		return dst[:0], false
	}
	if maxOut <= 0 {
		maxOut = n
	}

	base := s.initBase(n)
	dst = dst[:0]
	if cap(dst) < maxOut+16 {
		dst = make([]byte, 0, maxOut+16)
	}

	matchLimit := n - lz4LastLiterals
	mflimit := n - lz4MFLimit
	anchor := 0
	pos := 0

	// Insert first position hash and start scanning at pos = 1 (LZ4 rule).
	h0 := (binary.LittleEndian.Uint32(src[0:]) * lz4HashPrime) >> lz4HashShift
	s.table[h0] = base
	pos = 1

	for pos <= mflimit {
		var cand int
		step := 1
		searchMatchNb := 1 << 6
		found := false

		for {
			cur := binary.LittleEndian.Uint32(src[pos:])
			h := (cur * lz4HashPrime) >> lz4HashShift
			ref := s.table[h]
			s.table[h] = base + uint32(pos)

			if ref >= base {
				cand = int(ref - base)
				dist := pos - cand
				if dist > 0 && dist <= lz4MaxDist && binary.LittleEndian.Uint32(src[cand:]) == cur {
					found = true
					break
				}
			}

			nextPos := pos + step
			searchMatchNb++
			step = searchMatchNb >> 6
			if nextPos > mflimit {
				break
			}
			pos = nextPos
		}

		if !found {
			break
		}

		// Extend match backward if possible.
		for pos > anchor && cand > 0 && src[pos-1] == src[cand-1] {
			pos--
			cand--
		}

		// Extend match forward up to matchLimit.
		matchLen := lz4MinMatch
		for pos+matchLen < matchLimit && src[cand+matchLen] == src[pos+matchLen] {
			matchLen++
		}

		litLen := pos - anchor
		mlCode := matchLen - lz4MinMatch

		// Early bail-out if output will exceed maxOut (leaving room for remaining tail literals).
		if len(dst)+1+litLen+2+(litLen/255)+(mlCode/255) >= maxOut {
			return dst[:0], false
		}

		var token byte
		if litLen >= 15 {
			token = 0xf0
		} else {
			token = byte(litLen) << 4
		}
		if mlCode >= 15 {
			token |= 0x0f
		} else {
			token |= byte(mlCode)
		}
		dst = append(dst, token)

		if litLen >= 15 {
			rem := litLen - 15
			for rem >= 255 {
				dst = append(dst, 255)
				rem -= 255
			}
			dst = append(dst, byte(rem))
		}

		dst = append(dst, src[anchor:pos]...)

		offset := uint16(pos - cand)
		dst = append(dst, byte(offset), byte(offset>>8))

		if mlCode >= 15 {
			rem := mlCode - 15
			for rem >= 255 {
				dst = append(dst, 255)
				rem -= 255
			}
			dst = append(dst, byte(rem))
		}

		pos += matchLen
		anchor = pos

		if pos > mflimit {
			break
		}
		// Seed hash table with the position immediately preceding the new anchor.
		prev := pos - 2
		hp := (binary.LittleEndian.Uint32(src[prev:]) * lz4HashPrime) >> lz4HashShift
		s.table[hp] = base + uint32(prev)
	}

	// Emit trailing literals.
	lastLit := n - anchor
	if len(dst)+1+lastLit+(lastLit/255) >= maxOut {
		return dst[:0], false
	}
	if lastLit >= 15 {
		dst = append(dst, 0xf0)
		rem := lastLit - 15
		for rem >= 255 {
			dst = append(dst, 255)
			rem -= 255
		}
		dst = append(dst, byte(rem))
	} else {
		dst = append(dst, byte(lastLit)<<4)
	}
	dst = append(dst, src[anchor:]...)

	if len(dst) >= maxOut {
		return dst[:0], false
	}
	return dst, true
}

// lz4DecompressBlock decompresses a standard LZ4 block into dst[:0], validating exact origSize
// and enforcing strict bounds checks on every token, length extension, and match offset.
func lz4DecompressBlock(src, dst []byte, origSize int) ([]byte, error) {
	if origSize < 0 {
		return nil, errLZ4Corrupt
	}
	if cap(dst) < origSize {
		dst = make([]byte, 0, origSize)
	} else {
		dst = dst[:0]
	}
	if origSize == 0 {
		if len(src) != 0 {
			return nil, errLZ4Corrupt
		}
		return dst, nil
	}

	si := 0
	sn := len(src)
	for si < sn {
		token := src[si]
		si++

		// Literal length.
		litLen := int(token >> 4)
		if litLen == 15 {
			for {
				if si >= sn {
					return nil, errLZ4Corrupt
				}
				b := src[si]
				si++
				litLen += int(b)
				if litLen > origSize-len(dst) {
					return nil, errLZ4Corrupt
				}
				if b != 255 {
					break
				}
			}
		}
		if litLen > sn-si || litLen > origSize-len(dst) {
			return nil, errLZ4Corrupt
		}
		dst = append(dst, src[si:si+litLen]...)
		si += litLen

		if si == sn {
			// Last sequence contains literals only (standard LZ4 requires low nibble == 0).
			if token&0x0f != 0 {
				return nil, errLZ4Corrupt
			}
			break
		}

		// Match offset (16-bit little-endian).
		if sn-si < 2 {
			return nil, errLZ4Corrupt
		}
		offset := int(src[si]) | (int(src[si+1]) << 8)
		si += 2
		if offset == 0 || offset > len(dst) {
			return nil, errLZ4Corrupt
		}

		// Match length.
		matchLen := int(token&0x0f) + lz4MinMatch
		if token&0x0f == 15 {
			for {
				if si >= sn {
					return nil, errLZ4Corrupt
				}
				b := src[si]
				si++
				matchLen += int(b)
				if matchLen > origSize-len(dst) {
					return nil, errLZ4Corrupt
				}
				if b != 255 {
					break
				}
			}
		}
		if matchLen > origSize-len(dst) {
			return nil, errLZ4Corrupt
		}

		start := len(dst) - offset
		if offset >= matchLen {
			dst = append(dst, dst[start:start+matchLen]...)
		} else {
			// Overlapping copy (RLE / periodic pattern): copy byte-by-byte.
			dst = dst[:len(dst)+matchLen]
			for i := range matchLen {
				dst[start+offset+i] = dst[start+i]
			}
		}
	}

	if len(dst) != origSize {
		return nil, errLZ4Size
	}
	return dst, nil
}
