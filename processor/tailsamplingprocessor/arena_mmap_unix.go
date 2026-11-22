// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package tailsamplingprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"

import (
	"errors"
	"math"
	"os"
	"syscall"
)

const arenaMmapSupported = true

var errArenaInvalidMmapSize = errors.New("arena mmap: invalid allocation size")

func arenaAlignMmapSize(size int) (int, error) {
	if size <= 0 {
		return 0, errArenaInvalidMmapSize
	}
	ps := os.Getpagesize()
	if ps <= 0 {
		ps = 4096
	}
	if size > math.MaxInt-(ps-1) {
		return 0, errArenaInvalidMmapSize
	}
	return ((size + ps - 1) / ps) * ps, nil
}

func arenaMmapAlloc(size int) ([]byte, error) {
	aligned, err := arenaAlignMmapSize(size)
	if err != nil {
		return nil, err
	}
	return syscall.Mmap(-1, 0, aligned, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_PRIVATE|syscall.MAP_ANON)
}

func arenaMmapFree(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	return syscall.Munmap(b)
}
