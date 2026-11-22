// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build !linux && !darwin

package tailsamplingprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"

import "errors"

const arenaMmapSupported = false

var errArenaInvalidMmapSize = errors.New("arena mmap: invalid allocation size")

func arenaAlignMmapSize(size int) (int, error) {
	if size <= 0 {
		return 0, errArenaInvalidMmapSize
	}
	return size, nil
}

func arenaMmapAlloc(size int) ([]byte, error) {
	if size <= 0 {
		return nil, errArenaInvalidMmapSize
	}
	return make([]byte, size), nil
}

func arenaMmapFree(_ []byte) error { return nil }
