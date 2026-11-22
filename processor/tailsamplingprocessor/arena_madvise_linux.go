// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build linux

package tailsamplingprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"

import "syscall"

const arenaSupportsMadvDontNeed = true

func arenaMmapDontNeed(b []byte) {
	if len(b) == 0 {
		return
	}
	_ = syscall.Madvise(b, syscall.MADV_DONTNEED)
}
