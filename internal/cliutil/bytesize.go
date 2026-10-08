package cliutil

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ParseByteSize parses a human-readable byte size string like "256MB" or "1GB".
// Plain integers are treated as bytes. Returns 0 for "0" (unlimited). Shared by
// the agent (--buffer-max-bytes) and reconstruct (--chunk-size) commands.
func ParseByteSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return 0, nil
	}

	original := s
	s = strings.ToUpper(s)

	multiplier := int64(1)
	switch {
	// The IEC spellings (KiB, MiB, GiB), which DuckDB's memory_limit takes:
	// the same binary units as the short ones here.
	case strings.HasSuffix(s, "GIB"):
		multiplier = 1 << 30
		s = strings.TrimSuffix(s, "GIB")
	case strings.HasSuffix(s, "MIB"):
		multiplier = 1 << 20
		s = strings.TrimSuffix(s, "MIB")
	case strings.HasSuffix(s, "KIB"):
		multiplier = 1 << 10
		s = strings.TrimSuffix(s, "KIB")
	case strings.HasSuffix(s, "GB"):
		multiplier = 1 << 30
		s = strings.TrimSuffix(s, "GB")
	case strings.HasSuffix(s, "MB"):
		multiplier = 1 << 20
		s = strings.TrimSuffix(s, "MB")
	case strings.HasSuffix(s, "KB"):
		multiplier = 1 << 10
		s = strings.TrimSuffix(s, "KB")
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid byte size %q; expected a number with optional KB/MB/GB (or KiB/MiB/GiB) suffix, e.g. 256MB", original)
	}
	if n > math.MaxInt64/multiplier {
		return 0, fmt.Errorf("byte size %q overflows int64", original)
	}
	return n * multiplier, nil
}
