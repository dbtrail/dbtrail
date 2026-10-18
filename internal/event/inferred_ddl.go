package event

import (
	"strings"
	"time"
)

// InferredGapUncaptured is the marker's gap value for a table added back to
// the publication.
const InferredGapUncaptured = "uncaptured"

// InferredBracket is the window an inferred schema change (#2441) committed
// in, read back from its ddl_query marker: after Low (when HasLow) and at or
// before High.
type InferredBracket struct {
	Low    time.Time
	HasLow bool
	High   time.Time
	// Gap marks a table added back to the publication: changes made to it
	// while it was out, inside the bracket, were never captured.
	Gap bool
}

// ParseInferredBracket reads the bracket from a ddl_query that starts with
// InferredDDLPrefix. ok is false for any other text, and for a marker whose
// bounds do not parse: a caller must then treat the row as it treats any
// other, never as having no bracket.
func ParseInferredBracket(q string) (b InferredBracket, ok bool) {
	if !strings.HasPrefix(q, InferredDDLPrefix) {
		return InferredBracket{}, false
	}
	end := strings.Index(q, "*/")
	if end < 0 {
		return InferredBracket{}, false
	}
	var low, high string
	gap := false
	for f := range strings.SplitSeq(q[len(InferredDDLPrefix):end], ";") {
		k, v, _ := strings.Cut(strings.TrimSpace(f), "=")
		switch k {
		case "low":
			low = v
		case "high":
			high = v
		case "gap":
			gap = v == InferredGapUncaptured
		}
	}
	at := func(v string) (time.Time, bool) {
		_, ts, found := strings.Cut(v, "@")
		if !found {
			return time.Time{}, false
		}
		t, err := time.Parse(time.RFC3339, strings.TrimSpace(ts))
		return t, err == nil
	}
	h, ok := at(high)
	if !ok {
		return InferredBracket{}, false
	}
	b.High, b.Gap = h, gap
	if low == "none" {
		return b, true
	}
	if b.Low, b.HasLow = at(low); !b.HasLow {
		return InferredBracket{}, false
	}
	return b, true
}
