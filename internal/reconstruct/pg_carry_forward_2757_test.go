package reconstruct

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

func TestPGCarryForwardEligible(t *testing.T) {
	at := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	recorded := baseline.DumpMetadata{LSN: 0x500, PGDeltaSince: at.Add(-time.Hour).Format(time.RFC3339)}
	on := FullTableConfig{CarryForwardUnchanged: true, OutputFormat: OutputFormatParquet, At: at}
	with := func(f func(*FullTableConfig)) FullTableConfig { c := on; f(&c); return c }
	local := "/data/2026-11-15T11-00-00Z/public/orders.parquet"
	cases := []struct {
		name     string
		cfg      FullTableConfig
		src      string
		meta     baseline.DumpMetadata
		since    time.Duration // the file's bound, before the run
		newSince time.Duration // the bound a rewrite would record, before the run
		changes  int
		gap      *CaptureGap
		want     bool
	}{
		{name: "unchanged, bound recorded", cfg: on, src: local, meta: recorded, since: time.Hour, newSince: time.Minute, want: true},
		{name: "off", cfg: with(func(c *FullTableConfig) { c.CarryForwardUnchanged = false }), src: local, meta: recorded, since: time.Hour, newSince: time.Minute},
		{name: "psql output", cfg: with(func(c *FullTableConfig) { c.OutputFormat = OutputFormatPsql }), src: local, meta: recorded, since: time.Hour, newSince: time.Minute},
		{name: "read from a bucket", cfg: on, src: "s3://b/2026-11-15T11-00-00Z/public/orders.parquet", meta: recorded, since: time.Hour, newSince: time.Minute},
		{name: "a change", cfg: on, src: local, meta: recorded, since: time.Hour, newSince: time.Minute, changes: 1},
		{name: "a known capture gap", cfg: on, src: local, meta: recorded, since: time.Hour, newSince: time.Minute, gap: &CaptureGap{}},
		// A full read's file: its reader measures a margin from the
		// snapshot's time, which a later snapshot would move.
		{name: "no bound in the footer", cfg: on, src: local, meta: baseline.DumpMetadata{LSN: 0x500}, since: time.Hour, newSince: time.Minute},
		{name: "bound right after the line", cfg: with(func(c *FullTableConfig) { c.ChainStartFloor = at.Add(-time.Hour - time.Second) }),
			src: local, meta: recorded, since: time.Hour, newSince: time.Minute, want: true},
		{name: "bound on the line", cfg: with(func(c *FullTableConfig) { c.ChainStartFloor = at.Add(-time.Hour) }),
			src: local, meta: recorded, since: time.Hour, newSince: time.Minute},
		// A source with no change at all: the rewrite would record the same
		// bound.
		{name: "bound past the line, no newer one to record", cfg: with(func(c *FullTableConfig) { c.ChainStartFloor = at.Add(-time.Minute) }),
			src: local, meta: recorded, since: time.Hour, newSince: time.Hour, want: true},
		{name: "no line, under a day", cfg: on, src: local, meta: recorded, since: 24*time.Hour - time.Second, newSince: time.Minute, want: true},
		{name: "no line, a day", cfg: on, src: local, meta: recorded, since: 24 * time.Hour, newSince: time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := pgCarryForwardEligible(tc.cfg, tc.src, tc.meta, at.Add(-tc.since), at.Add(-tc.newSince), tc.changes, tc.gap)
			if got != tc.want {
				t.Fatalf("carried = %v, want %v", got, tc.want)
			}
		})
	}
}
