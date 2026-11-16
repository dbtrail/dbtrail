package reconstruct

import (
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

func TestPGTableDeltaOn(t *testing.T) {
	for raw, want := range map[string]bool{
		"": false, " ": false, "0": false, "false": false, "No": false, " OFF ": false,
		"1": true, "true": true, "YES": true, " on ": true,
	} {
		on, warning := pgTableDeltaOn(raw)
		if on != want || warning != "" {
			t.Errorf("%q: on %v warning %q, want %v and none", raw, on, warning, want)
		}
	}
	// A value that is neither spelling stays off, and says so.
	for _, raw := range []string{"2", "enabled", "si"} {
		if on, warning := pgTableDeltaOn(raw); on || warning == "" {
			t.Errorf("%q: on %v warning %q, want off with a warning", raw, on, warning)
		}
	}
}

func TestPGPairEligible(t *testing.T) {
	at := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	recorded := baseline.DumpMetadata{LSN: 0x500, PGDeltaSince: at.Add(-time.Hour).Format(time.RFC3339)}
	lined := FullTableConfig{CarryForwardUnchanged: true, OutputFormat: OutputFormatParquet, At: at, ChainStartFloor: at.Add(-2 * time.Hour)}
	with := func(f func(*FullTableConfig)) FullTableConfig { c := lined; f(&c); return c }
	local := "/data/2026-11-16T11-00-00Z/public/orders.parquet"
	changed := func() *foldResult { return &foldResult{Changes: map[string]*query.ResultRow{"1": {}}} }
	cases := []struct {
		name   string
		off    bool
		cfg    FullTableConfig
		src    string
		meta   baseline.DumpMetadata
		since  time.Duration // the file's bound, before the run
		fold   *foldResult
		gap    *CaptureGap
		want   bool
	}{
		{name: "changed, bound recorded and young", cfg: lined, src: local, meta: recorded, since: time.Hour, fold: changed(), want: true},
		{name: "the switch is off", off: true, cfg: lined, src: local, meta: recorded, since: time.Hour, fold: changed()},
		{name: "nothing changed: that is the carry's", cfg: lined, src: local, meta: recorded, since: time.Hour, fold: &foldResult{Changes: map[string]*query.ResultRow{}}},
		{name: "no fold", cfg: lined, src: local, meta: recorded, since: time.Hour},
		{name: "the changes spilled to disk", cfg: lined, src: local, meta: recorded, since: time.Hour, fold: &foldResult{Changes: map[string]*query.ResultRow{}, Spill: &changeSpill{records: 3}}},
		{name: "carrying is off", cfg: with(func(c *FullTableConfig) { c.CarryForwardUnchanged = false }), src: local, meta: recorded, since: time.Hour, fold: changed()},
		{name: "psql output", cfg: with(func(c *FullTableConfig) { c.OutputFormat = OutputFormatPsql }), src: local, meta: recorded, since: time.Hour, fold: changed()},
		{name: "mydumper output", cfg: with(func(c *FullTableConfig) { c.OutputFormat = OutputFormatMydumper }), src: local, meta: recorded, since: time.Hour, fold: changed()},
		{name: "the file is in S3", cfg: lined, src: "s3://b/2026-11-16T11-00-00Z/public/orders.parquet", meta: recorded, since: time.Hour, fold: changed()},
		{name: "a capture gap", cfg: lined, src: local, meta: recorded, since: time.Hour, fold: changed(), gap: &CaptureGap{}},
		{name: "the file records no bound (a full read's)", cfg: lined, src: local, meta: baseline.DumpMetadata{LSN: 0x500}, since: time.Hour, fold: changed()},
		{name: "no line", cfg: with(func(c *FullTableConfig) { c.ChainStartFloor = time.Time{} }), src: local, meta: recorded, since: time.Hour, fold: changed()},
		{name: "the bound is at the line", cfg: lined, src: local, meta: recorded, since: 2 * time.Hour, fold: changed()},
		{name: "the bound is before the line", cfg: lined, src: local, meta: recorded, since: 3 * time.Hour, fold: changed()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			prev := pgTableDeltas
			pgTableDeltas = func() bool { return !c.off }
			defer func() { pgTableDeltas = prev }()
			if got := pgPairEligible(c.cfg, c.src, c.meta, at.Add(-c.since), c.fold, c.gap); got != c.want {
				t.Fatalf("pgPairEligible = %v, want %v", got, c.want)
			}
		})
	}
}

// Off unless the variable says so: the default of a process that sets
// nothing.
func TestPGTableDeltasOffByDefault(t *testing.T) {
	if on, _ := pgTableDeltaOn(""); on {
		t.Fatal("PostgreSQL table deltas are on with the variable unset")
	}
}

func TestPGPairRun(t *testing.T) {
	at := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
	a, err := pgPairRun(at)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := pgPairRun(at)
	if a == b || len(a) != len("20261116T120000Z-")+16 {
		t.Fatalf("runs %q and %q: want two different names of one shape", a, b)
	}
}
