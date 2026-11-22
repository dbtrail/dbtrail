package reconstruct

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/pgdelta"
	"github.com/dbtrail/dbtrail/internal/query"
)

func TestPGTableDeltaOn(t *testing.T) {
	for raw, want := range map[string]bool{
		"0": false, "false": false, "No": false, " OFF ": false,
		"": true, " ": true, "1": true, "true": true, "YES": true, " on ": true,
	} {
		on, warning := pgTableDeltaOn(raw)
		if on != want || warning != "" {
			t.Errorf("%q: on %v warning %q, want %v and none", raw, on, warning, want)
		}
	}
	// A value that is neither spelling is the default, on, and says so.
	for _, raw := range []string{"2", "enabled", "si"} {
		if on, warning := pgTableDeltaOn(raw); !on || warning == "" {
			t.Errorf("%q: on %v warning %q, want on with a warning", raw, on, warning)
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
		name  string
		off   bool
		cfg   FullTableConfig
		src   string
		meta  baseline.DumpMetadata
		since time.Duration // the file's bound, before the run
		fold  *foldResult
		gap   *CaptureGap
		want  bool
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

// A decline after a write that left files removes them: the merge is about
// to write the table's file, and half a pair beside it is a broken chain.
func TestRemovePGPair(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "orders.parquet")
	dead, rows, err := pgdelta.Paths(dst, 0)
	if err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(dir, "items.000000.pgnew")
	for _, f := range []string{dead, rows, other, dst} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	removePGPair(dst)
	for f, want := range map[string]bool{dead: false, rows: false, other: true, dst: true} {
		if _, err := os.Stat(f); (err == nil) != want {
			t.Errorf("%s: there %v, want %v", filepath.Base(f), err == nil, want)
		}
	}
}

// pgPairBeside lists nothing for a table with no pair, whatever else is in
// the folder, and names a whole pair; half a pair is an error.
func TestPGPairBeside(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "orders.parquet")
	touch := func(names ...string) {
		t.Helper()
		for _, n := range names {
			if err := os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Other tables' files, one with a backslash in its name, another table's
	// pair, and a file still being written.
	touch("orders.parquet", `a\b.parquet`, "items.parquet", "items.000000.pgdead", "items.000000.pgnew", "orders.000000.pgnew.tmp-1")
	if got, err := pgPairBeside(base); err != nil || len(got) != 0 {
		t.Fatalf("no pair: %v, %v", got, err)
	}
	// The folder is not even listed: unreadable, the answer is the same.
	if got, err := pgPairBeside(filepath.Join(dir, "missing", "orders.parquet")); err != nil || len(got) != 0 {
		t.Fatalf("a folder that is not there: %v, %v", got, err)
	}
	touch("orders.000000.pgnew")
	if _, err := pgPairBeside(base); !errors.Is(err, pgdelta.ErrBrokenChain) || !strings.Contains(err.Error(), "new full snapshot") {
		t.Fatalf("half a pair: %v", err)
	}
	touch("orders.000000.pgdead")
	got, err := pgPairBeside(base)
	if err != nil || len(got) != 2 || got[0] != "orders.000000.pgdead" || got[1] != "orders.000000.pgnew" {
		t.Fatalf("a whole pair: %v, %v", got, err)
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
