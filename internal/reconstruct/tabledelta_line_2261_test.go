package reconstruct

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// Which line a table's chain is held to (#2261): the resolved one only for a
// table with a chain the daemon answers for, and only when that line is set
// above the ordinary one.
func TestChainUpsertsLine(t *testing.T) {
	chain := &baseline.TableDeltaChain{Files: []baseline.TableDeltaFile{{Seq: 0}, {Seq: 1}}}
	yes := func(string, *baseline.TableDeltaChain) bool { return true }
	no := func(string, *baseline.TableDeltaChain) bool { return false }
	for _, c := range []struct {
		name          string
		prev          *tableDelta
		max, resolved int64
		ask           func(string, *baseline.TableDeltaChain) bool
		want          int64
		wantResolved  bool
	}{
		{"a table with its pair", &tableDelta{Chain: chain}, 24, 192, yes, 192, true},
		{"a table without it", &tableDelta{Chain: chain}, 24, 192, no, 24, false},
		{"nobody to ask", &tableDelta{Chain: chain}, 24, 192, nil, 24, false},
		{"no chain yet", nil, 24, 192, yes, 24, false},
		{"no resolved line", &tableDelta{Chain: chain}, 24, 0, yes, 24, false},
		{"a resolved line under the ordinary one", &tableDelta{Chain: chain}, 24, 23, yes, 24, false},
		{"a resolved line equal to the ordinary one", &tableDelta{Chain: chain}, 24, 24, yes, 24, false},
		{"no ordinary line: no rule at all", &tableDelta{Chain: chain}, 0, 192, yes, 0, false},
	} {
		p := tableDeltaPublish{basePath: "/b/s/t.parquet", prev: c.prev,
			cfg: FullTableConfig{MaxChainUpserts: c.max, MaxResolvedChainUpserts: c.resolved, ChainResolved: c.ask}}
		got, resolved := chainUpsertsLine(p)
		if got != c.want || resolved != c.wantResolved {
			t.Errorf("%s: line = %d resolved = %v, want %d %v", c.name, got, resolved, c.want, c.wantResolved)
		}
	}
}

// The daemon is asked about the PREVIOUS snapshot's file and chain: that is
// the chain whose pair exists by the time this window is published.
func TestChainUpsertsLine_asksAboutThePreviousChain(t *testing.T) {
	chain := &baseline.TableDeltaChain{Files: []baseline.TableDeltaFile{{Seq: 0}, {Seq: 1}}}
	var gotBase string
	var gotChain *baseline.TableDeltaChain
	p := tableDeltaPublish{basePath: "/b/prev/s/t.parquet", prev: &tableDelta{Chain: chain},
		cfg: FullTableConfig{MaxChainUpserts: 1, MaxResolvedChainUpserts: 2, ChainResolved: func(b string, c *baseline.TableDeltaChain) bool {
			gotBase, gotChain = b, c
			return true
		}}}
	chainUpsertsLine(p)
	if gotBase != "/b/prev/s/t.parquet" || gotChain != chain {
		t.Fatalf("asked about %q %p, want the previous base and its chain %p", gotBase, gotChain, chain)
	}
}

// Through real windows: a table read through its resolved pair goes past the
// ordinary line and is ended at its own; the same windows with no pair end
// at the ordinary line, as before.
func TestTableDelta_resolvedTableGoesPastTheLine(t *testing.T) {
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	noCompaction(t)
	noSizeFloor(t)

	run := func(resolved bool) (ended []int, reasons []string) {
		root := t.TempDir()
		at1 := t0.Add(5 * time.Minute)
		base1, _, err := deltaWindow(t, root, src, t0, changeMap(upd(1, "v1")), at1, &query.BinlogPos{File: "binlog.000009", Pos: 1000}, nil)
		if err != nil {
			t.Fatal(err)
		}
		bmeta, _ := baseline.ReadParquetMetadata(base1)
		d, err := readTableDelta(context.Background(), base1, bmeta)
		if err != nil || d == nil {
			t.Fatalf("d=%v err=%v", d, err)
		}
		ui, err := os.Stat(d.Chain.Last().Upserts)
		if err != nil {
			t.Fatal(err)
		}
		// The ordinary line: two pairs under it, three over. The resolved
		// one: six under it, seven over.
		line, resolvedLine := 5*ui.Size()/2, 13*ui.Size()/2
		base, prevTime := base1, at1
		for i := 2; i <= 8; i++ {
			at := t0.Add(time.Duration(i) * 5 * time.Minute)
			nb, rep, err := deltaWindow(t, root, base, prevTime, changeMap(upd(1, fmt.Sprintf("v%d", i))), at,
				&query.BinlogPos{File: "binlog.000009", Pos: uint64(1000 * i)}, func(p *tableDeltaPublish) {
					p.cfg.MaxChainUpserts, p.cfg.MaxResolvedChainUpserts = line, resolvedLine
					p.cfg.ChainResolved = func(string, *baseline.TableDeltaChain) bool { return resolved }
				})
			if err != nil {
				t.Fatal(err)
			}
			if rep.DeltaCompacted != "" {
				ended, reasons = append(ended, i), append(reasons, rep.DeltaCompacted)
			}
			base, prevTime = nb, at
		}
		return ended, reasons
	}

	// Without a pair: window 4 finds three pairs and ends the chain, and
	// window 7 finds the new chain's three files past the line again.
	if ended, reasons := run(false); fmt.Sprint(ended) != "[4 7]" {
		t.Fatalf("no pair: chain ended at windows %v (%q), want [4 7]", ended, reasons)
	}
	ended, reasons := run(true)
	if fmt.Sprint(ended) != "[8]" {
		t.Fatalf("with a pair: chain ended at windows %v (%q), want [8]: seven pairs pass the resolved line", ended, reasons)
	}
	if !strings.Contains(reasons[0], "the most a refresh leaves beside a table for SQL on the copy") {
		t.Fatalf("with a pair: reason %q does not name the line", reasons[0])
	}
}

// A table read through its pair is still ended by the share rule: that is
// the rule that looks at the table's size.
func TestTableDelta_resolvedTableStillEndsAtTheShareOfTheTable(t *testing.T) {
	noSizeFloor(t)
	at := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	chain := &baseline.TableDeltaChain{Files: []baseline.TableDeltaFile{{Seq: 0}, {Seq: 1}}}
	prev := &tableDelta{Chain: chain, Meta: baseline.DumpMetadata{DeltaChainStart: at.Add(-time.Hour)}, PairSize: 501, ChainUpsertsSize: 400}
	p := tableDeltaPublish{basePath: "/b/t.parquet", prev: prev, cfg: FullTableConfig{MaxChainUpserts: 100, MaxResolvedChainUpserts: 800,
		ChainResolved: func(string, *baseline.TableDeltaChain) bool { return true }}}
	line, _ := chainUpsertsLine(p)
	if got := tableDeltaCompactReason(prev, p.basePath, 1000, nil, at, true, "", time.Time{}, time.Time{}, line); !strings.Contains(got, "passed 50% of the table") {
		t.Fatalf("501 bytes beside a table of 1000: %q, want the share rule", got)
	}
	if got := tableDeltaCompactReason(prev, p.basePath, 1003, nil, at, true, "", time.Time{}, time.Time{}, line); got != "" {
		t.Fatalf("501 bytes beside a table of 1003, 400 of upserts under the resolved line of 800: %q, want no rewrite", got)
	}
}

// The warning about a chain past its line follows the table's own line: a
// table read through its pair is past the ordinary one on purpose, at every
// refresh, and says nothing until it passes its own.
func TestWarnChainOverLine_resolvedTable(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, n int) string {
		p := dir + "/" + name
		if err := os.WriteFile(p, make([]byte, n), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	chain := &baseline.TableDeltaChain{Files: []baseline.TableDeltaFile{
		{Seq: 0, Upserts: write("t.000000.upserts", 600)},
		{Seq: 1, Upserts: write("t.000001.upserts", 500)},
	}}
	var buf strings.Builder
	prevLog := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prevLog) })
	resolved := true
	p := tableDeltaPublish{schema: "s", table: "t", prev: &tableDelta{Chain: chain},
		cfg: FullTableConfig{MaxChainUpserts: 100, MaxResolvedChainUpserts: 550,
			ChainResolved: func(string, *baseline.TableDeltaChain) bool { return resolved }}}
	warnChainOverLine(p, chain) // 1100 is twice its line: not past it
	if buf.Len() != 0 {
		t.Fatalf("a table with its pair, at its own line: %s", buf.String())
	}
	p.cfg.MaxResolvedChainUpserts = 549
	warnChainOverLine(p, chain)
	if !strings.Contains(buf.String(), "line_mb=") || !strings.Contains(buf.String(), "past the size a refresh keeps them under") {
		t.Fatalf("a table with its pair, past its own line: %q", buf.String())
	}
	buf.Reset()
	p.cfg.MaxResolvedChainUpserts, resolved = 550, false
	warnChainOverLine(p, chain)
	if !strings.Contains(buf.String(), "past the size a refresh keeps them under") {
		t.Fatalf("the same chain with no pair is past the ordinary line: %q", buf.String())
	}
}
