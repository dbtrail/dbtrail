package reconstruct

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The line rule (#2210): a chain whose upserts, every pair together, pass
// MaxChainUpserts is ended. At the line exactly it is not; zero is no rule.
func TestChainCompactReason_line(t *testing.T) {
	noSizeFloor(t)
	at := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	prev := &tableDelta{Meta: baseline.DumpMetadata{DeltaChainStart: at.Add(-time.Hour)}, PairSize: 1, ChainUpsertsSize: 24 << 20}
	reason := func(max int64) string {
		return tableDeltaCompactReason(prev, "/b/t.parquet", 1<<40, nil, at, true, "", time.Time{}, time.Time{}, max)
	}
	if got := reason(0); got != "" {
		t.Errorf("no line: %q", got)
	}
	if got := reason(24 << 20); got != "" {
		t.Errorf("at the line: %q", got)
	}
	if got := reason(24<<20 - 1); !strings.Contains(got, "(24 MB) passed 23 MB, half of what SQL on the copy reads at once") {
		t.Errorf("past the line: %q", got)
	}
}

// Through real windows: the rule weighs every pair's upserts, not the last
// one's, and a rewrite resets the chain.
func TestTableDelta_lineEndsTheChain(t *testing.T) {
	rows, nulls := zooRows()
	src := writeZooBaseline(t, rows, nulls)
	root := t.TempDir()
	t0 := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	noCompaction(t)
	noSizeFloor(t)
	prevFrac := tableDeltaMaxFraction
	tableDeltaMaxFraction = 1e9 // the size rule out of the way
	t.Cleanup(func() { tableDeltaMaxFraction = prevFrac })

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
	if d.ChainUpsertsSize != ui.Size() {
		t.Fatalf("one pair: ChainUpsertsSize = %d, its upserts file is %d", d.ChainUpsertsSize, ui.Size())
	}
	// Two pairs under the line, three over it.
	line := 5 * ui.Size() / 2
	base, prevTime := base1, at1
	for i := 2; i <= 5; i++ {
		at := t0.Add(time.Duration(i) * 5 * time.Minute)
		nb, rep, err := deltaWindow(t, root, base, prevTime, changeMap(upd(1, fmt.Sprintf("v%d", i))), at,
			&query.BinlogPos{File: "binlog.000009", Pos: uint64(1000 * i)}, func(p *tableDeltaPublish) { p.cfg.MaxChainUpserts = line })
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case i < 4 && rep.DeltaCompacted != "":
			t.Fatalf("window %d ended the chain (%q) under the line", i, rep.DeltaCompacted)
		case i == 4 && !strings.Contains(rep.DeltaCompacted, "half of what SQL on the copy reads at once"):
			t.Fatalf("window 4: want the line rule over three pairs, got %q", rep.DeltaCompacted)
		case i == 5 && rep.DeltaCompacted != "":
			t.Fatalf("window 5 ended the chain (%q) right after a rewrite", rep.DeltaCompacted)
		}
		base, prevTime = nb, at
	}
}
