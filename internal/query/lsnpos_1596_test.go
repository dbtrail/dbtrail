package query

import (
	"strings"
	"testing"
	"time"
)

// A PostgreSQL position is an LSN: BinlogFile holds its text ("0/16B3748")
// and Pos its number. Ordered by the number; the MySQL file-name rule would
// put "1/0" before "0/16B3748" (#1596).
func TestBinlogPos_LSNOrder(t *testing.T) {
	lo := BinlogPos{File: "0/16B3748", Pos: 0x16B3748}
	hi := BinlogPos{File: "1/0", Pos: 1 << 32}
	if !lo.IsLSN() || !hi.IsLSN() || (BinlogPos{File: "binlog.000001", Pos: 4}).IsLSN() {
		t.Fatal("IsLSN")
	}
	if !lo.AtOrBefore(hi) || hi.AtOrBefore(lo) {
		t.Errorf("0/16B3748 vs 1/0: at-or-before %v / %v, want true / false", lo.AtOrBefore(hi), hi.AtOrBefore(lo))
	}
	if !lo.AtOrBefore(lo) {
		t.Error("an LSN is at or before itself")
	}
	// MySQL keeps its rule, including the #840 rollover.
	if !(BinlogPos{File: "mysql-bin.999999", Pos: 9}).AtOrBefore(BinlogPos{File: "mysql-bin.1000000", Pos: 4}) {
		t.Error("MySQL rollover order changed")
	}
	// LSNs are one sequence per cluster.
	if binlogBaseName("0/16B3748") != binlogBaseName("1/0") {
		t.Error("two LSNs must share a base name")
	}
	if binlogBaseName("0/16B3748") == binlogBaseName("binlog.000001") {
		t.Error("an LSN must not share a base name with a binlog file")
	}
}

// buildQuery bounds a PostgreSQL fetch by the LSN number, never by the text:
// every row of a transaction has start_pos == end_pos == its commit LSN.
func TestBuildQuery_LSNBounds(t *testing.T) {
	since := time.Date(2026, 10, 17, 10, 30, 0, 0, time.UTC)
	until := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	q, args := buildQuery(Options{Schema: "s", Table: "t", Since: &since, Until: &until,
		SincePos: &BinlogPos{File: "0/16B3748", Pos: 0x16B3748},
		UntilPos: &BinlogPos{File: "1/0", Pos: 1 << 32}})
	if strings.Contains(q, "CHAR_LENGTH(binlog_file)") || strings.Contains(q, "binlog_file >") || strings.Contains(q, "binlog_file <") {
		t.Errorf("an LSN bound compares binlog_file text: %s", q)
	}
	for _, want := range []string{"start_pos >= ?", "end_pos <= ?", "event_timestamp >= ?"} {
		if !strings.Contains(q, want) {
			t.Errorf("query lacks %q: %s", want, q)
		}
	}
	var hasLo, hasHi, hasFloor bool
	for _, a := range args {
		switch v := a.(type) {
		case uint64:
			hasLo = hasLo || v == 0x16B3748
			hasHi = hasHi || v == 1<<32
		case time.Time:
			hasFloor = hasFloor || v.Equal(CoarseSinceFloor(since))
		}
	}
	if !hasLo || !hasHi || !hasFloor {
		t.Errorf("args %v: lsn lo %v hi %v coarse floor %v", args, hasLo, hasHi, hasFloor)
	}
	// The exact Since filter is dropped, as with a MySQL position.
	for _, a := range args {
		if v, ok := a.(time.Time); ok && v.Equal(since) {
			t.Errorf("exact Since %v still filters an LSN-anchored fetch", since)
		}
	}
}

// The archive-skip proof cannot see the LSN gate either: it declines.
func TestWindowSatisfiedLive_declinesOnAnLSNAnchor(t *testing.T) {
	since := time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC)
	plan := &QueryPlan{MySQLRanges: []TimeRange{{Start: since.Add(-time.Hour)}}, ArchivesBelowLive: true}
	if windowSatisfiedLive(Options{Since: &since, SincePos: &BinlogPos{File: "0/1", Pos: 1}}, plan) {
		t.Fatal("proof fired for an LSN-anchored fetch")
	}
	if ArchiveSkipNeedsPlan(Options{Since: &since, SincePos: &BinlogPos{File: "0/1", Pos: 1}}, nil) {
		t.Error("ArchiveSkipNeedsPlan wants a plan for an LSN-anchored fetch")
	}
}

// Which partitions may hold a change after an LSN anchor: by the number.
func TestPartitionHeads_belowAnLSNAnchor(t *testing.T) {
	since := time.Date(2026, 10, 17, 10, 0, 0, 0, time.UTC)
	floor := CoarseSinceFloor(since)
	h := &PartitionHeads{streamCaptured: true, parts: []partitionHead{
		{name: "p_old", lower: floor.Add(-3 * time.Hour), pos: BinlogPos{File: "0/FFFFFFFF", Pos: 0xFFFFFFFF}},
		{name: "p_new", lower: floor.Add(-2 * time.Hour), pos: BinlogPos{File: "1/10", Pos: 1<<32 + 0x10}},
	}}
	got := h.below(since, BinlogPos{File: "1/0", Pos: 1 << 32})
	if len(got) != 1 || got[0] != "p_new" {
		t.Fatalf("below = %v, want only p_new (its newest row is after 1/0)", got)
	}
}
