package status

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// #2022: a table that later snapshots stopped carrying (dropped at the source,
// renamed, or written under a made-up name by an older build) keeps its last
// copy forever, because prune keeps every table's newest snapshot. Once the
// index rotates past that copy it grades broken, and before this fix the
// server headline stayed red for good while every live table was fine.
//
// The rule: a table counts as no longer backed up only when its newest copy
// sat in a snapshot beside other tables of the same schema, and one NEWER
// snapshot holds every one of those other tables, not this one, and a table
// of the schema the old snapshot lacked (the name it lives on under).
// Anything short of that keeps the table graded.

type snapRow struct {
	db, table string
	v         BaselineStalenessVerdict
	bound     ReadBound
}

// snaps builds a listing: one entry per (snapshot, table), snapshot i at
// hour i (later index = newer).
func snaps(sets ...[]snapRow) []BaselineInfo {
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	var out []BaselineInfo
	for i, set := range sets {
		for _, r := range set {
			out = append(out, BaselineInfo{
				SnapshotTime: base.Add(time.Duration(i) * time.Hour),
				Database:     r.db, Table: r.table, Staleness: r.v, Bound: r.bound,
			})
		}
	}
	return out
}

func TestOverallBaselineStaleness_retiredTables2022(t *testing.T) {
	ok, broken := BaselineOK, BaselineBroken
	live := func(v BaselineStalenessVerdict, extra ...snapRow) []snapRow {
		rows := []snapRow{{"demo", "customers", v, ReadBound{}}, {"demo", "orders", v, ReadBound{}}}
		return append(rows, extra...)
	}
	cases := []struct {
		name string
		in   []BaselineInfo
		want BaselineStalenessVerdict
	}{
		{
			// The live case: an older build wrote order.items as mydumper_0;
			// every later snapshot carries the real name and the same
			// siblings. The old copy stays listed and broken on its own row,
			// but the headline grades the tables still being backed up.
			name: "renamed table from an older build",
			in: snaps(
				live(broken, snapRow{"demo", "mydumper_0", broken, ReadBound{}}),
				live(ok, snapRow{"demo", "order.items", ok, ReadBound{}}),
				live(ok, snapRow{"demo", "order.items", ok, ReadBound{}}),
			),
			want: ok,
		},
		{
			// A table missing from a newer snapshot with nothing new beside
			// it is a drop OR a subset (CLI --tables) and the listing cannot
			// tell them apart: it keeps grading.
			name: "table missing with nothing new beside it",
			in: snaps(
				live(broken, snapRow{"demo", "gone", broken, ReadBound{}}),
				live(ok),
			),
			want: broken,
		},
		{
			// The per-table baseline of a two-table schema: the newer
			// snapshot holds every other table, and the left-out one still
			// exists and is broken.
			name: "two-table schema, one table baselined again",
			in: snaps(
				[]snapRow{{"demo", "orders", broken, ReadBound{}}, {"demo", "customers", broken, ReadBound{}}},
				[]snapRow{{"demo", "orders", ok, ReadBound{}}},
			),
			want: broken,
		},
		{
			// Both conditions on the SAME newer snapshot: one holding every
			// sibling and another holding a new table do not add up.
			name: "siblings and new table in different snapshots",
			in: snaps(
				live(broken, snapRow{"demo", "gone", broken, ReadBound{}}),
				live(ok),
				[]snapRow{{"demo", "fresh", ok, ReadBound{}}},
			),
			want: broken,
		},
		{
			// Never hide a real problem: a table in the newest snapshot that
			// grades broken still turns the headline red.
			name: "table in the newest snapshot is broken",
			in: snaps(
				live(ok, snapRow{"demo", "gone", broken, ReadBound{}}),
				live(ok, snapRow{"demo", "slow", broken, ReadBound{}}),
			),
			want: broken,
		},
		{
			// A newer snapshot of another database only (CLI --database)
			// says nothing about demo: demo keeps grading.
			name: "newer snapshot holds only another database",
			in: snaps(
				live(broken),
				[]snapRow{{"crm", "leads", ok, ReadBound{}}},
			),
			want: broken,
		},
		{
			// A table alone in its snapshot (a per-table baseline) has no
			// siblings to compare: a newer per-table baseline of another
			// table must not retire it.
			name: "per-table baselines rotating within a schema",
			in: snaps(
				[]snapRow{{"demo", "orders", broken, ReadBound{}}},
				[]snapRow{{"demo", "customers", ok, ReadBound{}}},
			),
			want: broken,
		},
		{
			// A newer snapshot with only SOME of the siblings is a subset
			// (CLI --tables), not proof the table is gone.
			name: "newer snapshot holds part of the siblings",
			in: snaps(
				[]snapRow{{"demo", "a", broken, ReadBound{}}, {"demo", "b", broken, ReadBound{}}, {"demo", "c", broken, ReadBound{}}},
				[]snapRow{{"demo", "b", ok, ReadBound{}}},
			),
			want: broken,
		},
		{
			// Same table name in two schemas: only demo's siblings count
			// for demo.orders.
			name: "same name in another schema does not stand in",
			in: snaps(
				[]snapRow{{"demo", "orders", broken, ReadBound{}}, {"demo", "customers", broken, ReadBound{}}},
				[]snapRow{{"crm", "orders", ok, ReadBound{}}, {"crm", "customers", ok, ReadBound{}}},
			),
			want: broken,
		},
		{
			// Siblings are of the same schema only: a newer snapshot of crm
			// does not retire the one demo table that shared a snapshot
			// with it.
			name: "siblings in another schema do not count",
			in: snaps(
				[]snapRow{{"demo", "orders", broken, ReadBound{}}, {"crm", "leads", broken, ReadBound{}}},
				[]snapRow{{"crm", "leads", ok, ReadBound{}}, {"demo", "fresh", ok, ReadBound{}}},
			),
			want: broken,
		},
		{
			// The new table has to appear where this one went missing: a
			// table created later does not retire one that silently stopped
			// being backed up before.
			name: "new table created after the table went missing",
			in: snaps(
				live(broken, snapRow{"demo", "big", broken, ReadBound{}}),
				live(ok),
				live(ok, snapRow{"demo", "audit_log", ok, ReadBound{}}),
			),
			want: broken,
		},
		{
			// The new table has to be in the same schema: one created in
			// another database is not where this table went.
			name: "new table in another schema is not a rename",
			in: snaps(
				live(broken, snapRow{"demo", "gone", broken, ReadBound{}}),
				live(ok, snapRow{"crm", "leads", ok, ReadBound{}}),
			),
			want: broken,
		},
		{
			// Keys are (schema, table) pairs, not a dotted string: since
			// #2008 a table name can hold a dot, and a/b.c must not fold
			// into a.b/c and borrow its fresh copy.
			name: "dotted names do not collide",
			in: snaps(
				[]snapRow{{"a", "b.c", broken, ReadBound{}}},
				[]snapRow{{"a.b", "c", ok, ReadBound{}}},
			),
			want: broken,
		},
		{
			// Names compare exactly. A table that later snapshots spell in
			// another case (a case-folding source dumped by another tool)
			// is the renamed case: the old spelling retires.
			name: "case change across snapshots",
			in: snaps(
				live(ok, snapRow{"demo", "Items", broken, ReadBound{}}),
				live(ok, snapRow{"demo", "items", ok, ReadBound{}}),
			),
			want: ok,
		},
		{
			// Two tables differing only in case both sit in the newest
			// snapshot: both grade.
			name: "case-distinct tables both graded",
			in: snaps(
				live(ok, snapRow{"demo", "T", broken, ReadBound{}}, snapRow{"demo", "t", ok, ReadBound{}}),
			),
			want: broken,
		},
		{
			name: "only one snapshot",
			in:   snaps(live(broken, snapRow{"demo", "x", broken, ReadBound{}})),
			want: broken,
		},
		{
			// A table absent for a while and then back grades on its newest
			// copy, broken or not.
			name: "table came back broken",
			in: snaps(
				live(ok, snapRow{"demo", "back", ok, ReadBound{}}),
				live(ok),
				live(ok, snapRow{"demo", "back", broken, ReadBound{}}),
			),
			want: broken,
		},
		{
			name: "table came back ok",
			in: snaps(
				live(broken, snapRow{"demo", "back", broken, ReadBound{}}),
				live(ok),
				live(ok, snapRow{"demo", "back", ok, ReadBound{}}),
			),
			want: ok,
		},
		{
			// The tables of the newest snapshot can never retire, so a
			// graded, non-empty listing never reduces to "".
			name: "everything old still has a verdict",
			in: snaps(
				live(broken, snapRow{"demo", "gone", broken, ReadBound{}}),
				live(broken, snapRow{"demo", "renamed", broken, ReadBound{}}),
			),
			want: broken,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := OverallBaselineStaleness(tc.in); got != tc.want {
				t.Fatalf("headline = %q, want %q", got, tc.want)
			}
		})
	}
	if got := OverallBaselineStaleness(nil); got != "" {
		t.Fatalf("empty listing must have no verdict, got %q", got)
	}
}

// The CLI's "not evaluable" banner names tables by the same rule as the
// headline: a retired table's unreadable chain must not be named in a banner
// whose headline skipped it.
func TestUnknownNewestTables_skipsRetired2022(t *testing.T) {
	unknown := BaselineUnknown
	in := snaps(
		[]snapRow{{"demo", "orders", unknown, ReadBound{Unread: true}}, {"demo", "gone", unknown, ReadBound{Unread: true}}},
		[]snapRow{{"demo", "orders", unknown, ReadBound{Unread: true}}, {"demo", "renamed", BaselineOK, ReadBound{}}},
	)
	unread, floorUnknown := unknownNewestTables(in)
	if len(unread) != 1 || unread[0] != "demo.orders" || floorUnknown {
		t.Fatalf("unread = %v floorUnknown = %v, want [demo.orders] false", unread, floorUnknown)
	}
}

// The CLI table keeps the retired copy's honest "broken" but without the ⚠
// glyph, which marks only the rows the banner keys on, and no banner fires.
func TestWriteBaselines_retiredTablePlain2022(t *testing.T) {
	in := snaps(
		[]snapRow{{"demo", "orders", BaselineBroken, ReadBound{}}, {"demo", "mydumper_0", BaselineBroken, ReadBound{}}},
		[]snapRow{{"demo", "orders", BaselineOK, ReadBound{}}, {"demo", "order.items", BaselineOK, ReadBound{}}},
	)
	var buf bytes.Buffer
	writeBaselines(&buf, in)
	out := buf.String()
	if !strings.Contains(out, "mydumper_0") || strings.Count(out, "broken") != 2 {
		t.Fatalf("retired and superseded rows must still read broken:\n%s", out)
	}
	if strings.Contains(out, "⚠ broken") || strings.Contains(out, "BASELINE STALE") {
		t.Fatalf("a retired table must not raise the alarm:\n%s", out)
	}
}
