package pgcapture_test

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// The exact instant of an inferred schema change (#2441), from the commit
// times of the catalog rows the change wrote (track_commit_timestamp). The
// rule each case pins: a catalog row's time is the LAST statement that
// rewrote it, so a time is trusted only where a second row agrees with it or
// cannot have been rewritten since, the live catalog shows the shape the
// stream does, and the time lies in the bracket. Anything else is "at or
// before", never another statement's time.

// at is a time in the test hour, between the lower bound (11:00) and the
// transaction the change is seen in (12:00).
func at(hm string) *time.Time {
	t, err := time.Parse("15:04", hm)
	if err != nil {
		panic(err)
	}
	v := time.Date(2026, 6, 21, t.Hour(), t.Minute(), 0, 0, time.UTC)
	return &v
}

// stampRig is an inferRig whose catalog answers stamps.
type stampRig struct {
	*inferRig
	stamps map[uint32]pgcapture.CatalogStamps
	err    error
	calls  int
	logs   *bytes.Buffer
	// start, shapes and startAt are what rebuild makes the decoder from.
	start   []uint32
	shapes  []pgcapture.KnownShape
	startAt time.Time
}

func newStampRig(t *testing.T, start []uint32, shapes ...pgcapture.KnownShape) *stampRig {
	t.Helper()
	r := &stampRig{inferRig: &inferRig{facts: map[uint32]pgcapture.RelationFacts{}, lastOK: true},
		stamps: map[uint32]pgcapture.CatalogStamps{}, logs: &bytes.Buffer{}, start: start, shapes: shapes}
	r.rebuild(t)
	return r
}

// rebuild makes the decoder again from the rig's settings.
func (r *stampRig) rebuild(t *testing.T) {
	t.Helper()
	startSet := map[uint32]bool{}
	for _, o := range r.start {
		startSet[o] = true
	}
	logger := slog.New(slog.NewTextHandler(r.logs, nil))
	r.d = pgcapture.NewDecoder(pkResolver("id"), event.Filters{}, logger, pgcapture.WithDDLInference(pgcapture.DDLInference{
		Known:     r.shapes,
		StartOIDs: startSet,
		StartAt:   r.startAt,
		Lookup: func(oid uint32) (pgcapture.RelationFacts, error) {
			return r.facts[oid], nil
		},
		LastChange: func(schema, table string, before uint64) (uint64, time.Time, bool, error) {
			return uint64(lowLSN), lowTime, r.lastOK, nil
		},
		Stamps: func(oid uint32) (pgcapture.CatalogStamps, error) {
			r.calls++
			return r.stamps[oid], r.err
		},
	}))
}

// live is the catalog's columns, as a stamp set holds them.
func live(cols ...col) []pgcapture.CatalogColumn {
	out := make([]pgcapture.CatalogColumn, len(cols))
	for i, c := range cols {
		out[i] = pgcapture.CatalogColumn{ShapeColumn: pgcapture.ShapeColumn{Name: c.name, TypeOID: c.oid, TypeMod: c.mod}}
	}
	return out
}

// withACL marks the named columns as holding a column-level grant.
func withACL(cols []pgcapture.CatalogColumn, names ...string) []pgcapture.CatalogColumn {
	for i := range cols {
		for _, n := range names {
			if cols[i].Name == n {
				cols[i].HasACL = true
			}
		}
	}
	return cols
}

// stamped sets the commit time of each named column's catalog row.
func stamped(cols []pgcapture.CatalogColumn, times map[string]*time.Time) []pgcapture.CatalogColumn {
	for i := range cols {
		cols[i].At = times[cols[i].Name]
	}
	return cols
}

func wantExact(t *testing.T, e event.Event, want *time.Time) {
	t.Helper()
	if want == nil {
		if !strings.Contains(e.DDLQuery, "instant=at-or-before") || strings.Contains(e.DDLQuery, "instant=exact") {
			t.Fatalf("claimed an exact instant: %s", e.DDLQuery)
		}
		return
	}
	mustContain(t, e.DDLQuery, "instant=exact; at="+want.Format(time.RFC3339Nano)+";")
	// The bracket stays: the reconstruct guard places the row by it.
	mustContain(t, e.DDLQuery, "low=", "high=0/19DF9E8@2026-06-21T12:00:00Z")
	if !e.Timestamp.Equal(*want) {
		t.Errorf("detected_at = %v, want the exact instant %v", e.Timestamp, *want)
	}
	if b, ok := event.ParseInferredBracket(e.DDLQuery); !ok || !b.High.Equal(txnTime) {
		t.Errorf("bracket = %+v, %v", b, ok)
	}
}

func TestInstant_alter(t *testing.T) {
	prev := []col{int4("id"), text("legacy"), vc("v", 10)}
	cases := []struct {
		name  string
		cur   []col
		st    pgcapture.CatalogStamps
		exact *time.Time
	}{
		// ADD COLUMN writes the column's row and the table's (column count):
		// the two agree.
		{"an added column", []col{int4("id"), text("legacy"), vc("v", 10), text("note")},
			pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", ClassAt: at("11:30"),
				Cols: stamped(live(int4("id"), text("legacy"), vc("v", 10), text("note")), map[string]*time.Time{"id": at("09:00"), "note": at("11:30")})},
			at("11:30")},
		// A later statement on that column only (SET STATISTICS, SET NOT
		// NULL) moves its row and not the table's.
		{"an added column touched again", []col{int4("id"), text("legacy"), vc("v", 10), text("note")},
			pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", ClassAt: at("11:30"),
				Cols: stamped(live(int4("id"), text("legacy"), vc("v", 10), text("note")), map[string]*time.Time{"note": at("11:40")})},
			nil},
		// A later statement on the table (GRANT) moves the table's row.
		{"an added column, then a GRANT", []col{int4("id"), text("legacy"), vc("v", 10), text("note")},
			pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", ClassAt: at("11:50"),
				Cols: stamped(live(int4("id"), text("legacy"), vc("v", 10), text("note")), map[string]*time.Time{"note": at("11:30")})},
			nil},
		// ALTER ... OWNER TO rewrites the table's row and the row of every
		// column with a column-level grant: such a column cannot corroborate.
		{"an added column with a column grant", []col{int4("id"), text("legacy"), vc("v", 10), text("note")},
			pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", ClassAt: at("11:40"),
				Cols: withACL(stamped(live(int4("id"), text("legacy"), vc("v", 10), text("note")), map[string]*time.Time{"note": at("11:40")}), "note")},
			nil},
		{"a retyped column", []col{int4("id"), text("legacy"), vc("v", 20)},
			pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", ClassAt: at("11:15"),
				Cols: stamped(live(int4("id"), text("legacy"), vc("v", 20)), map[string]*time.Time{"v": at("11:15")})},
			at("11:15")},
		// A dropped column's row is never written again; one dropped before
		// the bracket is history.
		{"a dropped column", []col{int4("id"), vc("v", 10)},
			pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", ClassAt: at("09:00"),
				Cols: live(int4("id"), vc("v", 10)), DroppedAt: []time.Time{*at("08:00"), *at("11:20")}},
			at("11:20")},
		{"two drops at two times", []col{int4("id")},
			pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a",
				Cols: live(int4("id")), DroppedAt: []time.Time{*at("11:20"), *at("11:25")}},
			nil},
		// More dropped rows in the bracket than the stream shows dropped: a
		// column added and dropped again in between. Which is this one is not
		// known.
		{"more drops than the stream shows", []col{int4("id"), vc("v", 10)},
			pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", Cols: live(int4("id"), vc("v", 10)), DroppedAt: []time.Time{*at("11:20"), *at("11:20")}},
			nil},
		{"a drop with no stamped row in the bracket", []col{int4("id"), vc("v", 10)},
			pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", Cols: live(int4("id"), vc("v", 10)), DroppedAt: []time.Time{*at("08:00")}},
			nil},
		{"an add and a drop in one statement", []col{int4("id"), vc("v", 10), text("note")},
			pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", ClassAt: at("11:20"),
				Cols: stamped(live(int4("id"), vc("v", 10), text("note")), map[string]*time.Time{"note": at("11:20")}), DroppedAt: []time.Time{*at("11:20")}},
			at("11:20")},
		{"an add and a drop in two statements", []col{int4("id"), vc("v", 10), text("note")},
			pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", ClassAt: at("11:20"),
				Cols: stamped(live(int4("id"), vc("v", 10), text("note")), map[string]*time.Time{"note": at("11:20")}), DroppedAt: []time.Time{*at("11:25")}},
			nil},
		// The catalog is ahead of the stream (catch-up past a later ALTER).
		{"a catalog ahead of the stream", []col{int4("id"), text("legacy"), vc("v", 10), text("note")},
			pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", ClassAt: at("11:30"),
				Cols: stamped(live(int4("id"), text("legacy"), vc("v", 10), text("note"), text("later")), map[string]*time.Time{"note": at("11:30")})},
			nil},
		{"a time after the transaction", []col{int4("id"), text("legacy"), vc("v", 10), text("note")},
			pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", ClassAt: at("12:30"),
				Cols: stamped(live(int4("id"), text("legacy"), vc("v", 10), text("note")), map[string]*time.Time{"note": at("12:30")})},
			nil},
		{"a time before the lower bound", []col{int4("id"), text("legacy"), vc("v", 10), text("note")},
			pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", ClassAt: at("10:30"),
				Cols: stamped(live(int4("id"), text("legacy"), vc("v", 10), text("note")), map[string]*time.Time{"note": at("10:30")})},
			nil},
		{"no stamp (frozen, or written while the setting was off)", []col{int4("id"), text("legacy"), vc("v", 10), text("note")},
			pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", Cols: live(int4("id"), text("legacy"), vc("v", 10), text("note"))},
			nil},
		// A renamed column changes its row's name, not the table's row.
		{"a renamed column", []col{int4("id"), text("renamed"), vc("v", 10)},
			pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", ClassAt: at("09:00"),
				Cols: stamped(live(int4("id"), text("renamed"), vc("v", 10)), map[string]*time.Time{"renamed": at("11:20")})},
			nil},
		// A STORED generated column the stream does not carry (before
		// PostgreSQL 18) is not a difference; a column a publication's column
		// list leaves out is.
		{"a generated column the stream does not carry", []col{int4("id"), text("legacy"), vc("v", 10), text("note")},
			pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", ClassAt: at("11:30"),
				Cols: append(stamped(live(int4("id"), text("legacy"), vc("v", 10), text("note")), map[string]*time.Time{"note": at("11:30")}),
					pgcapture.CatalogColumn{ShapeColumn: pgcapture.ShapeColumn{Name: "g", TypeOID: 23, TypeMod: -1}, Generated: true})},
			at("11:30")},
		{"a column the publication leaves out", []col{int4("id"), text("legacy"), vc("v", 10), text("note")},
			pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", ClassAt: at("11:30"),
				Cols: append(stamped(live(int4("id"), text("legacy"), vc("v", 10), text("note")), map[string]*time.Time{"note": at("11:30")}),
					pgcapture.CatalogColumn{ShapeColumn: pgcapture.ShapeColumn{Name: "secret", TypeOID: 25, TypeMod: -1}})},
			nil},
		{"the table renamed again since", []col{int4("id"), text("legacy"), vc("v", 10), text("note")},
			pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "z", ClassAt: at("11:30"),
				Cols: stamped(live(int4("id"), text("legacy"), vc("v", 10), text("note")), map[string]*time.Time{"note": at("11:30")})},
			nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newStampRig(t, nil, known(10, "s", "a", prev...))
			r.stamps[10] = c.st
			d := ddls(r.relation(t, typedRel(10, "s", "a", c.cur...)))
			if len(d) != 1 || d[0].DDLType != event.DDLAlterTable {
				t.Fatalf("got %s", kinds(d))
			}
			wantExact(t, d[0], c.exact)
		})
	}
}

// RENAME rewrites the table's row and its row type's; a later GRANT or
// ALTER rewrites only the table's, so the row type's time is the rename's.
func TestInstant_rename(t *testing.T) {
	for _, c := range []struct {
		name  string
		st    pgcapture.CatalogStamps
		exact *time.Time
	}{
		{"a rename, then a GRANT", pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "b", ClassAt: at("11:50"), TypeAt: at("11:10"), Cols: live(int4("id"))}, at("11:10")},
		{"no row type stamp", pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "b", ClassAt: at("11:10"), Cols: live(int4("id"))}, nil},
		{"renamed again since", pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "c", TypeAt: at("11:40"), Cols: live(int4("id"))}, nil},
		{"a time after the transaction", pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "b", TypeAt: at("12:10"), Cols: live(int4("id"))}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newStampRig(t, nil, known(10, "s", "a", int4("id")))
			r.stamps[10] = c.st
			d := ddls(r.relation(t, typedRel(10, "s", "b", int4("id"))))
			if len(d) != 1 || d[0].DDLType != event.DDLRenameTable {
				t.Fatalf("got %s", kinds(d))
			}
			wantExact(t, d[0], c.exact)
		})
	}
}

// CREATE writes the row type and every column's row in one transaction; a
// later ADD COLUMN moves the table's row only, and the row type keeps the
// create's time. Trusted when a column created with the table agrees.
func TestInstant_create(t *testing.T) {
	for _, c := range []struct {
		name  string
		st    pgcapture.CatalogStamps
		exact *time.Time
	}{
		{"a create, then an ADD COLUMN", pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "new", ClassAt: at("11:30"), TypeAt: at("11:05"),
			Cols: stamped(live(int4("id"), text("x")), map[string]*time.Time{"id": at("11:05"), "x": at("11:30")})}, at("11:05")},
		// OWNER TO and ADD COLUMN in one statement after the create: the row
		// type and the new column agree on that statement's time, not the
		// create's. The create's time is the earliest column's.
		{"an OWNER TO and an ADD COLUMN in one statement", pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "new", ClassAt: at("11:30"), TypeAt: at("11:30"),
			Cols: stamped(live(int4("id"), text("x"), text("c")), map[string]*time.Time{"id": at("11:05"), "x": at("11:05"), "c": at("11:30")})}, nil},
		{"no column agrees with the row type", pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "new", TypeAt: at("11:05"),
			Cols: stamped(live(int4("id"), text("x")), map[string]*time.Time{"id": at("11:30"), "x": at("11:30")})}, nil},
		{"a time after the transaction", pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "new", TypeAt: at("12:05"),
			Cols: stamped(live(int4("id"), text("x")), map[string]*time.Time{"id": at("12:05")})}, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newStampRig(t, []uint32{10})
			r.stamps[12] = c.st
			cols := []col{int4("id"), text("x")}
			if len(c.st.Cols) == 3 {
				cols = append(cols, text("c"))
			}
			d := ddls(r.relation(t, typedRel(12, "s", "new", cols...)))
			if len(d) != 1 || d[0].DDLType != event.DDLCreateTable {
				t.Fatalf("got %s", kinds(d))
			}
			wantExact(t, d[0], c.exact)
		})
	}
}

// Catch-up past two ALTERs: the first Relation meets a catalog already
// showing the second; only the second may claim its instant.
func TestInstant_catchUpPastTwoAlters(t *testing.T) {
	r := newStampRig(t, nil, known(10, "s", "a", int4("id")))
	// b2 was added after the first transaction (12:00) and before the second.
	r.stamps[10] = pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", ClassAt: at("12:30"),
		Cols: stamped(live(int4("id"), text("b1"), text("b2")), map[string]*time.Time{"b1": at("11:20"), "b2": at("12:30")})}
	first := ddls(r.relation(t, typedRel(10, "s", "a", int4("id"), text("b1"))))
	mustDecode(t, r.d, &pglogrepl.CommitMessage{CommitLSN: txnLSN, CommitTime: txnTime})
	if len(first) != 1 {
		t.Fatalf("first: %s", kinds(first))
	}
	wantExact(t, first[0], nil)
	second := pglogrepl.LSN(0x2000000)
	mustDecode(t, r.d, &pglogrepl.BeginMessage{FinalLSN: second, CommitTime: txnTime.Add(time.Hour)})
	d := ddls(r.decodeAll(t, typedRel(10, "s", "a", int4("id"), text("b1"), text("b2"))))
	if len(d) != 1 {
		t.Fatalf("second: %s", kinds(d))
	}
	mustContain(t, d[0].DDLQuery, "instant=exact; at="+at("12:30").Format(time.RFC3339Nano)+";")
}

// The instant is an extra: a catalog that cannot answer leaves the row at or
// before, says so once, and never stops capture.
func TestInstant_catalogErrorFallsBack(t *testing.T) {
	r := newStampRig(t, nil, known(10, "s", "a", int4("id")), known(11, "s", "b", int4("id")))
	r.err = errors.New("permission denied for function pg_xact_commit_timestamp")
	for _, m := range []*pglogrepl.RelationMessage{typedRel(10, "s", "a", int4("id"), text("x")), typedRel(11, "s", "b", int4("id"), text("y"))} {
		d := ddls(r.relation(t, m))
		if len(d) != 1 {
			t.Fatalf("got %s", kinds(d))
		}
		wantExact(t, d[0], nil)
		mustDecode(t, r.d, &pglogrepl.CommitMessage{CommitLSN: txnLSN, CommitTime: txnTime})
	}
	if n := strings.Count(r.logs.String(), "level=WARN"); n != 1 || !strings.Contains(r.logs.String(), "permission denied") {
		t.Fatalf("warnings = %d:\n%s", n, r.logs.String())
	}
}

// A Relation that changes nothing reads only whether the table was added back
// to the publication, and writes nothing.
func TestInstant_noChangeWritesNothing(t *testing.T) {
	r := newStampRig(t, nil, known(10, "s", "a", int4("id")))
	if d := ddls(r.relation(t, typedRel(10, "s", "a", int4("id")))); len(d) != 0 {
		t.Fatalf("got %s", kinds(d))
	}
}

var errNoStamps = errors.New("commit timestamps are not kept")
