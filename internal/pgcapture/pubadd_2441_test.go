package pgcapture_test

import (
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// A table taken out of the publication and added back (#2441): its changes
// in between were never captured, and nothing in the stream says so but a
// new description with the same shape. The catalog does: the publication's
// row for the table was written after the shape was last seen. Recorded as an
// ALTER PUBLICATION row that marks the gap, so a reconstruct across it
// refuses.

// seenKnown is a known shape seen at 10:00.
func seenAtTen(oid uint32, schema, table string, cols ...col) pgcapture.KnownShape {
	k := known(oid, schema, table, cols...)
	k.SeenLSN, k.SeenAt = 0x800, *at("10:00")
	return k
}

func TestPubAdd_addedBack(t *testing.T) {
	for _, c := range []struct {
		name   string
		k      pgcapture.KnownShape
		pubAt  *time.Time
		noStmp bool
		want   bool
	}{
		{"added back after it was last seen", seenAtTen(10, "s", "a", int4("id")), at("11:30"), false, true},
		{"in the publication all along", seenAtTen(10, "s", "a", int4("id")), at("09:00"), false, false},
		// The catalog shows an add later than this transaction: PostgreSQL
		// sends a description after an add, so this one may follow an
		// earlier add the catalog no longer shows (taken out twice). Refused
		// over this stretch, conservatively.
		{"added again after this transaction", seenAtTen(10, "s", "a", int4("id")), at("12:30"), false, true},
		{"a shape seen with no time", known(10, "s", "a", int4("id")), at("11:30"), false, false},
		{"commit times not kept", seenAtTen(10, "s", "a", int4("id")), at("11:30"), true, false},
		{"not in a FOR TABLE publication", seenAtTen(10, "s", "a", int4("id")), nil, false, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newStampRig(t, nil, c.k)
			r.stamps[10] = pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", Cols: live(int4("id")), PubAt: c.pubAt}
			if c.noStmp {
				r.err = errNoStamps
			}
			d := ddls(r.relation(t, typedRel(10, "s", "a", int4("id"))))
			if !c.want {
				if len(d) != 0 {
					t.Fatalf("got %s: %s", kinds(d), d[0].DDLQuery)
				}
				return
			}
			if len(d) != 1 || d[0].DDLType != event.DDLAlterPublication {
				t.Fatalf("got %s", kinds(d))
			}
			t.Logf("ddl_query: %s", d[0].DDLQuery)
			mustContain(t, d[0].DDLQuery, "gap=uncaptured;", "low=0/1000@", "high=0/19DF9E8@", "s.a", "never captured")
			if c.pubAt.After(txnTime) {
				mustContain(t, d[0].DDLQuery, "instant=at-or-before", "again after this point")
			} else {
				mustContain(t, d[0].DDLQuery, "instant=exact; at="+c.pubAt.Format(time.RFC3339Nano)+";", "written again")
			}
			if b, ok := event.ParseInferredBracket(d[0].DDLQuery); !ok || !b.Gap {
				t.Errorf("bracket = %+v, %v: the gap must read back", b, ok)
			}
		})
	}
}

// A shape change and an add back in one description: both are recorded.
func TestPubAdd_alteredAndAddedBack(t *testing.T) {
	r := newStampRig(t, nil, seenAtTen(10, "s", "a", int4("id")))
	r.stamps[10] = pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", Cols: live(int4("id"), text("x")), PubAt: at("11:30")}
	if got := kinds(ddls(r.relation(t, typedRel(10, "s", "a", int4("id"), text("x"))))); got != "ALTER TABLE s.a | ALTER PUBLICATION s.a" {
		t.Fatalf("got %s", got)
	}
}

// A table first seen after capture started: created, or added to the
// publication. The publication row, later than the table's earliest column,
// says added; it then has no gap (its earlier changes are before capture
// covered it, like the changes before capture started).
func TestPubAdd_firstSeen(t *testing.T) {
	for _, c := range []struct {
		name string
		st   pgcapture.CatalogStamps
		kind event.DDLKind
	}{
		{"added after it was created", pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "new", PubAt: at("11:30"),
			Cols: stamped(live(int4("id")), map[string]*time.Time{"id": at("09:00")})}, event.DDLAlterPublication},
		{"older than the kept commit times", pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "new", PubAt: at("11:30"),
			Cols: live(int4("id"))}, event.DDLAlterPublication},
		{"created and added in one transaction", pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "new", PubAt: at("11:30"), TypeAt: at("11:30"),
			Cols: stamped(live(int4("id")), map[string]*time.Time{"id": at("11:30")})}, event.DDLCreateTable},
		{"no publication row", pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "new",
			Cols: stamped(live(int4("id")), map[string]*time.Time{"id": at("09:00")})}, event.DDLCreateTable},
		{"added after this transaction", pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "new", PubAt: at("12:30"),
			Cols: stamped(live(int4("id")), map[string]*time.Time{"id": at("09:00")})}, event.DDLCreateTable},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newStampRig(t, []uint32{10})
			r.stamps[12] = c.st
			d := ddls(r.relation(t, typedRel(12, "s", "new", int4("id"))))
			if len(d) != 1 || d[0].DDLType != c.kind {
				t.Fatalf("got %s", kinds(d))
			}
			t.Logf("ddl_query: %s", d[0].DDLQuery)
			if c.kind == event.DDLAlterPublication {
				mustContain(t, d[0].DDLQuery, "instant=exact; at="+c.st.PubAt.Format(time.RFC3339Nano)+";", "added to the publication", "existed before")
				if strings.Contains(d[0].DDLQuery, "gap=") {
					t.Errorf("a first add marks no gap: %s", d[0].DDLQuery)
				}
			}
		})
	}
}

// A table in the publication when capture started that no change showed yet:
// a publication row written after that start means it was taken out and added
// back since, and nothing captured in between.
func TestPubAdd_quietTableAddedBack(t *testing.T) {
	for _, c := range []struct {
		name    string
		startAt time.Time
		pubAt   *time.Time
		want    bool
	}{
		{"added back after capture started", *at("10:30"), at("11:30"), true},
		{"in the publication before capture started", *at("10:30"), at("10:00"), false},
		{"no start time", time.Time{}, at("11:30"), false},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := newStampRig(t, []uint32{10})
			r.startAt = c.startAt
			r.rebuild(t)
			r.stamps[10] = pgcapture.CatalogStamps{Exists: true, Schema: "s", Table: "a", Cols: live(int4("id")), PubAt: c.pubAt}
			d := ddls(r.relation(t, typedRel(10, "s", "a", int4("id"))))
			if !c.want {
				if len(d) != 0 {
					t.Fatalf("got %s", kinds(d))
				}
				return
			}
			if len(d) != 1 || d[0].DDLType != event.DDLAlterPublication {
				t.Fatalf("got %s", kinds(d))
			}
			t.Logf("ddl_query: %s", d[0].DDLQuery)
			mustContain(t, d[0].DDLQuery, "gap=uncaptured;", "low=0/0@2026-06-21T10:30:00Z;", "never captured")
		})
	}
}
