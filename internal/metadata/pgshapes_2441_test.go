package metadata

import (
	"reflect"
	"testing"
	"time"
)

func shape(id int, oid uint32, schema, table string) PGShape {
	return PGShape{SnapshotID: id, Relation: PGRelationSchema{OID: oid, Schema: schema, Table: table,
		Columns: []PGRelationColumn{{Name: "id", Ordinal: 1, TypeOID: 23, TypeMod: -1}}}}
}

func ids(s []PGShape) []int {
	out := make([]int, len(s))
	for i, x := range s {
		out[i] = x.SnapshotID
	}
	return out
}

// The rules for which stored shapes are still "the last one seen".
func TestLatestPGShapes(t *testing.T) {
	cases := []struct {
		name string
		in   []PGShape
		want []int
	}{
		{"empty", nil, []int{}},
		{"one table", []PGShape{shape(1, 10, "s", "a")}, []int{1}},
		// Older rows of the index carry no OID: kept, keyed by name.
		{"legacy alone", []PGShape{shape(1, 0, "s", "a")}, []int{1}},
		// Every restart wrote a snapshot: the newest per OID / per legacy
		// name wins.
		{"newest per oid", []PGShape{shape(1, 10, "s", "a"), shape(2, 10, "s", "a"), shape(3, 10, "s", "b")}, []int{3}},
		{"newest legacy per name", []PGShape{shape(1, 0, "s", "a"), shape(2, 0, "s", "a")}, []int{2}},
		// Identified under the name, then renamed away: the legacy shape of
		// the old name stays superseded.
		{"legacy superseded by a since-renamed table", []PGShape{shape(1, 0, "s", "a"), shape(2, 10, "s", "a"), shape(3, 10, "s", "b")}, []int{3}},
		// Trailing space is a different PostgreSQL name.
		{"trailing space is another name", []PGShape{shape(1, 0, "s", "a"), shape(2, 10, "s", "a ")}, []int{1, 2}},
		// After the upgrade the same table is written again with its OID:
		// the newer, identified shape replaces the legacy one.
		{"legacy superseded by identified", []PGShape{shape(1, 0, "s", "a"), shape(2, 10, "s", "a")}, []int{2}},
		// A legacy shape newer than an identified one of the same name (a
		// downgrade wrote it) is the last one seen; both are kept and the
		// caller takes the newest by name.
		{"legacy newer than identified", []PGShape{shape(1, 10, "s", "a"), shape(2, 0, "s", "a")}, []int{1, 2}},
		// Two OIDs under one name: a drop and a recreate. Both are kept; the
		// old OID is what tells the stream the name was reused.
		{"name reused by a new oid", []PGShape{shape(1, 10, "s", "a"), shape(2, 11, "s", "a")}, []int{1, 2}},
		// Same name in two schemas is two tables.
		{"same table name, two schemas", []PGShape{shape(1, 0, "s", "a"), shape(2, 10, "t", "a")}, []int{1, 2}},
		// Identifiers are case-sensitive in PostgreSQL.
		{"case-distinct names", []PGShape{shape(1, 0, "s", "a"), shape(2, 10, "s", "A")}, []int{1, 2}},
		{"case-distinct legacy names", []PGShape{shape(1, 0, "s", "a"), shape(2, 0, "s", "A")}, []int{1, 2}},
		// Input order does not matter; output is by snapshot_id.
		{"unsorted input", []PGShape{shape(3, 11, "s", "b"), shape(1, 0, "s", "a"), shape(2, 10, "s", "a")}, []int{2, 3}},
	}
	for _, c := range cases {
		if got := ids(latestPGShapes(c.in, 0)); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

func seenShape(id int, oid uint32, lsn uint64) PGShape {
	s := shape(id, oid, "s", "a")
	s.Relation.SeenLSN = lsn
	if lsn != 0 {
		s.Relation.SeenAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(lsn) * time.Second)
	}
	return s
}

// With a resume position, every version from it on is kept, plus the one in
// effect where the resend starts; older ones are not needed.
func TestLatestPGShapes_fromAResumePosition(t *testing.T) {
	in := []PGShape{seenShape(1, 10, 100), seenShape(2, 10, 200), seenShape(3, 10, 300), seenShape(4, 10, 400)}
	for _, c := range []struct {
		from uint64
		want []int
	}{
		{0, []int{4}},            // a new slot resends nothing
		{250, []int{2, 3, 4}},    // 200 was in effect at 250
		{300, []int{2, 3, 4}},    // 300 itself may be resent: 200 is the base
		{100, []int{1, 2, 3, 4}}, // nothing before 100 but itself... base is none, keep all
		{50, []int{1, 2, 3, 4}},
		{500, []int{4}},
	} {
		if got := ids(latestPGShapes(in, c.from)); !reflect.DeepEqual(got, c.want) {
			t.Errorf("from %d: got %v, want %v", c.from, got, c.want)
		}
	}
	// A shape with no recorded position counts as seen at 0: the base.
	in = []PGShape{seenShape(1, 10, 0), seenShape(2, 10, 300)}
	if got := ids(latestPGShapes(in, 250)); !reflect.DeepEqual(got, []int{1, 2}) {
		t.Errorf("unpositioned base: got %v", got)
	}
}

// A version seen while inference was off (position, no time) is never the
// base, but is kept when it comes after the base.
func TestLatestPGShapes_blindVersions(t *testing.T) {
	blind := func(id int, lsn uint64) PGShape {
		s := seenShape(id, 10, lsn)
		s.Relation.SeenAt = time.Time{}
		return s
	}
	seen := func(id int, lsn uint64) PGShape {
		s := seenShape(id, 10, lsn)
		s.Relation.SeenAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		return s
	}
	in := []PGShape{seen(1, 100), blind(2, 200), blind(3, 300)}
	if got := ids(latestPGShapes(in, 400)); !reflect.DeepEqual(got, []int{1, 2, 3}) {
		t.Errorf("got %v, want the last compared shape and the blind ones after it", got)
	}
}
