package pgcapture

import (
	"time"
)

// CatalogColumn is one column of a table as the live catalog holds it
// (pg_attribute, not dropped), with the commit time of its catalog row.
type CatalogColumn struct {
	ShapeColumn
	// Generated is a STORED generated column, which a stream before
	// PostgreSQL 18 does not carry.
	Generated bool
	// At is the commit time of the column's catalog row: the last statement
	// that rewrote it. nil when not known.
	At *time.Time
	// HasACL is a column-level grant on the column: ALTER ... OWNER TO then
	// rewrites its row along with the table's.
	HasACL bool
}

// CatalogStamps is what the live catalog says about one table for placing an
// inferred schema change at the commit time of the statement that made it
// (#2441). Every time is pg_xact_commit_timestamp of a catalog row's xmin:
// the LAST statement that rewrote that row, nil when not known (the server
// had track_commit_timestamp off when it was written, the row is frozen, or
// its transaction is older than the server keeps).
type CatalogStamps struct {
	// Exists is false when the OID is no longer in the catalog.
	Exists        bool
	Schema, Table string
	Cols          []CatalogColumn
	// ClassAt is the table's pg_class row: rewritten by a rename, a column
	// added or retyped, and by GRANT, ALTER ... SET and the like.
	ClassAt *time.Time
	// TypeAt is the table's row type (pg_type): written by CREATE and a
	// rename or schema move, not by a column change or a GRANT.
	TypeAt *time.Time
	// DroppedAt are the stamped rows of dropped columns: a dropped column's
	// row is never written again, so each is the time of its drop.
	DroppedAt []time.Time
}

// stampsWarnEvery bounds how often a failing stamp read is said.
const stampsWarnEvery = 10 * time.Minute

// stampsFor reads the catalog's stamps for oid; ok is false when none can be
// used. A failure never stops capture (the instant is an extra over the
// bracket) and is said at most once per stampsWarnEvery, with how many reads
// failed since the last time it was said.
func (s *inferState) stampsFor(oid uint32, warn func(msg string, args ...any)) (CatalogStamps, bool) {
	if s.cfg.Stamps == nil {
		return CatalogStamps{}, false
	}
	st, err := s.cfg.Stamps(oid)
	if err != nil {
		s.stampsFailed++
		if now := time.Now(); s.stampsWarnedAt.IsZero() || now.Sub(s.stampsWarnedAt) >= stampsWarnEvery {
			warn("pgcapture: cannot read when a schema change committed from the catalog; inferred schema changes are dated at or before the change that shows them",
				"oid", oid, "error", err, "failed_reads", s.stampsFailed)
			s.stampsWarnedAt, s.stampsFailed = now, 0
		}
		return CatalogStamps{}, false
	}
	return st, st.Exists
}

// showsShape reports whether the live catalog holds the table under name n
// with exactly the columns the stream describes. Otherwise the catalog is
// past this point (a later change), or holds columns the stream leaves out
// (a publication column list), and no time it holds is this change's. A
// generated column the stream does not carry is not a difference.
func (st CatalogStamps) showsShape(n relName, cols []ShapeColumn) bool {
	if st.Schema != n.schema || st.Table != n.table {
		return false
	}
	carried := make(map[string]bool, len(cols))
	for _, c := range cols {
		carried[c.Name] = true
	}
	live := make([]ShapeColumn, 0, len(st.Cols))
	for _, c := range st.Cols {
		if c.Generated && !carried[c.Name] {
			continue
		}
		live = append(live, c.ShapeColumn)
	}
	return equalShape(live, cols)
}

// inBracket reports whether t can be the commit of a change in (low, high].
func inBracket(t time.Time, low *bound, high bound) bool {
	if t.After(high.at) {
		return false
	}
	return low == nil || t.After(low.at)
}

// renameInstant is when a rename to n committed: the row type's time, which
// a later GRANT or column change does not move. Not caught: an ALTER ... OWNER
// TO in the bracket after the rename, which rewrites the row type too.
func (st CatalogStamps) renameInstant(n relName, cols []ShapeColumn, low *bound, high bound) *time.Time {
	if !st.showsShape(n, cols) || st.TypeAt == nil || !inBracket(*st.TypeAt, low, high) {
		return nil
	}
	return st.TypeAt
}

// alterInstant is when the statement that gave the table the columns after
// committed. Each added or retyped column's row must agree with the table's
// row (an ADD and a retype that rewrites the table write both; a later
// statement on the column alone, or on the table alone, moves one of them)
// and hold no column-level grant (ALTER ... OWNER TO rewrites such a row with
// the table's). Each column the change dropped is matched by a dropped row
// stamped in the bracket. All of them must name one time. Not caught: one
// later statement that rewrites both the column's row and the table's (a
// multi-part ALTER), which then stands in for the change.
func (st CatalogStamps) alterInstant(n relName, before, after []ShapeColumn, low *bound, high bound) *time.Time {
	if !st.showsShape(n, after) {
		return nil
	}
	old := make(map[string]ShapeColumn, len(before))
	for _, c := range before {
		old[c.Name] = c
	}
	var times []time.Time
	for _, c := range after {
		if o, ok := old[c.Name]; ok && o == c {
			continue
		}
		lc, ok := st.col(c.Name)
		if !ok || lc.At == nil || lc.HasACL || st.ClassAt == nil || !lc.At.Equal(*st.ClassAt) {
			return nil
		}
		times = append(times, *lc.At)
	}
	kept := make(map[string]bool, len(after))
	for _, c := range after {
		kept[c.Name] = true
	}
	dropped := 0
	for _, c := range before {
		if !kept[c.Name] {
			dropped++
		}
	}
	if dropped > 0 {
		var in []time.Time
		for _, t := range st.DroppedAt {
			if inBracket(t, low, high) {
				in = append(in, t)
			}
		}
		if len(in) != dropped {
			return nil
		}
		times = append(times, in...)
	}
	if len(times) == 0 {
		return nil
	}
	for _, t := range times[1:] {
		if !t.Equal(times[0]) {
			return nil
		}
	}
	if !inBracket(times[0], low, high) {
		return nil
	}
	return &times[0]
}

// createInstant is when a table created under n committed: the row type's
// time, when the earliest column carries the same one. A later ALTER ...
// OWNER TO rewrites the row type and no column without a grant; one that
// also adds a column gives the new column its time, never the earliest.
func (st CatalogStamps) createInstant(n relName, cols []ShapeColumn, high bound) *time.Time {
	if !st.showsShape(n, cols) || st.TypeAt == nil || !inBracket(*st.TypeAt, nil, high) {
		return nil
	}
	var earliest *time.Time
	for _, c := range st.Cols {
		if c.At != nil && (earliest == nil || c.At.Before(*earliest)) {
			earliest = c.At
		}
	}
	if earliest == nil || !earliest.Equal(*st.TypeAt) {
		return nil
	}
	return st.TypeAt
}

func (st CatalogStamps) col(name string) (CatalogColumn, bool) {
	for _, c := range st.Cols {
		if c.Name == name {
			return c, true
		}
	}
	return CatalogColumn{}, false
}
