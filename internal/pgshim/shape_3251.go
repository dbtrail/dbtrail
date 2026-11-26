package pgshim

import (
	"context"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// A table changed on the source since its snapshot (#3251).
//
// PostgreSQL's change stream does not announce an ALTER TABLE: capture
// learns a table's new columns from the next change made to it, and the
// copy reads the table with its snapshot's columns until the table's next
// snapshot. Between the two, the copy answers with rows PostgreSQL would not
// return (a column one type there and another here, a column added or
// dropped), and nothing says so. Once capture sees the new columns the copy
// stops laying recent changes over the table, and reads it from its
// snapshot alone, still with the old columns, until that snapshot.
//
// So, before the copy runs a statement, the source is asked (Binding.
// SourceShapes) what each table the statement reads is now: its OID and its
// columns, by name, type, modifier and attnum. That is compared with what
// the table's snapshot recorded (CatalogTable.Recorded). Any difference
// sends the statement to the source (ReasonCopyShapeDiffers), however
// expensive it is there: a table of other columns is not a value written a
// little otherwise, so the rule that keeps a heavy statement on the copy in
// spite of a difference does not hold for it. Where nothing can go to the
// source (the server hides tables or columns), the statement is refused, as
// a char(n) or a collation the copy cannot compare is. When the source
// cannot be asked, the statement is refused too (ReasonCopyShapeUnchecked):
// its columns on the copy may be the old ones, and sending it to a source
// that did not answer would only wait for it again.
//
// The comparison is by table: a statement that names only columns the
// change did not touch goes to the source all the same. Once capture sees
// the change, the copy reads the whole table from its snapshot alone, so
// every column of it may be old.
//
// What it does not see: a rewrite that keeps the column's type (ALTER
// COLUMN c TYPE int USING c + 1 on an int column) changes values and no
// column, so the shape is the snapshot's.

// SourceShape is a table's shape on the source now (Binding.SourceShapes):
// its pg_class OID and the columns a snapshot would store (not dropped, not
// generated), with Name, TypeOID, TypeMod and Ordinal set.
type SourceShape struct {
	OID     uint32
	Columns []baseline.PGColumn
}

// copyShapeDiffers says why the copy's columns of a table text reads are not
// the source's, "" when every table it reads is as its snapshot recorded it.
// refusal, when not nil, is the statement's error: its tables' columns could
// not be compared (the source could not be asked, the copy's catalog could
// not be read), or it was stopped meanwhile; the reason it is counted under
// is in busyRefused.
func (s *session) copyShapeDiffers(ctx context.Context, text string) (why string, refusal *pgErr) {
	if s.bound == nil || s.bound.SourceShapes == nil || s.bound.Catalog == nil {
		return "", nil
	}
	toks, _ := lexSQL(text)
	switch firstWord(toks) {
	case "show", "describe", "explain", "summarize":
		// The copy's own commands (copy_command): they describe the copy, and
		// the source has none of them.
		return "", nil
	}
	v, perr := s.catalog()
	if perr != nil {
		if busy := s.catalogBusy; busy != "" {
			s.busyRefused = busy
			return "", busyRefusal(busy)
		}
		return "", perr
	}
	read := shapeTablesRead(text, v.tables)
	if len(read) == 0 {
		return "", nil
	}
	names := make([][2]string, len(read))
	for i, at := range read {
		names[i] = [2]string{v.tables[at].Schema, v.tables[at].Name}
	}
	now, err := s.bound.SourceShapes(ctx, names)
	if perr := notCapturedStopped(ctx); perr != nil {
		return "", perr
	}
	if err != nil {
		// The error's own text (an address) goes to the log, not to the
		// client.
		logEveryMinute(s.logger, "pgshim: shapes unread", "pgshim: the columns of the tables a statement reads could not be read from the source; the statement is refused", "err", err)
		s.busyRefused = ReasonCopyShapeUnchecked
		return "", &pgErr{"55000", "DBTrail could not check that the columns of the tables this statement reads are on your PostgreSQL what they are on the copy (the source did not answer in time, or refused the question; DBTrail's log has the details), so the statement was not answered; run it again"}
	}
	for _, at := range read {
		t := &v.tables[at]
		sh, found := now[[2]string{t.Schema, t.Name}]
		if why := copyShapeWhy(t, sh, found); why != "" {
			return why, nil
		}
	}
	return "", nil
}

// shapeRefusal is the error of a statement for the copy that reads a table
// whose columns on the source are not its snapshot's, nil when the copy may
// run it. The statement then goes to the source (fallTo); where it cannot go
// there, it is refused.
func (s *session) shapeRefusal(ctx context.Context, text string) *pgErr {
	why, perr := s.copyShapeDiffers(ctx, text)
	if perr != nil {
		return perr
	}
	if why == "" {
		return nil
	}
	if !s.canFallBack() {
		s.busyRefused = ReasonCopyShapeDiffers
		return shapeCopyOnlyError(why)
	}
	s.logger.Debug("pgshim: the statement reads a table changed on the source since its snapshot; the source answers", "why", why)
	s.fallTo(ReasonCopyShapeDiffers)
	return &pgErr{"55000", why}
}

// shapeCopyOnlyError is the refusal of a statement on a connection that
// sends nothing to its source, which reads a changed table.
func shapeCopyOnlyError(why string) *pgErr {
	return &pgErr{"55000", why + ". This connection sends nothing to your PostgreSQL (the server hides tables or columns), so the statement is refused until the table's next snapshot records its columns"}
}

// copyShapeWhy says why the copy's table t is not, on the source, the table
// its snapshot recorded (now, found false when the source has no table of
// that name), "" when it is.
func copyShapeWhy(t *CatalogTable, now SourceShape, found bool) string {
	name := t.Schema + "." + t.Name
	switch {
	case !found:
		return name + " is not on your PostgreSQL under that name any more (dropped, or renamed), and the copy still holds it as its snapshot read it"
	case t.Recorded == nil:
		return "the copy's snapshot does not record the columns of " + name + ", so they cannot be compared with your PostgreSQL's (its next snapshot records them)"
	case now.OID != t.Recorded.OID:
		return name + " on your PostgreSQL is not the table the copy's snapshot was read from: it was dropped and created again"
	}
	if diff := baseline.PGColumnsDiff(t.Recorded.Columns, now.Columns); diff != "" {
		return "the columns of " + name + " on your PostgreSQL are not the ones of the copy's snapshot (" + diff + "): PostgreSQL does not announce an ALTER TABLE to capture, so the copy keeps the old columns until the table's next snapshot"
	}
	return ""
}

// shapeTablesRead are the tables of the copy's catalog the statement text
// may read, by their index in tables, as notCaptured reads them: with more
// tables than it reads, never fewer. A name without a schema is that name in
// every schema, case is not compared (the copy matches names so), a WITH
// named as a table counts as that table, and a text not read whole, or a
// name with a database before its schema, reads every table.
func shapeTablesRead(text string, tables []CatalogTable) []int {
	every := func() []int {
		out := make([]int, len(tables))
		for i := range tables {
			out[i] = i
		}
		return out
	}
	reads, why := readsOf(text, copyFuncs)
	if why != "" {
		return every()
	}
	hit := map[int]bool{}
	var out []int
	for _, ref := range reads.refs {
		schema, name := "", ref.parts[len(ref.parts)-1].text
		switch len(ref.parts) {
		case 1:
		case 2:
			schema = ref.parts[0].text
		default:
			return every()
		}
		for i := range tables {
			t := &tables[i]
			if !hit[i] && strings.EqualFold(t.Name, name) && (schema == "" || strings.EqualFold(t.Schema, schema)) {
				hit[i] = true
				out = append(out, i)
			}
		}
	}
	return out
}
