package pgshim

import (
	"context"
	"fmt"
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
// cannot be asked (down, slower than the question's time, no connection to
// it free), the copy answers as it would without the check, with a NOTICE
// that says the tables could not be checked (ReasonCopyShapeUnchecked): the
// copy stays a way to read while the source is away.
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
// unchecked, when not "", is the notice for the client: the source could not
// be asked, and the copy answers as if nothing changed. refusal, when not
// nil, is the statement's error: the copy's catalog could not be read, or the
// statement was stopped meanwhile; the reason it is counted under is in
// busyRefused.
func (s *session) copyShapeDiffers(ctx context.Context, text string) (why, unchecked string, refusal *pgErr) {
	if s.bound == nil || s.bound.SourceShapes == nil || s.bound.Catalog == nil {
		return "", "", nil
	}
	toks, _ := lexSQL(text)
	switch firstWord(toks) {
	case "show", "describe", "explain", "summarize":
		// The copy's own commands (copy_command): they describe the copy, and
		// the source has none of them.
		return "", "", nil
	}
	v, perr := s.catalog()
	if perr != nil {
		if busy := s.catalogBusy; busy != "" {
			s.busyRefused = busy
			return "", "", busyRefusal(busy)
		}
		return "", "", perr
	}
	read := shapeTablesRead(text, v.tables)
	if len(read) == 0 {
		return "", "", nil
	}
	names := make([][2]string, len(read))
	for i, at := range read {
		names[i] = [2]string{v.tables[at].Schema, v.tables[at].Name}
	}
	now, err := s.bound.SourceShapes(ctx, names)
	if perr := notCapturedStopped(ctx); perr != nil {
		return "", "", perr
	}
	if err != nil {
		// The error's own text (an address) goes to the log, not to the
		// client.
		logEveryMinute(s.logger, "pgshim: shapes unread", "pgshim: the columns of the tables a statement reads could not be read from the source; the copy answers, with a notice", "err", err)
		list := make([]string, len(names))
		for i, n := range names {
			list[i] = n[0] + "." + n[1]
		}
		return "", "DBTrail could not check whether " + strings.Join(list, ", ") + " changed on your PostgreSQL (the source did not answer in time; DBTrail's log has the details), so this was answered from the copy as of its snapshot.", nil
	}
	for _, at := range read {
		t := &v.tables[at]
		sh, found := now[[2]string{t.Schema, t.Name}]
		if why, detail := copyShapeWhy(t, sh, found); why != "" {
			// Which columns changed is the operator's, in the log: a
			// column the server hides from this client is in the snapshot's
			// shape too, and its name must not reach the client.
			logEveryMinute(s.logger, "pgshim: shape differs "+t.Schema+"."+t.Name, "pgshim: a table changed on the source since its snapshot; the copy does not answer statements that read it",
				"table", t.Schema+"."+t.Name, "change", detail)
			return why, "", nil
		}
	}
	return "", "", nil
}

// shapeRefusal is the error of a statement for the copy that reads a table
// whose columns on the source are not its snapshot's, nil when the copy may
// run it. The statement then goes to the source (fallTo); where it cannot go
// there, it is refused. When the source could not be asked, the copy runs
// it, and its notice waits in shapeNote for the copy's answer (runFree).
func (s *session) shapeRefusal(ctx context.Context, text string) *pgErr {
	why, unchecked, perr := s.copyShapeDiffers(ctx, text)
	if perr != nil {
		return perr
	}
	s.shapeNote = unchecked
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
// that name), "" when it is. why is for the client and names the table
// alone, never a column: the snapshot's shape holds the columns the server
// hides too. The table may be named: a table the server hides from the
// connection is not in its catalog, so never among t. detail says which
// columns changed, for the log.
func copyShapeWhy(t *CatalogTable, now SourceShape, found bool) (why, detail string) {
	name := t.Schema + "." + t.Name
	switch {
	case !found:
		why = name + " is not on your PostgreSQL under that name any more (dropped, or renamed), and the copy still holds it as its snapshot read it"
		return why, "not on the source"
	case t.Recorded == nil:
		why = "the copy's snapshot does not record the columns of " + name + ", so they cannot be compared with your PostgreSQL's (its next snapshot records them)"
		return why, "no shape recorded"
	case now.OID != t.Recorded.OID:
		why = name + " on your PostgreSQL is not the table the copy's snapshot was read from: it was dropped and created again"
		return why, fmt.Sprintf("OID %d on the source, %d in the snapshot", now.OID, t.Recorded.OID)
	}
	if diff := baseline.PGColumnsDiff(t.Recorded.Columns, now.Columns); diff != "" {
		why = "the columns of " + name + " on your PostgreSQL are not the ones of the copy's snapshot: PostgreSQL does not announce an ALTER TABLE to capture, so the copy keeps the old columns until the table's next snapshot"
		return why, diff
	}
	return "", ""
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
