package pgshim

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

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
// it free, a question it refused), or a table's snapshot records no shape
// (one last read whole before shapes were recorded, carried forward since),
// the copy answers as it would without the check, with a NOTICE that says
// so and why, counted apart from the statement's reason (RouteNote.
// ShapeUnchecked): the copy stays a way to read while the source is away,
// and an upgrade does not send a whole server to the source.
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

// ShapeCheckError is a failure of Binding.SourceShapes with what the client
// may be told of its cause: Cause is words that name no column, no address
// and no account ("your PostgreSQL did not answer within 2 seconds"). A host
// error that is not one is told as the source could not be asked.
type ShapeCheckError struct {
	Cause string
	Err   error
}

func (e *ShapeCheckError) Error() string { return e.Err.Error() }
func (e *ShapeCheckError) Unwrap() error { return e.Err }

// What RouteNote.ShapeUnchecked says of a statement the copy answered
// without its tables' columns compared with the source's.
const (
	// ShapeUncheckedSourceNotAsked: the source could not be asked.
	ShapeUncheckedSourceNotAsked = "source_not_asked"
	// ShapeUncheckedNoShapeRecorded: a table's snapshot records no shape.
	ShapeUncheckedNoShapeRecorded = "no_shape_recorded"
)

// shapeCheck is what copyShapeDiffers found of a statement's tables.
type shapeCheck struct {
	// why says how a table is not on the source what its snapshot recorded,
	// "" when none differs.
	why string
	// notice, when not "", is for the client of a statement the copy answers
	// though its tables could not all be compared, and unchecked its kind
	// (ShapeUnchecked*).
	notice, unchecked string
}

// copyShapeDiffers compares the tables text reads with the source's (see
// above). refusal, when not nil, is the statement's error: the copy's
// catalog could not be read, or the statement was stopped meanwhile; the
// reason it is counted under is in busyRefused.
func (s *session) copyShapeDiffers(ctx context.Context, text string) (c shapeCheck, refusal *pgErr) {
	if s.bound == nil || s.bound.SourceShapes == nil || s.bound.Catalog == nil {
		return c, nil
	}
	toks, _ := lexSQL(text)
	switch firstWord(toks) {
	case "show", "describe", "explain", "summarize":
		// The copy's own commands (copy_command): they describe the copy, and
		// the source has none of them.
		return c, nil
	}
	v, perr := s.catalog()
	if perr != nil {
		if busy := s.catalogBusy; busy != "" {
			s.busyRefused = busy
			return c, busyRefusal(busy)
		}
		return c, perr
	}
	var asked, unrecorded []*CatalogTable
	for _, at := range shapeTablesRead(text, v.tables) {
		if t := &v.tables[at]; t.Recorded == nil {
			unrecorded = append(unrecorded, t)
		} else {
			asked = append(asked, t)
		}
	}
	var notes []string
	if len(asked) > 0 {
		names := make([][2]string, len(asked))
		for i, t := range asked {
			names[i] = [2]string{t.Schema, t.Name}
		}
		now, err := s.bound.SourceShapes(ctx, names)
		if perr := notCapturedStopped(ctx); perr != nil {
			return c, perr
		}
		if err != nil {
			// The error's own text (an address) goes to the log, not to the
			// client.
			warnEveryMinute(s.logger, "pgshim: shapes unread\x00"+s.bound.Actor, "pgshim: the columns of the tables a statement reads could not be read from the source; the copy answers, with a notice",
				"server", s.bound.Actor, "err", err)
			cause := "the source could not be asked"
			var ce *ShapeCheckError
			if errors.As(err, &ce) && ce.Cause != "" {
				cause = ce.Cause
			}
			notes = append(notes, "DBTrail could not check whether "+tableList(asked)+" changed on your PostgreSQL ("+cause+"; DBTrail's log has the details).")
			c.unchecked = ShapeUncheckedSourceNotAsked
		} else {
			for _, t := range asked {
				sh, found := now[[2]string{t.Schema, t.Name}]
				if why, detail := copyShapeWhy(t, sh, found); why != "" {
					// Which columns changed is the operator's, in the log: a
					// column the server hides from this client is in the
					// snapshot's shape too, and its name must not reach the
					// client.
					logEveryMinute(s.logger, "pgshim: shape differs\x00"+s.bound.Actor+"\x00"+t.Schema+"\x00"+t.Name,
						"pgshim: a table changed on the source since its snapshot; the copy does not answer statements that read it",
						"server", s.bound.Actor, "table", t.Schema+"."+t.Name, "change", detail)
					return shapeCheck{why: why}, nil
				}
			}
		}
	}
	if len(unrecorded) > 0 {
		notes = append(notes, "The copy's snapshot of "+tableList(unrecorded)+" does not record its shape, so it cannot be compared with your PostgreSQL's; a full read records it.")
		if c.unchecked == "" {
			c.unchecked = ShapeUncheckedNoShapeRecorded
		}
	}
	if len(notes) > 0 {
		c.notice = strings.Join(notes, " ") + " This was answered from the copy as of its snapshot."
	}
	return c, nil
}

// tableList names tables for a notice, schema and name.
func tableList(tables []*CatalogTable) string {
	out := make([]string, len(tables))
	for i, t := range tables {
		out[i] = t.Schema + "." + t.Name
	}
	return strings.Join(out, ", ")
}

// shapeRefusal is the error of a statement for the copy that reads a table
// whose columns on the source are not its snapshot's, nil when the copy may
// run it. The statement then goes to the source (fallTo); where it cannot go
// there, it is refused. When the tables could not all be compared, the copy
// runs it, and its notice waits in shapeNote until the copy's answer is the
// client's (commitShapeNote).
func (s *session) shapeRefusal(ctx context.Context, text string) *pgErr {
	c, perr := s.copyShapeDiffers(ctx, text)
	if perr != nil {
		return perr
	}
	s.shapeNote, s.shapeUnchecked = c.notice, c.unchecked
	if c.why == "" {
		return nil
	}
	if !s.canFallBack() {
		s.busyRefused = ReasonCopyShapeDiffers
		return shapeCopyOnlyError(c.why)
	}
	s.logger.Debug("pgshim: the statement reads a table changed on the source since its snapshot; the source answers", "why", c.why)
	s.fallTo(ReasonCopyShapeDiffers)
	return &pgErr{"55000", c.why}
}

// commitShapeNote sends the notice of a statement the copy answered without
// its tables compared, once that answer is the client's; its kind stays for
// the statement's note (routeNote). dropShapeNote forgets both, for a
// statement the copy did not answer after all.
func (s *session) commitShapeNote() {
	if s.shapeNote != "" {
		s.notice(s.shapeNote)
	}
	s.shapeNote = ""
}

func (s *session) dropShapeNote() { s.shapeNote, s.shapeUnchecked = "", "" }

// warnEveryMinute is logEveryMinute at warn level.
func warnEveryMinute(logger *slog.Logger, key, msg string, args ...any) {
	loggedAt.mu.Lock()
	if loggedAt.at == nil {
		loggedAt.at = map[string]time.Time{}
	}
	due := time.Since(loggedAt.at[key]) >= time.Minute
	if due {
		loggedAt.at[key] = time.Now()
	}
	loggedAt.mu.Unlock()
	if due {
		logger.Warn(msg+" (said once a minute)", args...)
	} else {
		logger.Debug(msg, args...)
	}
}

// shapeCopyOnlyError is the refusal of a statement on a connection that
// sends nothing to its source, which reads a changed table.
func shapeCopyOnlyError(why string) *pgErr {
	return &pgErr{"55000", why + ". This connection sends nothing to your PostgreSQL (the server hides tables or columns), so the statement is refused until the table's next snapshot records its columns"}
}

// copyShapeWhy says why the copy's table t, whose snapshot recorded its
// shape, is not that table on the source (now, found false when the source
// has no table of that name), "" when it is. why is for the client and names
// the table alone, never a column: the snapshot's shape holds the columns
// the server hides too. The table may be named: a table the server hides
// from the connection is not in its catalog, so never among t. detail says
// which columns changed, for the log.
func copyShapeWhy(t *CatalogTable, now SourceShape, found bool) (why, detail string) {
	name := t.Schema + "." + t.Name
	switch {
	case !found:
		why = name + " is not on your PostgreSQL under that name any more (dropped, or renamed), and the copy still holds it as its snapshot read it"
		return why, "not on the source"
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
