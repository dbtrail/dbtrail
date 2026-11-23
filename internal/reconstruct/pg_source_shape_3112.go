package reconstruct

import (
	"context"
	"fmt"
	"strings"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// A change of columns that no write followed (#3112).
//
// PostgreSQL's change stream describes a table when it next carries a change
// of it, and the shapes a fold checks (checkPGShape) are those descriptions.
// Between an ALTER TABLE and the table's next write there is none, so a fold
// in that window finds the stream and the snapshot's file in agreement and
// publishes the old columns: a dropped column is still answered, an added one
// is missing. The MySQL fold does not have the window: it compares the file's
// CREATE TABLE with the newest schema snapshot, which is read from the source.
//
// The scheduled snapshot update closes it by handing the fold what the
// source's catalog says each table's columns are now (WithPGSourceShapes). A
// table whose file records other columns is refused the way a change the
// stream did record is refused. Only a fold that brings a copy up to now is
// handed them: a restore to an earlier point, or a fold from the command
// line, has no business comparing with the source as it is today.

// PGTableName names a PostgreSQL table by its two parts, so a dot inside a
// name cannot make two tables one.
type PGTableName struct{ Schema, Table string }

// PGSourceShape is a table's shape on the source now: its OID and the columns
// a snapshot file would store, in attnum order (Name, TypeOID, TypeMod and
// Ordinal are read).
type PGSourceShape struct {
	OID     uint32
	Columns []baseline.PGColumn
}

type pgSourceShapesKey struct{}

// WithPGSourceShapes returns ctx carrying the source's shapes for a fold to
// compare each table's file with. A nil map carries nothing (the source was
// not asked or did not answer): the fold then compares nothing, as before.
// A table absent from a non-nil map is not compared either: it is no longer
// among the tables a snapshot would read (dropped, or taken out of the
// publication), which the columns of a table say nothing about.
func WithPGSourceShapes(ctx context.Context, shapes map[PGTableName]PGSourceShape) context.Context {
	if shapes == nil {
		return ctx
	}
	return context.WithValue(ctx, pgSourceShapesKey{}, shapes)
}

// PGSourceShapesIn returns the shapes WithPGSourceShapes put on ctx, nil when
// it carries none.
func PGSourceShapesIn(ctx context.Context) map[PGTableName]PGSourceShape {
	shapes, _ := ctx.Value(pgSourceShapesKey{}).(map[PGTableName]PGSourceShape)
	return shapes
}

// checkPGSourceShape refuses a fold of schema.table when ctx carries the
// source's shape of it and that shape is not the one its file records (rel).
func checkPGSourceShape(ctx context.Context, schema, table string, rel baseline.PGRelation) error {
	now, ok := PGSourceShapesIn(ctx)[PGTableName{schema, table}]
	if !ok {
		return nil
	}
	name := schema + "." + table
	if now.OID != rel.OID {
		return fmt.Errorf("%s: the table on the source (OID %d) is not the one the snapshot was read from (OID %d): it was dropped and created again; %s",
			name, now.OID, rel.OID, pgRebaseline)
	}
	if diff := pgSourceShapeDiff(rel.Columns, now.Columns); diff != "" {
		return fmt.Errorf("%s: the table's columns on the source are not the snapshot's (%s); %s",
			name, diff, pgRebaseline)
	}
	return nil
}

// pgSourceShapeDiff says how the source's columns differ from the file's, or
// "" when they do not. Columns are matched by name and compared by type and
// modifier, as pgShapeDiff compares a streamed shape, and by attnum where the
// file recorded one (Ordinal 0: a file written before it was): a column
// dropped and added again under the same name and type has another attnum,
// and none of the values the file holds for it.
func pgSourceShapeDiff(file, source []baseline.PGColumn) string {
	want := make(map[string]baseline.PGColumn, len(file))
	for _, c := range file {
		want[c.Name] = c
	}
	got := map[string]bool{}
	var added, dropped, retyped, recreated []string
	for _, c := range source {
		got[c.Name] = true
		w, ok := want[c.Name]
		switch {
		case !ok:
			added = append(added, c.Name)
		case w.TypeOID != c.TypeOID || w.TypeMod != c.TypeMod:
			retyped = append(retyped, c.Name)
		case w.Ordinal != 0 && c.Ordinal != 0 && w.Ordinal != c.Ordinal:
			recreated = append(recreated, c.Name)
		}
	}
	for _, c := range file {
		if !got[c.Name] {
			dropped = append(dropped, c.Name)
		}
	}
	var parts []string
	if len(added) > 0 {
		parts = append(parts, "added "+strings.Join(added, ", "))
	}
	if len(dropped) > 0 {
		parts = append(parts, "dropped "+strings.Join(dropped, ", "))
	}
	if len(retyped) > 0 {
		parts = append(parts, "retyped "+strings.Join(retyped, ", "))
	}
	if len(recreated) > 0 {
		parts = append(parts, "dropped and added again "+strings.Join(recreated, ", "))
	}
	return strings.Join(parts, "; ")
}
