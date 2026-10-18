package reconstruct

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/metadata"
)

// Full-table reconstruct of a PostgreSQL source (#1596).
//
// A PostgreSQL baseline stores every column as PostgreSQL's own COPY text
// and records the table's shape (baseline.MetaKeyPGRelation) and an LSN
// floor (baseline.MetaKeyLSN) instead of a CREATE TABLE and a binlog
// coordinate. A fold of it keeps that form: the same text columns, the same
// shape, and an LSN where the next run starts.

// pgRebaseline is what every PostgreSQL shape refusal tells the operator.
const pgRebaseline = "take a new snapshot of the table (`bintrail-pg baseline`) and reconstruct from that one"

// rawTextColumns is the column list of a PostgreSQL fold: the baseline file's
// own columns, in its order, each stored as text the way pgbaseline wrote
// them.
func rawTextColumns(names []string) []baseline.Column {
	cols := make([]baseline.Column, len(names))
	for i, n := range names {
		cols[i] = baseline.Column{Name: n, RawText: true}
	}
	return cols
}

// checkPGFoldable refuses a PostgreSQL baseline a fold cannot start from:
// one with no LSN floor (taken before #593: the next run would have no
// anchor), no recorded shape (taken before #1596: nothing to check the
// table's columns against), or text rendered under other settings than the
// pinned ones (its primary keys would not join the deltas', and every
// update would land as a second copy of the row).
func checkPGFoldable(schema, table string, md baseline.DumpMetadata) (baseline.PGRelation, error) {
	name := schema + "." + table
	if md.LSN == 0 {
		return baseline.PGRelation{}, fmt.Errorf("%s: the snapshot records no LSN to continue from; %s", name, pgRebaseline)
	}
	rel, ok, err := baseline.ParsePGRelation(md.PGRelation)
	if err != nil {
		return baseline.PGRelation{}, fmt.Errorf("%s: the snapshot's recorded table shape cannot be read (%v); %s", name, err, pgRebaseline)
	}
	if !ok {
		return baseline.PGRelation{}, fmt.Errorf("%s: the snapshot records no table shape (it was taken by a version before this one); %s", name, pgRebaseline)
	}
	if md.RenderGUCs != baseline.RenderGUCsPinned {
		return baseline.PGRelation{}, fmt.Errorf("%s: the snapshot's values were rendered under other settings (%q) than the changes captured now (%q), so its rows would not match theirs; %s",
			name, md.RenderGUCs, baseline.RenderGUCsPinned, pgRebaseline)
	}
	return rel, nil
}

// checkPGShape refuses a fold of schema.table when the shapes the stream
// recorded, from the baseline's LSN floor up to the cut, are not the shape
// the baseline recorded (rel). shapes are metadata.LoadPGShapes from floor:
// every shape seen at or after floor, plus the one in effect at floor.
//
// Every recorded shape is an observation, whether DDL inference was on or
// off when it was seen: a change of columns always shows in the next
// description of the table, which the stream records either way. What
// refuses: a column added, dropped or retyped (type or modifier), the table
// renamed, and another table holding the name at or after the floor (a drop
// and a create). Another table that held the name before the floor and gave
// it up is history and does not count. A shape seen at or after cut is a
// change after the point being rebuilt. A shape stored before table OIDs were
// recorded (OID 0) is compared by name and columns only.
//
// A STORED generated column in a streamed shape is left out of the
// comparison: the baseline never holds one, and PostgreSQL 18 can stream it
// (publish_generated_columns).
func checkPGShape(schema, table string, rel baseline.PGRelation, floor, cut uint64, shapes []metadata.PGShape) error {
	name := schema + "." + table
	for _, s := range shapes {
		r := s.Relation
		if r.SeenLSN >= cut && r.SeenLSN != 0 {
			continue
		}
		sameName := r.Schema == schema && r.Table == table
		mine := r.OID == rel.OID || (r.OID == 0 && sameName)
		if !mine && !sameName {
			continue
		}
		if r.SeenLSN < floor && !inEffectAtFloor(shapes, s, floor, schema, table, rel.OID) {
			continue
		}
		if !mine {
			return fmt.Errorf("%s: a different table (OID %d, the snapshot's is %d) held this name between the snapshot and the requested point: the table was dropped and created again; %s",
				name, r.OID, rel.OID, pgRebaseline)
		}
		if !sameName {
			return fmt.Errorf("%s: the table was renamed to %s.%s before the requested point; %s", name, r.Schema, r.Table, pgRebaseline)
		}
		if diff := pgShapeDiff(rel.Columns, r.Columns); diff != "" {
			return fmt.Errorf("%s: the table's columns as the change stream recorded them, from the snapshot up to the requested point, are not the snapshot's (%s). "+
				"A change made shortly before the snapshot that no write followed reads the same way; %s",
				name, diff, pgRebaseline)
		}
	}
	return nil
}

// inEffectAtFloor reports whether s, seen before floor, is what held its
// part at floor: for one of the baseline's own shapes (or one with no OID),
// the newest of them before floor; for another table's shape under the
// baseline's name, whether it is newer than every shape of the baseline's own
// table before floor (the name was still the other table's there).
func inEffectAtFloor(shapes []metadata.PGShape, s metadata.PGShape, floor uint64, schema, table string, oid uint32) bool {
	r := s.Relation
	other := r.OID != oid && r.OID != 0
	for _, o := range shapes {
		q := o.Relation
		if q.SeenLSN >= floor || q.SeenLSN <= r.SeenLSN {
			continue
		}
		if !other && (q.OID == oid || (q.OID == 0 && q.Schema == schema && q.Table == table)) {
			return false
		}
		if other && (q.OID == oid || q.OID == r.OID) {
			return false
		}
	}
	return true
}

// pgShapeDiff says how a streamed shape differs from the baseline's, or ""
// when it does not.
func pgShapeDiff(base []baseline.PGColumn, seen []metadata.PGRelationColumn) string {
	type typ struct {
		oid uint32
		mod int32
	}
	want := make(map[string]typ, len(base))
	for _, c := range base {
		want[c.Name] = typ{c.TypeOID, c.TypeMod}
	}
	got := map[string]bool{}
	var added, dropped, retyped []string
	for _, c := range seen {
		if c.IsGenerated {
			continue
		}
		got[c.Name] = true
		w, ok := want[c.Name]
		switch {
		case !ok:
			added = append(added, c.Name)
		case w.oid != c.TypeOID || w.mod != c.TypeMod:
			retyped = append(retyped, c.Name)
		}
	}
	for _, c := range base {
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
	return strings.Join(parts, "; ")
}

// pgSnapshotFileMetadata is the footer of a PostgreSQL fold, built from an
// allowlist of what the MySQL builder wrote (so a key added there for MySQL
// never reaches a PostgreSQL footer) plus the PostgreSQL keys. Never a binlog
// coordinate, an event or DDL mark, or a CREATE TABLE: the next run would
// read an LSN as a binlog file name. The anchor is the cut (where the next
// run starts) or, with nothing folded, the source baseline's own floor; the
// shape and the rendering stamp carry over, since a fold re-renders nothing.
func pgSnapshotFileMetadata(in mergeInput, built map[string]string) map[string]string {
	md := map[string]string{}
	for _, k := range []string{baseline.MetaKeySnapshotTimestamp, "bintrail.source_database", "bintrail.source_table",
		"bintrail.bintrail_version", MetaKeySnapshotProducer, MetaKeyDerivedFrom, MetaKeyDerivedFromPath,
		baseline.MetaKeyCaptureGap, baseline.MetaKeyLockMode, baseline.MetaKeyLastDumpAt, baseline.MetaKeyFoldGeneration} {
		if v, ok := built[k]; ok {
			md[k] = v
		}
	}
	lsn := in.PGCutLSN
	if lsn == 0 {
		lsn = in.SourceBaseline.Metadata.LSN
	}
	md[baseline.MetaKeyLSN] = strconv.FormatUint(lsn, 10)
	if v := in.SourceBaseline.Metadata.PGRelation; v != "" {
		md[baseline.MetaKeyPGRelation] = v
	}
	if v := in.SourceBaseline.Metadata.RenderGUCs; v != "" {
		md[baseline.MetaKeyRenderGUCs] = v
	}
	if !in.PGDeltaSince.IsZero() {
		md[baseline.MetaKeyPGDeltaSince] = in.PGDeltaSince.UTC().Format(time.RFC3339)
	} else if v := in.SourceBaseline.Metadata.PGDeltaSince; v != "" {
		md[baseline.MetaKeyPGDeltaSince] = v
	}
	return md
}

// checkPGFileColumns refuses a PostgreSQL baseline whose file does not hold
// exactly the columns its recorded shape names: the shape check trusts the
// shape, so the two must agree.
func checkPGFileColumns(rel baseline.PGRelation, colNames []string, schema, table string) error {
	inShape := make(map[string]bool, len(rel.Columns))
	for _, c := range rel.Columns {
		inShape[c.Name] = true
	}
	inFile := make(map[string]bool, len(colNames))
	var onlyFile []string
	for _, n := range colNames {
		inFile[n] = true
		if !inShape[n] {
			onlyFile = append(onlyFile, n)
		}
	}
	var onlyShape []string
	for _, c := range rel.Columns {
		if !inFile[c.Name] {
			onlyShape = append(onlyShape, c.Name)
		}
	}
	if len(onlyFile) == 0 && len(onlyShape) == 0 {
		return nil
	}
	return fmt.Errorf("%s.%s: the snapshot file and its recorded table shape disagree (only in the shape: %s; only in the file: %s); %s",
		schema, table, strings.Join(onlyShape, ", "), strings.Join(onlyFile, ", "), pgRebaseline)
}
