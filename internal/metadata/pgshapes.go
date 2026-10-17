package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// PGShape is one PostgreSQL relation shape as last persisted by
// WritePGSnapshot, with the snapshot that holds it.
type PGShape struct {
	SnapshotID int
	Relation   PGRelationSchema
}

// LoadPGShapes returns the last shape the stream persisted for every
// PostgreSQL relation in the index, oldest snapshot first (#2441). It is what
// the decoder compares the next Relation message against, so a restart does
// not start from nothing and a DDL made while capture was down is still seen.
//
// Shapes written since #2441 are identified by OID: the newest per OID is
// returned, so a renamed table appears once, under its newest name. Shapes
// written before carry no OID and are keyed by (schema, table); such a shape
// is dropped once any newer identified shape was written under the same name,
// even if that table has been renamed since. Names compare exactly, as in
// PostgreSQL, never by the index's collation. Several
// OIDs can share a name (a table dropped and created again); all are returned,
// and the newest by snapshot is the one that currently holds the name.
//
// MySQL snapshots (no pg_type_oid) are ignored. An empty index returns an
// empty slice and no error.
func LoadPGShapes(ctx context.Context, db *sql.DB) ([]PGShape, error) {
	ctx, cancel := context.WithTimeout(ctx, latestPerTableLoadTimeout)
	defer cancel()

	// One row per PostgreSQL snapshot (WritePGSnapshot writes one table per
	// snapshot_id). Grouping by snapshot_id, never by name, keeps the
	// index's collation out of it: it ignores case and trailing spaces,
	// while PostgreSQL's "A" and a are two tables. Names are compared in Go.
	rows, err := db.QueryContext(ctx, `
		SELECT snapshot_id, MIN(pg_relation_oid), MAX(pg_relation_oid),
		       MIN(schema_name), MIN(table_name)
		FROM schema_snapshots
		WHERE pg_type_oid IS NOT NULL
		GROUP BY snapshot_id`)
	if err != nil {
		return nil, fmt.Errorf("metadata: list PostgreSQL shapes: %w", err)
	}
	var heads []PGShape
	for rows.Next() {
		var (
			id         int
			minOID     sql.NullInt64
			maxOID     sql.NullInt64
			sch, table string
		)
		if err := rows.Scan(&id, &minOID, &maxOID, &sch, &table); err != nil {
			rows.Close()
			return nil, fmt.Errorf("metadata: scan PostgreSQL shape list: %w", err)
		}
		if minOID != maxOID {
			rows.Close()
			return nil, fmt.Errorf("metadata: PostgreSQL snapshot %d has rows with different table OIDs", id)
		}
		heads = append(heads, PGShape{SnapshotID: id, Relation: PGRelationSchema{OID: uint32(minOID.Int64), Schema: sch, Table: table}})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("metadata: list PostgreSQL shapes: %w", err)
	}
	rows.Close()

	keep := latestPGShapes(heads)
	for i := 0; i < len(keep); i += pgShapeChunk {
		if err := loadPGShapeColumns(ctx, db, keep[i:min(len(keep), i+pgShapeChunk)]); err != nil {
			return nil, err
		}
	}
	return keep, nil
}

// pgShapeChunk bounds the IN list of one column query.
const pgShapeChunk = 500

// loadPGShapeColumns fills the columns of each shape, in ordinal order. A
// column without its type OID or modifier is an error naming the snapshot:
// -1 is a real "no modifier", so a NULL read as -1 would compare as a
// legitimate type.
func loadPGShapeColumns(ctx context.Context, db *sql.DB, shapes []PGShape) error {
	byID := make(map[int]*PGShape, len(shapes))
	args := make([]any, len(shapes))
	for i := range shapes {
		byID[shapes[i].SnapshotID] = &shapes[i]
		args[i] = shapes[i].SnapshotID
	}
	rows, err := db.QueryContext(ctx, `
		SELECT snapshot_id, column_name, ordinal_position, column_key,
		       pg_type_oid, pg_type_mod, is_generated, is_identity_always
		FROM schema_snapshots
		WHERE snapshot_id IN (`+strings.TrimSuffix(strings.Repeat("?,", len(shapes)), ",")+`)
		ORDER BY snapshot_id, ordinal_position`, args...)
	if err != nil {
		return fmt.Errorf("metadata: load PostgreSQL shape columns: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id      int
			c       PGRelationColumn
			key     string
			typeOID sql.NullInt64
			typeMod sql.NullInt32
		)
		if err := rows.Scan(&id, &c.Name, &c.Ordinal, &key, &typeOID, &typeMod, &c.IsGenerated, &c.IsIdentityAlways); err != nil {
			return fmt.Errorf("metadata: scan PostgreSQL shape column: %w", err)
		}
		if !typeOID.Valid || !typeMod.Valid {
			return fmt.Errorf("metadata: PostgreSQL snapshot %d column %q has no type OID or modifier", id, c.Name)
		}
		c.TypeOID, c.TypeMod, c.IsPK = uint32(typeOID.Int64), typeMod.Int32, key == "PRI"
		sh := byID[id]
		sh.Relation.Columns = append(sh.Relation.Columns, c)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("metadata: load PostgreSQL shape columns: %w", err)
	}
	for _, sh := range shapes {
		if len(sh.Relation.Columns) == 0 {
			return fmt.Errorf("metadata: PostgreSQL snapshot %d has no columns", sh.SnapshotID)
		}
	}
	return nil
}

// latestPGShapes picks, from one entry per stored snapshot, the shapes that
// are still the last one seen: the newest per OID, and each OID-less (legacy)
// shape that is the newest of its exact (schema, table) and that no later
// identified snapshot was written under. Names compare exactly, as
// PostgreSQL does. Output is ordered by snapshot.
func latestPGShapes(in []PGShape) []PGShape {
	type name struct{ schema, table string }
	newestByOID := map[uint32]PGShape{}
	newestLegacy := map[name]PGShape{}
	newestIdentified := map[name]int{}
	for _, s := range in {
		n := name{s.Relation.Schema, s.Relation.Table}
		if s.Relation.OID != 0 {
			if cur, ok := newestByOID[s.Relation.OID]; !ok || s.SnapshotID > cur.SnapshotID {
				newestByOID[s.Relation.OID] = s
			}
			newestIdentified[n] = max(newestIdentified[n], s.SnapshotID)
			continue
		}
		if cur, ok := newestLegacy[n]; !ok || s.SnapshotID > cur.SnapshotID {
			newestLegacy[n] = s
		}
	}
	out := make([]PGShape, 0, len(newestByOID)+len(newestLegacy))
	for _, s := range newestByOID {
		out = append(out, s)
	}
	for n, s := range newestLegacy {
		if newestIdentified[n] < s.SnapshotID {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SnapshotID < out[j].SnapshotID })
	return out
}
