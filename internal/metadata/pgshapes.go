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

// LoadPGShapes returns what the index holds of each PostgreSQL table's
// shapes for DDL inference (#2441), ordered by snapshot.
//
// The stream compares each Relation with the shape its table had just before
// it in the stream. After a restart PostgreSQL resends transactions from the
// slot's confirmed position, which can be before shapes this index already
// holds, so for each OID this returns every shape seen at or after from,
// plus the newest one seen before it (the shape in effect where the resend
// starts). The capture passes the slot's confirmed position, which is never
// 0 (a new slot reports its consistent point); from 0 returns only the newest
// shape per OID.
//
// Shapes written before #2441 carry no OID and are keyed by (schema, table);
// such a shape is returned only when it is the newest of its exact name and
// no later shape with an OID was written under that name, even if that table
// has been renamed since. Names compare exactly, as in PostgreSQL, never by
// the index's collation. Shapes with an OID but no recorded position count as
// seen at position 0.
//
// MySQL snapshots (no pg_type_oid) are ignored. An empty index returns an
// empty slice and no error.
func LoadPGShapes(ctx context.Context, db *sql.DB, from uint64) ([]PGShape, error) {
	ctx, cancel := context.WithTimeout(ctx, latestPerTableLoadTimeout)
	defer cancel()

	// One row per PostgreSQL snapshot (WritePGSnapshot writes one table per
	// snapshot_id). Grouping by snapshot_id, never by name, keeps the
	// index's collation out of it: it ignores case and trailing spaces,
	// while PostgreSQL's "A" and a are two tables. Names are compared in Go.
	rows, err := db.QueryContext(ctx, `
		SELECT snapshot_id, MIN(pg_relation_oid), MAX(pg_relation_oid),
		       MIN(schema_name), MIN(table_name), MAX(pg_seen_lsn), MAX(pg_seen_at)
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
			seenLSN    sql.NullInt64
			seenAt     sql.NullTime
		)
		if err := rows.Scan(&id, &minOID, &maxOID, &sch, &table, &seenLSN, &seenAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("metadata: scan PostgreSQL shape list: %w", err)
		}
		if minOID != maxOID {
			rows.Close()
			return nil, fmt.Errorf("metadata: PostgreSQL snapshot %d has rows with different table OIDs", id)
		}
		rel := PGRelationSchema{OID: uint32(minOID.Int64), Schema: sch, Table: table}
		// A position with no time: seen while DDL inference was off.
		if seenLSN.Valid {
			rel.SeenLSN = uint64(seenLSN.Int64)
			if seenAt.Valid {
				rel.SeenAt = seenAt.Time.UTC()
			}
		}
		heads = append(heads, PGShape{SnapshotID: id, Relation: rel})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("metadata: list PostgreSQL shapes: %w", err)
	}
	rows.Close()

	keep := latestPGShapes(heads, from)
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

// latestPGShapes picks, from one entry per stored snapshot, the shapes
// LoadPGShapes returns (see there). Output is ordered by snapshot.
func latestPGShapes(in []PGShape, from uint64) []PGShape {
	type name struct{ schema, table string }
	byOID := map[uint32][]PGShape{}
	newestLegacy := map[name]PGShape{}
	newestIdentified := map[name]int{}
	for _, s := range in {
		n := name{s.Relation.Schema, s.Relation.Table}
		if s.Relation.OID != 0 {
			byOID[s.Relation.OID] = append(byOID[s.Relation.OID], s)
			newestIdentified[n] = max(newestIdentified[n], s.SnapshotID)
			continue
		}
		if cur, ok := newestLegacy[n]; !ok || s.SnapshotID > cur.SnapshotID {
			newestLegacy[n] = s
		}
	}
	var out []PGShape
	for _, versions := range byOID {
		// Stream order: by position seen, then by snapshot.
		sort.Slice(versions, func(i, j int) bool {
			a, b := versions[i].Relation.SeenLSN, versions[j].Relation.SeenLSN
			if a != b {
				return a < b
			}
			return versions[i].SnapshotID < versions[j].SnapshotID
		})
		// The newest version inference compared that was seen before from;
		// versions seen while inference was off (a position, no time) are
		// never a reference, but those after it are kept: they mark where
		// the index cannot bound a change.
		base := -1
		for i, v := range versions {
			if (from == 0 || v.Relation.SeenLSN < from) && !(v.Relation.SeenLSN != 0 && v.Relation.SeenAt.IsZero()) {
				base = i
			}
		}
		if from == 0 {
			out = append(out, versions[len(versions)-1])
			continue
		}
		start := max(base, 0)
		out = append(out, versions[start:]...)
	}
	for n, s := range newestLegacy {
		if newestIdentified[n] < s.SnapshotID {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SnapshotID < out[j].SnapshotID })
	return out
}
