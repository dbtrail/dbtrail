package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
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
// even if that table has been renamed since. Several
// OIDs can share a name (a table dropped and created again); all are returned,
// and the newest by snapshot is the one that currently holds the name.
//
// MySQL snapshots (no pg_type_oid) are ignored. An empty index returns an
// empty slice and no error.
func LoadPGShapes(ctx context.Context, db *sql.DB) ([]PGShape, error) {
	ctx, cancel := context.WithTimeout(ctx, latestPerTableLoadTimeout)
	defer cancel()
	rows, err := db.QueryContext(ctx, `
		SELECT s.snapshot_id, COALESCE(s.pg_relation_oid, 0), s.schema_name, s.table_name,
		       s.column_name, s.ordinal_position, s.column_key,
		       COALESCE(s.pg_type_oid, 0), COALESCE(s.pg_type_mod, -1),
		       s.is_generated, s.is_identity_always
		FROM schema_snapshots s
		JOIN (
			SELECT MAX(snapshot_id) AS snapshot_id FROM schema_snapshots
			WHERE pg_relation_oid IS NOT NULL
			GROUP BY pg_relation_oid
			UNION
			SELECT MAX(l.snapshot_id) FROM schema_snapshots l
			WHERE l.pg_relation_oid IS NULL AND l.pg_type_oid IS NOT NULL
			GROUP BY l.schema_name, l.table_name
			-- Superseded by ANY later identified row of the name, not only by
			-- the newest one per OID: that table may have been renamed since.
			HAVING MAX(l.snapshot_id) > COALESCE((
				SELECT MAX(i.snapshot_id) FROM schema_snapshots i
				WHERE i.pg_relation_oid IS NOT NULL
				  AND i.schema_name = l.schema_name AND i.table_name = l.table_name), 0)
		) newest ON newest.snapshot_id = s.snapshot_id
		ORDER BY s.snapshot_id, s.ordinal_position`)
	if err != nil {
		return nil, fmt.Errorf("metadata: load PostgreSQL shapes: %w", err)
	}
	defer rows.Close()

	var shapes []PGShape
	for rows.Next() {
		var (
			id      int
			oid     uint32
			c       PGRelationColumn
			key     string
			sch, tb string
		)
		if err := rows.Scan(&id, &oid, &sch, &tb, &c.Name, &c.Ordinal, &key,
			&c.TypeOID, &c.TypeMod, &c.IsGenerated, &c.IsIdentityAlways); err != nil {
			return nil, fmt.Errorf("metadata: scan PostgreSQL shape: %w", err)
		}
		c.IsPK = key == "PRI"
		if n := len(shapes); n == 0 || shapes[n-1].SnapshotID != id {
			shapes = append(shapes, PGShape{SnapshotID: id, Relation: PGRelationSchema{OID: oid, Schema: sch, Table: tb}})
		}
		last := &shapes[len(shapes)-1].Relation
		last.Columns = append(last.Columns, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("metadata: load PostgreSQL shapes: %w", err)
	}
	return latestPGShapes(shapes), nil
}

// latestPGShapes drops each legacy (OID-less) shape that a newer identified
// shape of the same (schema, table) supersedes, and orders the rest by
// snapshot. See LoadPGShapes. The query already drops a legacy shape
// superseded by an identified row that has since been renamed away (which
// this function cannot see); this is the same rule over what it returns.
func latestPGShapes(in []PGShape) []PGShape {
	type name struct{ schema, table string }
	newestIdentified := map[name]int{}
	for _, s := range in {
		if s.Relation.OID != 0 {
			n := name{s.Relation.Schema, s.Relation.Table}
			newestIdentified[n] = max(newestIdentified[n], s.SnapshotID)
		}
	}
	out := make([]PGShape, 0, len(in))
	for _, s := range in {
		if s.Relation.OID == 0 && newestIdentified[name{s.Relation.Schema, s.Relation.Table}] > s.SnapshotID {
			continue
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SnapshotID < out[j].SnapshotID })
	return out
}
