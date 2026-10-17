package pgcapture

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/metadata"
)

// relColumn is one column of a cached relation, in ordinal (tuple) order. It
// retains the pgoutput per-column type identity (typeOID/typeMod) even though #530
// stores every value in text form: the RelationMessage is the only in-band source
// of PostgreSQL column types, and discarding them here would leave a future
// type-faithful renderer (#533) with no oracle to key on. #530 itself does not
// interpret these fields.
type relColumn struct {
	name    string
	typeOID uint32
	typeMod int32
	// isIdentityAlways = GENERATED ALWAYS AS IDENTITY (pg_attribute.attidentity='a');
	// isGenerated = STORED generated column (attgenerated='s'). Both come from a catalog
	// lookup (the RelationMessage carries neither) and drive #557 recovery: a reverse-
	// INSERT omits generated columns (and emits OVERRIDING SYSTEM VALUE for identity),
	// a reverse-UPDATE SET omits BOTH (PostgreSQL rejects SET on either).
	isIdentityAlways bool
	isGenerated      bool
}

// relationInfo is the decoder's cached knowledge of one relation, keyed by its
// pgoutput relation OID. It is the PostgreSQL analog of a metadata.Resolver entry,
// but built entirely from the in-band RelationMessage (column names arrive in the
// stream) plus a one-time catalog primary-key lookup — no schema_snapshots needed.
type relationInfo struct {
	oid     uint32 // pg_class OID = RelationMessage.RelationID; survives a rename (#2441)
	schema  string
	table   string
	columns []relColumn           // ordinal order, matching the tuple column order
	pkCols  []metadata.ColumnMeta // primary-key columns in table-ordinal order (cacheRelation reorders the PKResolver's key-order result so pk_values aligns with the offline resolver)
}

// PKResolver returns the primary-key columns of a relation, in primary-key (indkey)
// KEY order — the catalog order, which for a composite PK declared out of column
// order differs from table-ordinal order. cacheRelation reorders the result to
// table-ordinal before caching, so the pk_values it builds align with the offline
// resolver's metadata.PKColumnMetas (also table-ordinal). Used to build
// event.Event.PKValues.
//
// It must source the PK from the catalog (pg_index.indisprimary), NOT from the
// RelationMessage's per-column "key" flag: under REPLICA IDENTITY FULL — the mode
// bintrail requires (#531) — pgoutput sends an empty key bitmap, so the flag would
// yield an empty PK for every table; under REPLICA IDENTITY USING INDEX it can mark
// a non-PK unique index.
//
// A relation with no primary key returns an empty slice — the event still indexes
// under an empty PK string, mirroring the MySQL path. A genuine lookup failure must
// return a non-nil error: the decoder fails loud rather than index rows under a
// wrong or missing PK.
//
// The capturer implements this with a catalog query on a second, non-replication
// connection (the replication connection is in CopyBoth streaming mode and cannot
// run queries); unit tests stub it.
type PKResolver func(relationID uint32, schema, table string) ([]metadata.ColumnMeta, error)

// pkColumnsQuery returns a relation's primary-key column names in key order, given
// the relation's OID (which equals the pgoutput RelationMessage RelationID).
//
// i.indkey is an int2vector; it is cast to int2[] before array_position because
// array_position is declared over anyarray/anyelement and int2vector is a distinct
// internal type whose implicit coercion to int2[] is not guaranteed across the
// supported version range (PG14+). The explicit cast removes that version-dependence
// — without it the ordering query could error on a floor-version server.
const pkColumnsQuery = `SELECT a.attname
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = $1 AND i.indisprimary
ORDER BY array_position(i.indkey::int2[], a.attnum)`

// queryPK is the catalog-backed PKResolver body: it looks up a relation's primary-
// key columns by OID on a regular (non-replication) connection. The capturer wires
// it into a PKResolver closure over its query conn and the Run context. An empty
// result (no primary key) returns an empty slice; any query/scan error is returned
// so the decoder fails loud rather than index under a wrong or missing PK.
func queryPK(ctx context.Context, conn *pgx.Conn, relationID uint32) ([]metadata.ColumnMeta, error) {
	rows, err := conn.Query(ctx, pkColumnsQuery, relationID)
	if err != nil {
		return nil, fmt.Errorf("pgcapture: query PK columns for relation OID %d: %w", relationID, err)
	}
	defer rows.Close()

	var cols []metadata.ColumnMeta
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("pgcapture: scan PK column for relation OID %d: %w", relationID, err)
		}
		cols = append(cols, metadata.ColumnMeta{Name: name, OrdinalPosition: len(cols) + 1, IsPK: true})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgcapture: iterate PK columns for relation OID %d: %w", relationID, err)
	}
	return cols, nil
}

// ColumnAttrs are the per-column catalog flags the RelationMessage does not carry,
// needed for #557 recovery correctness. The two flags are mutually exclusive: a
// PostgreSQL column is an identity column OR a generated column, never both.
type ColumnAttrs struct {
	IsIdentityAlways bool // GENERATED ALWAYS AS IDENTITY (attidentity='a')
	IsGenerated      bool // STORED generated column (attgenerated='s')
}

// AttrResolver returns the identity/generated flags for a relation's columns, keyed
// by column name. Catalog-backed in the capturer (pg_attribute), stubbed in tests.
// A genuine lookup failure must return a non-nil error so the decoder fails loud
// rather than index rows that recovery would generate un-runnable SQL for.
type AttrResolver func(relationID uint32, schema, table string) (map[string]ColumnAttrs, error)

// columnAttrsQuery reports, per live column of a relation, whether it is a
// GENERATED ALWAYS identity column and/or a STORED generated column. attnum>0 skips
// system columns; NOT attisdropped skips dropped ones. attidentity/attgenerated are
// available on every supported version (PG14+).
//
// Intentional scope: attgenerated='s' flags STORED only. PG18 adds VIRTUAL generated
// columns (attgenerated='v') and publish_generated_columns; those are out of scope for
// a PG14–16 beta. A VIRTUAL column is computed on read and never materialized, so it is
// not in the row image and a reverse INSERT/UPDATE would never reference it — but if
// VIRTUAL support lands, extend this predicate to attgenerated IN ('s','v').
const columnAttrsQuery = `SELECT attname, attidentity = 'a', attgenerated = 's'
FROM pg_attribute
WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped`

// queryColumnAttrs is the catalog-backed AttrResolver body, on a regular (non-
// replication) connection. The capturer wires it into a closure over its query conn
// and the Run context; unit tests stub the AttrResolver directly.
func queryColumnAttrs(ctx context.Context, conn *pgx.Conn, relationID uint32) (map[string]ColumnAttrs, error) {
	rows, err := conn.Query(ctx, columnAttrsQuery, relationID)
	if err != nil {
		return nil, fmt.Errorf("pgcapture: query column attrs for relation OID %d: %w", relationID, err)
	}
	defer rows.Close()

	attrs := make(map[string]ColumnAttrs)
	for rows.Next() {
		var name string
		var isIdentityAlways, isGenerated bool
		if err := rows.Scan(&name, &isIdentityAlways, &isGenerated); err != nil {
			return nil, fmt.Errorf("pgcapture: scan column attrs for relation OID %d: %w", relationID, err)
		}
		attrs[name] = ColumnAttrs{IsIdentityAlways: isIdentityAlways, IsGenerated: isGenerated}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgcapture: iterate column attrs for relation OID %d: %w", relationID, err)
	}
	return attrs, nil
}
