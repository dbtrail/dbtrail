package pgbaseline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
)

// tableInfo is one table selected for the baseline, discovered from
// pg_publication_tables — the SAME view that defines what pgoutput streams, so
// the baseline's table naming matches the delta path's by construction:
// publish_via_partition_root=true lists (and streams) the partitioned PARENT;
// false lists (and streams) the LEAF partitions. Either way the name here is
// the name deltas arrive under, which is what makes the reconstruct merge join.
type tableInfo struct {
	Schema string
	Table  string
	// PartitionParent marks a partitioned table parent (relkind='p', i.e. the
	// publication has publish_via_partition_root=true). COPY (SELECT ...) on
	// the parent reads all partitions under the parent name — correct — but
	// the operator must not flip pubviaroot between baseline and stream, or
	// the names silently stop joining; discovery warns loudly on these.
	PartitionParent bool
	// LeafPartition marks a leaf of a partitioned table streamed under its own
	// name (pubviaroot=false). Same flip warning applies.
	LeafPartition bool

	// OID is the table's pg_class OID, read in the same discovery query
	// (#1596 slice 1). It equals the RelationID pgoutput puts on every
	// message for this table and survives a rename (#2441).
	OID uint32

	// Columns are the live, non-dropped, non-generated columns in attnum
	// order, resolved inside the snapshot transaction (loadColumns).
	Columns []string
	// Shape is the same column list with each column's type (#1596 slice
	// 1), recorded under baseline.MetaKeyPGRelation, with what a CREATE
	// TABLE needs beyond it (slice 2): Generated and PrimaryKey too.
	Shape      []baseline.PGColumn
	Generated  []baseline.PGGeneratedColumn
	PrimaryKey []string
}

// discoverTables lists the tables the publication streams, narrowed by the
// client-side schema/table filters. It must run INSIDE the snapshot
// transaction so the discovered set is consistent with the copied data.
func discoverTables(ctx context.Context, conn *pgx.Conn, publication string, filters event.Filters, logger *slog.Logger) ([]tableInfo, error) {
	// Existence first, so an absent publication gets the actionable message
	// rather than an empty-set one (mirrors pgcapture.validatePublication).
	var allTables bool
	err := conn.QueryRow(ctx, `SELECT puballtables FROM pg_publication WHERE pubname = $1`, publication).Scan(&allTables)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("pgbaseline: publication %q does not exist — create it (CREATE PUBLICATION) covering the tables to capture", publication)
	}
	if err != nil {
		return nil, fmt.Errorf("pgbaseline: checking publication %q: %w", publication, err)
	}

	rows, err := conn.Query(ctx, `
		SELECT pt.schemaname, pt.tablename, c.relkind = 'p', c.relispartition, c.oid
		FROM pg_publication_tables pt
		JOIN pg_namespace n ON n.nspname = pt.schemaname
		JOIN pg_class c ON c.relname = pt.tablename AND c.relnamespace = n.oid
		WHERE pt.pubname = $1
		ORDER BY pt.schemaname, pt.tablename`, publication)
	if err != nil {
		return nil, fmt.Errorf("pgbaseline: listing tables of publication %q: %w", publication, err)
	}
	defer rows.Close()

	var tables []tableInfo
	for rows.Next() {
		var t tableInfo
		if err := rows.Scan(&t.Schema, &t.Table, &t.PartitionParent, &t.LeafPartition, &t.OID); err != nil {
			return nil, fmt.Errorf("pgbaseline: scanning tables of publication %q: %w", publication, err)
		}
		if !filters.Matches(t.Schema, t.Table) {
			continue
		}
		tables = append(tables, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgbaseline: listing tables of publication %q: %w", publication, err)
	}

	warnPartitioned(tables, publication, logger)
	return tables, nil
}

// warnPartitioned surfaces the partitioned-table naming contract (#593 sharp
// edge, mirroring the TimescaleDB chunk warn in pgcapture): the baseline
// stores each table under the name pg_publication_tables lists — the same
// name pgoutput streams deltas under — so baseline and deltas join today.
// What silently breaks the join is CHANGING publish_via_partition_root
// between the baseline and the stream (parent-named baseline vs leaf-named
// deltas, or vice versa: the merge finds no rows and reconstruct degrades to
// baseline-only without an error). One warning per shape, not per table.
func warnPartitioned(tables []tableInfo, publication string, logger *slog.Logger) {
	var parents, leaves []string
	for _, t := range tables {
		if t.PartitionParent {
			parents = append(parents, t.Schema+"."+t.Table)
		}
		if t.LeafPartition {
			leaves = append(leaves, t.Schema+"."+t.Table)
		}
	}
	sort.Strings(parents)
	sort.Strings(leaves)
	if len(parents) > 0 {
		logger.Warn("pgbaseline: partitioned table parent(s) in publication — baseline reads ALL partitions under the PARENT name (publish_via_partition_root=true); do NOT change publish_via_partition_root afterwards or deltas will arrive under leaf names and silently never join this baseline",
			"publication", publication, "tables", parents)
	}
	if len(leaves) > 0 {
		logger.Warn("pgbaseline: leaf partition(s) in publication — baseline stores each LEAF under its own name (publish_via_partition_root=false); do NOT change publish_via_partition_root afterwards or deltas will arrive under the parent name and silently never join this baseline",
			"publication", publication, "tables", leaves)
	}
}

// loadColumns resolves a table's column list inside the snapshot transaction,
// in attnum order: live (attnum>0), not dropped, and NOT generated.
//
// Generated-column decision (#593 sharp edge): GENERATED ALWAYS ... STORED
// columns are EXCLUDED from the baseline on purpose. On PG 14–17 pgoutput
// never publishes generated columns — the RelationMessage column list (which
// internal/pgcapture's decoder maps tuples through) omits them, so delta row
// images (row_before/row_after) do not carry them. Reconstruct's merge is
// last-write-wins on whole row images: had the baseline included a generated
// column, every TOUCHED row would lose it (row_after wins) while untouched
// rows kept it — a silently inconsistent column set. Excluding it here keeps
// baseline and delta column sets identical; the column is recomputed by
// PostgreSQL on any re-INSERT anyway (recovery already omits generated
// columns from reverse-INSERTs, #557). Dropped columns are excluded too,
// which also matches the delta path.
//
// Publication COLUMN LISTS (PG15+, `FOR TABLE t (a, b)`) would make the
// stream carry fewer columns than this list (INSERTs still go through on such
// a table). What keeps the two equal is capture, not this function: the
// capturer refuses any published table with a column list
// (pgcapture restrictedPublicationError), so no delta is ever indexed for one.
//
// The table is looked up by the OID discovery read, not by name, so the
// columns are those of the exact relation discovery listed. Each column comes
// back with its type as pgoutput describes it (atttypid, atttypmod) and as a
// CREATE TABLE spells it (format_type).
//
// With each column comes what a CREATE TABLE needs beyond it (#1596 slice
// 2): NOT NULL, the default and identity, the sequence it owns, and its
// attnum. The generated columns come back apart, with their expressions.
func loadColumns(ctx context.Context, conn *pgx.Conn, oid uint32, schema, table string, logger *slog.Logger) ([]baseline.PGColumn, []baseline.PGGeneratedColumn, error) {
	rows, err := conn.Query(ctx, `
		SELECT a.attname, a.attgenerated <> '', a.attgenerated = 'v', a.atttypid, a.atttypmod,
		       format_type(a.atttypid, a.atttypmod), a.attnotnull,
		       COALESCE(pg_get_expr(d.adbin, d.adrelid), ''), a.attidentity::text,
		       COALESCE(seq.name, ''), a.attnum,
		       COALESCE(format_type(s.seqtypid, NULL), ''), COALESCE(s.seqstart, 0), COALESCE(s.seqincrement, 0),
		       COALESCE(s.seqmin, 0), COALESCE(s.seqmax, 0), COALESCE(s.seqcache, 0), COALESCE(s.seqcycle, false)
		FROM pg_attribute a
		LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		CROSS JOIN LATERAL (SELECT pg_get_serial_sequence(a.attrelid::regclass::text, a.attname) AS name) seq
		LEFT JOIN pg_sequence s ON s.seqrelid = seq.name::regclass
		WHERE a.attrelid = $1
		  AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum`, oid)
	if err != nil {
		return nil, nil, fmt.Errorf("pgbaseline: listing columns of %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	var cols []baseline.PGColumn
	var gens []baseline.PGGeneratedColumn
	var generated []string
	for rows.Next() {
		var c baseline.PGColumn
		var isGenerated, virtual bool
		if err := rows.Scan(&c.Name, &isGenerated, &virtual, &c.TypeOID, &c.TypeMod, &c.Type, &c.NotNull, &c.Default, &c.Identity, &c.Sequence, &c.Ordinal,
			&c.Seq.Type, &c.Seq.Start, &c.Seq.Increment, &c.Seq.Min, &c.Seq.Max, &c.Seq.Cache, &c.Seq.Cycle); err != nil {
			return nil, nil, fmt.Errorf("pgbaseline: scanning columns of %s.%s: %w", schema, table, err)
		}
		if isGenerated {
			generated = append(generated, c.Name)
			gens = append(gens, baseline.PGGeneratedColumn{Name: c.Name, Type: c.Type, Expr: c.Default, NotNull: c.NotNull, Ordinal: c.Ordinal, Virtual: virtual})
			continue
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("pgbaseline: listing columns of %s.%s: %w", schema, table, err)
	}
	if len(generated) > 0 {
		logger.Info("pgbaseline: excluding STORED generated column(s) — pgoutput does not stream them on PG 14–17, so the delta path never carries them; keeping them out of the baseline keeps the merged column set consistent (PostgreSQL recomputes them on re-INSERT)",
			"table", schema+"."+table, "columns", generated)
	}
	if len(cols) == 0 {
		return nil, nil, fmt.Errorf("pgbaseline: table %s.%s has no copyable columns (all dropped or generated)", schema, table)
	}
	return cols, gens, nil
}

// loadPrimaryKey names a table's primary key columns in key order (not table
// order); nil when it has none. Read in the snapshot transaction (#1596
// slice 2).
func loadPrimaryKey(ctx context.Context, conn *pgx.Conn, oid uint32, schema, table string) ([]string, error) {
	rows, err := conn.Query(ctx, `
		SELECT a.attname
		FROM pg_index i
		CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
		WHERE i.indrelid = $1 AND i.indisprimary
		ORDER BY k.ord`, oid)
	if err != nil {
		return nil, fmt.Errorf("pgbaseline: reading the primary key of %s.%s: %w", schema, table, err)
	}
	defer rows.Close()
	var key []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, fmt.Errorf("pgbaseline: reading the primary key of %s.%s: %w", schema, table, err)
		}
		key = append(key, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgbaseline: reading the primary key of %s.%s: %w", schema, table, err)
	}
	return key, nil
}
