package pgcapture

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

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
	// inIdentity reports that the column is one a key-only old tuple carries a
	// real value for. Below FULL it is what the RelationMessage flagged as part
	// of the replica identity (RelationMessageColumn.Flags bit 1): the primary
	// key under DEFAULT, the index's key columns under USING INDEX, none under
	// NOTHING (pinned on a live server by
	// TestPgoutputReplicaIdentityWire_Integration). Under FULL the message flags
	// every column, so the primary-key columns are taken instead (cacheRelation).
	inIdentity bool
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

	// replicaIdentity is the relation's REPLICA IDENTITY as this description
	// gave it: 'f' full, 'd' default, 'i' using index, 'n' nothing.
	replicaIdentity uint8

	// partial reports that the earlier row of every UPDATE and DELETE of the
	// relation is captured in part (decoder.go, partialBefore): the relation is
	// described below FULL.
	partial bool
	// byShape reports that an old tuple of the relation is taken as whole or as
	// key-only ROW BY ROW, by what it holds (decoder.go, holdsEmpty): the
	// relation is described at FULL, and is a partitioned table or one the
	// catalog did not answer for (Decoder.judgePartitions).
	byShape bool
	// partitioned reports that the catalog answered that the relation is a
	// partitioned table.
	partitioned bool
	// partsRetryAt, when not zero, is when the read of the relation's
	// partitions, which failed, is tried again (Decoder.relationFor).
	partsRetryAt time.Time
	// unsureStored counts the changes of the relation stored while
	// partsRetryAt was set, the first at commit LSN unsureFrom: should the
	// catalog then show that the table had to be skipped, the ledger is told
	// of that stretch.
	unsureStored int
	unsureFrom   uint64
	// keyNotSent counts the changes of the relation dropped under
	// SkipReplicaIdentityKeyNotSent, to space their warning.
	keyNotSent int
	// notFullDropped counts the changes of the relation dropped by
	// Decoder.dropBelowFull, to space their warning.
	notFullDropped int

	// skipped marks a relation whose row changes are dropped and recorded in the
	// capture-skip ledger instead of indexed, under skipReason with skipDetail:
	// a primary key declared DEFERRABLE, or a replica identity that does not
	// hold the primary key. Only oid/schema/table/replicaIdentity are set
	// beside them (no columns, no PK: nothing of it is decoded). The next
	// RelationMessage for the same OID replaces the entry, so a table that was
	// fixed resumes by itself.
	skipped    bool
	skipReason string
	skipDetail string
}

// PartitionsResolver says what the catalog knows of a relation's partitions.
//
// It exists for one case the stream cannot show. A partitioned table published
// through its root (publish_via_partition_root) is described with the ROOT's
// identity, and pgoutput labels the old tuple of every change by that
// description: 'O' (whole row) under a root at FULL. What the tuple holds is
// what the PARTITION logged, and REPLICA IDENTITY is set per partition (ALTER
// TABLE on the parent does not reach them). So a root at FULL over a partition
// at DEFAULT sends a key-only tuple labeled as a whole row: every column
// outside the key reads as NULL (seen on PostgreSQL 16,
// TestCapturer_BelowFull_Partitioned_Integration). Nothing in the message
// says which partition a change was made in.
//
// The same holds under a root below FULL: every tuple is labeled 'K' and
// stored in part, but a partition whose own identity leaves the primary key
// out sends NO old tuple for an UPDATE of the key, and a DELETE without it.
//
// The decoder asks at each description of a relation, whatever its identity,
// and decides from the answer (Decoder.judgePartitions). It reads the catalog
// as it is now, while capture that is behind reads changes made under an
// earlier one, so the answer is used only for what the rows cannot show
// (whether the relation is partitioned, and a partition that leaves the key
// out), never to say what an old row holds: that is read from the row
// (holdsEmpty).
type PartitionsResolver func(relationID uint32, schema, table string) (Partitions, error)

// Partitions is what the catalog says of a relation and its partitions.
type Partitions struct {
	// Known reports that the catalog holds the relation. A relation dropped
	// since its changes were written is not known: it may have been
	// partitioned.
	Known bool
	// Partitioned reports that the relation is a partitioned table.
	Partitioned bool
	// BelowFull counts its leaf partitions that are not at REPLICA IDENTITY
	// FULL: their UPDATE and DELETE cannot be undone.
	BelowFull int
	// MissesKey counts its leaf partitions whose own replica identity is an
	// index that leaves a primary-key column out. A change of the primary key
	// made in one arrives with no earlier row at all.
	MissesKey int
}

// partitionsQuery reads Partitions for relation $1: the relation and, when it
// is a partitioned table, every leaf under it, through pg_inherits. It reads
// catalogs only and takes no lock on any table: pg_partition_tree() locks each
// partition (ACCESS SHARE), so it waits behind a VACUUM FULL, a rewriting ALTER
// or a long TRUNCATE of one partition, and a tree of thousands takes as many
// locks (seen on PostgreSQL 16, TestPartitionCatalogReadsTakeNoLock_Integration).
//
// A leaf's identity index is the one flagged indisreplident, valid and not
// deferrable (relcache's rule); its key columns are the first indnkeyatts. The
// primary-key columns are read as the capture reads them (pkColumnsQuery).
const partitionsQuery = `
WITH RECURSIVE tree AS (
	SELECT c.oid, c.relkind, c.relreplident FROM pg_class c WHERE c.oid = $1::oid
	UNION ALL
	SELECT c.oid, c.relkind, c.relreplident
	FROM tree t
	JOIN pg_inherits i ON i.inhparent = t.oid
	JOIN pg_class c ON c.oid = i.inhrelid
	WHERE t.relkind = 'p'
), leaves AS (
	SELECT t.oid, t.relreplident FROM tree t WHERE t.relkind <> 'p' AND t.oid <> $1::oid
), idx AS (
	SELECT l.oid,
	       EXISTS (SELECT 1 FROM pg_index pk, unnest(pk.indkey::int2[]) AS a(att)
	               WHERE pk.indrelid = l.oid AND pk.indisprimary AND a.att > 0
	                 AND NOT EXISTS (SELECT 1 FROM unnest(ri.indkey::int2[]) WITH ORDINALITY AS k(att, ord)
	                                 WHERE k.ord <= ri.indnkeyatts AND k.att = a.att)) AS misses
	FROM leaves l
	JOIN pg_index ri ON ri.indrelid = l.oid AND ri.indisreplident AND ri.indisvalid AND ri.indimmediate
	WHERE l.relreplident = 'i'
)
SELECT EXISTS (SELECT 1 FROM tree WHERE oid = $1::oid),
       COALESCE((SELECT relkind = 'p' FROM tree WHERE oid = $1::oid), false),
       (SELECT count(*) FROM leaves WHERE relreplident <> 'f'),
       (SELECT count(*) FROM idx WHERE misses)`

// queryPartitions is the catalog-backed PartitionsResolver body.
func queryPartitions(ctx context.Context, conn *pgx.Conn, relationID uint32) (Partitions, error) {
	var p Partitions
	if err := conn.QueryRow(ctx, partitionsQuery, relationID).Scan(&p.Known, &p.Partitioned, &p.BelowFull, &p.MissesKey); err != nil {
		return Partitions{}, fmt.Errorf("pgcapture: reading the partitions of relation OID %d: %w", relationID, err)
	}
	return p, nil
}

// partitionsReadTimeout bounds one read of a relation's partitions, opening
// its connection included. One attempt a minute at most is made while reads
// fail (Decoder.partsDownUntil), so a catalog that hangs holds the
// replication loop for this long once a minute, well inside the server's
// wal_sender_timeout (60 s by default).
const partitionsReadTimeout = 3 * time.Second

// partitionsReader runs queryPartitions on a connection of its own, opened at
// the first read and again whenever it is found closed (pgx closes a
// connection whose query was cut by its deadline). Used from the decode loop
// alone: not safe for concurrent use.
type partitionsReader struct {
	dsn  string
	conn *pgx.Conn
}

func (r *partitionsReader) read(ctx context.Context, relationID uint32) (Partitions, error) {
	if r.conn == nil || r.conn.IsClosed() {
		conn, err := pgx.Connect(ctx, r.dsn)
		if err != nil {
			return Partitions{}, fmt.Errorf("pgcapture: opening the connection that reads the partitions of relation OID %d: %w", relationID, err)
		}
		r.conn = conn
	}
	return queryPartitions(ctx, r.conn, relationID)
}

func (r *partitionsReader) close() {
	if r.conn != nil && !r.conn.IsClosed() {
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.conn.Close(cctx)
	}
}

// DeferrableResolver returns the name of a relation's primary-key constraint
// when that key is declared DEFERRABLE, and "" when the table has no primary
// key or a key that is not deferrable. The capturer implements it with a
// catalog query by OID; tests stub it. A failed lookup must return an error:
// a table whose key could not be checked is not captured as if it were safe.
type DeferrableResolver func(relationID uint32, schema, table string) (constraint string, err error)

// deferrableKeyQuery reads the name of relation $1's primary-key constraint
// when it is deferrable. No row = no primary key, or one that is not.
const deferrableKeyQuery = `SELECT conname FROM pg_constraint
WHERE conrelid = $1 AND contype = 'p' AND condeferrable`

// queryDeferrableKey is the catalog-backed DeferrableResolver body.
func queryDeferrableKey(ctx context.Context, conn *pgx.Conn, relationID uint32) (string, error) {
	var name string
	err := conn.QueryRow(ctx, deferrableKeyQuery, relationID).Scan(&name)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("pgcapture: reading whether the primary key of relation OID %d is deferrable: %w", relationID, err)
	}
	return name, nil
}

// PKResolver returns the primary-key columns of a relation, in primary-key (indkey)
// KEY order — the catalog order, which for a composite PK declared out of column
// order differs from table-ordinal order. cacheRelation reorders the result to
// table-ordinal before caching, so the pk_values it builds align with the offline
// resolver's metadata.PKColumnMetas (also table-ordinal). Used to build
// event.Event.PKValues.
//
// It must source the PK from the catalog (pg_index.indisprimary), NOT from the
// RelationMessage's per-column "key" flag: under REPLICA IDENTITY FULL pgoutput
// flags EVERY column as key, so the flag would
// make every table's PK the whole row; under REPLICA IDENTITY USING INDEX it marks
// the index's columns, which need not be the PK; under NOTHING it marks none
// (pinned on a live server by TestPgoutputReplicaIdentityWire_Integration).
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
// supported version range (PG15+). The explicit cast removes that version-dependence
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
// available on every supported version (PG15+).
//
// Intentional scope: attgenerated='s' flags STORED only. PG18 adds VIRTUAL generated
// columns (attgenerated='v') and publish_generated_columns, which accepts only none or
// stored: pgoutput never publishes a VIRTUAL column. It is computed on read and never
// materialized, so it is not in the row image and a reverse INSERT/UPDATE would never
// reference it — but if VIRTUAL support lands, extend this predicate to attgenerated IN ('s','v').
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

// publicationOIDs lists the OIDs of the tables a publication streams, as the
// stream names them: the root under publish_via_partition_root = true, each
// leaf under false (pg_publication_tables applies the same rule). Read once at
// capture start: a table outside this set that the stream later describes is
// new since the start (#2441).
func publicationOIDs(ctx context.Context, conn *pgx.Conn, publication string) (map[uint32]bool, error) {
	rows, err := conn.Query(ctx, `
		SELECT c.oid
		FROM pg_publication_tables pt
		JOIN pg_namespace n ON n.nspname = pt.schemaname
		JOIN pg_class c ON c.relname = pt.tablename AND c.relnamespace = n.oid
		WHERE pt.pubname = $1`, publication)
	if err != nil {
		return nil, fmt.Errorf("pgcapture: list tables of publication %q: %w", publication, err)
	}
	defer rows.Close()
	out := map[uint32]bool{}
	for rows.Next() {
		var oid uint32
		if err := rows.Scan(&oid); err != nil {
			return nil, fmt.Errorf("pgcapture: scan tables of publication %q: %w", publication, err)
		}
		out[oid] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgcapture: list tables of publication %q: %w", publication, err)
	}
	return out, nil
}

// relationFactsQuery reads what the live catalog says about one relation OID:
// its current name and, for a partition, its parent. No row = the OID no
// longer exists.
const relationFactsQuery = `SELECT n.nspname, c.relname, c.relispartition,
	COALESCE((SELECT pn.nspname || '.' || pc.relname
	          FROM pg_inherits i
	          JOIN pg_class pc ON pc.oid = i.inhparent
	          JOIN pg_namespace pn ON pn.oid = pc.relnamespace
	          WHERE i.inhrelid = c.oid LIMIT 1), '')
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.oid = $1`

func queryRelationFacts(ctx context.Context, conn *pgx.Conn, oid uint32) (RelationFacts, error) {
	var f RelationFacts
	err := conn.QueryRow(ctx, relationFactsQuery, oid).Scan(&f.Schema, &f.Table, &f.IsPartition, &f.Parent)
	if errors.Is(err, pgx.ErrNoRows) {
		return RelationFacts{}, nil
	}
	if err != nil {
		return RelationFacts{}, fmt.Errorf("pgcapture: catalog lookup for relation OID %d: %w", oid, err)
	}
	f.Exists = true
	return f, nil
}

// stampsReader is DDLInference.Stamps over a connection of its own, or nil
// when the server does not record commit times (track_commit_timestamp off)
// or they cannot be read: an inferred change is then dated at or before, as
// without this. Its own connection, because a read that times out closes the
// connection it ran on, and the catalog reads capture needs must not share
// that with an optional one. close releases it.
func stampsReader(startupCtx, ctx context.Context, dsn, publication string, logger *slog.Logger) (read func(uint32) (CatalogStamps, error), close func()) {
	none := func() {}
	conn, err := pgx.Connect(startupCtx, dsn)
	if err != nil {
		logger.Warn("pgcapture: cannot open a connection to read when schema changes committed; inferred schema changes are dated at or before the change that shows them",
			"error", err)
		return nil, none
	}
	closeConn := func() { _ = conn.Close(context.Background()) }
	var on string
	if err := conn.QueryRow(startupCtx, "SELECT current_setting('track_commit_timestamp')").Scan(&on); err != nil {
		logger.Warn("pgcapture: cannot read track_commit_timestamp; inferred schema changes are dated at or before the change that shows them",
			"error", err)
		closeConn()
		return nil, none
	}
	if on != "on" {
		logger.Info("pgcapture: track_commit_timestamp is not on, so inferred schema changes are dated at or before the change that shows them, not at their exact commit",
			"setting", on)
		closeConn()
		return nil, none
	}
	return func(oid uint32) (CatalogStamps, error) {
		if conn.IsClosed() {
			return CatalogStamps{}, errors.New("pgcapture: the connection that reads commit times is closed")
		}
		qctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return queryCatalogStamps(qctx, conn, oid, publication)
	}, closeConn
}

const catalogStampsClassQuery = `SELECT n.nspname, c.relname,
	pg_xact_commit_timestamp(c.xmin), pg_xact_commit_timestamp(t.xmin),
	(SELECT pg_xact_commit_timestamp(pr.xmin) FROM pg_publication_rel pr
	 JOIN pg_publication p ON p.oid = pr.prpubid
	 WHERE p.pubname = $2 AND pr.prrelid = c.oid)
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_type t ON t.oid = c.reltype
WHERE c.oid = $1`

const catalogStampsColumnsQuery = `SELECT attname, atttypid, atttypmod, attisdropped, attgenerated <> '',
	attacl IS NOT NULL, pg_xact_commit_timestamp(xmin)
FROM pg_attribute WHERE attrelid = $1 AND attnum > 0 ORDER BY attnum`

// queryCatalogStamps reads the commit times of oid's catalog rows
// (CatalogStamps). No row = the OID no longer exists.
func queryCatalogStamps(ctx context.Context, conn *pgx.Conn, oid uint32, publication string) (CatalogStamps, error) {
	var st CatalogStamps
	err := conn.QueryRow(ctx, catalogStampsClassQuery, oid, publication).Scan(&st.Schema, &st.Table, &st.ClassAt, &st.TypeAt, &st.PubAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return CatalogStamps{}, nil
	}
	if err != nil {
		return CatalogStamps{}, fmt.Errorf("pgcapture: catalog commit times for relation OID %d: %w", oid, err)
	}
	rows, err := conn.Query(ctx, catalogStampsColumnsQuery, oid)
	if err != nil {
		return CatalogStamps{}, fmt.Errorf("pgcapture: catalog commit times of the columns of relation OID %d: %w", oid, err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			c       CatalogColumn
			dropped bool
		)
		if err := rows.Scan(&c.Name, &c.TypeOID, &c.TypeMod, &dropped, &c.Generated, &c.HasACL, &c.At); err != nil {
			return CatalogStamps{}, fmt.Errorf("pgcapture: scan catalog commit times of relation OID %d: %w", oid, err)
		}
		if dropped {
			if c.At != nil {
				st.DroppedAt = append(st.DroppedAt, c.At.UTC())
			}
			continue
		}
		if c.At != nil {
			u := c.At.UTC()
			c.At = &u
		}
		st.Cols = append(st.Cols, c)
	}
	if err := rows.Err(); err != nil {
		return CatalogStamps{}, fmt.Errorf("pgcapture: read catalog commit times of relation OID %d: %w", oid, err)
	}
	for _, p := range []**time.Time{&st.ClassAt, &st.TypeAt, &st.PubAt} {
		if *p != nil {
			u := (**p).UTC()
			*p = &u
		}
	}
	st.Exists = true
	return st, nil
}
