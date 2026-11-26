package pgbaseline

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/event"
)

// SourceTable is one table a snapshot taken now would hold, with its columns
// as the source's catalog has them now.
type SourceTable struct {
	Schema, Table string
	// OID is the table's pg_class OID: another one under the same name is
	// another table (dropped and created again).
	OID uint32
	// Columns are the columns a snapshot file would store (live, not
	// dropped, not generated), in attnum order. Only Name, TypeOID, TypeMod
	// and Ordinal (attnum) are set: what tells one list of columns from
	// another.
	Columns []baseline.PGColumn
}

// ShapesOfNextRun is TablesOfNextRun with each table's columns (#3112): the
// tables a snapshot taken now would hold, by the same discovery, and what the
// catalog says their columns are at this moment.
//
// It is what a snapshot update asks before it publishes. PostgreSQL's change
// stream describes a table only when it next carries a change of it, so a
// change of columns that no write followed is recorded nowhere else. One
// short-lived plain connection, one read-only transaction so the list of
// tables and the columns are of the same moment, and catalog reads only:
// nothing is written or created on the source. The caller bounds it with ctx.
//
// A table with no column left to store is not listed with an empty list: it
// is listed with none, and the caller sees the difference from its file.
func ShapesOfNextRun(ctx context.Context, queryDSN, publication string, filters event.Filters) ([]SourceTable, error) {
	conn, err := pgx.Connect(ctx, queryDSN)
	if err != nil {
		return nil, fmt.Errorf("pgbaseline: connect to list the publication's tables: %w", err)
	}
	defer conn.Close(context.Background())
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("pgbaseline: start the read of the publication's tables: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	// Discovery warns about partitioned tables each time it runs; a snapshot
	// says that once, and this is asked at every update.
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	tables, err := discoverTables(ctx, conn, publication, filters, quiet)
	if err != nil {
		return nil, err
	}
	tables, _ = splitStorable(tables, quiet)
	out := make([]SourceTable, len(tables))
	at := make(map[uint32]int, len(tables))
	oids := make([]uint32, len(tables))
	for i, t := range tables {
		out[i] = SourceTable{Schema: t.Schema, Table: t.Table, OID: t.OID}
		at[t.OID] = i
		oids[i] = t.OID
	}
	if len(oids) == 0 {
		return out, nil
	}
	// The columns loadColumns keeps for a snapshot's file, for every table
	// at once.
	rows, err := conn.Query(ctx, `
		SELECT a.attrelid, a.attname, a.atttypid, a.atttypmod, a.attnum
		FROM pg_attribute a
		WHERE a.attrelid = ANY($1::oid[])
		  AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
		ORDER BY a.attrelid, a.attnum`, oids)
	if err != nil {
		return nil, fmt.Errorf("pgbaseline: listing the columns of the publication's tables: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var oid uint32
		var c baseline.PGColumn
		if err := rows.Scan(&oid, &c.Name, &c.TypeOID, &c.TypeMod, &c.Ordinal); err != nil {
			return nil, fmt.Errorf("pgbaseline: scanning the columns of the publication's tables: %w", err)
		}
		i, ok := at[oid]
		if !ok {
			return nil, fmt.Errorf("pgbaseline: the catalog returned columns of a table (OID %d) that was not asked for", oid)
		}
		out[i].Columns = append(out[i].Columns, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pgbaseline: listing the columns of the publication's tables: %w", err)
	}
	return out, nil
}
