// Package pgdrill is the PostgreSQL scratch target of `bintrail drill`: it
// loads the psql output of a full-table reconstruct into a throwaway
// PostgreSQL and counts what landed. It links pgx, so only the PostgreSQL
// binary registers it (cli.SetPostgresDrillTarget); the drill command itself
// stays driver-free.
package pgdrill

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// Target is one scratch PostgreSQL. Probes and counts share one connection;
// each table loads on a connection of its own, so a load that fails midway
// dies with its connection and its transaction rolls back: the table is
// either all there or not there at all.
type Target struct {
	cfg  *pgx.ConnConfig
	conn *pgx.Conn
}

// Open connects to the scratch server a postgres:// URL names.
func Open(ctx context.Context, dsn string) (*Target, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid --target-dsn: %w", err)
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to --target-dsn: %w", err)
	}
	return &Target{cfg: cfg, conn: conn}, nil
}

func (t *Target) OutputFormat() string { return reconstruct.OutputFormatPsql }

// Empty refuses a target holding anything in one of the drill's schemas: any
// relation (a leftover sequence or view as much as a table), since the files
// create theirs without IF NOT EXISTS and a schema with other things in it
// is not a scratch.
func (t *Target) Empty(ctx context.Context, schemas []string) error {
	for _, s := range schemas {
		var name string
		err := t.conn.QueryRow(ctx, `SELECT c.relname FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 ORDER BY c.relname LIMIT 1`, s).Scan(&name)
		switch {
		case err == pgx.ErrNoRows:
		case err != nil:
			return fmt.Errorf("probe target schema %s: %w", s, err)
		default:
			return fmt.Errorf("target already has %s.%s: drill refuses a non-empty target; point --target-dsn at a THROWAWAY PostgreSQL (e.g. `docker compose --profile drill up -d drill-postgres`)", s, name)
		}
	}
	return nil
}

// Load runs one table's file as psql would: the SQL before the rows, the rows
// through COPY, the SQL after. The rows are found where the report says they
// are, not by reading the SQL, and the pieces around them must be the ones
// the file was written with.
func (t *Target) Load(ctx context.Context, outDir string, rep *reconstruct.TableReport) error {
	pc := rep.PsqlCopy
	if pc == nil || len(rep.Files) != 1 {
		return fmt.Errorf("the reconstruct of %s.%s did not report one psql file with its rows located", rep.Schema, rep.Table)
	}
	f, err := os.Open(filepath.Join(outDir, rep.Files[0]))
	if err != nil {
		return fmt.Errorf("read dump file %s: %w", rep.Files[0], err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("read dump file %s: %w", rep.Files[0], err)
	}
	copyLine := []byte(pc.Statement + ";\n")
	const endOfData = "\\.\n"
	if pc.DataEnd < pc.DataStart || pc.DataEnd+int64(len(endOfData)) > st.Size() {
		return fmt.Errorf("dump file %s does not match its report (rows at %d-%d of %d bytes)", rep.Files[0], pc.DataStart, pc.DataEnd, st.Size())
	}
	before := make([]byte, pc.DataStart)
	if _, err := io.ReadFull(f, before); err != nil {
		return fmt.Errorf("read dump file %s: %w", rep.Files[0], err)
	}
	after := make([]byte, st.Size()-pc.DataEnd)
	if _, err := f.ReadAt(after, pc.DataEnd); err != nil {
		return fmt.Errorf("read dump file %s: %w", rep.Files[0], err)
	}
	if !bytes.HasSuffix(before, copyLine) || !bytes.HasPrefix(after, []byte(endOfData)) {
		return fmt.Errorf("dump file %s does not match its report: the COPY is not where it says", rep.Files[0])
	}

	conn, err := pgx.ConnectConfig(ctx, t.cfg)
	if err != nil {
		return fmt.Errorf("target connection: %w", err)
	}
	defer conn.Close(context.WithoutCancel(ctx))
	if err := execScript(ctx, conn, before[:len(before)-len(copyLine)]); err != nil {
		return fmt.Errorf("apply %s: %w", rep.Files[0], err)
	}
	tag, err := conn.PgConn().CopyFrom(ctx, io.NewSectionReader(f, pc.DataStart, pc.DataEnd-pc.DataStart), pc.Statement)
	if err != nil {
		return fmt.Errorf("apply %s: the rows: %w", rep.Files[0], err)
	}
	if tag.RowsAffected() != rep.RowsWritten {
		return fmt.Errorf("apply %s: COPY took %d rows, the file holds %d; nothing was kept", rep.Files[0], tag.RowsAffected(), rep.RowsWritten)
	}
	if err := execScript(ctx, conn, after[len(endOfData):]); err != nil {
		if ctx.Err() != nil {
			// Cancelled with the COMMIT possibly in flight: the server may
			// have kept the table.
			return fmt.Errorf("apply %s: cancelled while finishing; whether the table was kept is unknown, check the target before the next drill: %w", rep.Files[0], err)
		}
		return fmt.Errorf("apply %s: %w", rep.Files[0], err)
	}
	// The file ends in its COMMIT; a session still in a transaction here
	// means it did not, and closing it would roll everything back silently.
	if s := conn.PgConn().TxStatus(); s != 'I' {
		return fmt.Errorf("apply %s: the file left its transaction open (status %q); nothing was kept", rep.Files[0], s)
	}
	return nil
}

// execScript runs SQL text of several statements as one simple query, the
// way psql sends a script with no COPY in it. ReadAll returns the first
// statement's error the server reported.
func execScript(ctx context.Context, conn *pgx.Conn, sql []byte) error {
	_, err := conn.PgConn().Exec(ctx, string(sql)).ReadAll()
	return err
}

func (t *Target) Count(ctx context.Context, schema, table string) (int64, error) {
	var n int64
	err := t.conn.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{schema, table}.Sanitize()).Scan(&n)
	return n, err
}

func (t *Target) Close() error { return t.conn.Close(context.Background()) }
