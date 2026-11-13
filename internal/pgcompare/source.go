package pgcompare

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/sqlcompare"
)

// sourceSettings are set on the source connection so its text is the text a
// snapshot is written with (pgbaseline's render settings), and so nothing
// this tool sends runs for ever. Read-only is not a setting here: a setting
// is something a statement can change, so each statement gets a read-only
// transaction of its own (Query).
var sourceSettings = []string{
	"SET TimeZone = 'UTC'",
	"SET DateStyle = 'ISO, YMD'",
	"SET IntervalStyle = 'postgres'",
	"SET bytea_output = 'hex'",
	"SET statement_timeout = '60s'",
}

// Source is a PostgreSQL connection that answers in text.
type Source struct {
	conn *pgx.Conn
}

// OpenSource connects to the PostgreSQL source and prepares the session.
// schema, when set, is where unqualified names resolve (search_path).
func OpenSource(ctx context.Context, dsn, schema string) (*Source, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	settings := sourceSettings
	if schema != "" {
		settings = append([]string{"SET search_path = " + pgx.Identifier{schema}.Sanitize()}, settings...)
	}
	for _, s := range settings {
		if _, err := conn.Exec(ctx, s); err != nil {
			conn.Close(ctx)
			return nil, fmt.Errorf("%s: %w", s, err)
		}
	}
	return &Source{conn: conn}, nil
}

// Close closes the connection.
func (s *Source) Close(ctx context.Context) error { return s.conn.Close(ctx) }

// Query runs one statement and returns its rows as the text PostgreSQL sent.
// The statement runs alone in a READ ONLY transaction that is rolled back:
// PostgreSQL does not let a statement turn such a transaction read-write,
// which a session setting would allow. It goes through the extended protocol
// with no parameters and no result formats: that asks for text and refuses a
// second statement after a `;`.
func (s *Source) Query(ctx context.Context, stmt string, max int) (sqlcompare.Rows, bool, error) {
	if _, err := s.conn.Exec(ctx, "BEGIN TRANSACTION READ ONLY"); err != nil {
		return sqlcompare.Rows{}, false, &FatalError{Err: fmt.Errorf("the source would not open a read-only transaction: %w", err)}
	}
	out, over, err := s.read(ctx, stmt, max)
	if _, rbErr := s.conn.Exec(ctx, "ROLLBACK"); rbErr != nil {
		return sqlcompare.Rows{}, false, &FatalError{Err: fmt.Errorf("the source would not close the read-only transaction: %w", rbErr)}
	}
	return out, over, err
}

func (s *Source) read(ctx context.Context, stmt string, max int) (sqlcompare.Rows, bool, error) {
	rr := s.conn.PgConn().ExecParams(ctx, stmt, nil, nil, nil, nil)
	var out sqlcompare.Rows
	for _, fd := range rr.FieldDescriptions() {
		out.Columns = append(out.Columns, fd.Name)
	}
	over := false
	for rr.NextRow() {
		if len(out.Rows) >= max {
			// Keep reading to the end: the protocol has no way to stop a
			// result halfway without closing the connection.
			over = true
			continue
		}
		vals := rr.Values()
		row := make([]*string, len(vals))
		for i, v := range vals {
			if v != nil {
				t := string(v)
				row[i] = &t
			}
		}
		out.Rows = append(out.Rows, row)
	}
	if _, err := rr.Close(); err != nil {
		if s.conn.IsClosed() {
			return sqlcompare.Rows{}, false, &FatalError{Err: fmt.Errorf("the connection to the source was lost: %w", err)}
		}
		var pg *pgconn.PgError
		if errors.As(err, &pg) && notAboutTheStatement(pg.Code) {
			return sqlcompare.Rows{}, false, &CutError{Kind: "source_fault", Err: err}
		}
		return sqlcompare.Rows{}, false, err
	}
	return out, over, nil
}

// notAboutTheStatement: the server stopped the statement for a reason of its
// own. 57 is operator intervention (cancelled, timed out), 53 insufficient
// resources, 40 a rollback forced by another transaction, 58 a system error.
func notAboutTheStatement(code string) bool {
	for _, class := range []string{"57", "53", "40", "58"} {
		if strings.HasPrefix(code, class) {
			return true
		}
	}
	return false
}
