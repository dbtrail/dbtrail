package pgcompare

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/sqlcompare"
)

// sourceSettings are set on the source connection so its text is the text a
// snapshot is written with (pgbaseline's render settings), and so nothing
// this tool sends can write.
var sourceSettings = []string{
	"SET TimeZone = 'UTC'",
	"SET DateStyle = 'ISO, YMD'",
	"SET IntervalStyle = 'postgres'",
	"SET bytea_output = 'hex'",
	"SET default_transaction_read_only = on",
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
// It goes through the extended protocol with no parameters and no result
// formats: that asks for text and refuses a second statement after a `;`.
func (s *Source) Query(ctx context.Context, stmt string, max int) (sqlcompare.Rows, bool, error) {
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
		return sqlcompare.Rows{}, false, err
	}
	return out, over, nil
}
