package mcptools

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/dbtrail/dbtrail/internal/query"
)

// A surface that knows its flavor (the console, Target.SourcePostgres) is
// never asked to read it: an index the console never migrated would fail the
// read on every call. The standalone tool reads it and, when the read fails,
// says so instead of passing silently for MySQL.
func TestQueryToolFlavorSource(t *testing.T) {
	for _, tc := range []struct {
		name     string
		known    *bool
		wantWarn bool
	}{
		{"console, MySQL", ptr(false), false},
		{"console, PostgreSQL", ptr(true), false},
		{"standalone, read fails", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.MatchExpectationsInOrder(false)
			if tc.known == nil {
				mock.ExpectQuery("SELECT flavor FROM stream_state").WillReturnError(errors.New("Unknown column 'flavor' in 'field list'"))
			}
			mock.ExpectQuery("FROM binlog_events").WillReturnRows(sqlmock.NewRows([]string{"event_id"}))
			cfg := Config{Resolve: func(context.Context, string) (*Target, error) {
				return &Target{DB: db, SourcePostgres: tc.known}, nil
			}}
			res, _, err := MakeQueryTool(cfg)(context.Background(), &mcp.CallToolRequest{}, QueryArgs{Schema: "app", Table: "t", NoArchive: true})
			if err != nil {
				t.Fatal(err)
			}
			text := resultText(res)
			if got := strings.Contains(text, query.FlavorUnreadableWarning); got != tc.wantWarn {
				t.Errorf("warning present = %v, want %v:\n%s", got, tc.wantWarn, text)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("queries: %v", err)
			}
		})
	}
}

func ptr(b bool) *bool { return &b }
