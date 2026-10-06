package cliapp

import (
	"context"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/parser"
)

// fixtureBinlogDir copies the parser's real captured binlog (three
// transactions on payloadtest.orders) into a temp dir.
func fixtureBinlogDir(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "internal", "parser", "testdata", "compressed_mysql8046.binlog"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "binlog.000001"), b, 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	return dir
}

// indexFile with a parser that drops every rows event of the file (no schema
// snapshot loaded): the file is marked failed with the dropped-changes error,
// and that error is what reaches the command's exit code (#2144). Before, it
// was marked completed and the command exited 0.
func TestIndexFile_droppedChangesMarkTheFileFailed(t *testing.T) {
	for _, c := range []struct {
		name       string
		schemas    map[string]bool
		wantStatus string
	}{
		{"drops fail the file", map[string]bool{"payloadtest": true}, "failed"},
		{"no drop stays completed", map[string]bool{"someotherdb": true}, "completed"},
	} {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery(regexp.QuoteMeta("SELECT status FROM index_state")).
				WillReturnRows(sqlmock.NewRows([]string{"status"}))
			mock.ExpectExec(regexp.QuoteMeta("'in_progress'")).WillReturnResult(sqlmock.NewResult(0, 1))
			var gotMsg string
			if c.wantStatus == "failed" {
				mock.ExpectExec(regexp.QuoteMeta("status         = 'failed'")).
					WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), containsArg{parser.SkipNoResolver}, "binlog.000001").
					WillReturnResult(sqlmock.NewResult(0, 1))
			} else {
				mock.ExpectExec(regexp.QuoteMeta("status         = 'completed'")).
					WillReturnResult(sqlmock.NewResult(0, 1))
			}

			dir := fixtureBinlogDir(t)
			p := parser.New(dir, nil, parser.Filters{Schemas: c.schemas}, nil)
			_, err = indexFile(context.Background(), p, indexer.New(db, 100), db, dir, "binlog.000001", "")

			if c.wantStatus == "completed" {
				if err != nil {
					t.Fatalf("a file with no drop must complete, got %v", err)
				}
			} else {
				var dce *parser.DroppedChangesError
				if !errors.As(err, &dce) {
					t.Fatalf("got %T (%v), want *parser.DroppedChangesError", err, err)
				}
				gotMsg = err.Error()
				for _, want := range []string{parser.SkipNoResolver, "payloadtest.orders", "marked failed"} {
					if !strings.Contains(gotMsg, want) {
						t.Errorf("missing %q in:\n%s", want, gotMsg)
					}
				}
				// The summary wraps it, so the exit code and the telemetry
				// class both carry it.
				sum := indexFailureSummary(1, 1, err)
				if !errors.As(sum, &dce) {
					t.Errorf("summary lost the dropped-changes cause: %v", sum)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("index_state writes: %v", err)
			}
		})
	}
}

// containsArg matches a string argument holding sub: index_state.error_message
// must carry the dropped-changes text, not just any failure.
type containsArg struct{ sub string }

func (c containsArg) Match(v driver.Value) bool {
	s, ok := v.(string)
	return ok && strings.Contains(s, c.sub)
}
