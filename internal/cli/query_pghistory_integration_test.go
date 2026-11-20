//go:build integration

package cli

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The seam the query package's tests cannot see: runQuery must read the
// index's flavor and hand it to the engine and the formatter. Through the
// real command, on a real index: the same rows answer differently on a
// PostgreSQL index (the unknown UPDATE comes back, annotated, with a note on
// stderr) and on a MySQL one (exactly as before).
func TestQueryPostgresHistorySeam_CLI(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	for _, flavor := range []string{"postgres", "mysql"} {
		t.Run(flavor, func(t *testing.T) {
			db, dbName := testutil.CreateTestDB(t)
			testutil.InitIndexTables(t, db)
			if err := indexer.EnsureSchema(db); err != nil {
				t.Fatalf("EnsureSchema: %v", err)
			}
			testutil.MustExec(t, db, "INSERT INTO stream_state (id, mode, flavor, last_checkpoint, server_id) VALUES (1, 'position', ?, NOW(), 1)", flavor)
			ts := "2026-11-20 10:00:00"
			testutil.InsertEvent(t, db, "0/16B3748", 100, 150, ts, nil, "public", "docs", 2, "1",
				[]byte(`["title"]`), []byte(`{"id": "1", "title": "a"}`), []byte(`{"id": "1", "title": "b"}`))
			testutil.InsertEvent(t, db, "0/16B3748", 200, 250, ts, nil, "public", "docs", 2, "2",
				[]byte(`null`), []byte(`{"id": "2", "title": {"__bintrail_not_captured__": true}}`),
				[]byte(`{"id": "2", "title": "b", "body": {"__bintrail_unchanged_toast__": true}}`))

			resetQueryGlobals(t)
			qIndexDSN, qFormat, qNoArchive = testutil.IntegrationDSN(dbName), "json", true
			qSchema, qTable, qChangedCol = "public", "docs", "title"

			var runErr error
			var out string
			stderr := captureStderr(t, func() {
				out = captureStdout(t, func() { runErr = runQuery(newQueryTestCmd(), nil) })
			})
			if runErr != nil {
				t.Fatalf("runQuery: %v", runErr)
			}
			var rows []struct {
				PKValues         string   `json:"pk_values"`
				BeforeImage      *string  `json:"before_image"`
				UnchangedColumns []string `json:"unchanged_columns"`
			}
			if err := json.Unmarshal([]byte(out), &rows); err != nil {
				t.Fatalf("unmarshal %q: %v", out, err)
			}
			var pks []string
			for _, r := range rows {
				pks = append(pks, r.PKValues)
			}
			t.Logf("stdout:\n%s\nstderr:\n%s", out, stderr)
			note := "Note: 1 UPDATE row may have changed title:"
			if flavor == "mysql" {
				if !slices.Equal(pks, []string{"1"}) {
					t.Fatalf("MySQL index returned %v, want [1]", pks)
				}
				if strings.Contains(out, "before_image") || strings.Contains(stderr, note) {
					t.Errorf("a MySQL index was annotated:\n%s\n%s", out, stderr)
				}
				return
			}
			if !slices.Equal(pks, []string{"1", "2"}) {
				t.Fatalf("PostgreSQL index returned %v, want [1 2]", pks)
			}
			if r := rows[1]; r.BeforeImage == nil || *r.BeforeImage != "key_only" || !slices.Equal(r.UnchangedColumns, []string{"body"}) {
				t.Errorf("row 2: before_image %v, unchanged_columns %v", r.BeforeImage, r.UnchangedColumns)
			}
			if rows[0].BeforeImage != nil {
				t.Errorf("a whole row got before_image %q", *rows[0].BeforeImage)
			}
			if !strings.Contains(stderr, note) {
				t.Errorf("stderr lacks %q:\n%s", note, stderr)
			}
		})
	}
}
