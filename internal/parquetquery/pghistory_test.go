package parquetquery

import (
	"context"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/dbtrail/dbtrail/internal/archive"
	"github.com/dbtrail/dbtrail/internal/query"
)

// The archive half of the PostgreSQL changed-column filter must admit exactly
// the rows the MySQL half admits (internal/query's
// TestChangedColumnFilterPostgresIntegration has the same rows): the merged
// fetch skips the archives when the live index already answers, which is only
// right if both sides agree. The JSON text is spelled the way an archived
// MySQL JSON value is (", " and ": "), and a nil changed-columns list appears
// both as the text "null" (an archived JSON null) and as a Parquet NULL.
func TestChangedColumnFilterPostgresArchive(t *testing.T) {
	if os.Getenv("CGO_ENABLED") == "0" {
		t.Skip("DuckDB requires CGO")
	}
	const marker = `{"__bintrail_not_captured__": true}`
	type fx struct {
		id, et, pk, changed, before, after string
		changedNull, beforeNull, afterNull bool
	}
	fixtures := []fx{
		{id: "1", et: "2", pk: "1", changed: `["title"]`, before: `{"id": "1", "title": "a"}`, after: `{"id": "1", "title": "b"}`},
		{id: "2", et: "2", pk: "2", changed: "null", before: `{"id": "2", "title": ` + marker + `}`, after: `{"id": "2", "title": "b"}`},
		{id: "3", et: "2", pk: "3", changed: "null", before: `{"id": "3", "title": "a"}`, after: `{"id": "3", "title": "a"}`},
		{id: "4", et: "1", pk: "4", changedNull: true, beforeNull: true, after: `{"id": "4", "title": "a"}`},
		{id: "5", et: "3", pk: "5", changed: "null", before: `{"id": "5", "title": ` + marker + `}`, afterNull: true},
		{id: "6", et: "2", pk: "6", changed: `["body"]`, before: `{"id": "6", "body": "a"}`, after: `{"id": "6", "body": "b"}`},
		{id: "7", et: "2", pk: "7", changed: "null", before: `{"id": "7", "title": "__bintrail_not_captured__"}`, after: `{"id": "7", "title": "__bintrail_not_captured__"}`},
		{id: "8", et: "2", pk: "8", changedNull: true, before: `{"id": "8", "title": ` + marker + `}`, after: `{"id": "8", "title": "b"}`},
	}
	b := func(v bool) string {
		if v {
			return "1"
		}
		return "0"
	}
	var rows [][2][]string
	for i, f := range fixtures {
		ts := "2026-11-20 10:00:0" + strconv.Itoa(i+1)
		rows = append(rows, [2][]string{
			{f.id, "0/16B3748", strconv.Itoa(100 * (i + 1)), strconv.Itoa(100*(i+1) + 50), ts, "", "", "public", "docs", f.et, f.pk,
				f.changed, f.before, f.after, "0", "", "", ""},
			{"0", "0", "0", "0", "0", "1", "1", "0", "0", "0", "0", b(f.changedNull), b(f.beforeNull), b(f.afterNull), "0", "1", "1", "1"},
		})
	}
	dir := writeArchiveFixture(t, archive.BinlogEventColumns, rows)

	pks := func(pg bool) []string {
		t.Helper()
		got, err := Fetch(context.Background(), query.Options{Schema: "public", Table: "docs", ChangedColumn: "title", Limit: 100, PostgresHistory: pg}, dir)
		if err != nil {
			t.Fatalf("Fetch(pg=%v): %v", pg, err)
		}
		var out []string
		for _, r := range got {
			out = append(out, r.PKValues)
		}
		return out
	}
	if got, want := pks(false), []string{"1"}; !slices.Equal(got, want) {
		t.Errorf("MySQL archive filter returned %v, want %v", got, want)
	}
	if got, want := pks(true), []string{"1", "2", "8"}; !slices.Equal(got, want) {
		t.Errorf("PostgreSQL archive filter returned %v, want %v", got, want)
	}
}
