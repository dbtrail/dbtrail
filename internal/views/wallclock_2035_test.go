package views

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// writeFixtureClockBaseline writes a table with a DATETIME, a TIMESTAMP and a
// DECIMAL through the real writer, CREATE TABLE in the footer. Both time
// columns hold the same text, 10:00, which is what a dump hands over: the
// wall clock for the DATETIME, the UTC instant for the TIMESTAMP.
func writeFixtureClockBaseline(t *testing.T, root string) string {
	t.Helper()
	createSQL := "CREATE TABLE `visits` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `seen_at` datetime DEFAULT NULL,\n" +
		"  `Odd \"name\"` datetime(6) DEFAULT NULL,\n" +
		"  `stamped` timestamp NULL DEFAULT NULL,\n" +
		"  `fee` decimal(6,2) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		");\n"
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	path := filepath.Join(root, "2026-04-30T03-00-00Z", "shop", "visits.parquet")
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL},
	})
	if err != nil {
		t.Fatalf("baseline writer: %v", err)
	}
	row := []string{"1", "2026-10-04 10:00:00", "2026-10-04 10:00:00.250000", "2026-10-04 10:00:00", "1.50"}
	if err := w.WriteRow(row, make([]bool, len(row))); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// The seam #2035 found: the writer stores DATETIME and TIMESTAMP alike, as an
// instant, so under a session zone other than UTC a DATETIME prints shifted.
// With WallClockDatetimes the state view reads each DATETIME as the zone-less
// wall clock MySQL holds, in every zone, and a TIMESTAMP stays an instant.
// Run in the real engine, on the real footer read: the list of DATETIME
// columns comes from the file, not from the test.
func TestStateView_datetimesAreWallClockUnderASessionZone(t *testing.T) {
	root := t.TempDir()
	path := writeFixtureClockBaseline(t, root)
	footers, err := baseline.TableFootersFor(context.Background(), []string{path})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := footers[path].Datetimes, []string{"seen_at", `Odd "name"`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("footer Datetimes = %q, want %q (the DATETIME columns, never the TIMESTAMP)", got, want)
	}

	read := func(wallClock bool, zone string) (seenType, seen, odd, stampedType, stamped, fee string) {
		t.Helper()
		in := Input{
			GeneratedAt:      time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
			Version:          "test",
			BaselineSource:   root,
			BaselineSnapshot: time.Date(2026, 4, 30, 3, 0, 0, 0, time.UTC),
			Baselines:        []BaselineTable{{Schema: "shop", Table: "visits", Path: path, WallClockDatetimes: wallClock}},
		}
		in.ApplyFooters(footers)
		sqlText := Generate(in)
		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		db.SetMaxOpenConns(1)
		if _, err := db.Exec(sqlText); err != nil {
			t.Fatalf("DuckDB rejected the generated views:\n%v\n\n--- generated ---\n%s", err, sqlText)
		}
		// After the script, as the sandbox worker does: the script pins UTC.
		if _, err := db.Exec("SET TimeZone = '" + zone + "'"); err != nil {
			t.Fatal(err)
		}
		err = db.QueryRow(`SELECT typeof(seen_at), seen_at::VARCHAR, "Odd ""name"""::VARCHAR, typeof(stamped), stamped::VARCHAR, fee::VARCHAR
			FROM shop.visits WHERE seen_at >= '2026-10-04 09:30:00' AND seen_at < '2026-10-04 10:30:00'`).
			Scan(&seenType, &seen, &odd, &stampedType, &stamped, &fee)
		if err != nil {
			t.Fatalf("wallClock=%v zone=%s: %v (no row means the literal was compared in another zone)\n%s", wallClock, zone, err, sqlText)
		}
		return
	}

	for _, zone := range []string{"UTC", "America/Argentina/Buenos_Aires", "Asia/Tokyo"} {
		seenType, seen, odd, stampedType, stamped, fee := read(true, zone)
		if seenType != "TIMESTAMP" || seen != "2026-10-04 10:00:00" || odd != "2026-10-04 10:00:00.25" {
			t.Errorf("zone %s: DATETIME columns = %s %q and %q, want a zone-less TIMESTAMP holding the stored wall clock", zone, seenType, seen, odd)
		}
		if stampedType != "TIMESTAMP WITH TIME ZONE" {
			t.Errorf("zone %s: the TIMESTAMP column is %s, want it left an instant", zone, stampedType)
		}
		if fee != "1.50" {
			t.Errorf("zone %s: fee = %s, want the decimal cast kept beside the DATETIME ones", zone, fee)
		}
		_ = stamped
	}
	// A TIMESTAMP prints in the session's zone: 10:00 UTC is 19:00 in Tokyo.
	if _, _, _, _, stamped, _ := read(true, "Asia/Tokyo"); stamped != "2026-10-04 19:00:00+09" {
		t.Errorf("TIMESTAMP under Asia/Tokyo = %q, want 2026-10-04 19:00:00+09", stamped)
	}
	// Without the flag nothing changes for the readers that run under UTC.
	if seenType, seen, _, _, _, _ := read(false, "UTC"); seenType != "TIMESTAMP WITH TIME ZONE" || seen != "2026-10-04 10:00:00+00" {
		t.Errorf("flag off, UTC: seen_at = %s %q, want the column as it always was", seenType, seen)
	}
}

// replaceClause, by its inputs: no flag, no cast; unknown schema, nothing.
func TestReplaceClause_datetimes(t *testing.T) {
	tbl := BaselineTable{SchemaKnown: true, Datetimes: []string{"a", `b"c`},
		Decimals: []DecimalColumn{{Name: "m", Precision: 6, Scale: 2}}}
	if got := replaceClause(tbl); strings.Contains(got, "TIME ZONE") || !strings.Contains(got, "DECIMAL(6,2)") {
		t.Errorf("flag off: %q, want the decimal cast alone", got)
	}
	tbl.WallClockDatetimes = true
	want := `"a" AT TIME ZONE 'UTC' AS "a", "b""c" AT TIME ZONE 'UTC' AS "b""c", CAST("m" AS DECIMAL(6,2)) AS "m"`
	if got := replaceClause(tbl); got != want {
		t.Errorf("flag on:\n got %s\nwant %s", got, want)
	}
	tbl.SchemaKnown = false
	if got := replaceClause(tbl); got != "" {
		t.Errorf("schema unknown: %q, want nothing (the names cannot be trusted)", got)
	}
}

// events.commit_time under a session zone: an instant, like event_timestamp,
// not a UTC wall clock the zone would read as its own.
func TestEventsView_commitTimeIsAnInstantUnderASessionZone(t *testing.T) {
	archiveRoot := t.TempDir()
	const id = "11111111-2222-3333-4444-555555555555"
	writeFixtureArchive(t, archiveRoot, id)
	read := func(nonUTC bool, zone string) (typ string, sameInstant bool) {
		t.Helper()
		sqlText := Generate(Input{
			GeneratedAt:    time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
			Version:        "test",
			ArchiveSources: []string{filepath.Join(archiveRoot, "bintrail_id="+id)},
			NonUTCSession:  nonUTC,
		})
		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		db.SetMaxOpenConns(1)
		if _, err := db.Exec(sqlText); err != nil {
			t.Fatalf("DuckDB rejected the generated views:\n%v\n\n--- generated ---\n%s", err, sqlText)
		}
		if _, err := db.Exec("SET TimeZone = '" + zone + "'"); err != nil {
			t.Fatal(err)
		}
		// Compared with the instant the stored microseconds are: a wall clock
		// read in the session's zone is hours away from it.
		if err := db.QueryRow(`SELECT typeof(commit_time), commit_time = make_timestamptz(CAST(commit_ts_us AS BIGINT)) FROM events`).
			Scan(&typ, &sameInstant); err != nil {
			t.Fatalf("nonUTC=%v zone=%s: %v", nonUTC, zone, err)
		}
		return
	}
	for _, zone := range []string{"UTC", "Asia/Tokyo", "America/Argentina/Buenos_Aires"} {
		if typ, same := read(true, zone); typ != "TIMESTAMP WITH TIME ZONE" || !same {
			t.Errorf("NonUTCSession, zone %s: commit_time is %s, the stored instant: %v; want an instant equal to it", zone, typ, same)
		}
	}
	// Untouched for every reader under UTC.
	if typ, same := read(false, "UTC"); typ != "TIMESTAMP" || !same {
		t.Errorf("flag off, UTC: commit_time is %s, the stored instant: %v; want the zone-less TIMESTAMP it always was", typ, same)
	}
}
