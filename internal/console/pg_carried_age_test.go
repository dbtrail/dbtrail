package console

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/views"
)

// A table an update carried forward unchanged keeps the footer of the update
// that last wrote it, hours or days old for a table nobody writes; the
// folder's record of where its update ended says the update found it
// unchanged through the update's own bound, and that bound is the age of an
// answer that reads it (pgHeldThrough). On the live boards a statement that
// read the never-written item table from its snapshot alone was said to be
// 2 to 5 hours behind, a minute after the snapshot was taken.
func TestPGCarriedTableAgeIsTheRunsBound(t *testing.T) {
	read := func(t *testing.T, s *Server, tbls ...views.BaselineTable) string {
		t.Helper()
		got, why := s.pgReadThroughWith(tbls, func(views.BaselineTable, baseline.DumpMetadata) string { return "" })
		if why != "" {
			return "unknown: " + why
		}
		return got.UTC().Format(time.RFC3339)
	}
	record := func(t *testing.T, root string, body string) {
		t.Helper()
		dir := filepath.Join(root, snapshotdir.Name(pgAgeDir))
		if err := os.WriteFile(filepath.Join(dir, reconstruct.PGRunCutFile), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	const held = `{"version":1,"lsn":300,"at":"2026-11-20T12:00:00Z","tables":[["public","countries"],["public","orders"]],"since":"2026-11-20T11:58:00Z"}`

	t.Run("a carried table takes the run's bound", func(t *testing.T) {
		root := t.TempDir()
		carried := pgAgeTable(t, root, pgAgeDir, "countries", 40, "2026-11-17T09:00:00Z")
		written := pgAgeTable(t, root, pgAgeDir, "orders", 250, "2026-11-20T11:58:30Z")
		other := pgAgeTable(t, root, pgAgeDir, "legacy_kept", 40, "2026-11-18T10:00:00Z")
		record(t, root, held)
		s := &Server{}
		if got := read(t, s, carried); got != "2026-11-20T11:58:00Z" {
			t.Errorf("carried: %s, want the run's bound", got)
		}
		// A file the run wrote with a newer bound keeps its own.
		if got := read(t, s, written); got != "2026-11-20T11:58:30Z" {
			t.Errorf("written: %s", got)
		}
		// A table the record does not hold keeps its file's bound, and the
		// statement is as old as its oldest table.
		if got := read(t, s, carried, other); got != "2026-11-18T10:00:00Z" {
			t.Errorf("with a table the run did not hold: %s", got)
		}
	})
	// An update's bound is at least a minute before its folder's time: a
	// record past that is wrong, and the file's own bound is used.
	for _, since := range []string{"2026-11-20T13:00:00Z", "2026-11-20T12:00:00Z", "2026-11-20T11:59:30Z"} {
		t.Run("a bound an update cannot record: "+since, func(t *testing.T) {
			root := t.TempDir()
			carried := pgAgeTable(t, root, pgAgeDir, "countries", 40, "2026-11-17T09:00:00Z")
			record(t, root, `{"version":1,"lsn":300,"at":"2026-11-20T12:00:00Z","tables":[["public","countries"]],"since":"`+since+`"}`)
			if got := read(t, &Server{}, carried); got != "2026-11-17T09:00:00Z" {
				t.Errorf("%s, want the file's own bound", got)
			}
		})
	}
	t.Run("a bound a minute before the folder is taken", func(t *testing.T) {
		root := t.TempDir()
		carried := pgAgeTable(t, root, pgAgeDir, "countries", 40, "2026-11-17T09:00:00Z")
		record(t, root, `{"version":1,"lsn":300,"at":"2026-11-20T12:00:00Z","tables":[["public","countries"]],"since":"2026-11-20T11:59:00Z"}`)
		if got := read(t, &Server{}, carried); got != "2026-11-20T11:59:00Z" {
			t.Errorf("%s", got)
		}
	})
	for name, body := range map[string]string{
		"a record from before it named the bound": `{"version":1,"lsn":300,"at":"2026-11-20T12:00:00Z","tables":[["public","countries"]]}`,
		"a record written for another folder":     `{"version":1,"lsn":300,"at":"2026-11-19T12:00:00Z","tables":[["public","countries"]],"since":"2026-11-19T11:58:00Z"}`,
		"a record that does not parse":            `{`,
		"a bound that does not parse":             `{"version":1,"lsn":300,"at":"2026-11-20T12:00:00Z","tables":[["public","countries"]],"since":"yesterday"}`,
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			carried := pgAgeTable(t, root, pgAgeDir, "countries", 40, "2026-11-17T09:00:00Z")
			record(t, root, body)
			if got := read(t, &Server{}, carried); got != "2026-11-17T09:00:00Z" {
				t.Errorf("%s, want the file's own bound", got)
			}
		})
	}
	t.Run("a folder still marked incomplete", func(t *testing.T) {
		root := t.TempDir()
		carried := pgAgeTable(t, root, pgAgeDir, "countries", 40, "2026-11-17T09:00:00Z")
		record(t, root, held)
		dir := filepath.Join(root, snapshotdir.Name(pgAgeDir))
		if err := os.WriteFile(filepath.Join(dir, baseline.IncompleteMarker), nil, 0o644); err != nil {
			t.Fatal(err)
		}
		if got := read(t, &Server{}, carried); got != "2026-11-17T09:00:00Z" {
			t.Errorf("%s, want the file's own bound", got)
		}
	})
	t.Run("through a link to the folder", func(t *testing.T) {
		root := t.TempDir()
		pgAgeTable(t, root, pgAgeDir, "countries", 40, "2026-11-17T09:00:00Z")
		record(t, root, held)
		linkRoot := t.TempDir()
		link := filepath.Join(linkRoot, snapshotdir.Name(pgAgeDir))
		if err := os.Symlink(filepath.Join(root, snapshotdir.Name(pgAgeDir)), link); err != nil {
			t.Fatal(err)
		}
		via := views.BaselineTable{Schema: "public", Table: "countries", Path: filepath.Join(link, "public", "countries.parquet")}
		if got := read(t, &Server{}, via); got == "2026-11-20T11:58:00Z" {
			t.Errorf("a folder reached through a link took the record: %s", got)
		}
	})
}
