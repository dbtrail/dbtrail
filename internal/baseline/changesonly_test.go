package baseline

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A table that `bintrail reconstruct --allow-changes-only` wrote holds only
// the rows its recorded changes touched. Converted into a snapshot it would
// be trusted as the whole table by every later reconstruct, with no flag and
// no warning. Run refuses it, by the folder's list and by the table's own
// schema file, and refuses a folder its writer did not finish.
func TestRun_refusesATableWrittenFromChangesOnly(t *testing.T) {
	at := time.Date(2026, 10, 10, 13, 51, 42, 0, time.UTC)
	run := func(in string, tables ...string) (string, Stats, error) {
		out := t.TempDir()
		stats, err := Run(context.Background(), Config{InputDir: in, OutputDir: out, Compression: "zstd", Timestamp: at, Tables: tables})
		return out, stats, err
	}
	nothingConverted := func(t *testing.T, out string) {
		t.Helper()
		if ents, _ := os.ReadDir(out); len(ents) != 0 {
			t.Errorf("the refused conversion left %d entries in the output", len(ents))
		}
	}
	markSchemaFile := func(t *testing.T, in, table string) {
		t.Helper()
		path := filepath.Join(in, "shop."+table+"-schema.sql")
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		marked := ChangesOnlySchemaPrefix + " The data file(s) of shop." + table + " hold only the rows its recorded\n-- changes touched.\n" + string(body)
		if err := os.WriteFile(path, []byte(marked), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("control: the same dump with no mark converts", func(t *testing.T) {
		in := t.TempDir()
		writeDump1938(t, in, false)
		if _, stats, err := run(in); err != nil || stats.TablesProcessed != 2 {
			t.Fatalf("stats = %+v, err = %v", stats, err)
		}
	})

	t.Run("listed in the folder's marker file", func(t *testing.T) {
		in := t.TempDir()
		writeDump1938(t, in, false)
		if err := AppendChangesOnlyTable(in, "shop", "b"); err != nil {
			t.Fatal(err)
		}
		out, _, err := run(in)
		if !errors.Is(err, ErrChangesOnlyTable) {
			t.Fatalf("err = %v, want the table refused", err)
		}
		msg := strings.ReplaceAll(err.Error(), in, "<dump>")
		t.Logf("rendered: %s", msg)
		for _, want := range []string{"shop.b in <dump>", "only the rows the recorded changes touched", "missing every other row",
			"Nothing was converted", "`bintrail dump`", "name them with --tables"} {
			if !strings.Contains(msg, want) {
				t.Errorf("refusal does not say %q", want)
			}
		}
		if strings.Contains(msg, "shop.a") {
			t.Errorf("shop.a is complete and is named as refused")
		}
		if strings.Contains(msg, "—") {
			t.Error("refusal holds an em dash")
		}
		nothingConverted(t, out)
	})

	t.Run("the complete tables of that folder convert when named", func(t *testing.T) {
		in := t.TempDir()
		writeDump1938(t, in, false)
		if err := AppendChangesOnlyTable(in, "shop", "b"); err != nil {
			t.Fatal(err)
		}
		if _, stats, err := run(in, "shop.a"); err != nil || stats.TablesProcessed != 1 {
			t.Fatalf("stats = %+v, err = %v, want shop.a converted alone", stats, err)
		}
		if _, _, err := run(in, "shop.b"); !errors.Is(err, ErrChangesOnlyTable) {
			t.Fatalf("naming the changes-only table converted it: %v", err)
		}
	})

	t.Run("marker file gone, the schema file still says it", func(t *testing.T) {
		in := t.TempDir()
		writeDump1938(t, in, false)
		markSchemaFile(t, in, "b")
		out, _, err := run(in)
		if !errors.Is(err, ErrChangesOnlyTable) || !strings.Contains(err.Error(), "shop.b in ") {
			t.Fatalf("err = %v, want shop.b refused by its schema file", err)
		}
		nothingConverted(t, out)
	})

	t.Run("a marker file that cannot be read refuses the folder", func(t *testing.T) {
		in := t.TempDir()
		writeDump1938(t, in, false)
		if err := os.WriteFile(filepath.Join(in, ChangesOnlyMarkerFile), []byte("shop.b\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		out, _, err := run(in)
		if !errors.Is(err, ErrChangesOnlyTable) {
			t.Fatalf("err = %v, want the folder refused", err)
		}
		t.Logf("rendered: %s", strings.ReplaceAll(err.Error(), in, "<dump>"))
		nothingConverted(t, out)
	})

	t.Run("a folder its writer did not finish", func(t *testing.T) {
		in := t.TempDir()
		writeDump1938(t, in, false)
		if err := WriteIncompleteMarker(in); err != nil {
			t.Fatal(err)
		}
		out, _, err := run(in)
		if !errors.Is(err, ErrUnfinishedDump) {
			t.Fatalf("err = %v, want the folder refused", err)
		}
		t.Logf("rendered: %s", strings.ReplaceAll(err.Error(), in, "<dump>"))
		nothingConverted(t, out)
		if _, _, err := run(in, "shop.a"); !errors.Is(err, ErrUnfinishedDump) {
			t.Fatalf("naming a table converted from an unfinished folder: %v", err)
		}
	})
}

// The marker file keeps any name whole, appends, and is removed for a new run.
func TestChangesOnlyMarkerFile_roundTrip(t *testing.T) {
	dir := t.TempDir()
	if listed, err := ReadChangesOnlyTables(dir); err != nil || listed != nil {
		t.Fatalf("no file: %v, %v", listed, err)
	}
	names := [][2]string{{"shop", "orders"}, {"shop", "order.items"}, {"sh\"op", "line\nbreak"}}
	for _, n := range names {
		if err := AppendChangesOnlyTable(dir, n[0], n[1]); err != nil {
			t.Fatal(err)
		}
	}
	listed, err := ReadChangesOnlyTables(dir)
	if err != nil || len(listed) != len(names) {
		t.Fatalf("listed = %v, err = %v", listed, err)
	}
	for _, n := range names {
		if !listed[n[0]+"."+n[1]] {
			t.Errorf("%q.%q is not listed", n[0], n[1])
		}
	}
	raw, _ := os.ReadFile(filepath.Join(dir, ChangesOnlyMarkerFile))
	t.Logf("rendered:\n%s", raw)
	if err := RemoveChangesOnlyMarker(dir); err != nil {
		t.Fatal(err)
	}
	if err := RemoveChangesOnlyMarker(dir); err != nil {
		t.Fatalf("removing a file that is not there: %v", err)
	}
	if listed, _ := ReadChangesOnlyTables(dir); listed != nil {
		t.Errorf("still listed after removal: %v", listed)
	}
}
