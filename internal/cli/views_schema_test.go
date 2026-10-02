package cli

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/spf13/cobra"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// writeServerBaseline writes one server's baseline root through the REAL
// writer: a single snapshot holding shop.orders with the given rows, marked
// complete, and published through the `current` pointer when asked.
func writeServerBaseline(t *testing.T, stamp string, pointer bool, statuses ...string) string {
	t.Helper()
	root := t.TempDir()
	createSQL := "CREATE TABLE `orders` (\n  `id` int NOT NULL,\n  `status` varchar(32) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n);\n"
	cols, err := baseline.ParseSchemaText(createSQL)
	if err != nil {
		t.Fatalf("ParseSchemaText: %v", err)
	}
	w, err := baseline.NewWriter(filepath.Join(root, stamp, "shop", "orders.parquet"), cols, baseline.WriterConfig{
		Compression: "none", RowGroupSize: 100,
		Metadata: map[string]string{baseline.MetaKeyCreateTableSQL: createSQL},
	})
	if err != nil {
		t.Fatalf("baseline writer: %v", err)
	}
	for i, st := range statuses {
		if err := w.WriteRow([]string{string(rune('1' + i)), st}, []bool{false, false}); err != nil {
			t.Fatalf("write row: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, stamp, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatalf("write marker: %v", err)
	}
	if pointer {
		if err := baseline.PublishCurrentPointer(filepath.Join(root, stamp)); err != nil {
			t.Fatalf("publish pointer: %v", err)
		}
	}
	return root
}

// viewsFor runs the command for one server and returns the file it wrote.
func viewsFor(t *testing.T, schema, baselineDir string) string {
	t.Helper()
	vIndexDSN, vArchiveDir, vArchiveS3, vBintrailID, vBaselineS3 = "", "", "", "", ""
	vNoBaselines, vIncludeLive, vIncludeEvents, vPinSnapshot = false, false, false, false
	vBaselineDir, vOut, vDatabase = baselineDir, "-", schema
	out, err := runViewsToString(t)
	if err != nil {
		t.Fatalf("views --database %q --baseline-dir %s: %v", schema, baselineDir, err)
	}
	return out
}

func orderStatuses(t *testing.T, db *sql.DB, view string) string {
	t.Helper()
	var got sql.NullString
	if err := db.QueryRow(`SELECT string_agg("status", ',' ORDER BY "status") FROM ` + view).Scan(&got); err != nil {
		t.Fatalf("query %s: %v", view, err)
	}
	return got.String
}

// TestRunViews_schemaKeepsTwoServersApart is the acceptance of #1874 driven
// through the COMMAND: `views --schema a` and `views --schema b` over two
// baselines that share shop.orders, outputs concatenated, loaded into one
// DuckDB database file, and each view read for its ROWS.
//
// The generator has the same test over its own Input. This one exists because
// what can regress here is the command forgetting to pass the name on, and
// that file would still be valid SQL.
func TestRunViews_schemaKeepsTwoServersApart(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pointer bool
	}{
		{"pinned to the snapshot", false},
		{"following the current pointer", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			saveViewsFlags(t)
			rootA := writeServerBaseline(t, "2026-04-30T03-00-00Z", tc.pointer, "from-a")
			rootB := writeServerBaseline(t, "2026-05-02T03-00-00Z", tc.pointer, "from-b-1", "from-b-2")
			fileA := viewsFor(t, "a", rootA)
			fileB := viewsFor(t, "b", rootB)
			if follows := strings.Contains(fileA, "/current/"); follows != tc.pointer {
				t.Fatalf("the file follows the pointer = %v, want %v", follows, tc.pointer)
			}

			db, err := sql.Open("duckdb", filepath.Join(t.TempDir(), "lake.db"))
			if err != nil {
				t.Fatalf("open duckdb: %v", err)
			}
			defer db.Close()
			if _, err := db.Exec(fileA + "\n" + fileB); err != nil {
				t.Fatalf("DuckDB rejected the two files loaded together:\n%v\n\n--- a ---\n%s\n--- b ---\n%s", err, fileA, fileB)
			}
			if got := orderStatuses(t, db, "a.shop.orders"); got != "from-a" {
				t.Errorf("a.shop.orders returned %q, want server a's rows", got)
			}
			if got := orderStatuses(t, db, "b.shop.orders"); got != "from-b-1,from-b-2" {
				t.Errorf("b.shop.orders returned %q, want server b's rows", got)
			}
		})
	}
}

// TestRunViews_noSchemaIsTheFileAsItWas: the default writes nothing about a
// schema, so a scripted invocation that never heard of the flag gets the file
// it got before.
func TestRunViews_noSchemaIsTheFileAsItWas(t *testing.T) {
	saveViewsFlags(t)
	root := writeServerBaseline(t, "2026-04-30T03-00-00Z", false, "x")
	out := viewsFor(t, "", root)
	if strings.Contains(out, "ATTACH IF NOT EXISTS") {
		t.Errorf("a file generated with no --database attaches one:\n%s", out)
	}
	if !strings.Contains(out, "CREATE OR REPLACE VIEW \"shop\".\"orders\" AS\n") {
		t.Errorf("the table view is not named after the source:\n%s", out)
	}
}

// TestRunViews_refusesASchemaItCannotUse covers the boundary: refused before
// anything is read or written, naming the flag and the reason.
func TestRunViews_refusesASchemaItCannotUse(t *testing.T) {
	for _, tc := range []struct{ name, schema, want string }{
		{"the default schema", "main", "already has"},
		{"the default schema in capitals", "MAIN", "lowercase"},
		{"uppercase", "WP", "lowercase"},
		{"a space", "my server", "letters, digits and underscore"},
		{"a dot", "wp.prod", "letters, digits and underscore"},
		{"a double quote", `wp"`, "letters, digits and underscore"},
		{"a dash", "wp-prod", "letters, digits and underscore"},
		{"a leading digit", "1wp", "start with a letter"},
		{"a reserved word", "select", "SQL keyword"},
		{"another server's index alias", "wp_live", "_live"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			saveViewsFlags(t)
			root := writeServerBaseline(t, "2026-04-30T03-00-00Z", false, "x")
			out := filepath.Join(t.TempDir(), "views.sql")
			vIndexDSN, vArchiveDir, vArchiveS3, vBintrailID, vBaselineS3 = "", "", "", "", ""
			vNoBaselines, vIncludeLive, vIncludeEvents, vPinSnapshot = false, false, false, false
			vBaselineDir, vOut, vDatabase = root, out, tc.schema

			_, err := runViewsToString(t)
			if err == nil {
				t.Fatalf("--database %q was accepted", tc.schema)
			}
			if !strings.HasPrefix(err.Error(), "--database: ") || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the refusal does not name the flag and say %q: %v", tc.want, err)
			}
			if _, statErr := os.Stat(out); statErr == nil {
				t.Error("a file was written for a name that was refused")
			}
		})
	}
}

// TestRunViews_refusesASchemaBeforeAnyOtherRefusal: with nothing else passed
// the command has its own refusal ("no view at all"). A bad name must not
// hide behind it, or the operator fixes one and meets the other.
func TestRunViews_refusesASchemaBeforeAnyOtherRefusal(t *testing.T) {
	saveViewsFlags(t)
	resetViewsFlags()
	vDatabase = "main"
	_, err := runViewsToString(t)
	if err == nil || !strings.HasPrefix(err.Error(), "--database: ") {
		t.Errorf("got %v, want the refusal of the schema name", err)
	}
}

// TestViewsCmd_refusesAnEmptySchemaThatWasTyped goes through cobra, which is
// the only layer that can tell `--schema ""` from no flag at all. It is the
// shape `--schema "$SERVER"` takes with the variable unset, and reading it as
// "no schema" writes the views to the default one, silently.
func TestViewsCmd_refusesAnEmptySchemaThatWasTyped(t *testing.T) {
	saveViewsFlags(t)
	root := writeServerBaseline(t, "2026-04-30T03-00-00Z", false, "x")
	f := viewsCmd.Flags().Lookup("database")
	if f == nil {
		t.Fatal("no --database flag")
	}
	t.Cleanup(func() { f.Changed = false })

	if err := viewsCmd.ParseFlags([]string{"--database", "", "--baseline-dir", root, "--output", "-"}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	t.Cleanup(func() {
		for _, name := range []string{"baseline-dir", "output"} {
			if fl := viewsCmd.Flags().Lookup(name); fl != nil {
				fl.Changed = false
			}
		}
	})
	// A context, or a command that gets PAST the refusal hangs on its first
	// read instead of failing here: the refusal is the only thing that returns
	// before one is needed.
	viewsCmd.SetContext(context.Background())
	err := runViews(viewsCmd, nil)
	if err == nil || !strings.Contains(err.Error(), "--database") || !strings.Contains(err.Error(), "empty") {
		t.Errorf("got %v, want the empty name refused", err)
	}
}

// TestSchemaHelp_saysWhatItIsFor: the flag help has to carry the reason, since
// the failure it prevents has no error for the operator to search for.
func TestSchemaHelp_saysWhatItIsFor(t *testing.T) {
	f := viewsCmd.Flags().Lookup("database")
	if f == nil {
		t.Fatal("no --database flag")
	}
	for _, want := range []string{"several servers", "without an error", "<database>_live", "Lowercase", "<database>.<schema>.<table>"} {
		if !strings.Contains(f.Usage, want) {
			t.Errorf("--database help does not say %q:\n%s", want, f.Usage)
		}
	}
	if !strings.Contains(viewsCmd.Long, "--database") {
		t.Error("the long help does not mention --database")
	}
}

// TestViewsCmd_oldSchemaOptionSaysItWasRenamed (#2013): --schema meant a DuckDB
// schema per server; the source's schemas are DuckDB schemas now, so the server
// moved up to a database. A script that still passes --schema is told so, and
// no file is written, whatever the value.
func TestViewsCmd_oldSchemaOptionSaysItWasRenamed(t *testing.T) {
	saveViewsFlags(t)
	root := writeServerBaseline(t, "2026-04-30T03-00-00Z", false, "x")
	out := filepath.Join(t.TempDir(), "views.sql")
	f := viewsCmd.Flags().Lookup("schema")
	if f == nil || !f.Hidden {
		t.Fatal("--schema should still be registered, hidden, to answer old scripts")
	}
	t.Cleanup(func() {
		for _, name := range []string{"schema", "baseline-dir", "output"} {
			if fl := viewsCmd.Flags().Lookup(name); fl != nil {
				fl.Changed = false
			}
		}
	})
	if err := viewsCmd.ParseFlags([]string{"--schema", "wp", "--baseline-dir", root, "--output", out}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	viewsCmd.SetContext(context.Background())
	err := runViews(viewsCmd, nil)
	if err == nil || err.Error() != "--schema is now --database. Views are named after the source (shop.orders, "+
		"no longer state_shop_orders), so a server gets its own DuckDB database instead of a schema: "+
		"--database wp, then query wp.shop.orders and wp.events" {
		t.Errorf("got %v, want the rename explained", err)
	}
	if _, statErr := os.Stat(out); statErr == nil {
		t.Error("a file was written for the old option")
	}
}

// TestRunViews_namesTheTablesItLeavesOut (#2013): a table in a schema DuckDB
// keeps for itself has no view, and the command says so on stderr, where a
// person running it looks; a file where that is EVERY table is refused for
// that reason, not as "no baseline".
func TestRunViews_namesTheTablesItLeavesOut(t *testing.T) {
	saveViewsFlags(t)
	const stamp = "2026-04-30T03-00-00Z"
	root := writeServerBaseline(t, stamp, false, "x")
	src := filepath.Join(root, stamp, "shop", "orders.parquet")
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, stamp, "temp"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, stamp, "temp", "orders.parquet"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	run := func() (string, string, error) {
		vIndexDSN, vArchiveDir, vArchiveS3, vBintrailID, vBaselineS3 = "", "", "", "", ""
		vNoBaselines, vIncludeLive, vIncludeEvents, vPinSnapshot = false, false, false, true
		vBaselineDir, vOut, vDatabase = root, "-", ""
		var out, errOut bytes.Buffer
		cmd := &cobra.Command{}
		cmd.SetOut(&out)
		cmd.SetErr(&errOut)
		cmd.SetContext(context.Background())
		err := runViews(cmd, nil)
		return out.String(), errOut.String(), err
	}
	out, stderr, err := run()
	if err != nil {
		t.Fatalf("views: %v", err)
	}
	if !strings.Contains(stderr, "note: temp.orders: not defined") || !strings.Contains(stderr, "--database") {
		t.Errorf("stderr does not name the table left out and the way to reach it:\n%s", stderr)
	}
	if !strings.Contains(out, `CREATE OR REPLACE VIEW "shop"."orders" AS`) || strings.Contains(out, `VIEW "temp"`) {
		t.Errorf("the file should define shop.orders and not temp.orders:\n%s", out)
	}

	if err := os.Remove(src); err != nil {
		t.Fatal(err)
	}
	_, stderr, err = run()
	if err == nil || !strings.Contains(err.Error(), "schema DuckDB keeps for itself") {
		t.Errorf("a snapshot whose only table is left out gave %v, want that reason", err)
	}
	if !strings.Contains(stderr, "note: temp.orders") {
		t.Errorf("the refusal is not preceded by the note naming the table:\n%s", stderr)
	}
}
