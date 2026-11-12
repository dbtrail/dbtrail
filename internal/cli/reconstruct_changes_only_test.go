package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/audittest"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// A table with no snapshot can only be written from its recorded changes,
// which leaves out every row no change touched. These tests cover what the
// command says about it: the refusal, and with --allow-changes-only the
// summary. The engine sits behind reconstructTablesDetailed, so they need no
// index. Every sentence an operator reads is printed with t.Log.

var changesOnlyAt = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

// stubEngine makes the command's engine return these reports and failures;
// it returns a pointer to the configuration the command passed.
func stubEngine(t *testing.T, reports []*reconstruct.TableReport, failures []reconstruct.TableFailure) *reconstruct.FullTableConfig {
	t.Helper()
	got := &reconstruct.FullTableConfig{}
	orig := reconstructTablesDetailed
	t.Cleanup(func() { reconstructTablesDetailed = orig })
	reconstructCmd.SetContext(context.Background())
	t.Cleanup(func() { reconstructCmd.SetContext(nil) })
	reconstructTablesDetailed = func(_ context.Context, cfg reconstruct.FullTableConfig) ([]*reconstruct.TableReport, []reconstruct.TableFailure, error) {
		*got = cfg
		var errs []error
		for _, f := range failures {
			errs = append(errs, fmt.Errorf("%s.%s: %w", f.Schema, f.Table, f.Err))
		}
		return reports, failures, errors.Join(errs...)
	}
	return got
}

func refusedTable(schema, table string, createdAt time.Time) reconstruct.TableFailure {
	return reconstruct.TableFailure{Schema: schema, Table: table, Err: &reconstruct.ChangesOnlyError{
		Schema: schema, Table: table, At: changesOnlyAt, CreateRecordedAt: createdAt,
	}}
}

// The refusal reaches the operator with the table, what its output would have
// held, why the location had no snapshot for it, and the flag. One fixture per
// reason the lookup comes back empty.
func TestReconstructChangesOnly_refusalSaysWhatToDo(t *testing.T) {
	cases := []struct {
		name     string
		snapshot func(t *testing.T, dir string)
		created  time.Time
		want     []string
	}{
		{
			name:     "the table is in no snapshot",
			snapshot: func(t *testing.T, dir string) { putSnapshot(t, dir, "2026-08-01T00-00-00Z", "mydb", "customers") },
			want:     []string{"holds 1 snapshot, and it does not include mydb.orders", "baseline` writes them"},
		},
		{
			name:     "the moment is before the table's first snapshot",
			snapshot: func(t *testing.T, dir string) { putSnapshot(t, dir, "2026-09-02T00-00-00Z", "mydb", "orders") },
			want:     []string{"are all from after 2026-09-01T12:00:00Z; the earliest is 2026-09-02T00:00:00Z", "ask for a moment at or after 2026-09-02T00:00:00Z"},
		},
		{
			name:     "the location holds no snapshot",
			snapshot: func(*testing.T, string) {},
			want:     []string{"holds no snapshots"},
		},
		{
			name:     "the index recorded the table's CREATE TABLE",
			snapshot: func(t *testing.T, dir string) { putSnapshot(t, dir, "2026-08-01T00-00-00Z", "mydb", "customers") },
			created:  time.Date(2026, 8, 20, 9, 30, 0, 0, time.UTC),
			want: []string{"If mydb.orders did not exist before 2026-08-20T09:30:00Z, when the index recorded the newest CREATE TABLE for it, " +
				"and every change since then is still recorded, those rows are the whole table; the statement alone proves neither."},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetMydumperFlags(t)
			dir := t.TempDir()
			tc.snapshot(t, dir)
			recBaselineDir = dir
			recAt = changesOnlyAt.Format(time.RFC3339)
			cfg := stubEngine(t, nil, []reconstruct.TableFailure{refusedTable("mydb", "orders", tc.created)})

			err := runReconstruct(reconstructCmd, nil)
			if err == nil {
				t.Fatal("the command succeeded with a table that has no snapshot")
			}
			if cfg.AllowChangesOnly {
				t.Error("the engine was asked for changes-only output without the flag")
			}
			if !errors.Is(err, reconstruct.ErrChangesOnly) {
				t.Errorf("the error no longer carries reconstruct.ErrChangesOnly: %v", err)
			}
			msg := strings.ReplaceAll(err.Error(), dir, "<snapshots>")
			t.Logf("rendered:\n%s", msg)
			for _, want := range append(tc.want,
				"full-table reconstruct: mydb.orders: there is no snapshot at or before 2026-09-01T12:00:00Z to start it from",
				"only the rows they touched, with every other row missing",
				"nothing was written for it",
				"To write only those rows anyway, add --allow-changes-only",
			) {
				if !strings.Contains(msg, want) {
					t.Errorf("refusal does not say %q", want)
				}
			}
			// One table, one line: the binary prints the error inside JSON,
			// where a line break shows as a literal \n. And the table is
			// named once before the colon, not twice.
			if strings.Contains(msg, "\n") {
				t.Error("the refusal of a single table spans more than one line")
			}
			if strings.Contains(msg, "mydb.orders: mydb.orders") {
				t.Error("the refusal names the table twice in a row")
			}
			if strings.Contains(msg, "is not known") {
				t.Error("the refusal says the reason is not known although the listing told the case")
			}
			if tc.created.IsZero() && strings.Contains(msg, "CREATE TABLE") {
				t.Error("refusal speaks of a CREATE TABLE the index did not record")
			}
			if strings.Contains(msg, "—") {
				t.Error("refusal holds an em dash")
			}
		})
	}
}

// Every refused table gets its own line, and a failure of another kind is
// left as the engine worded it.
func TestReconstructChangesOnly_eachRefusedTableIsNamed(t *testing.T) {
	resetMydumperFlags(t)
	recBaselineDir = t.TempDir()
	recTables = "mydb.orders,mydb.users,mydb.items"
	recAt = changesOnlyAt.Format(time.RFC3339)
	stubEngine(t, nil, []reconstruct.TableFailure{
		refusedTable("mydb", "users", time.Time{}),
		{Schema: "mydb", Table: "items", Err: errors.New("no primary key")},
		refusedTable("mydb", "orders", time.Time{}),
	})
	err := runReconstruct(reconstructCmd, nil)
	if err == nil {
		t.Fatal("the command succeeded")
	}
	msg := err.Error()
	t.Logf("rendered:\n%s", msg)
	lines := strings.Split(msg, "\n")
	if len(lines) != 3 {
		t.Fatalf("%d lines, want one per failed table", len(lines))
	}
	for i, name := range []string{"mydb.orders", "mydb.users"} {
		line := strings.TrimPrefix(lines[i], "full-table reconstruct: ")
		if !strings.HasPrefix(line, name+": there is no snapshot") || !strings.HasSuffix(line, "reports the table as changes only") {
			t.Errorf("line %d is not the refusal and the advice for %s: %s", i+1, name, line)
		}
	}
	if lines[2] != "mydb.items: no primary key" {
		t.Errorf("the other table's failure was not kept as the engine worded it: %s", lines[2])
	}
}

// A failure that is not this refusal never mentions the flag.
func TestReconstructChangesOnly_otherFailuresDoNotOfferTheFlag(t *testing.T) {
	resetMydumperFlags(t)
	recBaselineDir = t.TempDir()
	stubEngine(t, nil, []reconstruct.TableFailure{{Schema: "mydb", Table: "orders", Err: errors.New("no primary key")}})
	err := runReconstruct(reconstructCmd, nil)
	if err == nil || strings.Contains(err.Error(), "--allow-changes-only") {
		t.Fatalf("err = %v, want the engine's failure with no mention of the flag", err)
	}
}

// With the flag the engine is asked for the output, the table's summary line
// is a warning that says changes only, and the closing line lists the table.
// A table built from its snapshot keeps the ordinary line.
func TestReconstructChangesOnly_summaryMarksTheTable(t *testing.T) {
	resetMydumperFlags(t)
	recBaselineDir = t.TempDir()
	recTables = "mydb.orders,mydb.users"
	recAllowChangesOnly = true
	cfg := stubEngine(t, []*reconstruct.TableReport{
		{Schema: "mydb", Table: "users", BaselineRows: 10},
		{Schema: "mydb", Table: "orders", BinlogOnly: true, InsertsEmitted: 2, EventsApplied: 2},
	}, nil)
	audit := audittest.Install(t)

	var logs bytes.Buffer
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))

	if err := runReconstruct(reconstructCmd, nil); err != nil {
		t.Fatalf("runReconstruct: %v", err)
	}
	if !cfg.AllowChangesOnly {
		t.Error("--allow-changes-only did not reach the engine")
	}
	t.Logf("rendered:\n%s", logs.String())
	var orders, users, closing string
	for _, line := range strings.Split(logs.String(), "\n") {
		switch {
		case strings.Contains(line, "table=orders"):
			orders = line
		case strings.Contains(line, "table=users"):
			users = line
		case strings.Contains(line, "full-table reconstruct complete"):
			closing = line
		}
	}
	if !strings.Contains(orders, "level=WARN") || !strings.Contains(orders, changesOnlySummaryMsg) {
		t.Errorf("mydb.orders was written from its changes alone and its summary line does not say so: %s", orders)
	}
	if strings.Contains(orders, "table dump complete") {
		t.Errorf("mydb.orders is reported as a complete dump: %s", orders)
	}
	if !strings.Contains(users, "level=INFO") || !strings.Contains(users, "table dump complete") || strings.Contains(users, "changes only") {
		t.Errorf("mydb.users was built from its snapshot; its line changed: %s", users)
	}
	if !strings.Contains(closing, "changes_only_tables=mydb.orders") {
		t.Errorf("the closing line does not list the changes-only table: %s", closing)
	}

	// The audit record of each table says which kind of output was read: a
	// reader of the trail must be able to tell a whole table from the
	// changed rows alone.
	got := map[string]string{}
	for _, ev := range audit.Events() {
		if ev.Action != "reconstruct.run" || ev.Detail["mode"] != "full-table" {
			t.Errorf("unexpected audit event %+v", ev)
			continue
		}
		v, ok := ev.Detail["changes_only"]
		if !ok {
			v = "(absent)"
		}
		got[ev.Schema+"."+ev.Table] = v
	}
	if len(got) != 2 || got["mydb.orders"] != "true" || got["mydb.users"] != "(absent)" {
		t.Errorf("audit changes_only by table = %v, want mydb.orders=true and mydb.users without the key", got)
	}
}

// A run with no changes-only table says nothing about one.
func TestReconstructChangesOnly_ordinaryRunIsUnchanged(t *testing.T) {
	resetMydumperFlags(t)
	recBaselineDir = t.TempDir()
	stubEngine(t, []*reconstruct.TableReport{{Schema: "mydb", Table: "orders", BaselineRows: 10}}, nil)
	var logs bytes.Buffer
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	if err := runReconstruct(reconstructCmd, nil); err != nil {
		t.Fatalf("runReconstruct: %v", err)
	}
	if strings.Contains(logs.String(), "changes") {
		t.Errorf("a run built from snapshots mentions changes only:\n%s", logs.String())
	}
}

// The flag is refused wherever it would change nothing: accepted in silence
// it would read as honored.
func TestReconstructChangesOnly_flagRefusedOutsideMydumperOutput(t *testing.T) {
	for _, format := range []string{"parquet", "psql", ""} {
		t.Run("output-format="+format, func(t *testing.T) {
			resetMydumperFlags(t)
			recOutputFormat = format
			recAllowChangesOnly = true
			if format == "" { // single-row mode
				recTables, recOutputDir = "", ""
				recSchema, recTable, recPK, recPKColumns = "mydb", "orders", "1", "id"
			}
			stubEngine(t, nil, nil)
			err := runReconstruct(reconstructCmd, nil)
			if err == nil || err.Error() != allowChangesOnlyMisuse {
				t.Fatalf("err = %v, want the flag refused", err)
			}
			t.Logf("rendered: %s", err)
		})
	}
}

// The flag's help line says what the output lacks.
func TestReconstructChangesOnly_flagHelp(t *testing.T) {
	f := reconstructCmd.Flags().Lookup("allow-changes-only")
	if f == nil {
		t.Fatal("--allow-changes-only is not registered")
	}
	t.Logf("rendered: --allow-changes-only   %s", f.Usage)
	if f.DefValue != "false" {
		t.Errorf("default = %s, want off", f.DefValue)
	}
	if !strings.Contains(f.Usage, "every other row missing") {
		t.Errorf("help does not say what the output lacks: %s", f.Usage)
	}
	if !strings.Contains(reconstructCmd.Long, "--allow-changes-only") {
		t.Error("the command's long help does not mention the flag")
	}
}

// When the location cannot be listed, or shows a snapshot the lookup did not
// use, the command cannot say why the table has none. It says that, before
// the flag, so the flag is not added on a guess.
func TestReconstructChangesOnly_saysWhenTheReasonIsNotKnown(t *testing.T) {
	t.Run("the location could not be listed", func(t *testing.T) {
		resetMydumperFlags(t)
		recBaselineDir = t.TempDir()
		recAt = changesOnlyAt.Format(time.RFC3339)
		orig := listSnapshots
		t.Cleanup(func() { listSnapshots = orig })
		listSnapshots = func(context.Context, string) ([]reconstruct.BaselineFile, error) {
			return nil, errors.New("listing refused")
		}
		stubEngine(t, nil, []reconstruct.TableFailure{refusedTable("mydb", "orders", time.Time{})})
		err := runReconstruct(reconstructCmd, nil)
		if err == nil {
			t.Fatal("the command succeeded")
		}
		msg := strings.ReplaceAll(err.Error(), recBaselineDir, "<snapshots>")
		t.Logf("rendered:\n%s", msg)
		unknown := "Why there is none is not known, because the location could not be listed. Find that out before adding the flag."
		if !strings.Contains(msg, unknown) {
			t.Errorf("refusal does not say %q", unknown)
		}
		if strings.Index(msg, unknown) > strings.Index(msg, "--allow-changes-only") {
			t.Error("the flag is offered before what is not known")
		}
	})
	t.Run("the location shows a snapshot the lookup did not use", func(t *testing.T) {
		resetMydumperFlags(t)
		dir := t.TempDir()
		putSnapshot(t, dir, "2026-08-01T00-00-00Z", "mydb", "orders")
		recBaselineDir = dir
		recAt = changesOnlyAt.Format(time.RFC3339)
		stubEngine(t, nil, []reconstruct.TableFailure{refusedTable("mydb", "orders", time.Time{})})
		err := runReconstruct(reconstructCmd, nil)
		if err == nil {
			t.Fatal("the command succeeded")
		}
		msg := strings.ReplaceAll(err.Error(), dir, "<snapshots>")
		t.Logf("rendered:\n%s", msg)
		unknown := "Why there is none is not known: the location shows a snapshot of it from at or before that moment, and the lookup did not use it. Find that out before adding the flag."
		if !strings.Contains(msg, unknown) {
			t.Errorf("refusal does not say %q", unknown)
		}
	})
}

// The hint for an index that holds no events is kept when the same run also
// refuses a table with no snapshot.
func TestReconstructChangesOnly_keepsTheEmptyIndexHint(t *testing.T) {
	resetMydumperFlags(t)
	recBaselineDir = t.TempDir()
	recTables = "mydb.orders,mydb.items"
	stubEngine(t, nil, []reconstruct.TableFailure{
		refusedTable("mydb", "orders", time.Time{}),
		{Schema: "mydb", Table: "items", Err: &query.SourceEmptyError{}},
	})
	err := runReconstruct(reconstructCmd, nil)
	if err == nil || !strings.Contains(err.Error(), sourceEmptyHint) || !strings.Contains(err.Error(), "--allow-changes-only") {
		t.Fatalf("err = %v, want the refusal's advice and the empty-index hint", err)
	}
}
