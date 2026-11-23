package consoleapp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/mydumperlock"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
)

// #2640, the read of some tables alone, staged for an update. Everything but
// mydumper is real here: the lock mode, the attempt and its retry, the
// conversion, the journal. The real mydumper, with table names it has to
// quote, is the integration test's.

const rereadMetadata = "# Started dump at: 2026-10-02 01:57:48\n[config]\nquote-character = BACKTICK\n\n[source]\n" +
	"# SOURCE_LOG_FILE = \"binlog.000002\"\n# SOURCE_LOG_POS = 839\n\n# Finished dump at: 2026-10-02 01:57:49\n"

// writeRereadDump writes what mydumper 1.0.3 leaves for tables, each named
// "schema.table".
func writeRereadDump(t *testing.T, dir string, tables []string) {
	t.Helper()
	files := map[string]string{"metadata": rereadMetadata}
	for _, st := range tables {
		schema, table, _ := strings.Cut(st, ".")
		files[schema+"-schema-create.sql"] = dumpHeader + "CREATE DATABASE /*!32312 IF NOT EXISTS*/ `" + schema + "`;\n"
		files[st+"-schema.sql"] = dumpHeader + "CREATE TABLE `" + table + "` (\n  `id` int(11) NOT NULL,\n  `label` varchar(30) DEFAULT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
		files[st+".00000.sql"] = dumpHeader + "INSERT INTO `" + table + "` (`id`,`label`) VALUES(1,\"uno\")\n,(2,\"dos\")\n;\n"
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

type rereadDumpCall struct {
	tables []string
	mode   baseline.LockMode
	dir    string
}

// stubRereadDump replaces mydumper with fn, which is told the tables the
// attempt asked for, and records every call.
func stubRereadDump(t *testing.T, fn func(n int, c rereadDumpCall) error) *[]rereadDumpCall {
	t.Helper()
	prevDump, prevDDL, prevEv := runMydumperFunc, dumpDDLMarkFunc, dumpEventMarkFunc
	t.Cleanup(func() { runMydumperFunc, dumpDDLMarkFunc, dumpEventMarkFunc = prevDump, prevDDL, prevEv })
	dumpDDLMarkFunc = func(console.BaselineRequest) string { return "" }
	dumpEventMarkFunc = func(console.BaselineRequest) string { return "" }
	calls := &[]rereadDumpCall{}
	runMydumperFunc = func(ctx context.Context, _ string, _ config.SSL, _ []string, dir string, mode baseline.LockMode, _ lockModeSource) error {
		c := rereadDumpCall{tables: dumpTablesFrom(ctx), mode: mode, dir: dir}
		*calls = append(*calls, c)
		return fn(len(*calls), c)
	}
	return calls
}

// rereadLeft is what a read left in the working folder: nothing, after one
// that failed or was refused.
func rereadLeft(t *testing.T, stage string) []string {
	t.Helper()
	entries, err := os.ReadDir(stage)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func rereadReq(j console.BaselineDirJournal) console.BaselineRequest {
	return console.BaselineRequest{ServerID: "s1", ServerName: "shop-db", SourceDSN: "src", LocalDir: "/nonexistent/snapshots", Journal: j}
}

func TestStageReread_stagesEachTableAsAFinishedRead(t *testing.T) {
	stage := t.TempDir()
	calls := stubRereadDump(t, func(_ int, c rereadDumpCall) error { writeRereadDump(t, c.dir, c.tables); return nil })
	s := newBaselineSupervisor(context.Background(), stage, baseline.LockModeFTWRL)
	j := &recordingJournal{}
	// A table of another schema, a name with capitals, and one asked twice.
	got, err := s.stageReread(rereadReq(j), []string{"shop.orders", "crm.People", "shop.orders"})
	if err != nil {
		t.Fatalf("stageReread: %v", err)
	}
	if len(*calls) != 1 || !slices.Equal((*calls)[0].tables, []string{"shop.orders", "crm.People"}) {
		t.Fatalf("mydumper was asked for %+v, want one dump of the two tables, each once", *calls)
	}
	if len(got.files) != 2 {
		t.Fatalf("files = %v, want the two tables", got.files)
	}
	if filepath.Dir(got.root) != stage || !strings.HasPrefix(filepath.Base(got.root), "reread-") || !isStagingName(filepath.Base(got.root)) {
		t.Fatalf("the read is staged at %s, want a reread- folder of the working folder %s", got.root, stage)
	}
	if !slices.Contains(j.dirs, got.root) {
		t.Fatalf("the journal was told %v, not the staged folder %s: a restart would leave it", j.dirs, got.root)
	}
	folder := filepath.Join(got.root, reconstruct.SnapshotDirName(got.at))
	if _, err := os.Stat(filepath.Join(folder, baseline.SuccessMarker)); err != nil {
		t.Fatalf("the read's folder is not marked finished: %v", err)
	}
	if _, err := os.Stat(filepath.Join(folder, baseline.IncompleteMarker)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the read's folder is marked unfinished (%v)", err)
	}
	for _, st := range []string{"shop.orders", "crm.People"} {
		schema, table, _ := strings.Cut(st, ".")
		if want := filepath.Join(folder, schema, table+".parquet"); got.files[st] != want {
			t.Fatalf("%s is staged at %q, want %q", st, got.files[st], want)
		}
		meta, err := baseline.ReadParquetMetadata(got.files[st])
		if err != nil {
			t.Fatal(err)
		}
		// What the engine asks of a table file given in place of a snapshot's.
		if meta.Producer != baseline.ProducerDump || !meta.SnapshotTimestamp.Equal(got.at) || meta.BinlogFile != "binlog.000002" || meta.BinlogPos != 839 {
			t.Fatalf("%s: footer %+v, want a read from the source stamped %s at binlog.000002:839", st, meta, got.at)
		}
		if deltas, _ := filepath.Glob(filepath.Join(folder, schema, table+".*delta*")); len(deltas) != 0 {
			t.Fatalf("%s has a table delta beside it: %v", st, deltas)
		}
	}
	if dumps, _ := filepath.Glob(filepath.Join(stage, "dump-*")); len(dumps) != 0 {
		t.Fatalf("the dump is still in the working folder: %v", dumps)
	}
	if err := got.discard(); err != nil {
		t.Fatal(err)
	}
	if left := rereadLeft(t, stage); len(left) != 0 {
		t.Fatalf("after the discard the working folder holds %v", left)
	}
}

func TestStageReread_refusesWhatCannotBeReadAlone(t *testing.T) {
	for _, c := range []struct {
		name    string
		mode    baseline.LockMode
		chosen  bool
		flavor  string
		tables  []string
		partial bool // errRereadUnavailable: the caller takes a full read
		says    string
	}{
		{"lock mode no-lock", baseline.LockModeNoLock, true, "", []string{"shop.orders"}, true, "no-lock"},
		{"lock mode safe-no-lock", baseline.LockModeSafeNoLock, true, "", []string{"shop.orders"}, true, "safe-no-lock"},
		{"a comma in a table name", baseline.LockModeFTWRL, false, "", []string{"shop.orders", "shop.a,b"}, true, `"shop.a,b"`},
		{"a comma in a schema name", baseline.LockModeFTWRL, false, "", []string{"sh,op.orders"}, true, `"sh,op.orders"`},
		{"a dot in a table name", baseline.LockModeFTWRL, false, "", []string{"shop.v1.2"}, true, `"shop.v1.2"`},
		{"a PostgreSQL server",baseline.LockModeFTWRL, false, console.FlavorPostgres, []string{"public.orders"}, true, "PostgreSQL"},
		{"no table", baseline.LockModeFTWRL, false, "", nil, false, "no table"},
		{"an empty list", baseline.LockModeFTWRL, false, "", []string{}, false, "no table"},
		{"a name with no schema", baseline.LockModeFTWRL, false, "", []string{"orders"}, false, `"orders"`},
		{"an empty table name", baseline.LockModeFTWRL, false, "", []string{"shop."}, false, `"shop."`},
		{"an empty schema name", baseline.LockModeFTWRL, false, "", []string{".orders"}, false, `".orders"`},
	} {
		t.Run(c.name, func(t *testing.T) {
			stage := filepath.Join(t.TempDir(), "work")
			calls := stubRereadDump(t, func(int, rereadDumpCall) error { return nil })
			s := newBaselineSupervisor(context.Background(), stage, c.mode)
			s.lockModeChosen = c.chosen
			req := rereadReq(nil)
			req.Flavor = c.flavor
			got, err := s.stageReread(req, c.tables)
			if err == nil {
				t.Fatalf("staged %+v, want a refusal", got)
			}
			if errors.Is(err, errRereadUnavailable) != c.partial {
				t.Fatalf("err = %v; a refusal that sends the caller to a full read = %v, want %v", err, !c.partial, c.partial)
			}
			if !strings.Contains(err.Error(), c.says) {
				t.Fatalf("err = %v, want it to say %s", err, c.says)
			}
			if len(*calls) != 0 {
				t.Fatalf("mydumper ran %d time(s) for a read that is refused", len(*calls))
			}
			if left := rereadLeft(t, stage); len(left) != 0 || len(got.files) != 0 {
				t.Fatalf("a refused read left %v and returned %v", left, got.files)
			}
		})
	}
}

// A table dropped from the source between the refusal and the read: mydumper
// ends well without its file. The other table's file is not handed out alone.
func TestStageReread_aTableTheDumpDoesNotHoldFailsTheWholeRead(t *testing.T) {
	stage := t.TempDir()
	stubRereadDump(t, func(_ int, c rereadDumpCall) error { writeRereadDump(t, c.dir, []string{"shop.orders"}); return nil })
	s := newBaselineSupervisor(context.Background(), stage, baseline.LockModeFTWRL)
	got, err := s.stageReread(rereadReq(nil), []string{"shop.orders", "shop.gone"})
	if !errors.Is(err, errRereadTableMissing) || !strings.Contains(err.Error(), "shop.gone") || strings.Contains(err.Error(), "shop.orders") {
		t.Fatalf("err = %v, want the missing table named, and only it", err)
	}
	if errors.Is(err, errRereadUnavailable) {
		t.Fatalf("err = %v reads as a server that cannot be read by table", err)
	}
	if len(got.files) != 0 || got.root != "" {
		t.Fatalf("a read with a table missing returned %+v", got)
	}
	if left := rereadLeft(t, stage); len(left) != 0 {
		t.Fatalf("the working folder holds %v after a read that failed", left)
	}
}

// Two tables and the second one's data cut short: the conversion fails, and
// the first table's finished file goes with the folder.
func TestStageReread_aFailedConversionLeavesNothing(t *testing.T) {
	stage := t.TempDir()
	stubRereadDump(t, func(_ int, c rereadDumpCall) error { writeHalfBrokenDump(t, c.dir); return nil })
	s := newBaselineSupervisor(context.Background(), stage, baseline.LockModeFTWRL)
	got, err := s.stageReread(rereadReq(nil), []string{"shop.good", "shop.bad"})
	if err == nil || !strings.HasPrefix(err.Error(), "convert: ") {
		t.Fatalf("err = %v, want the conversion's failure", err)
	}
	if len(got.files) != 0 {
		t.Fatalf("a failed read returned %v", got.files)
	}
	if left := rereadLeft(t, stage); len(left) != 0 {
		t.Fatalf("the working folder holds %v after a read that failed", left)
	}
}

func TestStageReread_aFullWorkingFolderIsADiskFailure(t *testing.T) {
	t.Run("while mydumper wrote", func(t *testing.T) {
		stage := t.TempDir()
		stubFailedDump(t, stage, errNoSpaceDump, 0, nil, 1<<40)
		s := newBaselineSupervisor(context.Background(), stage, baseline.LockModeFTWRL)
		_, err := s.stageReread(rereadReq(nil), []string{"shop.big"})
		var full *workingFolderFullError
		if !errors.As(err, &full) || !foldDiskRefused(err) {
			t.Fatalf("err = %v, want the working folder's disk failure", err)
		}
		if left := rereadLeft(t, stage); len(left) != 0 {
			t.Fatalf("the working folder holds %v", left)
		}
	})
	t.Run("while the dump was converted", func(t *testing.T) {
		stage := t.TempDir()
		stubRereadDump(t, func(_ int, c rereadDumpCall) error { writeRereadDump(t, c.dir, c.tables); return nil })
		prevDisk, prevConv := diskSpaceFn, rereadConvert
		t.Cleanup(func() { diskSpaceFn, rereadConvert = prevDisk, prevConv })
		diskSpaceFn = func(string) (uint64, uint64, error) { return 0, 1 << 40, nil }
		rereadConvert = func(ctx context.Context, cfg baseline.Config) (baseline.Stats, error) {
			// Half written, as a disk that fills leaves it.
			if _, err := prevConv(ctx, cfg); err != nil {
				t.Errorf("the conversion: %v", err)
			}
			return baseline.Stats{}, fmt.Errorf("write shop/big.parquet: %w", syscall.ENOSPC)
		}
		s := newBaselineSupervisor(context.Background(), stage, baseline.LockModeFTWRL)
		got, err := s.stageReread(rereadReq(nil), []string{"shop.big"})
		var full *workingFolderFullError
		if !errors.As(err, &full) || !foldDiskRefused(err) || full.folder != stage {
			t.Fatalf("err = %v, want the working folder's disk failure", err)
		}
		if len(got.files) != 0 {
			t.Fatalf("a failed read returned %v", got.files)
		}
		if left := rereadLeft(t, stage); len(left) != 0 {
			t.Fatalf("the working folder holds %v", left)
		}
	})
}

// The automatic mode's one retry (#1986) reads the same tables, never the
// whole server.
func TestStageReread_theLockAllRetryKeepsTheTableList(t *testing.T) {
	stage := t.TempDir()
	calls := stubRereadDump(t, func(n int, c rereadDumpCall) error {
		if n == 1 {
			return &ftwrlDeniedError{err: errors.New("mydumper failed: Access denied; you need (at least one of) the RELOAD privilege(s)")}
		}
		writeRereadDump(t, c.dir, c.tables)
		return nil
	})
	s := newBaselineSupervisor(context.Background(), stage, baseline.LockModeFTWRL)
	got, err := s.stageReread(rereadReq(nil), []string{"shop.orders"})
	if err != nil {
		t.Fatalf("stageReread: %v", err)
	}
	if len(*calls) != 2 || (*calls)[0].mode != baseline.LockModeFTWRL || (*calls)[1].mode != baseline.LockModeLockAll {
		t.Fatalf("calls = %+v, want ftwrl then lock-all", *calls)
	}
	for _, c := range *calls {
		if !slices.Equal(c.tables, []string{"shop.orders"}) {
			t.Fatalf("an attempt asked for %v, want the one table", c.tables)
		}
	}
	if len(got.files) != 1 {
		t.Fatalf("files = %v", got.files)
	}
}

// A dump with no binlog position is refused like a full read's (#1688): it
// could not be folded from anywhere.
func TestStageReread_aDumpWithNoPositionIsRefused(t *testing.T) {
	stage := t.TempDir()
	stubRereadDump(t, func(_ int, c rereadDumpCall) error {
		writeRereadDump(t, c.dir, c.tables)
		return os.WriteFile(filepath.Join(c.dir, "metadata"), []byte("# Started dump at: 2026-10-02 01:57:48\n# Finished dump at: 2026-10-02 01:57:49\n"), 0o644)
	})
	s := newBaselineSupervisor(context.Background(), stage, baseline.LockModeFTWRL)
	got, err := s.stageReread(rereadReq(nil), []string{"shop.orders"})
	if !errors.Is(err, baseline.ErrDumpNotAnchored) {
		t.Fatalf("err = %v, want the dump refused for having no position", err)
	}
	if len(got.files) != 0 {
		t.Fatalf("files = %v", got.files)
	}
	if left := rereadLeft(t, stage); len(left) != 0 {
		t.Fatalf("the working folder holds %v", left)
	}
}

// A restart in the middle (#2180): the next start removes the staged folder
// whole, finished or not. A staged read is never picked up by a later run.
func TestStageReread_aRestartRemovesTheStagedFolder(t *testing.T) {
	for _, finished := range []bool{false, true} {
		t.Run(fmt.Sprintf("finished=%v", finished), func(t *testing.T) {
			stage := t.TempDir()
			resolved, err := filepath.EvalSymlinks(stage)
			if err != nil {
				t.Fatal(err)
			}
			read := filepath.Join(stage, "reread-123", "2026-11-23T10-00-00Z")
			if err := os.MkdirAll(filepath.Join(read, "shop"), 0o755); err != nil {
				t.Fatal(err)
			}
			marker := baseline.IncompleteMarker
			if finished {
				marker = baseline.SuccessMarker
			}
			for _, f := range []string{filepath.Join(read, marker), filepath.Join(read, "shop", "orders.parquet"), filepath.Join(stage, "reread-123", ".current.lock")} {
				if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Symlink("2026-11-23T10-00-00Z", filepath.Join(stage, "reread-123", "current")); err != nil {
				t.Fatal(err)
			}
			res := reclaimJobDir(console.BaselineJobDir{Root: stage, ResolvedRoot: resolved, Name: "reread-123"})
			if !res.removed || res.err != nil || res.keptBecause != "" || res.published != "" {
				t.Fatalf("reclaim = %+v, want the folder removed and nothing reported as published", res)
			}
			if left := rereadLeft(t, stage); len(left) != 0 {
				t.Fatalf("the working folder holds %v", left)
			}
		})
	}
}

// Through runMydumper itself: the tables go as --tables-list in place of the
// schema selection, and the privilege check is asked about their schemas.
func TestRunMydumper_aTableListReplacesTheSchemaSelection_2640(t *testing.T) {
	argv, _ := fakeMydumperOnPath(t, "mydumper v1.0.3-1, built against MariaDB 10.11.18 with SSL support")
	prevList, prevPriv := listUserSchemas, checkMydumperPrivileges
	t.Cleanup(func() { listUserSchemas, checkMydumperPrivileges = prevList, prevPriv })
	listUserSchemas = func(context.Context, string, config.SSL) ([]string, error) {
		t.Error("the source was asked for its schemas for a read of some tables")
		return nil, nil
	}
	var checked []string
	checkMydumperPrivileges = func(_ context.Context, _ string, _ config.SSL, _ baseline.LockMode, _ mydumperlock.Remedy, schemas []string) error {
		checked = schemas
		return nil
	}
	ctx := withDumpTables(context.Background(), []string{"shop.orders", "crm.People", "shop.items"})
	if err := runMydumper(ctx, wiringDSN, config.SSL{}, []string{"shop", "crm", "other"}, t.TempDir(), baseline.LockModeLockAll, lockModeFromEnv); err != nil {
		t.Fatalf("runMydumper: %v", err)
	}
	args := strings.Split(strings.TrimSpace(argv()), "\n")
	i := slices.Index(args, "--tables-list")
	if i < 0 || i+1 >= len(args) || args[i+1] != "shop.orders,crm.People,shop.items" {
		t.Fatalf("argv = %q, want --tables-list with the three tables", args)
	}
	for _, flag := range []string{"--database", "--regex"} {
		if slices.Contains(args, flag) {
			t.Fatalf("argv = %q: %s beside a table list selects more than the tables", args, flag)
		}
	}
	if j := slices.Index(args, "--sync-thread-lock-mode"); j < 0 || args[j+1] != baseline.LockModeLockAll.MydumperValue() {
		t.Fatalf("argv = %q, want the lock mode sent", args)
	}
	if args[len(args)-2] != "--outputdir" {
		t.Fatalf("argv = %q, want --outputdir last", args)
	}
	if !slices.Equal(checked, []string{"shop", "crm"}) {
		t.Fatalf("the privilege check was asked about %v, want the schemas of the tables", checked)
	}
}

// A mydumper too old to be told the lock mode takes its own: a read of some
// tables is only taken under a mode that was sent.
func TestRunMydumper_aTableListNeedsTheLockModeSent_2640(t *testing.T) {
	record := fakeConsoleMydumper(t, printsVersion(versionDistro))
	calls := stubPreflight(t, nil)
	ctx := withDumpTables(context.Background(), []string{"shop.orders"})
	err := runMydumper(ctx, "u:p@tcp(127.0.0.1:1)/", config.SSL{Mode: "disabled"}, nil, filepath.Join(t.TempDir(), "out"), baseline.LockModeFTWRL, lockModeAutomatic)
	if !errors.Is(err, errRereadUnavailable) || !strings.Contains(err.Error(), "0.10.0") {
		t.Fatalf("err = %v, want the read of some tables refused, naming the installed 0.10.0", err)
	}
	if *calls != 0 {
		t.Errorf("preflight ran %d time(s) before a refusal that needs no server", *calls)
	}
	assertNeverLaunched(t, record)
}
