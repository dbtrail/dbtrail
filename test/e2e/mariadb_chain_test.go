//go:build integration

package e2e_test

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The MariaDB snapshot chain (the Parquet copy on a MariaDB source), end to
// end through the real binary and a real mydumper:
//
//	capture (stream) → snapshot (dump + baseline) → fold (baseline refresh)
//	→ reconstruct (single row and full table) → drill → verify
//
// with MariaDB's own column types (UUID, INET4, INET6, and VECTOR where the
// server has it) in plain columns, in a UUID primary key and in a composite
// INET4+INET6 primary key. Every restored value is compared with the source
// through HEX(), so "the same text" is not mistaken for "the same bytes".
//
// Needs, besides the MySQL index (BINTRAIL_TEST_DSN) and the MariaDB source
// (BINTRAIL_TEST_MARIADB_DSN):
//   - mydumper on $PATH;
//   - a second, empty MariaDB for drill to load into
//     (BINTRAIL_TEST_MARIADB_SCRATCH_DSN, same form as the source DSN). It
//     has to be MariaDB: a MySQL server has no UUID/INET column type.
//
// Under BINTRAIL_REQUIRE_MARIADB=1 (the MariaDB CI job) a missing piece fails
// the test instead of skipping it.

// mariaDBScratchBaseDSN is the drill target, without a database name.
func mariaDBScratchBaseDSN() string {
	return os.Getenv("BINTRAIL_TEST_MARIADB_SCRATCH_DSN")
}

// requireMydumper returns mydumper's path, or skips (fails under
// BINTRAIL_REQUIRE_MARIADB=1) when it is not installed.
func requireMydumper(t *testing.T) string {
	t.Helper()
	p, err := exec.LookPath("mydumper")
	if err != nil {
		testutil.SkipOrFailMariaDB(t, "mydumper not on $PATH: %v", err)
	}
	return p
}

// mydumperClientLibrary reports which client library mydumper was built
// against, from its version line ("mydumper v1.0.3-1, built against MySQL
// 8.4.9 ..." or "... built against MariaDB 10.8.8 ...").
func mydumperClientLibrary(t *testing.T, path string) string {
	t.Helper()
	out, err := exec.Command(path, "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("mydumper --version: %v\n%s", err, out)
	}
	s := string(out)
	switch {
	case strings.Contains(s, "built against MariaDB"):
		return "mariadb"
	case strings.Contains(s, "built against MySQL"):
		return "mysql"
	}
	t.Fatalf("cannot tell which client library mydumper uses from %q", strings.TrimSpace(s))
	return ""
}

// chainBinary builds cmd/bintrail with coverage, like TestEndToEnd_fullPipeline.
func chainBinary(t *testing.T) (binPath, coverDir string) {
	t.Helper()
	tmp := t.TempDir()
	binPath = filepath.Join(tmp, "bintrail")
	build := exec.Command("go", "build", "-cover", "-o", binPath, "./cmd/bintrail")
	build.Dir = projectRoot(t)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build -cover failed: %v\n%s", err, out)
	}
	coverDir = filepath.Join(tmp, "covdata")
	if err := os.MkdirAll(coverDir, 0o755); err != nil {
		t.Fatalf("mkdir covdata: %v", err)
	}
	return binPath, coverDir
}

// runResult runs the binary and returns stdout, stderr and the exit error,
// without failing the test (for the steps whose failure is the assertion).
func runResult(binPath, coverDir string, args ...string) (string, string, error) {
	cmd := exec.Command(binPath, args...)
	cmd.Env = append(os.Environ(), "GOCOVERDIR="+coverDir, "DO_NOT_TRACK=1")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

// mariaDBServerVersion returns (major, minor) of the MariaDB server.
func mariaDBServerVersion(t *testing.T, db *sql.DB) (int, int) {
	t.Helper()
	var v string
	if err := db.QueryRow("SELECT VERSION()").Scan(&v); err != nil {
		t.Fatalf("SELECT VERSION(): %v", err)
	}
	parts := strings.SplitN(v, ".", 3)
	if len(parts) < 2 {
		t.Fatalf("unparseable MariaDB version %q", v)
	}
	major, err1 := strconv.Atoi(parts[0])
	minor, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		t.Fatalf("unparseable MariaDB version %q", v)
	}
	return major, minor
}

// dsnWith returns a MariaDB DSN for schema, logged in as user/pass.
func dsnWith(t *testing.T, base, user, pass, schema string) string {
	t.Helper()
	cfg, err := drivermysql.ParseDSN(base + "/" + schema)
	if err != nil {
		t.Fatalf("ParseDSN(%q): %v", base, err)
	}
	cfg.User, cfg.Passwd = user, pass
	return cfg.FormatDSN()
}

// chainTable is one source table of the chain, and how to read it back.
type chainTable struct {
	name string
	cols []string // every column, in order
	pk   []string
}

// hexRows returns every row of schema.table as one line of HEX() per column
// ("NULL" for NULL), ordered by the primary key. HEX is the byte-exact form:
// two values that print alike but differ in bytes (a trimmed INET6, a
// re-encoded VECTOR) differ here.
func (ct chainTable) hexRows(t *testing.T, db *sql.DB, schema string) []string {
	t.Helper()
	parts := make([]string, len(ct.cols))
	for i, c := range ct.cols {
		parts[i] = fmt.Sprintf("IFNULL(HEX(`%s`),'NULL')", c)
	}
	q := fmt.Sprintf("SELECT CONCAT_WS(',', %s) FROM `%s`.`%s` ORDER BY %s",
		strings.Join(parts, ", "), schema, ct.name, "`"+strings.Join(ct.pk, "`,`")+"`")
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("hex capture of %s.%s: %v", schema, ct.name, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatalf("hex capture scan: %v", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("hex capture rows: %v", err)
	}
	return out
}

// sourceRow returns one source row as the value a reader must hand back for
// each column: the server's text form (CAST AS CHAR) for everything but
// VECTOR, whose value is bytes and is compared as HEX.
func (ct chainTable) sourceRow(t *testing.T, db *sql.DB, schema, where string, args ...any) map[string]*string {
	t.Helper()
	parts := make([]string, len(ct.cols))
	for i, c := range ct.cols {
		if c == "v" {
			parts[i] = "HEX(`v`)"
		} else {
			parts[i] = fmt.Sprintf("CAST(`%s` AS CHAR)", c)
		}
	}
	q := fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE %s", strings.Join(parts, ", "), schema, ct.name, where)
	cells := make([]sql.NullString, len(ct.cols))
	ptrs := make([]any, len(cells))
	for i := range cells {
		ptrs[i] = &cells[i]
	}
	if err := db.QueryRow(q, args...).Scan(ptrs...); err != nil {
		t.Fatalf("source row %s WHERE %s: %v", ct.name, where, err)
	}
	out := make(map[string]*string, len(ct.cols))
	for i, c := range ct.cols {
		if cells[i].Valid {
			s := cells[i].String
			out[c] = &s
		} else {
			out[c] = nil
		}
	}
	return out
}

// assertReconstructedRow compares `bintrail reconstruct` single-row JSON with
// a source row taken by sourceRow.
func assertReconstructedRow(t *testing.T, label, out string, want map[string]*string) {
	t.Helper()
	var got map[string]any
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("%s: reconstruct output is not a JSON row: %v\n%s", label, err, out)
	}
	for col, w := range want {
		g, present := got[col]
		if !present {
			t.Errorf("%s: column %q missing from %v", label, col, got)
			continue
		}
		if w == nil {
			if g != nil {
				t.Errorf("%s: column %q = %v, want NULL", label, col, g)
			}
			continue
		}
		var gs string
		switch v := g.(type) {
		case nil:
			t.Errorf("%s: column %q = NULL, want %q", label, col, *w)
			continue
		case string:
			gs = v
			if col == "v" {
				// []byte reaches the JSON output as base64.
				b, err := base64.StdEncoding.DecodeString(v)
				if err != nil {
					t.Errorf("%s: VECTOR column is not base64 bytes: %q", label, v)
					continue
				}
				gs = strings.ToUpper(hex.EncodeToString(b))
			}
		case float64:
			gs = strconv.FormatFloat(v, 'f', -1, 64)
		default:
			gs = fmt.Sprint(v)
		}
		if gs != *w {
			t.Errorf("%s: column %q = %q, want %q (the source value)", label, col, gs, *w)
		}
	}
}

// lockedBuffer is a bytes.Buffer safe to read while the process writes it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// streamProc is a running `bintrail stream`.
type streamProc struct {
	cmd *exec.Cmd
	log *lockedBuffer
}

func startStream(t *testing.T, binPath, coverDir string, args ...string) *streamProc {
	t.Helper()
	cmd := exec.Command(binPath, append([]string{"stream"}, args...)...)
	cmd.Env = append(os.Environ(), "GOCOVERDIR="+coverDir, "DO_NOT_TRACK=1")
	log := &lockedBuffer{}
	cmd.Stdout = log
	cmd.Stderr = log
	if err := cmd.Start(); err != nil {
		t.Fatalf("start bintrail stream: %v", err)
	}
	p := &streamProc{cmd: cmd, log: log}
	t.Cleanup(func() {
		if p.cmd.ProcessState == nil {
			_ = p.cmd.Process.Kill()
			_ = p.cmd.Wait()
		}
	})
	// The first run starts at the source's CURRENT position, found at startup:
	// a change made before that is not captured, so wait for it.
	deadline := time.Now().Add(30 * time.Second)
	for !strings.Contains(log.String(), "Streaming started") {
		if time.Now().After(deadline) {
			_ = p.cmd.Process.Kill()
			t.Fatalf("bintrail stream did not start within 30s\n%s", log.String())
		}
		time.Sleep(100 * time.Millisecond)
	}
	return p
}

func (p *streamProc) stop(t *testing.T) {
	t.Helper()
	_ = p.cmd.Process.Signal(syscall.SIGINT)
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("bintrail stream did not stop cleanly: %v\n%s", err, p.log.String())
		}
	case <-time.After(30 * time.Second):
		_ = p.cmd.Process.Kill()
		t.Fatalf("bintrail stream did not stop within 30s\n%s", p.log.String())
	}
}

// waitIndexed waits until the index holds exactly want row events for schema.
func waitIndexed(t *testing.T, indexDB *sql.DB, schema string, want int, p *streamProc) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	var n int
	for time.Now().Before(deadline) {
		if p.cmd.ProcessState != nil {
			t.Fatalf("bintrail stream exited early\n%s", p.log.String())
		}
		if err := indexDB.QueryRow(
			"SELECT COUNT(*) FROM binlog_events WHERE schema_name = ?", schema).Scan(&n); err == nil && n >= want {
			if n != want {
				t.Fatalf("index holds %d events for %s, want exactly %d\n%s", n, schema, want, p.log.String())
			}
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("index holds %d events for %s after 45s, want %d\n%s", n, schema, want, p.log.String())
}

// secondBoundary sleeps past the next whole second. Event timestamps and the
// --at flag have one-second resolution, so each phase of the chain has to
// start in a later second than the point in time the previous one is read at.
func secondBoundary() {
	time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(1100 * time.Millisecond)))
}

func TestEndToEnd_MariaDBSnapshotChain(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	testutil.SkipIfNoMariaDB(t)
	mydumper := requireMydumper(t)
	scratchBase := mariaDBScratchBaseDSN()
	if scratchBase == "" {
		testutil.SkipOrFailMariaDB(t, "BINTRAIL_TEST_MARIADB_SCRATCH_DSN is not set: drill needs an empty MariaDB to load into")
	}
	scratchDB, err := sql.Open("mysql", scratchBase+"/?parseTime=true")
	if err != nil {
		t.Fatalf("open scratch MariaDB: %v", err)
	}
	if err := scratchDB.Ping(); err != nil {
		testutil.SkipOrFailMariaDB(t, "scratch MariaDB not reachable: %v", err)
	}

	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	major, minor := mariaDBServerVersion(t, sourceDB)
	if major < 10 || (major == 10 && minor < 10) {
		t.Skipf("MariaDB %d.%d has no INET4 (10.10+); the supported minimum is 10.11", major, minor)
	}
	hasVector := major > 11 || (major == 11 && minor >= 7)
	t.Logf("MariaDB %d.%d, VECTOR covered: %v", major, minor, hasVector)

	rootDB, err := sql.Open("mysql", testutil.MariaDBBaseDSN()+"/")
	if err != nil {
		t.Fatalf("open MariaDB root: %v", err)
	}
	defer rootDB.Close()
	// Registered after CreateTestMariaDB, so it runs before that cleanup and
	// while the connection is still open. A leftover from an earlier run that
	// died would make drill refuse the target, so clear it first as well.
	testutil.MustExec(t, scratchDB, "DROP DATABASE IF EXISTS `"+sourceName+"`")
	t.Cleanup(func() {
		scratchDB.Exec("DROP DATABASE IF EXISTS `" + sourceName + "`")
		scratchDB.Close()
	})

	// The dump logs in as a user that states its password method, so what is
	// proven is "mysql_native_password works", not "whatever root uses".
	dumpUser := fmt.Sprintf("chain_native_%d", time.Now().UnixNano()%1_000_000)
	const dumpPass = "chain-native-pass"
	testutil.MustExec(t, rootDB, fmt.Sprintf(
		"CREATE USER '%s'@'%%' IDENTIFIED VIA mysql_native_password USING PASSWORD('%s')", dumpUser, dumpPass))
	t.Cleanup(func() { rootDB.Exec(fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", dumpUser)) })
	testutil.MustExec(t, rootDB, fmt.Sprintf("GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%'", dumpUser))

	vecCol, vecDef := "", ""
	vec := func(lit string) string { return "" }
	if hasVector {
		vecCol, vecDef = ", v", ", v VECTOR(3)"
		vec = func(lit string) string {
			if lit == "NULL" {
				return ", NULL"
			}
			return ", VEC_FromText('" + lit + "')"
		}
	}
	items := chainTable{name: "items", cols: []string{"id", "u", "i4", "i6", "note"}, pk: []string{"id"}}
	if hasVector {
		items.cols = []string{"id", "u", "i4", "i6", "v", "note"}
	}
	keyed := chainTable{name: "keyed", cols: []string{"u", "i6", "n"}, pk: []string{"u"}}
	netkeys := chainTable{name: "netkeys", cols: []string{"a", "b", "n"}, pk: []string{"a", "b"}}
	tables := []chainTable{items, keyed, netkeys}

	itemRow := func(id, u, i4, i6, v, note string) string {
		return fmt.Sprintf("INSERT INTO items (id, u, i4, i6%s, note) VALUES (%s, %s, %s, %s%s, '%s')",
			vecCol, id, u, i4, i6, vec(v), note)
	}
	vecSet := func(lit string) string {
		if !hasVector {
			return ""
		}
		if lit == "NULL" {
			return ", v = NULL"
		}
		return ", v = VEC_FromText('" + lit + "')"
	}

	// ── Setup: tables and seed rows, before capture starts ──────────────────
	// Values chosen for the ways these types break: trailing zero bytes
	// (10.0.0.0, 2001:db8::, the nil UUID), all ones, the IPv4-mapped and
	// IPv4-compatible INET6 spellings, and bytes 0x7C/0x5C ('|' and '\'),
	// which the index escapes inside pk_values.
	for _, stmt := range []string{
		"CREATE TABLE items (id INT PRIMARY KEY, u UUID, i4 INET4, i6 INET6" + vecDef + ", note VARCHAR(20))",
		"CREATE TABLE keyed (u UUID PRIMARY KEY, i6 INET6, n INT)",
		"CREATE TABLE netkeys (a INET4 NOT NULL, b INET6 NOT NULL, n INT, PRIMARY KEY (a, b))",
		itemRow("1", "'123e4567-e89b-12d3-a456-426614174000'", "'10.0.0.0'", "'2001:db8::'", "[1,2.5,-3]", "seed"),
		itemRow("2", "'00000000-0000-0000-0000-000000000000'", "'0.0.0.0'", "'::'", "[0,0,0]", "seed"),
		itemRow("3", "'ffffffff-ffff-ffff-ffff-ffffffffffff'", "'255.255.255.255'", "'::ffff:1.2.3.4'", "NULL", "seed"),
		itemRow("4", "NULL", "NULL", "NULL", "NULL", "seed"),
		itemRow("5", "'7c5c7c5c-0000-0000-0000-000000000000'", "'124.92.0.0'", "'::1.2.3.4'", "[0.001,0,-1]", "seed"),
		"INSERT INTO keyed VALUES ('00000000-0000-0000-0000-000000000000', '::1', 1)",
		"INSERT INTO keyed VALUES ('ffffffff-ffff-ffff-ffff-ffffffffffff', '::', 2)",
		"INSERT INTO keyed VALUES ('7c5c7c5c-5c7c-0000-0000-000000000000', '::', 3)",
		"INSERT INTO netkeys VALUES ('124.92.0.0', '::', 1), ('0.0.0.0', '::1', 2), ('10.0.0.1', '2001:db8::', 3)",
	} {
		testutil.MustExec(t, sourceDB, stmt)
	}

	binPath, coverDir := chainBinary(t)
	indexDB, indexName := testutil.CreateTestDB(t)
	indexDSN := testutil.SnapshotDSN(indexName)
	sourceDSN := testutil.MariaDBBaseDSN() + "/" + sourceName
	tmp := t.TempDir()
	baseDir := filepath.Join(tmp, "baselines")

	// ── 1. Capture ───────────────────────────────────────────────────────────
	run(t, binPath, coverDir, "init", "--index-dsn", indexDSN, "--partitions", "4")
	run(t, binPath, coverDir, "snapshot", "--source-dsn", sourceDSN, "--index-dsn", indexDSN, "--schemas", sourceName)
	stream := startStream(t, binPath, coverDir,
		"--index-dsn", indexDSN, "--source-dsn", sourceDSN,
		"--server-id", strconv.Itoa(990000+int(time.Now().UnixNano()%9999)),
		"--schemas", sourceName, "--checkpoint", "1")

	// Phase 1, before the snapshot: captured, and already inside the dump.
	for _, stmt := range []string{
		"UPDATE items SET note = 'p1', i6 = 'fe80::1:0:0:0' WHERE id = 1",
		itemRow("6", "'a0000000-0000-7000-8000-000000000001'", "'192.168.0.0'", "'1::'", "[9,8,7]", "p1"),
		"DELETE FROM items WHERE id = 3",
		"INSERT INTO keyed VALUES ('018f3a2b-1c4d-7e5f-8a9b-0c1d2e3f4a5b', '1::', 4)",
	} {
		testutil.MustExec(t, sourceDB, stmt)
	}
	waitIndexed(t, indexDB, sourceName, 4, stream)
	secondBoundary()

	// ── 2. Snapshot: real mydumper, then the Parquet baseline ───────────────
	dumpDir := filepath.Join(tmp, "dump")
	run(t, binPath, coverDir, "dump",
		"--source-dsn", dsnWith(t, testutil.MariaDBBaseDSN(), dumpUser, dumpPass, sourceName),
		"--output-dir", dumpDir, "--schemas", sourceName, "--mydumper-path", mydumper)
	run(t, binPath, coverDir, "baseline", "--input", dumpDir, "--output", baseDir)
	secondBoundary()

	// Phase 2, after the snapshot: only the index knows about it.
	for _, stmt := range []string{
		"UPDATE items SET u = 'b0000000-0000-0000-0000-00000000000b', i4 = '10.1.0.0', i6 = '::ffff:0.0.0.0'" + vecSet("[0.5,0,1]") + " WHERE id = 4",
		"UPDATE items SET u = '018f3a2b-1c4d-7e5f-8a9b-0c1d2e3f4a5b', i6 = '1:0:0:2::3' WHERE id = 2",
		"DELETE FROM items WHERE id = 5",
		itemRow("7", "'00000000-0000-0000-0000-000000000001'", "'0.0.0.1'", "'::'", "NULL", "p2"),
		"UPDATE items SET u = NULL, i4 = NULL, i6 = NULL" + vecSet("NULL") + " WHERE id = 6",
		"UPDATE keyed SET i6 = '::ffff:0.0.0.0', n = 10 WHERE u = '00000000-0000-0000-0000-000000000000'",
		"DELETE FROM keyed WHERE n = 2",
		"INSERT INTO keyed VALUES ('a0000000-0000-0000-0000-000000000000', NULL, 5)",
		"UPDATE netkeys SET n = 20 WHERE a = '0.0.0.0'",
		"UPDATE netkeys SET n = 30 WHERE a = '124.92.0.0'",
		"INSERT INTO netkeys VALUES ('255.255.255.255', 'ffff::', 4)",
	} {
		testutil.MustExec(t, sourceDB, stmt)
	}
	waitIndexed(t, indexDB, sourceName, 15, stream)
	secondBoundary()
	t1 := time.Now().UTC().Truncate(time.Second).Add(-time.Second)
	at1 := t1.Format("2006-01-02 15:04:05")
	state1 := map[string][]string{}
	for _, ct := range tables {
		state1[ct.name] = ct.hexRows(t, sourceDB, sourceName)
	}
	row1Item4 := items.sourceRow(t, sourceDB, sourceName, "id = 4")
	row1Item2 := items.sourceRow(t, sourceDB, sourceName, "id = 2")
	row1Nil := keyed.sourceRow(t, sourceDB, sourceName, "u = '00000000-0000-0000-0000-000000000000'")
	row1Net := netkeys.sourceRow(t, sourceDB, sourceName, "a = '124.92.0.0' AND b = '::'")

	// ── 3. Reconstruct at T1 from the mydumper snapshot ─────────────────────
	// Single row: the value each reader hands back is the source's own value.
	for _, c := range []struct {
		label, table, pk, pkCols string
		want                     map[string]*string
	}{
		{"items id=4 at T1", "items", "4", "id", row1Item4},
		{"items id=2 at T1", "items", "2", "id", row1Item2},
		{"keyed nil UUID at T1", "keyed", "00000000-0000-0000-0000-000000000000", "u", row1Nil},
		{"keyed nil UUID at T1, no dashes", "keyed", "00000000000000000000000000000000", "u", row1Nil},
		{"netkeys 124.92.0.0|:: at T1", "netkeys", "124.92.0.0|::", "a,b", row1Net},
	} {
		out := run(t, binPath, coverDir, "reconstruct", "--index-dsn", indexDSN, "--baseline-dir", baseDir,
			"--schema", sourceName, "--table", c.table, "--pk", c.pk, "--pk-columns", c.pkCols, "--at", at1)
		assertReconstructedRow(t, c.label, out, c.want)
	}

	// Full table: drill reconstructs every table, loads the dump into the
	// scratch MariaDB and counts; the HEX comparison below is the byte check.
	drill := func(label, at string, want map[string][]string) {
		t.Helper()
		var names []string
		for _, ct := range tables {
			names = append(names, sourceName+"."+ct.name)
		}
		out, errOut, err := runResult(binPath, coverDir, "drill", "--index-dsn", indexDSN,
			"--target-dsn", scratchBase+"/", "--baseline-dir", baseDir,
			"--tables", strings.Join(names, ","), "--at", at, "--format", "json")
		if err != nil {
			t.Fatalf("%s: drill failed: %v\nstdout:\n%s\nstderr:\n%s", label, err, out, errOut)
		}
		for _, ct := range tables {
			got := ct.hexRows(t, scratchDB, sourceName)
			if strings.Join(got, "\n") != strings.Join(want[ct.name], "\n") {
				t.Errorf("%s: %s restored by drill differs from the source (HEX per column)\n got:\n  %s\nwant:\n  %s",
					label, ct.name, strings.Join(got, "\n  "), strings.Join(want[ct.name], "\n  "))
			}
		}
		testutil.MustExec(t, scratchDB, "DROP DATABASE IF EXISTS `"+sourceName+"`")
	}
	drill("drill at T1 from the mydumper snapshot", at1, state1)

	// ── 4. Fold: baseline refresh to T1 ─────────────────────────────────────
	// --table-deltas=false writes each table in full, so the new Parquet file
	// is the fold's own output, and every later step reads it.
	run(t, binPath, coverDir, "baseline", "refresh", "--index-dsn", indexDSN, "--baseline-dir", baseDir,
		"--at", at1, "--table-deltas=false")
	secondBoundary()

	// Phase 3, after the fold.
	for _, stmt := range []string{
		"UPDATE items SET i4 = '1.2.3.4'" + vecSet("[3,3,3]") + " WHERE id = 1",
		itemRow("8", "'c0000000-0000-0000-0000-00000000000c'", "'8.8.8.8'", "'2001:4860:4860::8888'", "[1,1,1]", "p3"),
		"DELETE FROM items WHERE id = 7",
		"UPDATE keyed SET n = 11 WHERE u = '7c5c7c5c-5c7c-0000-0000-000000000000'",
		"INSERT INTO keyed VALUES ('c0000000-0000-0000-0000-000000000000', '2001:db8::1', 6)",
		"DELETE FROM netkeys WHERE a = '10.0.0.1'",
	} {
		testutil.MustExec(t, sourceDB, stmt)
	}
	waitIndexed(t, indexDB, sourceName, 21, stream)
	secondBoundary()
	at2 := time.Now().UTC().Truncate(time.Second).Add(-time.Second).Format("2006-01-02 15:04:05")
	state2 := map[string][]string{}
	for _, ct := range tables {
		state2[ct.name] = ct.hexRows(t, sourceDB, sourceName)
	}
	stream.stop(t)

	// ── 5. Reconstruct and drill at the end, on top of the folded snapshot ──
	for _, c := range []struct {
		label, table, pk, pkCols, where string
	}{
		{"items id=1 at end", "items", "1", "id", "id = 1"},
		{"items id=4 at end (from the folded snapshot)", "items", "4", "id", "id = 4"},
		{"keyed 7c5c.. at end", "keyed", "7c5c7c5c-5c7c-0000-0000-000000000000", "u", "u = '7c5c7c5c-5c7c-0000-0000-000000000000'"},
		{"keyed 7c5c.. at end, upper case", "keyed", "7C5C7C5C-5C7C-0000-0000-000000000000", "u", "u = '7c5c7c5c-5c7c-0000-0000-000000000000'"},
		{"netkeys 255.255.255.255|ffff:: at end", "netkeys", "255.255.255.255|FFFF::", "a,b", "a = '255.255.255.255' AND b = 'ffff::'"},
	} {
		want := tables[map[string]int{"items": 0, "keyed": 1, "netkeys": 2}[c.table]].sourceRow(t, sourceDB, sourceName, c.where)
		out := run(t, binPath, coverDir, "reconstruct", "--index-dsn", indexDSN, "--baseline-dir", baseDir,
			"--schema", sourceName, "--table", c.table, "--pk", c.pk, "--pk-columns", c.pkCols, "--at", at2)
		assertReconstructedRow(t, c.label, out, want)
	}
	drill("drill at end from the folded snapshot", at2, state2)

	// ── 6. verify, both content modes ───────────────────────────────────────
	// Baseline-anchored verify compares two READS of the database, so take a
	// second mydumper snapshot now: verify rebuilds the first one forward
	// through the index to the second one's position and compares. Live mode
	// compares the index with the source. A MariaDB type the readers cannot
	// render shows up as "inconclusive", which this test does not accept.
	dump2 := filepath.Join(tmp, "dump2")
	run(t, binPath, coverDir, "dump",
		"--source-dsn", dsnWith(t, testutil.MariaDBBaseDSN(), dumpUser, dumpPass, sourceName),
		"--output-dir", dump2, "--schemas", sourceName, "--mydumper-path", mydumper)
	run(t, binPath, coverDir, "baseline", "--input", dump2, "--output", baseDir)
	var names []string
	for _, ct := range tables {
		names = append(names, sourceName+"."+ct.name)
	}
	for _, mode := range []struct {
		label string
		args  []string
	}{
		{"baseline-anchored", []string{"--baseline-dir", baseDir}},
		{"live source", []string{"--source-dsn", sourceDSN, "--baseline-dir", baseDir}},
	} {
		args := append([]string{"verify", "--index-dsn", indexDSN, "--tables", strings.Join(names, ","), "--format", "json"}, mode.args...)
		out, errOut, err := runResult(binPath, coverDir, args...)
		if err != nil {
			t.Errorf("verify (%s) failed: %v\nstdout:\n%s\nstderr:\n%s", mode.label, err, out, errOut)
			continue
		}
		var rep struct {
			Tables []struct {
				Schema string `json:"schema"`
				Table  string `json:"table"`
				Status string `json:"status"`
				Reason string `json:"reason"`
			} `json:"tables"`
		}
		if err := json.Unmarshal([]byte(out), &rep); err != nil {
			t.Fatalf("verify (%s) output: %v\n%s", mode.label, err, out)
		}
		if len(rep.Tables) != len(tables) {
			t.Errorf("verify (%s) reported %d tables, want %d:\n%s", mode.label, len(rep.Tables), len(tables), out)
		}
		for _, tr := range rep.Tables {
			if tr.Status != "match" {
				t.Errorf("verify (%s): %s.%s is %q, want match: %s", mode.label, tr.Schema, tr.Table, tr.Status, tr.Reason)
			}
		}
	}
}

// TestEndToEnd_MariaDBSnapshotEd25519 dumps as a MariaDB user authenticated
// with ed25519. Whether that can work depends only on the client library
// mydumper was built against, which is why the test branches on it:
//   - built against MariaDB Connector/C (the arm64 packages, Homebrew): the
//     plugin ships with the library, and the dump must succeed;
//   - built against the MySQL client library (the amd64 packages, and the
//     console image on amd64): MySQL has no client_ed25519 plugin, and the
//     dump must fail with that exact reason, not with some other error that
//     would read the same.
//
// There is no workaround inside dbtrail by design: the snapshot is mydumper's
// job. The documented method is a mysql_native_password user.
func TestEndToEnd_MariaDBSnapshotEd25519(t *testing.T) {
	testutil.SkipIfNoMySQL(t)
	testutil.SkipIfNoMariaDB(t)
	mydumper := requireMydumper(t)
	lib := mydumperClientLibrary(t, mydumper)

	sourceDB, sourceName := testutil.CreateTestMariaDB(t)
	testutil.MustExec(t, sourceDB, "CREATE TABLE t (id INT PRIMARY KEY, n INT)")
	testutil.MustExec(t, sourceDB, "INSERT INTO t VALUES (1, 1)")

	rootDB, err := sql.Open("mysql", testutil.MariaDBBaseDSN()+"/")
	if err != nil {
		t.Fatalf("open MariaDB root: %v", err)
	}
	defer rootDB.Close()
	var loaded int
	if err := rootDB.QueryRow("SELECT COUNT(*) FROM information_schema.PLUGINS WHERE PLUGIN_NAME = 'ed25519' AND PLUGIN_STATUS = 'ACTIVE'").Scan(&loaded); err != nil {
		t.Fatalf("look up the ed25519 plugin: %v", err)
	}
	if loaded == 0 {
		testutil.MustExec(t, rootDB, "INSTALL SONAME 'auth_ed25519'")
	}
	user := fmt.Sprintf("chain_ed_%d", time.Now().UnixNano()%1_000_000)
	const pass = "chain-ed-pass"
	testutil.MustExec(t, rootDB, fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED VIA ed25519 USING PASSWORD('%s')", user, pass))
	t.Cleanup(func() { rootDB.Exec(fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", user)) })
	testutil.MustExec(t, rootDB, fmt.Sprintf("GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%'", user))

	// The user itself works: the Go driver speaks ed25519, so a failure below
	// is mydumper's, not a wrong password or grant.
	edDB, err := sql.Open("mysql", dsnWith(t, testutil.MariaDBBaseDSN(), user, pass, sourceName))
	if err != nil {
		t.Fatalf("open as the ed25519 user: %v", err)
	}
	defer edDB.Close()
	// Go's driver fails about one ed25519 login in two hundred with
	// "malformed packet" (measured on MariaDB 10.11, with and without TLS;
	// never with mysql_native_password). That is not what this test is about,
	// so those two steps get a few attempts, and each flake is logged.
	var n int
	for attempt := 1; ; attempt++ {
		err = edDB.QueryRow("SELECT n FROM t WHERE id = 1").Scan(&n)
		if err == nil {
			break
		}
		if attempt == 3 || !strings.Contains(err.Error(), "malformed packet") {
			t.Fatalf("the ed25519 user cannot log in from Go, so this test proves nothing: %v", err)
		}
		t.Logf("Go driver ed25519 login flake (attempt %d): %v", attempt, err)
	}

	binPath, coverDir := chainBinary(t)
	var out, errOut string
	var dumpDir string
	for attempt := 1; ; attempt++ {
		dumpDir = filepath.Join(t.TempDir(), "dump")
		out, errOut, err = runResult(binPath, coverDir, "dump",
			"--source-dsn", dsnWith(t, testutil.MariaDBBaseDSN(), user, pass, sourceName),
			"--output-dir", dumpDir, "--schemas", sourceName, "--mydumper-path", mydumper)
		// bintrail's own connectivity check (Go driver) runs before mydumper.
		if err == nil || attempt == 3 || !strings.Contains(errOut, "failed to ping MySQL: malformed packet") {
			break
		}
		t.Logf("Go driver ed25519 login flake before mydumper ran (attempt %d): %s", attempt, strings.TrimSpace(errOut))
	}
	t.Logf("mydumper client library: %s", lib)
	switch lib {
	case "mariadb":
		if err != nil {
			t.Fatalf("mydumper built against MariaDB Connector/C must dump as an ed25519 user: %v\nstdout:\n%s\nstderr:\n%s", err, out, errOut)
		}
		if _, statErr := os.Stat(filepath.Join(dumpDir, sourceName+".t.00000.sql")); statErr != nil {
			t.Fatalf("dump reported success but wrote no data file: %v\n%s", statErr, errOut)
		}
	case "mysql":
		if err == nil {
			t.Fatalf("mydumper built against the MySQL client library dumped as an ed25519 user; "+
				"the documented limitation no longer holds, update docs/mariadb.md\nstdout:\n%s\nstderr:\n%s", out, errOut)
		}
		if !strings.Contains(errOut, "client_ed25519") || !strings.Contains(errOut, "cannot be loaded") {
			t.Fatalf("the dump failed, but not for the known reason (the MySQL client library has no client_ed25519 plugin): %v\nstderr:\n%s", err, errOut)
		}
	}
}
