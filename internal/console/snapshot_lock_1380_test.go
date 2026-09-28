package console

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #1380: the Snapshots page says how each snapshot was locked when the
// database was read. These drive the real endpoints over real table files,
// and the page's own functions over what the endpoints answer.

const (
	lockMixedDir, lockMixedAt   = "2026-06-10T12-00-00Z", "2026-06-10 12:00:00"
	lockLockedDir, lockLockedAt = "2026-06-09T12-00-00Z", "2026-06-09 12:00:00"
	lockTornDir, lockTornAt     = "2026-06-08T12-00-00Z", "2026-06-08 12:00:00"
	lockOldDir, lockOldAt       = "2026-06-07T12-00-00Z", "2026-06-07 12:00:00"
	lockBadDir, lockBadAt       = "2026-06-06T12-00-00Z", "2026-06-06 12:00:00"
)

func writeLockTable(t *testing.T, root, snap, table, stamp string) string {
	t.Helper()
	cols, err := baseline.ParseSchemaText("CREATE TABLE `t` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n);\n")
	if err != nil {
		t.Fatal(err)
	}
	md := map[string]string{}
	if stamp != "" {
		md[baseline.MetaKeyLockMode] = stamp
	}
	path := filepath.Join(root, snap, "shop", table+".parquet")
	w, err := baseline.NewWriter(path, cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 10, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// newLockFixture: a snapshot that took one table from a read with no locks
// (by hard link, as an update carries a table that did not change) and one
// from a locked read; a locked one; one read with no locks; one written
// before the record; and one with a table file that cannot be read.
func newLockFixture(t *testing.T) string {
	t.Helper()
	snapshotLockMemo = &lockMemo{m: map[string]lockMemoEntry{}}
	root := t.TempDir()
	writeLockTable(t, root, lockLockedDir, "orders", "ftwrl")
	writeLockTable(t, root, lockLockedDir, "users", "lock-all")
	torn := writeLockTable(t, root, lockTornDir, "orders", "no-lock")
	writeLockTable(t, root, lockTornDir, "users", "no-lock")
	writeLockTable(t, root, lockOldDir, "orders", "")
	writeLockTable(t, root, lockOldDir, "users", "")
	writeLockTable(t, root, lockBadDir, "orders", "ftwrl")
	writeBaselineFixture(t, root, lockBadDir, "shop", "users.parquet")

	carried := filepath.Join(root, lockMixedDir, "shop", "orders.parquet")
	if err := os.MkdirAll(filepath.Dir(carried), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(torn, carried); err != nil {
		t.Fatal(err)
	}
	writeLockTable(t, root, lockMixedDir, "users", "ftwrl")
	for _, dir := range []string{lockMixedDir, lockLockedDir, lockTornDir, lockOldDir, lockBadDir} {
		writeBaselineFixture(t, root, dir, "_SUCCESS")
	}
	return root
}

func rowLock(t *testing.T, row map[string]json.RawMessage) string {
	t.Helper()
	raw, ok := row["lock"]
	if !ok {
		return "(absent)"
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestBaselinesAPI_lockOnTheRow(t *testing.T) {
	srv := newBaselineServer(t, newLockFixture(t), true)
	for round := 1; round <= 2; round++ { // the second answer comes from the memo
		rows := rawRows(t, srv)
		if len(rows) != 5 {
			t.Fatalf("listed %d snapshots, want 5", len(rows))
		}
		for at, want := range map[string]string{
			lockMixedAt:  "torn", // the worst of its tables
			lockLockedAt: "consistent",
			lockTornAt:   "torn",
			lockOldAt:    "unknown", // no record is never consistent
			lockBadAt:    "unknown", // a footer that cannot be read
		} {
			if got := rowLock(t, rows[at]); got != want {
				t.Errorf("round %d, snapshot %s: lock %q, want %q", round, at, got, want)
			}
			if len(rows[at]["tables"]) == 0 || len(rows[at]["time"]) == 0 {
				t.Errorf("snapshot %s lost a field it had: %v", at, rows[at])
			}
		}
	}
}

// A file replaced at the same path is read again, not remembered.
func TestLockMemo_readsAReplacedFileAgain(t *testing.T) {
	root := t.TempDir()
	memo := &lockMemo{m: map[string]lockMemoEntry{}}
	path := writeLockTable(t, root, lockLockedDir, "orders", "ftwrl")
	if got, read := memo.of(path); !read || got != baseline.ReadConsistent {
		t.Fatalf("first read: %s, %v", got, read)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if got, read := memo.of(path); read || got != baseline.ReadUnknown {
		t.Fatalf("a file that is gone reads %s, read %v; want unknown, not read", got, read)
	}
	// A different file at the same path: another size, since the record is
	// longer.
	writeLockTable(t, root, lockLockedDir, "orders", "safe-no-lock-is-not-this")
	if got, read := memo.of(path); !read || got != baseline.ReadUnknown {
		t.Fatalf("the replaced file reads %s, read %v; want unknown, read", got, read)
	}
}

// A table that is only in S3 is not looked at, so the snapshot has no word,
// unless a table that was looked at is torn.
func TestSnapshotLocks_verdict(t *testing.T) {
	c, u, x := baseline.ReadConsistent, baseline.ReadUnknown, baseline.ReadTorn
	type look struct {
		lock baseline.ReadConsistency
		read bool
	}
	for _, tc := range []struct {
		name      string
		looked    []look
		notLooked int
		want      string
	}{
		{"no table", nil, 0, ""},
		{"nothing looked at", nil, 3, ""},
		{"all consistent", []look{{c, true}, {c, true}}, 0, "consistent"},
		{"one with no record", []look{{c, true}, {u, true}}, 0, "unknown"},
		{"one torn", []look{{c, true}, {x, true}, {u, true}}, 0, "torn"},
		{"a footer that was not read is not consistent", []look{{c, true}, {c, false}}, 0, "unknown"},
		{"consistent beside a table not looked at", []look{{c, true}}, 1, ""},
		{"no record beside a table not looked at", []look{{u, true}}, 1, ""},
		{"torn beside a table not looked at", []look{{x, true}}, 1, "torn"},
		{"a value out of range is not consistent", []look{{baseline.ReadConsistency(42), true}}, 0, "unknown"},
	} {
		l := snapshotLocks{notLooked: tc.notLooked}
		for _, k := range tc.looked {
			l.add(k.lock, k.read)
		}
		if got := l.verdict(); got != tc.want {
			t.Errorf("%s: %q, want %q", tc.name, got, tc.want)
		}
	}
}

func lockDetail(t *testing.T, srv *Server, at string) (baselineFilesResponse, map[string]string) {
	t.Helper()
	rec, body := doServersReq(t, srv, "GET", "/api/baselines/files"+detailQuery(at), "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var resp baselineFilesResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	tables := map[string]string{}
	for _, tb := range resp.Tables {
		tables[tb.Table] = tb.Lock
	}
	return resp, tables
}

func TestBaselineFilesAPI_lockOfTheSnapshotAndOfEachTable(t *testing.T) {
	srv := newBaselineServer(t, newLockFixture(t), true)
	for _, tc := range []struct {
		at, lock      string
		torn, unknown int
		orders, users string
	}{
		{lockMixedAt, "torn", 1, 0, "torn", "consistent"},
		{lockLockedAt, "consistent", 0, 0, "consistent", "consistent"},
		{lockTornAt, "torn", 2, 0, "torn", "torn"},
		{lockOldAt, "unknown", 0, 2, "unknown", "unknown"},
		// The table whose footer cannot be read has no word of its own and
		// counts as unknown for the snapshot.
		{lockBadAt, "unknown", 0, 1, "consistent", ""},
	} {
		resp, tables := lockDetail(t, srv, tc.at)
		if resp.Lock != tc.lock || resp.LockTorn != tc.torn || resp.LockUnknown != tc.unknown {
			t.Errorf("%s: lock %q, %d torn, %d unknown; want %q, %d, %d",
				tc.at, resp.Lock, resp.LockTorn, resp.LockUnknown, tc.lock, tc.torn, tc.unknown)
		}
		if tables["orders"] != tc.orders || tables["users"] != tc.users {
			t.Errorf("%s: orders %q, users %q; want %q, %q", tc.at, tables["orders"], tables["users"], tc.orders, tc.users)
		}
	}
}

// The snapshot the mixed one took its torn table from is as it was: the
// carried file is that snapshot's own file, and reading it changed nothing.
func TestLockFixture_carriedFileIsTheOlderSnapshotsFile(t *testing.T) {
	root := newLockFixture(t)
	a, err := os.Stat(filepath.Join(root, lockTornDir, "shop", "orders.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.Stat(filepath.Join(root, lockMixedDir, "shop", "orders.parquet"))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(a, b) {
		t.Fatal("the fixture's carried table is not a hard link")
	}
}

// The page's own words, over the documents the real endpoints answered.
func TestSnapshotLock_wordsOfTheRealListingAndDetail(t *testing.T) {
	srv := newBaselineServer(t, newLockFixture(t), true)
	rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var pills map[string]*struct {
		Class string `json:"class"`
		Text  string `json:"text"`
	}
	runViewsScript(t, "const b = "+string(body)+";\n"+
		"console.log(JSON.stringify(Object.fromEntries(b.snapshots.map((sn) => [sn.time, flat(snapshotLockPill(sn.lock)) || null]))));", &pills)
	for at, want := range map[string]string{
		lockMixedAt:  "no locks",
		lockLockedAt: "",
		lockTornAt:   "no locks",
		lockOldAt:    "locks not recorded",
		lockBadAt:    "locks not recorded",
	} {
		got := ""
		if p := pills[at]; p != nil {
			got = p.Text
			if !strings.Contains(p.Class, "tag-pill") || !strings.Contains(p.Class, "snap-lock") {
				t.Errorf("row %s: the mark has class %q", at, p.Class)
			}
		}
		if got != want {
			t.Errorf("row %s: %q, want %q", at, got, want)
		}
	}

	var lines map[string]struct {
		Line  string            `json:"line"`
		Marks map[string]string `json:"marks"`
	}
	docs := []string{}
	for _, at := range []string{lockMixedAt, lockLockedAt, lockTornAt, lockOldAt, lockBadAt} {
		docs = append(docs, `"`+at+`": `+detailDoc(t, srv, at))
	}
	runViewsScript(t, "const docs = {"+strings.Join(docs, ", ")+"};\n"+
		"const out = {};\n"+
		"for (const [at, d] of Object.entries(docs)) out[at] = { line: snapshotLockLine(d),\n"+
		"  marks: Object.fromEntries(d.tables.map((t) => [t.table, (flat(tableLockMark(t)) || { text: '' }).text])) };\n"+
		"console.log(JSON.stringify(out));", &lines)
	for at, want := range map[string]struct{ line, orders, users string }{
		lockMixedAt:  {"Read with no locks: 1 of 2 tables. Rows were copied at different moments and may not agree with each other.", " · no locks", ""},
		lockLockedAt: {"Read with locks: every row is from one moment.", "", ""},
		lockTornAt:   {"Read with no locks: 2 of 2 tables. Rows were copied at different moments and may not agree with each other.", " · no locks", " · no locks"},
		lockOldAt:    {"Locks not recorded: 2 of 2 tables. They may have been read with no locks.", " · locks not recorded", " · locks not recorded"},
		lockBadAt:    {"Locks not recorded: 1 of 2 tables. They may have been read with no locks.", "", ""},
	} {
		got := lines[at]
		if got.Line != want.line {
			t.Errorf("detail %s:\n got %q\nwant %q", at, got.Line, want.line)
		}
		if got.Marks["orders"] != want.orders || got.Marks["users"] != want.users {
			t.Errorf("detail %s: orders %q, users %q; want %q, %q", at, got.Marks["orders"], got.Marks["users"], want.orders, want.users)
		}
	}
}

// Every way the wire can say it, and every way it can fail to.
func TestSnapshotLock_words(t *testing.T) {
	var got []string
	values := []string{`"consistent"`, `"torn"`, `"unknown"`, `undefined`, `null`, `""`, `"Consistent"`, `"ok"`, `true`, `0`, `"consistent "`, `{}`}
	runViewsScript(t, "console.log(JSON.stringify(["+strings.Join(values, ",")+"].map((v) => (flat(snapshotLockPill(v)) || { text: '' }).text)));", &got)
	want := []string{"", "no locks", "locks not recorded",
		"locks not checked", "locks not checked", "locks not checked", "locks not checked", "locks not checked",
		"locks not checked", "locks not checked", "locks not checked", "locks not checked"}
	if len(got) != len(want) {
		t.Fatalf("%d answers for %d values", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("lock %s: %q, want %q", values[i], got[i], want[i])
		}
	}

	// The detail's line: nothing looked up says nothing, and a count of zero
	// is never said.
	var lines []string
	docs := []string{
		`null`, `{}`, `{"tables":[{"table":"a"}]}`,
		`{"lock":"consistent","tables":[{"table":"a"}]}`,
		`{"lock":"torn","lock_torn":1,"tables":[{"table":"a"}]}`,
		`{"lock":"torn","lock_torn":1,"lock_unknown":2,"tables":[{"table":"a"},{"table":"b"},{"table":"c"}]}`,
		`{"lock":"unknown","lock_unknown":1,"tables":[{"table":"a"}]}`,
	}
	runViewsScript(t, "console.log(JSON.stringify(["+strings.Join(docs, ",")+"].map(snapshotLockLine)));", &lines)
	wantLines := []string{"", "", "",
		"Read with locks: every row is from one moment.",
		"Read with no locks: 1 of 1 table. Rows were copied at different moments and may not agree with each other.",
		"Read with no locks: 1 of 3 tables. Rows were copied at different moments and may not agree with each other. Locks not recorded: 2 of 3 tables. They may have been read with no locks.",
		"Locks not recorded: 1 of 1 table. They may have been read with no locks.",
	}
	for i := range wantLines {
		if i >= len(lines) || lines[i] != wantLines[i] {
			t.Errorf("detail %s:\n got %q\nwant %q", docs[i], lines, wantLines[i])
		}
	}
	for _, s := range append(append([]string{}, got...), lines...) {
		if strings.ContainsAny(s, "\u2014\u2013") || strings.Contains(s, "undefined") || strings.Contains(s, "NaN") {
			t.Errorf("drawn text %q", s)
		}
	}
}

// Where the marks are mounted: once on the row, once in the opened row.
func TestSnapshotLock_isMounted(t *testing.T) {
	js := readAsset(t, "app.js")
	panel := functionBody(t, js, "function baselinesPanel(")
	if n := strings.Count(panel, "snapshotLockPill(sn.lock)"); n != 1 {
		t.Errorf("the snapshot list mounts the mark %d times, want once", n)
	}
	detail := functionBody(t, js, "async function loadBackupDetail(")
	for _, want := range []string{"snapshotLockLine(d)", "tableLockMark(t)"} {
		if n := strings.Count(detail, want); n != 1 {
			t.Errorf("the opened row holds %q %d times, want once", want, n)
		}
	}
	for _, fn := range []string{"snapshotLockPill", "snapshotLockLine", "tableLockMark"} {
		span := jsFunctionSpan(t, js, fn)
		for _, sink := range []string{"innerHTML", "outerHTML", "insertAdjacentHTML", "svgEl(", "DOMParser"} {
			if strings.Contains(span, sink) {
				t.Errorf("%s uses %s", fn, sink)
			}
		}
	}
}
