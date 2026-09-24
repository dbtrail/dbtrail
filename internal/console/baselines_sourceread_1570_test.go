package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
)

// #1570 through the real endpoint, over the layout table deltas (#1638, on by
// default since v0.84.0) leave on disk: every refresh carries the table file
// forward by hard link, changed or not, and writes the run's footer on the
// chain's newest pair beside it.

const srCreateSQL = "CREATE TABLE `t` (\n  `id` int NOT NULL,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB;\n"

func srCols(t *testing.T) []baseline.Column {
	t.Helper()
	cols, err := baseline.ParseSchemaText(srCreateSQL)
	if err != nil {
		t.Fatal(err)
	}
	return cols
}

// srFooter is a footer as a writer of that kind leaves it.
func srFooter(producer, stampedAt string, read *baseline.SourceRead) map[string]string {
	md := map[string]string{
		baseline.MetaKeyCreateTableSQL:    srCreateSQL,
		baseline.MetaKeySnapshotTimestamp: stampedAt,
		baseline.MetaKeyBinlogFile:        "binlog.000007",
		baseline.MetaKeyBinlogPos:         "4",
	}
	if producer != "" {
		md[baseline.MetaKeySnapshotProducer] = producer
	}
	if read != nil {
		read.Stamp(md)
	}
	return md
}

func srWriteTable(t *testing.T, path string, md map[string]string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	w, err := baseline.NewWriter(path, srCols(t), baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRow([]string{"1"}, []bool{false}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func srWritePair(t *testing.T, base string, seq int, md map[string]string) {
	t.Helper()
	if err := baseline.WriteTableDeltaPair(base, seq, srCols(t), md, nil, nil); err != nil {
		t.Fatal(err)
	}
}

// srLink hard-links every file of a table (the file and its chain) from one
// snapshot into another, the way a refresh carries them.
func srLink(t *testing.T, fromBase, toBase string, seqs ...int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(toBase), 0o755); err != nil {
		t.Fatal(err)
	}
	link := func(a, b string) {
		if err := os.Link(a, b); err != nil {
			t.Fatal(err)
		}
	}
	link(fromBase, toBase)
	for _, s := range seqs {
		fp, fu := baseline.TableDeltaPaths(fromBase, s)
		tp, tu := baseline.TableDeltaPaths(toBase, s)
		link(fp, tp)
		link(fu, tu)
	}
}

func srGet(t *testing.T, root, at string) baselineFilesResponse {
	t.Helper()
	srv := newBaselineServer(t, root, true)
	rec, body := doServersReq(t, srv, "GET", "/api/baselines/files?at="+at, "")
	if rec.Code != 200 {
		t.Fatalf("code = %d, body = %s", rec.Code, body)
	}
	var got baselineFilesResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestBaselineFilesAPI_tableDeltasAndTheLastRead(t *testing.T) {
	root, t2 := srChainFixture(t)
	got := srGet(t, root, t2)
	type want struct {
		producedBy, from, read string
		folds                  int // -1: absent
	}
	wants := map[string]want{
		"orders": {baseline.ProducedByFold, "2026-06-01 03:00:00", "2026-06-01 03:00:00", 2},
		"quiet":  {baseline.ProducedByCarriedForward, "2026-06-01 09:00:00", "2026-06-01 03:00:00", 1},
		"fresh":  {baseline.ProducedByDump, "", "2026-06-01 15:00:00", 0},
		"old":    {baseline.ProducedByFold, "2026-06-01 09:00:00", "", -1},
		"broken": {"", "", "", -1},
	}
	if len(got.Tables) != len(wants) {
		t.Fatalf("tables = %+v", got.Tables)
	}
	for _, row := range got.Tables {
		w := wants[row.Table]
		if row.ProducedBy != w.producedBy {
			t.Errorf("%s: made by %q, want %q", row.Table, row.ProducedBy, w.producedBy)
		}
		if row.From != w.from {
			t.Errorf("%s: from %q, want %q", row.Table, row.From, w.from)
		}
		if row.SourceReadAt != w.read {
			t.Errorf("%s: source read %q, want %q", row.Table, row.SourceReadAt, w.read)
		}
		switch {
		case w.folds < 0 && row.FoldsSinceRead != nil:
			t.Errorf("%s: folds %d, want none reported", row.Table, *row.FoldsSinceRead)
		case w.folds >= 0 && (row.FoldsSinceRead == nil || *row.FoldsSinceRead != w.folds):
			t.Errorf("%s: folds %v, want %d", row.Table, row.FoldsSinceRead, w.folds)
		}
	}
	// The snapshot's line: the OLDEST read among the dated tables, twelve
	// hours before the snapshot, at most two folds since, and two tables it
	// does not speak for.
	if got.SourceReadAt != "2026-06-01 03:00:00" || got.SourceReadAgeSeconds != 12*3600 {
		t.Errorf("snapshot source read = %q (%vs before it), want 2026-06-01 03:00:00, 43200s", got.SourceReadAt, got.SourceReadAgeSeconds)
	}
	if got.MaxFoldsSinceRead == nil || *got.MaxFoldsSinceRead != 2 {
		t.Errorf("max folds = %v, want 2", got.MaxFoldsSinceRead)
	}
	if got.SourceReadMissing != 2 {
		t.Errorf("tables not dated = %d, want 2 (old, broken)", got.SourceReadMissing)
	}
}

// srChainFixture lays out one snapshot (returned as its RFC3339 time) whose
// tables cover every shape the reader distinguishes.
func srChainFixture(t *testing.T) (root, snapshotAt string) {
	t.Helper()
	root = t.TempDir()
	const (
		t0 = "2026-06-01T03:00:00Z" // the full backup
		t1 = "2026-06-01T09:00:00Z" // a refresh
		t2 = "2026-06-01T15:00:00Z" // the snapshot under test
	)
	s0, s1, s2 := "2026-06-01T03-00-00Z", "2026-06-01T09-00-00Z", "2026-06-01T15-00-00Z"
	dumpRead := baseline.DumpSourceRead(mustTime(t, t0))
	at := func(snap, table string) string { return filepath.Join(root, snap, "shop", table+".parquet") }

	// orders: dumped at t0 with the empty pair a full backup starts a chain
	// with, a pair written at t1, and at t2 a pair written by THIS snapshot's
	// run. Before #1570 the page read only the table file, which is carried
	// forward on every refresh, and called this changed table "reused
	// unchanged".
	srWriteTable(t, at(s0, "orders"), srFooter(baseline.ProducerDump, t0, &dumpRead))
	srWritePair(t, at(s0, "orders"), 0, srFooter("", t0, nil))
	srLink(t, at(s0, "orders"), at(s1, "orders"), 0)
	one := dumpRead.Next()
	// A fold's pair records the backup its CHAIN started from as its source,
	// not the one just before it (reconstruct: SourceBaseline.Time is the
	// chain start), so both pairs name t0.
	foldPair := func(at string, read *baseline.SourceRead) map[string]string {
		md := srFooter(baseline.ProducerReconstruct, at, read)
		md[baseline.MetaKeyDerivedFrom] = t0
		return md
	}
	srWritePair(t, at(s1, "orders"), 1, foldPair(t1, &one))
	srLink(t, at(s1, "orders"), at(s2, "orders"), 0, 1)
	two := one.Next()
	srWritePair(t, at(s2, "orders"), 2, foldPair(t2, &two))

	// quiet: the same chain up to t1, and nothing since, so at t2 its newest
	// pair is carried: reused unchanged, from the t1 run.
	srWriteTable(t, at(s0, "quiet"), srFooter(baseline.ProducerDump, t0, &dumpRead))
	srWritePair(t, at(s0, "quiet"), 0, srFooter("", t0, nil))
	srLink(t, at(s0, "quiet"), at(s1, "quiet"), 0)
	srWritePair(t, at(s1, "quiet"), 1, foldPair(t1, &one))
	srLink(t, at(s1, "quiet"), at(s2, "quiet"), 0, 1)

	// fresh: a full backup of its own at t2, with its empty pair; it IS the
	// read, zero folds.
	freshRead := baseline.DumpSourceRead(mustTime(t, t2))
	srWriteTable(t, at(s2, "fresh"), srFooter(baseline.ProducerDump, t2, &freshRead))
	srWritePair(t, at(s2, "fresh"), 0, srFooter("", t2, nil))

	// old: folded at t2 by a build before #1570 over files that did not
	// record it either. How it was made is known; when the source was last
	// read is not, and the snapshot's line must not speak for it.
	oldMD := srFooter(baseline.ProducerReconstruct, t2, nil)
	oldMD[baseline.MetaKeyDerivedFrom] = t1
	srWriteTable(t, at(s2, "old"), oldMD)

	// broken: half a pair beside it. Its chain cannot be read, so nothing is
	// claimed about it; the other tables of the directory still answer.
	srWriteTable(t, at(s2, "broken"), srFooter(baseline.ProducerDump, t0, &dumpRead))
	bp, _ := baseline.TableDeltaPaths(at(s2, "broken"), 0)
	if err := os.WriteFile(bp, []byte("half"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, s2, baseline.SuccessMarker), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return root, t2
}

// TestBaselineFilesAPI_sourceReadOfAFullBackup: a snapshot every table of
// which was just read from the source says so, with no age and no folds.
func TestBaselineFilesAPI_sourceReadOfAFullBackup(t *testing.T) {
	root := t.TempDir()
	const t0 = "2026-06-01T03:00:00Z"
	read := baseline.DumpSourceRead(mustTime(t, t0))
	for _, tbl := range []string{"a", "b"} {
		srWriteTable(t, filepath.Join(root, "2026-06-01T03-00-00Z", "shop", tbl+".parquet"), srFooter(baseline.ProducerDump, t0, &read))
	}
	got := srGet(t, root, t0)
	if got.SourceReadAt != "2026-06-01 03:00:00" || got.SourceReadAgeSeconds != 0 || got.SourceReadMissing != 0 {
		t.Fatalf("full read: read %q, age %v, missing %d", got.SourceReadAt, got.SourceReadAgeSeconds, got.SourceReadMissing)
	}
	if got.MaxFoldsSinceRead == nil || *got.MaxFoldsSinceRead != 0 {
		t.Fatalf("full read: max folds %v, want 0 (sent, not omitted)", got.MaxFoldsSinceRead)
	}
}

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// TestBackupDetail_drawsTheLastRead runs the real page code (sourceReadLine,
// madeByCell) over the response the real endpoint produced for the chain
// fixture and for a fresh full backup, and pins the sentences an operator
// reads. The sentences are logged so a reviewer reads them as drawn.
func TestBackupDetail_drawsTheLastRead(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	root, at := srChainFixture(t)
	chain, err := json.Marshal(srGet(t, root, at))
	if err != nil {
		t.Fatal(err)
	}
	fullRoot := t.TempDir()
	read := baseline.DumpSourceRead(mustTime(t, "2026-06-01T03:00:00Z"))
	srWriteTable(t, filepath.Join(fullRoot, "2026-06-01T03-00-00Z", "shop", "a.parquet"), srFooter(baseline.ProducerDump, "2026-06-01T03:00:00Z", &read))
	full, err := json.Marshal(srGet(t, fullRoot, "2026-06-01T03:00:00Z"))
	if err != nil {
		t.Fatal(err)
	}
	uRoot, uAt := srUncountedFixture(t)
	uncounted, err := json.Marshal(srGet(t, uRoot, uAt))
	if err != nil {
		t.Fatal(err)
	}
	qRoot, qAt := srQuietFixture(t)
	quiet, err := json.Marshal(srGet(t, qRoot, qAt))
	if err != nil {
		t.Fatal(err)
	}
	dRoot, dAt := srUndatedFixture(t)
	undated, err := json.Marshal(srGet(t, dRoot, dAt))
	if err != nil {
		t.Fatal(err)
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	script := renderHarnessJS + `
const flat = (n) => !n ? "" : n.nodeType === 3 ? n.textContent : (n._text || "") + (n.children || []).map(flat).join("");
const line = vm.runInContext("sourceReadLine", ctx);
const cell = vm.runInContext("madeByCell", ctx);
const chain = ` + string(chain) + `, full = ` + string(full) + `, uncounted = ` + string(uncounted) +
		`, quiet = ` + string(quiet) + `, undated = ` + string(undated) + `;
// The detail itself, through loadBackupDetail with the API answering the
// chain fixture, so the line is proven to reach the page and not only to be
// computable.
vm.runInContext("api = async () => (" + JSON.stringify(chain) + ");", ctx);
const box = document.createElement("div");
const cells = {};
for (const t of chain.tables) { const c = cell(t); cells[t.table] = { text: flat(c), title: c.attrs.title || "" }; }
vm.runInContext("loadBackupDetail", ctx)("2026-06-01T15:00:00Z", box).then(() => console.log(JSON.stringify({
  detail: flat(box),
  chainLine: line(chain), fullLine: line(full),
  s3Line: line({ time: "2026-06-01 03:00:00", tables: [] }),
  undatedLine: line(undated),
  quietLine: line(quiet),
  days: vm.runInContext("fmtAge", ctx)(3 * 86400),
  uncountedLine: line(uncounted),
  cells,
})));
`
	path := filepath.Join(t.TempDir(), "sourceread.js")
	if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got struct {
		ChainLine, FullLine, S3Line, UndatedLine, UncountedLine, QuietLine, Days, Detail string
		Cells                                                                            map[string]struct{ Text, Title string }
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	t.Logf("chain line:   %s", got.ChainLine)
	t.Logf("full line:    %s", got.FullLine)
	t.Logf("undated line: %s", got.UndatedLine)
	t.Logf("uncounted:    %s", got.UncountedLine)
	t.Logf("quiet:        %s", got.QuietLine)
	for _, name := range []string{"orders", "quiet", "fresh", "old", "broken"} {
		t.Logf("cell %-7s %q  (title: %s)", name, got.Cells[name].Text, got.Cells[name].Title)
	}

	if want := "Last real read of your database: 2026-06-01 03:00:00 UTC, 12h before this snapshot. " +
		"Updated from the recorded changes up to twice since. 2 tables do not record when."; got.ChainLine != want {
		t.Errorf("chain line:\n got %q\nwant %q", got.ChainLine, want)
	}
	if got.FullLine != "Read from your database when it was taken." {
		t.Errorf("full read line = %q", got.FullLine)
	}
	if got.S3Line != "" {
		t.Errorf("nothing looked up (an S3 source) drew %q; it must draw nothing", got.S3Line)
	}
	if got.QuietLine != "Last real read of your database: 2026-06-01 03:00:00 UTC, 6h before this snapshot." {
		t.Errorf("quiet line = %q; a snapshot that carried a six-hour-old full read forward must not read as taken from the database", got.QuietLine)
	}
	if got.Days != "3 days" {
		t.Errorf("fmtAge(3 days) = %q", got.Days)
	}
	if !strings.Contains(got.Detail, "Last real read of your database: 2026-06-01 03:00:00 UTC, 12h before this snapshot.") {
		t.Errorf("the snapshot detail does not draw the line: %q", got.Detail)
	}
	if got.UndatedLine != "When your database was last read for this snapshot is not recorded." {
		t.Errorf("undated line = %q", got.UndatedLine)
	}
	if want := "Last real read of your database: 2026-06-01 03:00:00 UTC, 24h before this snapshot. " +
		"How many updates were built since is not recorded for 1 table."; got.UncountedLine != want {
		t.Errorf("uncounted line:\n got %q\nwant %q", got.UncountedLine, want)
	}
	cells := map[string]string{
		"orders": "built from changes · last read 2026-06-01 03:00:00",
		"quiet":  "reused unchanged · last read 2026-06-01 03:00:00",
		"fresh":  "read from source",
		// Too old to record a read: it still names the backup it came from,
		// as the page did before #1570.
		"old": "built from changes · 2026-06-01 09:00:00",
	}
	for name, want := range cells {
		if got.Cells[name].Text != want {
			t.Errorf("cell %s = %q, want %q", name, got.Cells[name].Text, want)
		}
	}
	if !strings.Contains(got.Cells["quiet"].Title, "Reused from the snapshot of 2026-06-01 09:00:00 UTC.") {
		t.Errorf("the reused table's tooltip does not say whose file (and date) it is: %q", got.Cells["quiet"].Title)
	}
	if !strings.Contains(got.Cells["orders"].Title, "updated twice from the recorded changes since") {
		t.Errorf("orders tooltip = %q", got.Cells["orders"].Title)
	}
	// orders' chain started at the read itself: the read sentence says it all.
	if strings.Contains(got.Cells["orders"].Title, "Built") {
		t.Errorf("orders tooltip names its chain start twice: %q", got.Cells["orders"].Title)
	}
	if !strings.Contains(got.Cells["old"].Title, "Built from the snapshot of 2026-06-01 09:00:00 UTC plus the changes recorded since.") {
		t.Errorf("the updated table's tooltip does not name the snapshot its file came from: %q", got.Cells["old"].Title)
	}
	for name, c := range got.Cells {
		for _, bad := range []string{"— ", "fold", "carried", "undefined", "null", "NaN"} {
			if strings.Contains(c.Text+c.Title, bad) {
				t.Errorf("cell %s holds %q: %q / %q", name, bad, c.Text, c.Title)
			}
		}
	}
	for _, l := range []string{got.ChainLine, got.FullLine, got.UndatedLine, got.UncountedLine} {
		for _, bad := range []string{"—", "fold", "undefined", "null", "NaN"} {
			if strings.Contains(l, bad) {
				t.Errorf("line holds %q: %q", bad, l)
			}
		}
	}
}

// srUncountedFixture is the shape an upgrade leaves: table "legacy" has a
// dated base and a chain whose newest pair was written before #1570 (no
// count), table "counted" one pair with a count, and table "damaged" a newest
// pair that records no writer instant at all.
func srUncountedFixture(t *testing.T) (root, snapshotAt string) {
	t.Helper()
	root = t.TempDir()
	const t0, t1 = "2026-06-01T03:00:00Z", "2026-06-02T03:00:00Z"
	s0, s1 := "2026-06-01T03-00-00Z", "2026-06-02T03-00-00Z"
	at := func(snap, table string) string { return filepath.Join(root, snap, "shop", table+".parquet") }
	read := baseline.DumpSourceRead(mustTime(t, t0))
	for _, tbl := range []string{"legacy", "counted", "damaged"} {
		srWriteTable(t, at(s0, tbl), srFooter(baseline.ProducerDump, t0, &read))
		srWritePair(t, at(s0, tbl), 0, srFooter("", t0, nil))
		srLink(t, at(s0, tbl), at(s1, tbl), 0)
	}
	srWritePair(t, at(s1, "legacy"), 1, srFooter(baseline.ProducerReconstruct, t1, nil))
	one := read.Next()
	srWritePair(t, at(s1, "counted"), 1, srFooter(baseline.ProducerReconstruct, t1, &one))
	damaged := srFooter(baseline.ProducerReconstruct, t1, &one)
	delete(damaged, baseline.MetaKeySnapshotTimestamp)
	srWritePair(t, at(s1, "damaged"), 1, damaged)
	return root, t1
}

// TestBaselineFilesAPI_uncountedTablesHideTheMaximum: a maximum over the
// tables that record a count is not the most when others do not, so it is
// left out and the uncounted tables are counted instead. And a newest pair
// with no writer instant gets no verdict on how the table was made (the file
// alone would say "reused unchanged"), while its read is still the base's.
func TestBaselineFilesAPI_uncountedTablesHideTheMaximum(t *testing.T) {
	root, at := srUncountedFixture(t)
	got := srGet(t, root, at)
	if got.SourceReadAt != "2026-06-01 03:00:00" || got.MaxFoldsSinceRead != nil || got.SourceReadUncounted != 1 || got.SourceReadMissing != 0 {
		t.Fatalf("snapshot: read %q, max %v, uncounted %d, missing %d; want the read, no maximum, 1 uncounted, 0 missing",
			got.SourceReadAt, got.MaxFoldsSinceRead, got.SourceReadUncounted, got.SourceReadMissing)
	}
	for _, row := range got.Tables {
		switch row.Table {
		case "legacy":
			if row.ProducedBy != baseline.ProducedByFold || row.SourceReadAt != "2026-06-01 03:00:00" || row.FoldsSinceRead != nil {
				t.Errorf("legacy: %+v", row)
			}
		case "damaged":
			if row.ProducedBy != "" || row.SourceReadAt != "2026-06-01 03:00:00" {
				t.Errorf("damaged: made by %q, read %q; want no verdict and the base's read", row.ProducedBy, row.SourceReadAt)
			}
		}
	}
}

// srQuietFixture: a full backup at t0 carried whole into a snapshot six
// hours later, no table changed (hard links, the empty pairs only). Every
// footer says {t0, 0 updates} while the snapshot is newer: the line must say
// how old the read is, never "when it was taken".
func srQuietFixture(t *testing.T) (root, snapshotAt string) {
	t.Helper()
	root = t.TempDir()
	const t0, t1 = "2026-06-01T03:00:00Z", "2026-06-01T09:00:00Z"
	s0, s1 := "2026-06-01T03-00-00Z", "2026-06-01T09-00-00Z"
	at := func(snap, table string) string { return filepath.Join(root, snap, "shop", table+".parquet") }
	read := baseline.DumpSourceRead(mustTime(t, t0))
	for _, tbl := range []string{"a", "b"} {
		srWriteTable(t, at(s0, tbl), srFooter(baseline.ProducerDump, t0, &read))
		srWritePair(t, at(s0, tbl), 0, srFooter("", t0, nil))
		srLink(t, at(s0, tbl), at(s1, tbl), 0)
	}
	return root, t1
}

// srUndatedFixture: every table folded by a build before #1570, the whole
// snapshot undated (what every fold snapshot on disk looks like right after
// the upgrade), plus a whole pair whose newest upserts file is not Parquet.
func srUndatedFixture(t *testing.T) (root, snapshotAt string) {
	t.Helper()
	root = t.TempDir()
	const t1 = "2026-06-01T09:00:00Z"
	s1 := "2026-06-01T09-00-00Z"
	at := func(table string) string { return filepath.Join(root, s1, "shop", table+".parquet") }
	srWriteTable(t, at("a"), srFooter(baseline.ProducerReconstruct, t1, nil))
	srWriteTable(t, at("b"), srFooter(baseline.ProducerReconstruct, t1, nil))
	srWriteTable(t, at("garbled"), srFooter(baseline.ProducerReconstruct, t1, nil))
	p, u := baseline.TableDeltaPaths(at("garbled"), 0)
	for _, f := range []string{p, u} {
		if err := os.WriteFile(f, []byte("not parquet"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root, t1
}

func TestBaselineFilesAPI_quietAndUndatedSnapshots(t *testing.T) {
	root, at := srQuietFixture(t)
	got := srGet(t, root, at)
	if got.SourceReadAt != "2026-06-01 03:00:00" || got.SourceReadAgeSeconds != 6*3600 ||
		got.MaxFoldsSinceRead == nil || *got.MaxFoldsSinceRead != 0 {
		t.Fatalf("quiet snapshot: read %q, age %v, max %v; want the dump's read six hours back and 0 updates",
			got.SourceReadAt, got.SourceReadAgeSeconds, got.MaxFoldsSinceRead)
	}
	for _, row := range got.Tables {
		if row.ProducedBy != baseline.ProducedByCarriedForward {
			t.Errorf("%s: made by %q, want reused unchanged", row.Table, row.ProducedBy)
		}
	}

	root, at = srUndatedFixture(t)
	got = srGet(t, root, at)
	if got.SourceReadAt != "" || got.SourceReadMissing != 3 {
		t.Fatalf("undated snapshot: read %q, missing %d; want no read and all 3 tables counted", got.SourceReadAt, got.SourceReadMissing)
	}
	for _, row := range got.Tables {
		if row.Table == "garbled" && (row.ProducedBy != "" || row.SourceReadAt != "") {
			t.Errorf("a table whose newest delta will not open reported %q / %q; nothing was found out", row.ProducedBy, row.SourceReadAt)
		}
	}
}
