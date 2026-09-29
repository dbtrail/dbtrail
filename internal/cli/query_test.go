package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"

	binparser "github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

// ─── cobra command wiring ─────────────────────────────────────────────────────

func TestQueryCmd_registered(t *testing.T) {
	root := &cobra.Command{Use: "root"}
	AddReadCommands(root)
	found := false
	for _, cmd := range root.Commands() {
		if cmd.Use == "query" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected 'query' command to be registered by AddReadCommands")
	}
}

func TestQueryCmd_indexDSN_required(t *testing.T) {
	flag := queryCmd.Flag("index-dsn")
	if flag == nil {
		t.Fatal("flag --index-dsn not registered")
	}
	if flag.Annotations["cobra_annotation_bash_completion_one_required_flag"] == nil {
		t.Error("flag --index-dsn is not marked required")
	}
}

func TestQueryCmd_defaults(t *testing.T) {
	cases := []struct {
		flag string
		want string
	}{
		{"format", "table"},
		{"limit", "100"},
	}
	for _, tc := range cases {
		f := queryCmd.Flag(tc.flag)
		if f == nil {
			t.Errorf("flag --%s not registered", tc.flag)
			continue
		}
		if f.DefValue != tc.want {
			t.Errorf("flag --%s: expected default %q, got %q", tc.flag, tc.want, f.DefValue)
		}
	}
}

func TestQueryCmd_emptyStringDefaults(t *testing.T) {
	for _, name := range []string{
		"schema", "table", "pk", "event-type", "gtid", "since", "until", "changed-column", "flag",
	} {
		f := queryCmd.Flag(name)
		if f == nil {
			t.Errorf("flag --%s not registered", name)
			continue
		}
		if f.DefValue != "" {
			t.Errorf("flag --%s: expected empty default, got %q", name, f.DefValue)
		}
	}
}

func TestQueryCmd_allFlagsRegistered(t *testing.T) {
	for _, name := range []string{
		"index-dsn", "schema", "table", "pk", "pks", "pk-min", "pk-max", "limit-per-pk", "event-type",
		"gtid", "since", "until", "changed-column", "flag", "format", "limit",
		"no-archive",
	} {
		if queryCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on queryCmd", name)
		}
	}
}

func TestRunQuery_pksRequiresSchemaTable(t *testing.T) {
	saved, savedS, savedT := qPKs, qSchema, qTable
	t.Cleanup(func() { qPKs = saved; qSchema = savedS; qTable = savedT })

	qPKs = []string{"1", "2"}
	qSchema = ""
	qTable = ""

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --pks used without --schema/--table")
	}
	if !strings.Contains(err.Error(), "--pks requires") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunQuery_pkAndPksMutuallyExclusive(t *testing.T) {
	savedPK, savedPKs, savedS, savedT := qPK, qPKs, qSchema, qTable
	t.Cleanup(func() { qPK = savedPK; qPKs = savedPKs; qSchema = savedS; qTable = savedT })

	qPK = "42"
	qPKs = []string{"1", "2"}
	qSchema = "db"
	qTable = "t"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when both --pk and --pks are set")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunQuery_limitPerPKRequiresPK(t *testing.T) {
	savedLPP, savedPK, savedPKs := qLimitPerPK, qPK, qPKs
	t.Cleanup(func() { qLimitPerPK = savedLPP; qPK = savedPK; qPKs = savedPKs })

	qLimitPerPK = 1
	qPK = ""
	qPKs = nil

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --limit-per-pk set without --pk/--pks")
	}
	if !strings.Contains(err.Error(), "--limit-per-pk requires") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// ─── runQuery validation (no DB required) ─────────────────────────────────────

func TestRunQuery_pkRequiresSchemaTable(t *testing.T) {
	saved, savedS, savedT := qPK, qSchema, qTable
	t.Cleanup(func() { qPK = saved; qSchema = savedS; qTable = savedT })

	qPK = "42"
	qSchema = ""
	qTable = ""

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --pk used without --schema/--table, got nil")
	}
	if !strings.Contains(err.Error(), "--pk requires") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunQuery_columnEqRequiresSchemaTable(t *testing.T) {
	saved, savedS, savedT := qColumnEq, qSchema, qTable
	t.Cleanup(func() { qColumnEq = saved; qSchema = savedS; qTable = savedT })

	qColumnEq = []string{"status=active"}
	qSchema = ""
	qTable = ""

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --column-eq used without --schema/--table")
	}
	if !strings.Contains(err.Error(), "--column-eq requires") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunQuery_columnEqMalformed(t *testing.T) {
	saved, savedS, savedT := qColumnEq, qSchema, qTable
	t.Cleanup(func() { qColumnEq = saved; qSchema = savedS; qTable = savedT })

	qColumnEq = []string{"no-equals-sign"}
	qSchema = "mydb"
	qTable = "orders"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error for --column-eq entry without '='")
	}
	if !strings.Contains(err.Error(), "missing '='") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunQuery_changedColRequiresSchemaTable(t *testing.T) {
	saved, savedS, savedT := qChangedCol, qSchema, qTable
	t.Cleanup(func() { qChangedCol = saved; qSchema = savedS; qTable = savedT })

	qChangedCol = "status"
	qSchema = ""
	qTable = ""

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --changed-column used without --schema/--table, got nil")
	}
	if !strings.Contains(err.Error(), "--changed-column requires") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunQuery_invalidFormat(t *testing.T) {
	savedFmt, savedPK, savedCol := qFormat, qPK, qChangedCol
	t.Cleanup(func() { qFormat = savedFmt; qPK = savedPK; qChangedCol = savedCol })

	qPK = ""
	qChangedCol = ""
	qFormat = "xml"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --format, got nil")
	}
	if !strings.Contains(err.Error(), "invalid --format") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunQuery_invalidEventType(t *testing.T) {
	savedET, savedPK, savedCol, savedFmt := qEventType, qPK, qChangedCol, qFormat
	t.Cleanup(func() {
		qEventType = savedET
		qPK = savedPK
		qChangedCol = savedCol
		qFormat = savedFmt
	})

	qPK = ""
	qChangedCol = ""
	qFormat = "table"
	qEventType = "UPSERT"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --event-type, got nil")
	}
	if !strings.Contains(err.Error(), "UPSERT") {
		t.Errorf("expected 'UPSERT' in error, got: %v", err)
	}
}

func TestRunQuery_invalidSince(t *testing.T) {
	savedSince, savedPK, savedCol, savedFmt, savedET := qSince, qPK, qChangedCol, qFormat, qEventType
	t.Cleanup(func() {
		qSince = savedSince
		qPK = savedPK
		qChangedCol = savedCol
		qFormat = savedFmt
		qEventType = savedET
	})

	qPK = ""
	qChangedCol = ""
	qFormat = "table"
	qEventType = ""
	qSince = "not-a-date"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --since, got nil")
	}
	if !strings.Contains(err.Error(), "--since") {
		t.Errorf("expected '--since' in error, got: %v", err)
	}
}

func TestRunQuery_invalidUntil(t *testing.T) {
	savedUntil, savedPK, savedCol, savedFmt, savedET, savedSince :=
		qUntil, qPK, qChangedCol, qFormat, qEventType, qSince
	t.Cleanup(func() {
		qUntil = savedUntil
		qPK = savedPK
		qChangedCol = savedCol
		qFormat = savedFmt
		qEventType = savedET
		qSince = savedSince
	})

	qPK = ""
	qChangedCol = ""
	qFormat = "table"
	qEventType = ""
	qSince = ""
	qUntil = "not-a-date"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error for invalid --until, got nil")
	}
	if !strings.Contains(err.Error(), "--until") {
		t.Errorf("expected '--until' in error, got: %v", err)
	}
}

// ─── --pk partial flag combinations ──────────────────────────────────────────

// TestRunQuery_pkWithSchemaOnly verifies that --pk + --schema (no --table)
// is rejected — the guard uses OR, so having only one of schema/table fails.
func TestRunQuery_pkWithSchemaOnly(t *testing.T) {
	savedPK, savedS, savedT := qPK, qSchema, qTable
	t.Cleanup(func() { qPK = savedPK; qSchema = savedS; qTable = savedT })

	qPK = "42"
	qSchema = "mydb"
	qTable = ""

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --pk used with --schema but no --table, got nil")
	}
	if !strings.Contains(err.Error(), "--pk requires") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestRunQuery_pkWithTableOnly verifies that --pk + --table (no --schema) is
// also rejected — the symmetric case to pkWithSchemaOnly.
func TestRunQuery_pkWithTableOnly(t *testing.T) {
	savedPK, savedS, savedT := qPK, qSchema, qTable
	t.Cleanup(func() { qPK = savedPK; qSchema = savedS; qTable = savedT })

	qPK = "42"
	qSchema = ""
	qTable = "orders"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --pk used with --table but no --schema, got nil")
	}
	if !strings.Contains(err.Error(), "--pk requires") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestRunQuery_changedColWithSchemaOnly verifies that --changed-column + --schema
// (no --table) is rejected — the OR guard applies here too.
func TestRunQuery_changedColWithSchemaOnly(t *testing.T) {
	savedCol, savedS, savedT := qChangedCol, qSchema, qTable
	t.Cleanup(func() { qChangedCol = savedCol; qSchema = savedS; qTable = savedT })

	qChangedCol = "status"
	qSchema = "mydb"
	qTable = ""

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --changed-column used with --schema but no --table, got nil")
	}
	if !strings.Contains(err.Error(), "--changed-column requires") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// ─── archive flag wiring ──────────────────────────────────────────────────────

func TestQueryCmd_archiveFlagsRegistered(t *testing.T) {
	for _, name := range []string{"archive-dir", "archive-s3", "bintrail-id"} {
		if queryCmd.Flag(name) == nil {
			t.Errorf("flag --%s not registered on queryCmd", name)
		}
	}
}

func TestQueryCmd_archiveFlagDefaults(t *testing.T) {
	for _, name := range []string{"archive-dir", "archive-s3", "bintrail-id"} {
		f := queryCmd.Flag(name)
		if f == nil {
			t.Fatalf("flag --%s not registered", name)
		}
		if f.DefValue != "" {
			t.Errorf("flag --%s: expected empty default, got %q", name, f.DefValue)
		}
	}
}

func TestQueryCmd_noArchiveDefault(t *testing.T) {
	f := queryCmd.Flag("no-archive")
	if f == nil {
		t.Fatal("flag --no-archive not registered")
	}
	if f.DefValue != "false" {
		t.Errorf("expected --no-archive default %q, got %q", "false", f.DefValue)
	}
}

func TestRunQuery_noArchiveConflictsWithArchiveDir(t *testing.T) {
	saved := struct {
		na                        bool
		ad, as3, pk, cc, fmt, bid string
	}{qNoArchive, qArchiveDir, qArchiveS3, qPK, qChangedCol, qFormat, qBintrailID}
	t.Cleanup(func() {
		qNoArchive = saved.na
		qArchiveDir = saved.ad
		qArchiveS3 = saved.as3
		qPK = saved.pk
		qChangedCol = saved.cc
		qFormat = saved.fmt
		qBintrailID = saved.bid
	})

	qPK = ""
	qChangedCol = ""
	qFormat = "table"
	qNoArchive = true
	qArchiveDir = "/some/dir"
	qArchiveS3 = ""
	qBintrailID = "abc"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error for --no-archive + --archive-dir")
	}
	if !strings.Contains(err.Error(), "--no-archive cannot be combined") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ─── archiveSources ──────────────────────────────────────────────────────────

func TestArchiveSources_both(t *testing.T) {
	savedDir, savedS3, savedID := qArchiveDir, qArchiveS3, qBintrailID
	t.Cleanup(func() { qArchiveDir = savedDir; qArchiveS3 = savedS3; qBintrailID = savedID })

	qArchiveDir = "/data/archives"
	qArchiveS3 = "s3://bucket/prefix"
	qBintrailID = "abc-123"

	srcs := archiveSources()
	if len(srcs) != 2 {
		t.Fatalf("expected 2 sources, got %d: %v", len(srcs), srcs)
	}
	if srcs[0] != "/data/archives/bintrail_id=abc-123" {
		t.Errorf("unexpected dir source: %q", srcs[0])
	}
	if srcs[1] != "s3://bucket/prefix/bintrail_id=abc-123" {
		t.Errorf("unexpected s3 source: %q", srcs[1])
	}
}

func TestArchiveSources_dirOnly(t *testing.T) {
	savedDir, savedS3, savedID := qArchiveDir, qArchiveS3, qBintrailID
	t.Cleanup(func() { qArchiveDir = savedDir; qArchiveS3 = savedS3; qBintrailID = savedID })

	qArchiveDir = "/data/archives"
	qArchiveS3 = ""
	qBintrailID = "abc-123"

	srcs := archiveSources()
	if len(srcs) != 1 || srcs[0] != "/data/archives/bintrail_id=abc-123" {
		t.Errorf("expected [/data/archives/bintrail_id=abc-123], got %v", srcs)
	}
}

func TestArchiveSources_none(t *testing.T) {
	savedDir, savedS3 := qArchiveDir, qArchiveS3
	t.Cleanup(func() { qArchiveDir = savedDir; qArchiveS3 = savedS3 })

	qArchiveDir = ""
	qArchiveS3 = ""

	if srcs := archiveSources(); len(srcs) != 0 {
		t.Errorf("expected empty sources, got %v", srcs)
	}
}

// ─── query.MergeResults ─────────────────────────────────────────────────────────────

func TestMergeResults_deduplicatesByEventID(t *testing.T) {
	t0 := time.Date(2026, 2, 28, 10, 0, 0, 0, time.UTC)
	rows := []query.ResultRow{
		{EventID: 1, EventTimestamp: t0, SchemaName: "db", TableName: "t"},
		{EventID: 1, EventTimestamp: t0, SchemaName: "db", TableName: "t"}, // duplicate
		{EventID: 2, EventTimestamp: t0.Add(time.Second), SchemaName: "db", TableName: "t"},
	}
	got := query.MergeResults(rows, 0, "")
	if len(got) != 2 {
		t.Fatalf("expected 2 unique rows, got %d", len(got))
	}
}

func TestMergeResults_sortsByTimestampThenEventID(t *testing.T) {
	t0 := time.Date(2026, 2, 28, 10, 0, 0, 0, time.UTC)
	rows := []query.ResultRow{
		{EventID: 3, EventTimestamp: t0.Add(2 * time.Second)},
		{EventID: 1, EventTimestamp: t0},
		{EventID: 2, EventTimestamp: t0.Add(time.Second)},
	}
	got := query.MergeResults(rows, 0, "")
	if got[0].EventID != 1 || got[1].EventID != 2 || got[2].EventID != 3 {
		t.Errorf("expected sorted by timestamp, got event IDs %d %d %d",
			got[0].EventID, got[1].EventID, got[2].EventID)
	}
}

// TestMergeResults_sameTimestampSortsByEventID verifies the secondary sort key
// when two rows share the same event_timestamp.
func TestMergeResults_sameTimestampSortsByEventID(t *testing.T) {
	t0 := time.Date(2026, 2, 28, 10, 0, 0, 0, time.UTC)
	rows := []query.ResultRow{
		{EventID: 5, EventTimestamp: t0},
		{EventID: 2, EventTimestamp: t0},
		{EventID: 8, EventTimestamp: t0},
	}
	got := query.MergeResults(rows, 0, "")
	if got[0].EventID != 2 || got[1].EventID != 5 || got[2].EventID != 8 {
		t.Errorf("expected sorted by event_id at same timestamp, got %d %d %d",
			got[0].EventID, got[1].EventID, got[2].EventID)
	}
}

func TestMergeResults_appliesLimit(t *testing.T) {
	t0 := time.Date(2026, 2, 28, 10, 0, 0, 0, time.UTC)
	rows := []query.ResultRow{
		{EventID: 1, EventTimestamp: t0},
		{EventID: 2, EventTimestamp: t0.Add(time.Second)},
		{EventID: 3, EventTimestamp: t0.Add(2 * time.Second)},
	}
	got := query.MergeResults(rows, 2, "")
	if len(got) != 2 {
		t.Fatalf("expected limit 2, got %d rows", len(got))
	}
	if got[0].EventID != 1 || got[1].EventID != 2 {
		t.Errorf("expected first two rows, got event IDs %d %d", got[0].EventID, got[1].EventID)
	}
}

func TestMergeResults_zeroLimitNoTruncation(t *testing.T) {
	t0 := time.Date(2026, 2, 28, 10, 0, 0, 0, time.UTC)
	rows := []query.ResultRow{
		{EventID: 1, EventTimestamp: t0},
		{EventID: 2, EventTimestamp: t0.Add(time.Second)},
		{EventID: 3, EventTimestamp: t0.Add(2 * time.Second)},
	}
	got := query.MergeResults(rows, 0, "")
	if len(got) != 3 {
		t.Errorf("expected all 3 rows when limit=0, got %d", len(got))
	}
}

func TestMergeResults_empty(t *testing.T) {
	if got := query.MergeResults(nil, 10, ""); len(got) != 0 {
		t.Errorf("expected empty result for nil input, got %v", got)
	}
}

// ─── queryArchiveSources (issue #203) ────────────────────────────────────────
//
// These tests pin the silent-failure fix for #203. The production loop in
// `runQuery` was wrapping every parquetquery.Fetch error in a slog.Warn and
// continuing, producing empty or partial results with exit 0 and no visible
// stderr signal at the default log level. The fix extracts the archive loop
// into queryArchiveSources so the exact code path production hits can be
// driven by a fake fetcher — no DuckDB, no real DB, no integration tag.
//
// Each test below pins a specific clause from the queryArchiveSources doc
// comment. If you add a new clause to the contract, add a matching test
// here. The tests are the enforceable half of the contract the doc describes.

// captureSlogDefault redirects slog.Default() to a text handler writing into
// buf for the lifetime of the test, restoring the previous default on
// t.Cleanup. This lets tests assert that slog.Warn is actually emitted by the
// helper — without it, a future refactor could silently delete the structured
// log line and every stderr-only assertion would still pass, regressing the
// "dual-channel reporting" contract from #203.
//
// Callers MUST NOT use t.Parallel() in conjunction with this helper.
// slog.SetDefault mutates process-global state; two parallel tests that both
// call captureSlogDefault would race on the default handler and produce
// flaky assertions. Go's testing package still runs t.Cleanup on panicked
// tests, so the restore is safe against a t.Fatalf mid-test — the hazard is
// exclusively t.Parallel, not panics.
func captureSlogDefault(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	orig := slog.Default()
	// Capture at Debug so the cancellation tests' `buf.Len() == 0`
	// assertions actually prove "no output on cancel" — not just "no WARN
	// output on cancel." A future refactor that emits slog.Info or
	// slog.Debug from the cancellation path would silently pass a
	// WARN-filtered handler but correctly fail a Debug-filtered one.
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(orig) })
	return &buf
}

// fakeFetcher returns a parquetquery.Fetch-shaped function that replays a
// scripted sequence of (rows, err) results and records every call. The test
// uses the recorded calls to prove loop semantics: e.g. that a canceled
// context halts iteration before the next source is touched.
//
// If a test scripts fewer responses than the helper makes calls, the excess
// calls return a synthetic "unexpected call" error rather than panicking so
// over-iteration bugs surface through the stderr/error assertions with a
// clear message.
//
// Call .fn() exactly once per test. The method returns a fresh closure
// every invocation, and two closures from the same fakeFetcher would share
// the response index and calls slice — a subtle copy-paste hazard. The
// method enforces this by panicking on the second call, turning an advisory
// warning into a runtime contract.
type fakeFetcher struct {
	responses []fakeResponse
	calls     []string // sources that were actually invoked
	fnIssued  bool
}

type fakeResponse struct {
	rows []query.ResultRow
	err  error
}

func (f *fakeFetcher) fn() query.ArchiveFetcher {
	if f.fnIssued {
		panic("fakeFetcher.fn() called twice — each test must call .fn() exactly once")
	}
	f.fnIssued = true
	return func(_ context.Context, _ query.Options, src string) ([]query.ResultRow, error) {
		idx := len(f.calls)
		f.calls = append(f.calls, src)
		if idx >= len(f.responses) {
			return nil, fmt.Errorf("fakeFetcher: unexpected call #%d for %q", idx, src)
		}
		r := f.responses[idx]
		return r.rows, r.err
	}
}

// TestQueryArchiveSources_plainErrorKeepsGoingWithDualChannel exercises the
// core #203 contract: a failing source must surface on BOTH stderr and
// slog.Warn, and the loop must continue to the next source. All three
// regression vectors are checked in a single test because they're
// indivisible — the whole point of the fix is that one broken archive does
// not blind the user to the next source's rows.
func TestQueryArchiveSources_plainErrorKeepsGoingWithDualChannel(t *testing.T) {
	slogBuf := captureSlogDefault(t)
	var stderr bytes.Buffer

	wantRow := query.ResultRow{EventID: 42, SchemaName: "db", TableName: "t"}
	f := &fakeFetcher{responses: []fakeResponse{
		{err: errors.New("DuckDB Binder Error: column connection_id not found")},
		{rows: []query.ResultRow{wantRow}},
	}}

	got, err := queryArchiveSources(
		context.Background(),
		[]string{"s3://bucket/bintrail_id=abc", "/local/bintrail_id=abc"},
		query.Options{},
		f.fn(),
		&stderr,
	)
	if err != nil {
		t.Fatalf("expected nil error for plain-error + success mix, got %v", err)
	}

	// Loop continued to src2 → rows present.
	if len(got) != 1 || got[0].EventID != wantRow.EventID {
		t.Errorf("expected rows from src2 in results, got %+v", got)
	}
	if len(f.calls) != 2 {
		t.Errorf("expected both sources to be fetched, got %d calls: %v", len(f.calls), f.calls)
	}

	// stderr received the visible warning for src1.
	stderrOut := stderr.String()
	if !strings.Contains(stderrOut, "Warning: archive query failed") {
		t.Errorf("stderr missing 'Warning: archive query failed' prefix: %q", stderrOut)
	}
	if !strings.Contains(stderrOut, "s3://bucket/bintrail_id=abc") {
		t.Errorf("stderr missing src1 path: %q", stderrOut)
	}
	if !strings.Contains(stderrOut, "Binder Error") {
		t.Errorf("stderr missing underlying error text: %q", stderrOut)
	}
	if strings.Contains(stderrOut, "/local/bintrail_id=abc") {
		t.Errorf("stderr should NOT mention src2 (which succeeded): %q", stderrOut)
	}
	if !strings.HasSuffix(stderrOut, "\n") {
		t.Errorf("stderr warning must end with a newline for clean line framing: %q", stderrOut)
	}

	// slog.Default() received the structured warning. Assert on the msg and
	// both attribute keys so a future refactor that drops the slog.Warn call
	// (even while keeping the stderr line) regresses this test.
	slogOut := slogBuf.String()
	if !strings.Contains(slogOut, "archive query failed, skipping") {
		t.Errorf("slog record missing: %q", slogOut)
	}
	if !strings.Contains(slogOut, "source=") {
		t.Errorf("slog record missing 'source=' attribute: %q", slogOut)
	}
	if !strings.Contains(slogOut, "error=") {
		t.Errorf("slog record missing 'error=' attribute: %q", slogOut)
	}
	if !strings.Contains(slogOut, "level=WARN") {
		t.Errorf("slog record wrong level (expected WARN): %q", slogOut)
	}
}

// TestQueryArchiveSources_preCanceledCtxShortCircuits verifies that a context
// canceled BEFORE queryArchiveSources is called halts the loop on the first
// iteration with a wrapped context.Canceled, emits NOTHING to stderr or
// slog, and does not touch subsequent sources.
func TestQueryArchiveSources_preCanceledCtxShortCircuits(t *testing.T) {
	slogBuf := captureSlogDefault(t)
	var stderr bytes.Buffer

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// The fetcher will be called once for src1 — it returns a generic error
	// because parquetquery.Fetch typically does return an error when its ctx
	// is dead. The helper then inspects ctx.Err() and short-circuits.
	f := &fakeFetcher{responses: []fakeResponse{
		{err: errors.New("fetch aborted")},
		{rows: []query.ResultRow{{EventID: 99}}}, // must never be reached
	}}

	got, err := queryArchiveSources(
		ctx,
		[]string{"src1", "src2"},
		query.Options{},
		f.fn(),
		&stderr,
	)
	if err == nil {
		t.Fatal("expected error for canceled ctx, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected errors.Is(err, context.Canceled), got %v", err)
	}
	if !strings.Contains(err.Error(), "query canceled") {
		t.Errorf("expected error to mention 'query canceled', got %v", err)
	}
	if got != nil {
		t.Errorf("expected nil results on cancellation, got %v", got)
	}

	// Loop must have stopped after src1 — src2 never called.
	if len(f.calls) != 1 || f.calls[0] != "src1" {
		t.Errorf("expected exactly 1 call (src1) before short-circuit, got %v", f.calls)
	}

	// No stderr noise on cancellation — Ctrl-C should exit clean, not dump
	// per-source warnings for every archive that happened to be queued.
	if stderr.Len() != 0 {
		t.Errorf("expected no stderr output on cancellation, got %q", stderr.String())
	}
	if slogBuf.Len() != 0 {
		t.Errorf("expected no slog output on cancellation, got %q", slogBuf.String())
	}
}

// TestQueryArchiveSources_wrappedCanceledFetchErrShortCircuits guards against
// the race where the fetch error wraps context.Canceled before the ambient
// ctx.Err() has transitioned — child-context races, DuckDB/httpfs
// cancellation propagation. The helper's second check (errors.Is on the
// fetch error chain) catches this case; without it, a canceled query would
// silently degrade to "archive failed, keep going" per-source warnings,
// exactly the UX the #203 fix prevents.
func TestQueryArchiveSources_wrappedCanceledFetchErrShortCircuits(t *testing.T) {
	slogBuf := captureSlogDefault(t)
	var stderr bytes.Buffer

	// Parent ctx is live — the only signal is the wrapped error.
	ctx := context.Background()

	f := &fakeFetcher{responses: []fakeResponse{
		{err: fmt.Errorf("duckdb: %w", context.Canceled)},
		{rows: []query.ResultRow{{EventID: 99}}}, // must not be reached
	}}

	got, err := queryArchiveSources(
		ctx,
		[]string{"src1", "src2"},
		query.Options{},
		f.fn(),
		&stderr,
	)
	if err == nil {
		t.Fatal("expected error for wrapped context.Canceled, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected errors.Is(err, context.Canceled), got %v", err)
	}
	if got != nil {
		t.Errorf("expected nil results on cancellation, got %v", got)
	}
	if len(f.calls) != 1 {
		t.Errorf("expected short-circuit after src1, got %d calls: %v", len(f.calls), f.calls)
	}
	if stderr.Len() != 0 {
		t.Errorf("expected no stderr output for wrapped cancellation, got %q", stderr.String())
	}
	if slogBuf.Len() != 0 {
		t.Errorf("expected no slog output for wrapped cancellation, got %q", slogBuf.String())
	}
}

// TestQueryArchiveSources_wrappedDeadlineExceededShortCircuits is the
// symmetric case to the wrapped-Canceled test above. Deadline expiry can
// also arrive wrapped in a DuckDB/S3 error chain before the ambient context
// reports it. Captures slog.Default() for the same symmetry reason the
// canceled test does — a future refactor that emits a slog.Warn on this
// path would break the "no output on cancel" contract without the test
// catching it.
func TestQueryArchiveSources_wrappedDeadlineExceededShortCircuits(t *testing.T) {
	slogBuf := captureSlogDefault(t)
	var stderr bytes.Buffer

	f := &fakeFetcher{responses: []fakeResponse{
		{err: fmt.Errorf("s3: %w", context.DeadlineExceeded)},
	}}

	_, err := queryArchiveSources(
		context.Background(),
		[]string{"src1"},
		query.Options{},
		f.fn(),
		&stderr,
	)
	if err == nil {
		t.Fatal("expected error for wrapped DeadlineExceeded, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected errors.Is(err, context.DeadlineExceeded), got %v", err)
	}
	if stderr.Len() != 0 {
		t.Errorf("expected no stderr output, got %q", stderr.String())
	}
	if slogBuf.Len() != 0 {
		t.Errorf("expected no slog output for wrapped DeadlineExceeded, got %q", slogBuf.String())
	}
}

// TestQueryArchiveSources_multipleFailuresAllReported exercises the UX
// guarantee that one broken archive does not kill the whole query: when
// every source fails with a plain (non-cancellation) error, each failure
// gets its own stderr warning and slog record, the function returns nil
// rows and nil error, and the caller falls through to MergeResults on the
// (possibly live-MySQL-only) result set.
func TestQueryArchiveSources_multipleFailuresAllReported(t *testing.T) {
	slogBuf := captureSlogDefault(t)
	var stderr bytes.Buffer

	f := &fakeFetcher{responses: []fakeResponse{
		{err: errors.New("AccessDenied")},
		{err: errors.New("memory_limit exceeded")},
		{err: errors.New("no such bucket")},
	}}

	rows, err := queryArchiveSources(
		context.Background(),
		[]string{"src1", "src2", "src3"},
		query.Options{},
		f.fn(),
		&stderr,
	)
	if err != nil {
		t.Fatalf("expected nil error for all-plain-failures, got %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected empty rows, got %v", rows)
	}
	if len(f.calls) != 3 {
		t.Errorf("expected all 3 sources fetched, got %v", f.calls)
	}

	stderrOut := stderr.String()
	// Exact-count assertion — catches accidental dedup AND accidental
	// double-printing in one check.
	if got := strings.Count(stderrOut, "Warning: archive query failed"); got != 3 {
		t.Errorf("expected exactly 3 stderr warning lines, got %d: %q", got, stderrOut)
	}
	for _, want := range []string{"AccessDenied", "memory_limit", "no such bucket"} {
		if !strings.Contains(stderrOut, want) {
			t.Errorf("stderr missing %q: %q", want, stderrOut)
		}
	}

	if got := strings.Count(slogBuf.String(), "archive query failed, skipping"); got != 3 {
		t.Errorf("expected 3 slog records, got %d: %q", got, slogBuf.String())
	}
}

// TestQueryArchiveSources_stderrSanitizesNewlines verifies the newline
// collapsing behavior on stderr AND the complementary invariant that
// slog.Warn receives the RAW error (with newlines intact). Together these
// assertions pin the divergence documented at the slog.Warn call site:
// stderr gets sanitized for line-oriented consumers, structured logs get
// the raw text for full-fidelity debugging. A future "consistency" refactor
// that passes sanitizeArchiveErrorMessage(err) to slog.Warn would silently
// degrade the primary debuggability channel — this test catches it.
func TestQueryArchiveSources_stderrSanitizesNewlines(t *testing.T) {
	slogBuf := captureSlogDefault(t)
	var stderr bytes.Buffer

	multiline := "line1\nline2\nline3"
	f := &fakeFetcher{responses: []fakeResponse{
		{err: errors.New(multiline)},
		{rows: []query.ResultRow{}},
	}}

	_, err := queryArchiveSources(
		context.Background(),
		[]string{"src1", "src2"},
		query.Options{},
		f.fn(),
		&stderr,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	out := stderr.String()
	// Stderr: the " | " separator must be present.
	if !strings.Contains(out, "line1 | line2 | line3") {
		t.Errorf("stderr did not collapse newlines to ' | ': %q", out)
	}
	// Stderr: the raw multiline form must be gone — no stray \n in the
	// middle of the warning that would break line-oriented consumers.
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 1 {
		t.Errorf("expected exactly 1 stderr line for 1 failure, got %d: %q", len(lines), lines)
	}

	// Slog: the structured record MUST contain the raw text with escaped
	// newlines preserved. slog's text handler encodes embedded newlines as
	// the literal escape sequence `\n` inside a quoted string, so a
	// sanitized value (" | ") and a raw value ("\\n") produce visibly
	// different serializations. A refactor that silently swaps in
	// sanitizeArchiveErrorMessage(err) for slog would fail the next check.
	slogOut := slogBuf.String()
	if !strings.Contains(slogOut, `\nline2\nline3`) {
		t.Errorf("slog record missing raw escaped newlines — slog must "+
			"receive the unsanitized error so structured-log consumers "+
			"preserve the full text. got: %q", slogOut)
	}
	if strings.Contains(slogOut, "line1 | line2 | line3") {
		t.Errorf("slog record contains the sanitized form — slog should "+
			"receive the RAW error, not the stderr-sanitized one. got: %q", slogOut)
	}
}

// TestQueryArchiveSources_emptySources is a trivial-but-important base case:
// with no archive sources configured, the helper returns (nil, nil) without
// touching stderr or slog. Prevents an accidental nil-dereference or stray
// warning when the whole call is a no-op.
func TestQueryArchiveSources_emptySources(t *testing.T) {
	slogBuf := captureSlogDefault(t)
	var stderr bytes.Buffer
	f := &fakeFetcher{}

	rows, err := queryArchiveSources(
		context.Background(),
		nil,
		query.Options{},
		f.fn(),
		&stderr,
	)
	if err != nil {
		t.Errorf("expected nil error for empty sources, got %v", err)
	}
	if rows != nil {
		t.Errorf("expected nil rows for empty sources, got %v", rows)
	}
	if len(f.calls) != 0 {
		t.Errorf("fetcher should not be called for empty sources, got %v", f.calls)
	}
	if stderr.Len() != 0 || slogBuf.Len() != 0 {
		t.Errorf("expected no output for empty sources, got stderr=%q slog=%q", stderr.String(), slogBuf.String())
	}
}

// TestRunQueryCallsQueryArchiveSources is a meta-test that asserts the
// `runQuery` function in query.go actually calls `queryArchiveSources`,
// captures its return value, and appends that value into the merged
// results slice. Every other test in this file drives queryArchiveSources
// directly with a fake fetcher, so without this guard a regression at the
// call site would leave the helper as dead code while every contract test
// still passed.
//
// The AST walk enforces three structural properties against the runQuery
// FuncDecl body (outer scope only — nested closure literals are explicitly
// skipped so a "dead-closure stub call" mutation cannot game the test):
//
//  1. The identifier "queryArchiveSources" resolves to a package-level
//     function. A local shadow like `queryArchiveSources := func(...) {...}`
//     creates an ast.Object with Kind == ast.Var; the real helper has
//     Kind == ast.Fun. This is why the parser runs with object resolution
//     enabled (no SkipObjectResolution flag).
//  2. The call is the right-hand side of an ast.AssignStmt — not bare,
//     not `_, _ = queryArchiveSources(...)`, not discarded. At least one
//     LHS identifier must be a real named variable.
//  3. Some later statement in the same outer block appends one of those
//     LHS identifiers into a variable named "results" via a call to the
//     builtin "append". This catches the "call helper but discard archive
//     rows" refactor that silently drops archive data while keeping the
//     stderr/slog warnings intact.
//
// The test is NOT a behavioral test — it cannot verify that the arguments
// are correct, the context is threaded properly, or the loop semantics are
// right. For those, see the TestQueryArchiveSources_* suite. The meta-test
// is a structural backstop: it catches mechanical reverts (inline the old
// broken loop, shadow the helper with a local no-op, drop the return
// append) that would defeat behavioral tests by reaching below them.
//
// Mutation shapes empirically verified to trip this test:
//
//   - Inline revert: delete the queryArchiveSources call, paste the
//     pre-#203 `slog.Warn; continue` loop back in.
//   - Dead-closure stub: keep a `_ = func() { queryArchiveSources(...) }()`
//     call inside runQuery but inline the broken loop alongside it.
//   - Local shadow: declare `queryArchiveSources := func(...) {...}` inside
//     runQuery and call the shadow.
//   - Discard-return: write `_, _ = queryArchiveSources(...)` and delete
//     the `append(results, archResults...)` line.
//
// The test anchors query.go's path to this test file via runtime.Caller so
// it works regardless of how `go test` is invoked — from the repo root,
// from the package directory, or with -C <dir>.
func TestRunQueryCallsQueryArchiveSources(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed — cannot locate query.go")
	}
	queryPath := filepath.Join(filepath.Dir(thisFile), "query.go")

	fset := token.NewFileSet()
	// Object resolution MUST be enabled (no parser.SkipObjectResolution) so
	// the walker can distinguish a real package-level function reference
	// from a locally-shadowed variable with the same name.
	f, err := parser.ParseFile(fset, queryPath, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", queryPath, err)
	}

	var runQueryFn *ast.FuncDecl
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fn.Name.Name == "runQuery" && fn.Recv == nil {
			runQueryFn = fn
			break
		}
	}
	if runQueryFn == nil {
		t.Fatal("could not find top-level func runQuery in query.go")
	}

	// Walk only the outer function body. ast.Inspect normally descends into
	// *ast.FuncLit.Body, which would let a dead-closure stub call count as
	// a match. Return false from the callback on FuncLit to prune those
	// subtrees entirely.
	type callSite struct {
		assign *ast.AssignStmt
		call   *ast.CallExpr
		parent *ast.BlockStmt
	}
	var sites []callSite

	// Track the enclosing block for each assign statement we see, so we can
	// look for the subsequent append in step 3. A simple outer-scope walk
	// over runQueryFn.Body.List (plus any non-FuncLit nested blocks)
	// suffices because #203's call site is at the top level of runQuery.
	var walk func(block *ast.BlockStmt)
	walk = func(block *ast.BlockStmt) {
		for _, stmt := range block.List {
			switch s := stmt.(type) {
			case *ast.AssignStmt:
				for _, rhs := range s.Rhs {
					call, ok := rhs.(*ast.CallExpr)
					if !ok {
						continue
					}
					id, ok := call.Fun.(*ast.Ident)
					if !ok {
						continue
					}
					if id.Name != "queryArchiveSources" {
						continue
					}
					// Step 1: must resolve to a real package-level function.
					// A local shadow would give Obj.Kind == ast.Var (or a
					// non-FuncDecl Decl); the real helper has Kind == ast.Fun.
					if id.Obj == nil {
						// Package-level refs sometimes come back with nil Obj
						// because they resolve at link time, not parse time.
						// Accept this case — there's no local shadow binding
						// the name at the outer scope because the walker
						// would have visited that binding first.
					} else if id.Obj.Kind != ast.Fun {
						continue
					}
					sites = append(sites, callSite{assign: s, call: call, parent: block})
				}
			case *ast.IfStmt:
				if s.Body != nil {
					walk(s.Body)
				}
				if elseBlock, ok := s.Else.(*ast.BlockStmt); ok {
					walk(elseBlock)
				}
			case *ast.BlockStmt:
				walk(s)
			case *ast.ForStmt:
				if s.Body != nil {
					walk(s.Body)
				}
			case *ast.RangeStmt:
				if s.Body != nil {
					walk(s.Body)
				}
			}
			// FuncLit and other nested constructs are intentionally NOT
			// descended into — see the doc comment's rationale for dead
			// closures.
		}
	}
	walk(runQueryFn.Body)

	if len(sites) == 0 {
		t.Fatal(
			"runQuery must call queryArchiveSources — reverting to an inline " +
				"slog.Warn; continue loop silently re-introduces issue #203. If " +
				"you are intentionally removing the helper, delete this test AND " +
				"the queryArchiveSources function together, and make sure the " +
				"replacement still pins the dual-channel stderr+slog contract " +
				"with a runQuery-level test.")
	}

	// Step 2 + 3: at least one of the call sites must bind its return into
	// a named LHS variable that is later used as an element being appended
	// into a variable named "results".
	appendCheckPassed := false
	for _, site := range sites {
		// Collect the LHS variable names (skip "_").
		var lhsNames []string
		for _, lhs := range site.assign.Lhs {
			id, ok := lhs.(*ast.Ident)
			if !ok {
				continue
			}
			if id.Name == "_" || id.Name == "err" {
				// "err" is the conventional second return; we don't care
				// about it for the append check — we want the first return
				// value to land in a named slice variable.
				continue
			}
			lhsNames = append(lhsNames, id.Name)
		}
		if len(lhsNames) == 0 {
			continue
		}

		// Find the index of this assign in its parent block, then scan
		// forward for an append(results, <name>...) call.
		startIdx := -1
		for i, stmt := range site.parent.List {
			if stmt == site.assign {
				startIdx = i
				break
			}
		}
		if startIdx < 0 {
			continue
		}

		for i := startIdx + 1; i < len(site.parent.List); i++ {
			found := false
			ast.Inspect(site.parent.List[i], func(n ast.Node) bool {
				// Skip nested FuncLit bodies for the same reason the outer
				// walk does.
				if _, isFuncLit := n.(*ast.FuncLit); isFuncLit {
					return false
				}
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				funcID, ok := call.Fun.(*ast.Ident)
				if !ok || funcID.Name != "append" {
					return true
				}
				if len(call.Args) < 2 {
					return true
				}
				// First arg must be the identifier "results".
				firstArg, ok := call.Args[0].(*ast.Ident)
				if !ok || firstArg.Name != "results" {
					return true
				}
				// Some later arg must reference one of our LHS names.
				for _, arg := range call.Args[1:] {
					var argID *ast.Ident
					switch a := arg.(type) {
					case *ast.Ident:
						argID = a
					case *ast.SliceExpr:
						// `archResults[:]` — accept the underlying ident.
						if id, ok := a.X.(*ast.Ident); ok {
							argID = id
						}
					}
					if argID == nil {
						continue
					}
					for _, name := range lhsNames {
						if argID.Name == name {
							found = true
							return false
						}
					}
				}
				return true
			})
			if found {
				appendCheckPassed = true
				break
			}
		}
		if appendCheckPassed {
			break
		}
	}

	if !appendCheckPassed {
		t.Error(
			"runQuery calls queryArchiveSources but its return value is not " +
				"appended into 'results' — reverting the append silently drops " +
				"archive rows, a subset of the #203 silent-data-loss bug. If " +
				"you are intentionally changing the result-handling shape, " +
				"update this meta-test to match the new pattern AND add a " +
				"behavioral test at the runQuery level.")
	}
}

// TestSanitizeArchiveErrorMessage is a focused unit test for the newline
// sanitizer — it's trivial but the behavior is part of the stderr contract
// and deserves a direct pin. The CRLF and CR-only cases guard against
// regression to a naive ReplaceAll("\n", ...) that would leave stray
// carriage returns on stderr and break line-oriented consumers on a tty.
// The ordering inside strings.NewReplacer is load-bearing here: "\r\n" must
// be processed before the bare "\r" and "\n" rules, otherwise CRLF input
// would expand to " |  | " instead of a single separator.
func TestSanitizeArchiveErrorMessage(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"single_line", "single line", "single line"},
		{"lf", "two\nlines", "two | lines"},
		{"leading_lf", "leading\n", "leading | "},
		{"trailing_lf", "\ntrailing", " | trailing"},
		{"many_lfs", "many\nlines\nhere", "many | lines | here"},
		{"empty", "", ""},
		// CRLF must collapse to a SINGLE separator — not " |  | ".
		{"crlf", "win\r\nlf", "win | lf"},
		{"multiple_crlf", "a\r\nb\r\nc", "a | b | c"},
		// Bare CR alone (old Mac, partial HTTP responses) — must not
		// leak a carriage return onto stderr where it would overwrite
		// the line on a tty.
		{"bare_cr", "cr\ronly", "cr | only"},
		{"trailing_cr", "trailing\r", "trailing | "},
		// Mixed: CRLF followed by bare CR followed by LF.
		{"mixed", "a\r\nb\rc\nd", "a | b | c | d"},
		// Vertical tab and form feed — rare but valid line terminators.
		{"vtab", "a\vb", "a | b"},
		{"formfeed", "a\fb", "a | b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeArchiveErrorMessage(errors.New(tc.in))
			if got != tc.want {
				t.Errorf("sanitizeArchiveErrorMessage(%q): got %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// ─── cleanPKList ─────────────────────────────────────────────────────────────

func TestCleanPKList_dedupAndTrim(t *testing.T) {
	got, err := cleanPKList([]string{"1", " 2 ", "1", "3", "2"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"1", "2", "3"}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i, v := range want {
		if got[i] != v {
			t.Errorf("index %d: want %q got %q", i, v, got[i])
		}
	}
}

func TestCleanPKList_rejectsEmpty(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []string
	}{
		{"empty string", []string{""}},
		{"whitespace only", []string{"   "}},
		{"empty in middle", []string{"1", "", "2"}},
		{"trailing comma artifact", []string{"1", "2", ""}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := cleanPKList(tc.in)
			if err == nil {
				t.Fatalf("expected error for input %v", tc.in)
			}
			if !strings.Contains(err.Error(), "--pks") {
				t.Errorf("expected error to mention --pks, got: %v", err)
			}
		})
	}
}

func TestCleanPKList_nilAndEmpty(t *testing.T) {
	got, err := cleanPKList(nil)
	if err != nil || got != nil {
		t.Errorf("expected (nil, nil) for nil input, got (%v, %v)", got, err)
	}
	got, err = cleanPKList([]string{})
	if err != nil || len(got) != 0 {
		t.Errorf("expected empty result for empty input, got (%v, %v)", got, err)
	}
}

// ─── writeGroupedJSON ────────────────────────────────────────────────────────

func TestWriteGroupedJSON_preservesInputOrderAndEmits_emptyGroups(t *testing.T) {
	rows := []query.ResultRow{
		{EventID: 1, SchemaName: "db", TableName: "t", PKValues: "b", EventType: binparser.EventDelete},
		{EventID: 2, SchemaName: "db", TableName: "t", PKValues: "a", EventType: binparser.EventDelete},
	}
	var buf bytes.Buffer
	n, err := writeGroupedJSON([]string{"a", "b", "c"}, nil, rows, &buf)
	if err != nil {
		t.Fatalf("writeGroupedJSON: %v", err)
	}
	if n != 2 {
		t.Errorf("expected total=2 (one event per matched PK), got %d", n)
	}

	var got struct {
		Results []struct {
			PK     string           `json:"pk"`
			Events []map[string]any `json:"events"`
		} `json:"results"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v\noutput: %s", err, buf.String())
	}
	if len(got.Results) != 3 {
		t.Fatalf("expected 3 groups (one per input PK), got %d", len(got.Results))
	}
	// Input order preserved.
	if got.Results[0].PK != "a" || got.Results[1].PK != "b" || got.Results[2].PK != "c" {
		t.Errorf("group order mismatch: %v", []string{got.Results[0].PK, got.Results[1].PK, got.Results[2].PK})
	}
	if len(got.Results[2].Events) != 0 {
		t.Errorf("expected empty events for PK with no matches, got %d", len(got.Results[2].Events))
	}
}

// A MariaDB UUID key typed as text is stored as its bytes in hex, so its rows
// carry that spelling. The group must still be the typed label's, with the
// rows in it; before, the group came back empty while the rows were fetched.
func TestWriteGroupedJSON_findsRowsUnderTheStoredSpelling(t *testing.T) {
	const text, stored = "cc5c4e6e-bc5a-11f1-9a0c-0affd251cac9", "0xCC5C4E6EBC5A11F19A0C0AFFD251CAC9"
	rows := []query.ResultRow{
		{EventID: 1, SchemaName: "shop", TableName: "sessions", PKValues: stored, EventType: binparser.EventUpdate},
	}
	var buf bytes.Buffer
	n, err := writeGroupedJSON([]string{text, "other"}, map[string]string{text: stored}, rows, &buf)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Results []struct {
			PK     string           `json:"pk"`
			Events []map[string]any `json:"events"`
		} `json:"results"`
	}
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v\noutput: %s", err, buf.String())
	}
	if n != 1 || len(got.Results) != 2 || got.Results[0].PK != text || len(got.Results[0].Events) != 1 {
		t.Errorf("n=%d, output %s; want the one event grouped under the typed label", n, buf.String())
	}
}

// ─── --include-snapshot flag validation ───────────────────────────────────────

func TestRunQuery_includeSnapshotRequiresBaseline(t *testing.T) {
	saved := qIncludeSnapshot
	savedB := qBaseline
	t.Cleanup(func() { qIncludeSnapshot = saved; qBaseline = savedB })

	qIncludeSnapshot = true
	qBaseline = ""

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --include-snapshot used without --baseline")
	}
	if !strings.Contains(err.Error(), "--include-snapshot requires --baseline") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunQuery_includeSnapshotRequiresSchemaTable(t *testing.T) {
	saved := qIncludeSnapshot
	savedB, savedS, savedT := qBaseline, qSchema, qTable
	t.Cleanup(func() {
		qIncludeSnapshot = saved
		qBaseline = savedB
		qSchema = savedS
		qTable = savedT
	})

	qIncludeSnapshot = true
	qBaseline = "/data/baseline.parquet"
	qSchema = ""
	qTable = ""

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --include-snapshot used without --schema/--table")
	}
	if !strings.Contains(err.Error(), "--schema and --table") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunQuery_includeSnapshotConflictsWithProfile(t *testing.T) {
	saved := qIncludeSnapshot
	savedB, savedS, savedT, savedP := qBaseline, qSchema, qTable, qProfile
	t.Cleanup(func() {
		qIncludeSnapshot = saved
		qBaseline = savedB
		qSchema = savedS
		qTable = savedT
		qProfile = savedP
	})

	qIncludeSnapshot = true
	qBaseline = "/data/baseline.parquet"
	qSchema = "db"
	qTable = "t"
	qProfile = "rbac1"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --include-snapshot combined with --profile (silent RBAC bypass)")
	}
	if !strings.Contains(err.Error(), "--profile cannot be combined with --include-snapshot") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunQuery_includeSnapshotRejectsPKFilter(t *testing.T) {
	saved := qIncludeSnapshot
	savedB, savedS, savedT, savedPK := qBaseline, qSchema, qTable, qPK
	t.Cleanup(func() {
		qIncludeSnapshot = saved
		qBaseline = savedB
		qSchema = savedS
		qTable = savedT
		qPK = savedPK
	})

	qIncludeSnapshot = true
	qBaseline = "/data/baseline.parquet"
	qSchema = "db"
	qTable = "t"
	qPK = "42"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --pk combined with --include-snapshot (snapshot rows have no pk_values)")
	}
	if !strings.Contains(err.Error(), "--pk and --pks are not supported with --include-snapshot") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunQuery_baselineRequiresIncludeSnapshot(t *testing.T) {
	saved := qIncludeSnapshot
	savedB := qBaseline
	t.Cleanup(func() { qIncludeSnapshot = saved; qBaseline = savedB })

	qIncludeSnapshot = false
	qBaseline = "/data/baseline.parquet"

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --baseline used without --include-snapshot")
	}
	if !strings.Contains(err.Error(), "--baseline requires --include-snapshot") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRunQuery_eventTypeSnapshotRequiresIncludeSnapshot(t *testing.T) {
	saved := qEventType
	savedIS := qIncludeSnapshot
	t.Cleanup(func() { qEventType = saved; qIncludeSnapshot = savedIS })

	qEventType = "SNAPSHOT"
	qIncludeSnapshot = false

	err := runQuery(queryCmd, nil)
	if err == nil {
		t.Fatal("expected error when --event-type SNAPSHOT set without --include-snapshot")
	}
	if !strings.Contains(err.Error(), "--event-type SNAPSHOT requires --include-snapshot") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// ─── resolveSnapshotPath ──────────────────────────────────────────────────────

func TestResolveSnapshotPath(t *testing.T) {
	cases := []struct {
		name     string
		baseline string
		schema   string
		table    string
		want     string
	}{
		{"direct .parquet path passes through", "/data/baseline.parquet", "db", "t", "/data/baseline.parquet"},
		{"local dir appends schema/table", "/data/dir", "db", "t", "/data/dir/db/t.parquet"},
		{"trailing slash collapsed", "/data/dir/", "db", "t", "/data/dir/db/t.parquet"},
		{"s3 prefix appends schema/table", "s3://bucket/prefix/", "db", "t", "s3://bucket/prefix/db/t.parquet"},
		{"s3 .parquet path passes through", "s3://bucket/file.parquet", "db", "t", "s3://bucket/file.parquet"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveSnapshotPath(tc.baseline, tc.schema, tc.table)
			if got != tc.want {
				t.Errorf("resolveSnapshotPath(%q,%q,%q) = %q, want %q", tc.baseline, tc.schema, tc.table, got, tc.want)
			}
		})
	}
}

// ─── eventTypeJSONName ────────────────────────────────────────────────────────

func TestEventTypeJSONName_snapshot(t *testing.T) {
	if got := eventTypeJSONName(binparser.EventSnapshot); got != "SNAPSHOT" {
		t.Errorf("eventTypeJSONName(EventSnapshot) = %q, want \"SNAPSHOT\"", got)
	}
}
