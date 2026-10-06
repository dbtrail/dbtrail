package shim

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/server"
	_ "github.com/go-sql-driver/mysql" // database/sql driver registration

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/parser"
	"github.com/dbtrail/dbtrail/internal/query"
)

func mustFormatText(t *testing.T, v any) string {
	t.Helper()
	b, err := gomysql.FormatTextValue(v)
	if err != nil {
		t.Fatalf("FormatTextValue(%#v): %v", v, err)
	}
	return string(b)
}

// parseRowCells parses every RowData in a BuildSimpleTextResultset result into
// string cells (RowDatas are populated, not Values, so GetString can't be used).
func parseRowCells(t *testing.T, rs *gomysql.Resultset) [][]string {
	t.Helper()
	out := make([][]string, 0, len(rs.RowDatas))
	for _, rd := range rs.RowDatas {
		fvs, err := rd.Parse(rs.Fields, false, nil)
		if err != nil {
			t.Fatalf("parse row data: %v", err)
		}
		cells := make([]string, len(fvs))
		for i := range fvs {
			switch v := fvs[i].Value().(type) {
			case nil:
				cells[i] = "NULL"
			case []byte:
				cells[i] = string(v)
			default:
				cells[i] = fmt.Sprintf("%v", v)
			}
		}
		out = append(out, cells)
	}
	return out
}

// TestResultsetValue_jsonNumber locks the json.Number → resultset conversion
// (#496). BuildSimpleTextResultset rejects json.Number and fixes a column's wire
// type from its first row, so numbers are pre-rendered to uniform text bytes via
// FormatTextValue — exact for BIGINT UNSIGNED above 2^63.
func TestResultsetValue_jsonNumber(t *testing.T) {
	cases := []struct{ in, want string }{
		{"12345", "12345"},
		{"-7", "-7"},
		{"-9223372036854775808", "-9223372036854775808"}, // BIGINT signed min → int64 branch
		{"9223372036854775807", "9223372036854775807"},   // 2^63-1 → int64 branch
		{"9223372036854775808", "9223372036854775808"},   // exactly 2^63 → uint64 branch
		{"18446744073709551615", "18446744073709551615"}, // BIGINT UNSIGNED max → uint64
		{"3.14", "3.14"},     // fractional → float64
		{"1e1000", "1e1000"}, // beyond float64 range → literal passthrough
	}
	for _, c := range cases {
		b, ok := resultsetValue(json.Number(c.in)).([]byte)
		if !ok {
			t.Errorf("resultsetValue(%q) = %T, want []byte", c.in, resultsetValue(json.Number(c.in)))
			continue
		}
		if string(b) != c.want {
			t.Errorf("resultsetValue(json.Number(%q)) = %q, want %q", c.in, b, c.want)
		}
	}
	// json.Number renders byte-identically to FormatTextValue of the equivalent
	// native value — the path baseline-origin cells take, so the two agree.
	if got := string(resultsetValue(json.Number("18446744073709551615")).([]byte)); got != mustFormatText(t, uint64(18446744073709551615)) {
		t.Errorf("json.Number vs native uint64 render diverge: %q", got)
	}
	// Non-json.Number values pass through unchanged.
	if got := resultsetValue("hi"); got != "hi" {
		t.Errorf("string passthrough = %#v, want \"hi\"", got)
	}
	if got := resultsetValue(nil); got != nil {
		t.Errorf("nil passthrough = %#v, want nil", got)
	}
}

// TestImagesToResult_jsonNumberMixedAndExact is the regression test for the
// review-caught crash (#496/#505): a DOUBLE column with an integral value in one
// row and a fractional in another must NOT trip "row types aren't consistent",
// and a BIGINT UNSIGNED max must render exactly on the wire.
func TestImagesToResult_jsonNumberMixedAndExact(t *testing.T) {
	images := []map[string]any{
		{"id": json.Number("1"), "score": json.Number("100"), "big": json.Number("18446744073709551615")},
		{"id": json.Number("2"), "score": json.Number("100.5"), "big": json.Number("0")},
	}
	res, err := imagesToResult(images, []string{"id", "score", "big"})
	if err != nil {
		t.Fatalf("imagesToResult must not crash on a mixed integral/fractional column: %v", err)
	}
	got := parseRowCells(t, res.Resultset)
	want := [][]string{
		{"1", "100", "18446744073709551615"},
		{"2", "100.5", "0"},
	}
	if len(got) != len(want) {
		t.Fatalf("rows = %d, want %d", len(got), len(want))
	}
	for i := range want {
		for j := range want[i] {
			if got[i][j] != want[i][j] {
				t.Errorf("cell[%d][%d] = %q, want %q", i, j, got[i][j], want[i][j])
			}
		}
	}
}

// TestFullTableTextCell_jsonNumberMatchesNative locks the baseline/event
// consistency the silent-failure review flagged: a json.Number event cell renders
// byte-identically to FormatTextValue of the equivalent native Go value (the path
// baseline-origin INT/DOUBLE cells take), including extreme doubles where the raw
// literal (1e+21) differs from FormatTextValue's decimal form. FLOAT (float32) is
// the documented exception (separate sub-test below).
func TestFullTableTextCell_jsonNumberMatchesNative(t *testing.T) {
	h := NewHandler(nil, nil)
	cases := []struct {
		num    json.Number
		native any
	}{
		{"18446744073709551615", uint64(18446744073709551615)}, // BIGINT UNSIGNED max
		{"100", int64(100)},
		{"-9223372036854775808", int64(-9223372036854775808)}, // BIGINT signed min
		{"1e+21", float64(1e21)},                              // extreme double: "1e+21" vs decimal
		{"-1e+21", float64(-1e21)},                            // negative extreme double
		{"0.0000001", float64(1e-7)},
		{"100.5", float64(100.5)},
	}
	for _, c := range cases {
		event, _ := h.fullTableTextCell("s", "t", "c", c.num).([]byte)
		baseline, _ := h.fullTableTextCell("s", "t", "c", c.native).([]byte)
		if string(event) != string(baseline) {
			t.Errorf("json.Number(%q) → %q, native %T → %q (must be identical)", c.num, event, c.native, baseline)
		}
	}

	// Documented KNOWN exception (pre-existing, baseline-side, tracked as a
	// follow-up): a baseline FLOAT column is scanned by DuckDB as float32, which
	// FormatTextValue widens — so it does NOT match the event side's shortest
	// float32 literal. This locks the current behavior so a future baseline
	// float32 fix has to update it deliberately.
	eventFloat := string(h.fullTableTextCell("s", "t", "c", json.Number("0.1")).([]byte))
	baselineFloat := string(h.fullTableTextCell("s", "t", "c", float32(0.1)).([]byte))
	if eventFloat != "0.1" {
		t.Errorf("event FLOAT 0.1 = %q, want \"0.1\"", eventFloat)
	}
	if eventFloat == baselineFloat {
		t.Errorf("FLOAT baseline (float32) is expected to DIVERGE from the event side today; "+
			"event=%q baseline=%q — if a baseline float32 fix made them match, update this test and the comments", eventFloat, baselineFloat)
	}
}

// TestHandlerHandshakeNoise verifies the small allow-list for queries
// MySQL clients send during connection setup — these shouldn't be
// rejected as "non-flashback" because that would abort the handshake
// before the customer ever runs a real query.
func TestHandlerHandshakeNoise(t *testing.T) {
	h := NewHandler(nil, nil)

	cases := []string{
		"SET NAMES 'utf8mb4'",
		"SET autocommit=1",
		"SET session transaction isolation level read committed",
		"SET sql_mode = 'TRADITIONAL'",
		"SELECT @@version",
		"SELECT @@session.tx_isolation",
		"SHOW WARNINGS",
		"select database()",
	}
	for _, q := range cases {
		t.Run(q, func(t *testing.T) {
			res, err := h.HandleQuery(q)
			if err != nil {
				t.Errorf("expected handshake noise to succeed, got %v", err)
			}
			if res == nil {
				t.Error("expected non-nil result")
			}
		})
	}
}

// TestHandlerHandshakeNoiseRejectsPrivileged — narrow allow-listing
// matters: an over-broad `set ` prefix would let a caller smuggle
// privileged DDL past the shim with a fake-success response. Verify
// the dangerous shapes are NOT silently accepted.
func TestHandlerHandshakeNoiseRejectsPrivileged(t *testing.T) {
	h := NewHandler(nil, nil)
	h.UseDB("myapp")

	cases := []string{
		"SET PASSWORD = 'x'",
		"SET ROLE admin",
		"SET GLOBAL read_only = 0",
		"DROP TABLE orders",
		"INSERT INTO orders VALUES (1)",
	}
	for _, q := range cases {
		t.Run(q, func(t *testing.T) {
			_, err := h.HandleQuery(q)
			if err == nil {
				t.Errorf("query %q should NOT be silently accepted as handshake noise", q)
			}
		})
	}
}

// TestHandlerRejectsNonFlashbackQuery — anything that's not a
// _flashback statement and not handshake noise should fail with a
// clear error to the client.
func TestHandlerRejectsNonFlashbackQuery(t *testing.T) {
	h := NewHandler(nil, nil)
	h.UseDB("myapp")

	_, err := h.HandleQuery("SELECT * FROM orders WHERE id = 1")
	if err == nil {
		t.Fatal("expected error for non-flashback query")
	}
	if !strings.Contains(err.Error(), "_flashback") {
		t.Errorf("error should mention _flashback, got %v", err)
	}
}

// TestHandlerWireErrorCodes pins the wire codes the shim returns to
// MySQL clients. ORMs and monitoring rely on these to tell user input
// errors apart from server crashes; an untyped `fmt.Errorf` collapses
// to ER_UNKNOWN_ERROR (1105) which is the wrong signal.
//
//   - malformed time-travel (recognised virtual schema, bad shape) → 1064
//   - non-time-travel routed to the shim                            → 1235
func TestHandlerWireErrorCodes(t *testing.T) {
	h := NewHandler(nil, nil)
	h.UseDB("myapp")

	cases := []struct {
		name    string
		query   string
		wantErr uint16
	}{
		{
			name:    "malformed_flashback_missing_as_of",
			query:   "SELECT * FROM _flashback.orders WHERE id = 1",
			wantErr: gomysql.ER_PARSE_ERROR,
		},
		{
			name:    "malformed_diff_bad_between",
			query:   "SELECT * FROM _diff.orders WHERE id = 1",
			wantErr: gomysql.ER_PARSE_ERROR,
		},
		{
			name:    "non_time_travel_query",
			query:   "SELECT * FROM orders WHERE id = 1",
			wantErr: gomysql.ER_NOT_SUPPORTED_YET,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.HandleQuery(tc.query)
			if err == nil {
				t.Fatalf("expected error for %q", tc.query)
			}
			var myErr *gomysql.MyError
			if !errors.As(err, &myErr) {
				t.Fatalf("expected *mysql.MyError so server emits a typed wire code, got %T: %v", err, err)
			}
			if myErr.Code != tc.wantErr {
				t.Errorf("wire code = %d, want %d (msg=%q)", myErr.Code, tc.wantErr, myErr.Message)
			}
		})
	}

	// The "no USE <db>" path is structurally distinct: parser sees a
	// virtual schema prefix but no default DB and returns its own
	// error before even attempting the regex match. Pin this case
	// separately so a future split between "syntax error" (1064) and
	// "session-state error" (e.g. 1046) is a deliberate decision, not
	// a silent regression.
	t.Run("missing_use_db_returns_1064", func(t *testing.T) {
		h := NewHandler(nil, nil) // deliberately no UseDB
		_, err := h.HandleQuery("SELECT * FROM _flashback.orders AS OF '2026-01-01' WHERE id = 1")
		if err == nil {
			t.Fatal("expected error when no schema is selected")
		}
		var myErr *gomysql.MyError
		if !errors.As(err, &myErr) {
			t.Fatalf("expected *mysql.MyError, got %T: %v", err, err)
		}
		if myErr.Code != gomysql.ER_PARSE_ERROR {
			t.Errorf("wire code = %d, want %d (msg=%q)", myErr.Code, gomysql.ER_PARSE_ERROR, myErr.Message)
		}
		// The operator hint ("USE <database>") is the actionable
		// part of this error — without it the 1064 is correctly
		// typed but useless to the human reading it.
		if !strings.Contains(myErr.Message, "USE") {
			t.Errorf("error should hint at USE <database>; got %q", myErr.Message)
		}
	})
}

// TestHandlerInternalErrorsKeepDefaultWireCode pins the *inverse* half
// of #277's contract: failures inside runPointInTime / runDiff (DB
// timeouts, FetchMerged errors, archive_state lookup failures) must
// NOT be wrapped in *mysql.MyError so go-mysql/server emits the
// catch-all ER_UNKNOWN_ERROR (1105). A future refactor that wraps
// these in mysql.NewError(ER_PARSE_ERROR, ...) would silently flip
// "the server is broken" into "your query is malformed" — exactly
// the user-vs-server-fault confusion #277 was filed to eliminate.
func TestHandlerInternalErrorsKeepDefaultWireCode(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// We force *some* internal failure inside FetchMerged by failing
	// the archive_state lookup. The exact propagation path is
	// implementation detail (ResolveArchiveSources may swallow the
	// first hit; the planner's archive_state re-query may carry it;
	// an unmocked information_schema query may surface it instead).
	// What matters for the contract is that whatever error reaches
	// HandleQuery's caller is a plain Go error, not a pre-typed
	// *mysql.MyError. The ExpectationsWereMet check below ensures
	// the archive_state query was actually issued — a refactor that
	// stops touching FetchMerged would fail the test loudly rather
	// than silently keeping a passing assertion.
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("FROM archive_state").
		WillReturnError(errors.New("simulated archive_state lookup failure"))
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("sqlmock expectation not met — the FetchMerged path "+
				"this test pins is no longer being exercised: %v", err)
		}
	})

	h := &Handler{
		indexDB: db,
		// AllowGaps=false is the production default and is the mode
		// that propagates archive errors rather than degrading to a
		// Warn. Pin it explicitly so a flip in the zero-value default
		// doesn't quietly change which path this test exercises.
		cfg:    Config{IndexDBName: "bintrail_index", AllowGaps: false},
		logger: slog.Default(),
		archiveFetcher: func(ctx context.Context, opts query.Options, src string) ([]query.ResultRow, error) {
			return nil, nil
		},
	}
	h.UseDB("myapp")

	_, err = h.HandleQuery("SELECT * FROM _flashback.orders AS OF '2026-01-01 00:00:00' WHERE id = 1")
	if err == nil {
		t.Fatal("expected error from failing archive_state lookup")
	}
	var myErr *gomysql.MyError
	if errors.As(err, &myErr) {
		t.Errorf("internal failure must NOT be wrapped in *mysql.MyError "+
			"(go-mysql/server would then emit %d instead of the catch-all 1105); "+
			"got code=%d msg=%q", myErr.Code, myErr.Code, myErr.Message)
	}
}

// TestWrapFetchErrorClassifiesGapAsTyped pins the error-classification
// contract that runPointInTime / runDiff use for FetchMerged failures
// (issue #283). Coverage gaps are client-input concerns and must wire
// as ER_NO_PARTITION_FOR_GIVEN_VALUE (1526); everything else stays a
// plain Go error so go-mysql/server emits the catch-all 1105.
//
// Testing the helper directly (not via sqlmock + the planner) keeps
// the contract pinned to the *classification rule*, not to whatever
// query path happens to surface a GapError today.
func TestWrapFetchErrorClassifiesGapAsTyped(t *testing.T) {
	gap := &query.GapError{GapHours: []time.Time{time.Date(2026, 5, 4, 18, 0, 0, 0, time.UTC)}}

	cases := []struct {
		name     string
		in       error
		isTyped  bool
		wantCode uint16
	}{
		{
			name:     "bare_gap_error",
			in:       gap,
			isTyped:  true,
			wantCode: gomysql.ER_NO_PARTITION_FOR_GIVEN_VALUE,
		},
		{
			// errors.As must unwrap through %w wrappers — protects against
			// a future refactor that wraps the planner's GapError before
			// it reaches the shim.
			name:     "wrapped_gap_error",
			in:       fmt.Errorf("planner reported gaps: %w", gap),
			isTyped:  true,
			wantCode: gomysql.ER_NO_PARTITION_FOR_GIVEN_VALUE,
		},
		{
			name:    "internal_db_failure_stays_untyped",
			in:      errors.New("connection reset by peer"),
			isTyped: false,
		},
		{
			name:    "wrapped_internal_failure_stays_untyped",
			in:      fmt.Errorf("planner failed: %w", errors.New("information_schema unavailable")),
			isTyped: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := wrapFetchError(context.Background(), TypeFlashback, tc.in, slog.Default())
			var myErr *gomysql.MyError
			gotTyped := errors.As(out, &myErr)
			if gotTyped != tc.isTyped {
				t.Fatalf("isTyped = %v, want %v (out=%v)", gotTyped, tc.isTyped, out)
			}
			if !tc.isTyped {
				return
			}
			if myErr.Code != tc.wantCode {
				t.Errorf("wire code = %d, want %d (msg=%q)", myErr.Code, tc.wantCode, myErr.Message)
			}
			// Pin the qType prefix on both branches: operators with
			// concurrent shim sessions need to attribute the error to
			// a specific query type without correlating logs.
			if !strings.Contains(myErr.Message, "flashback") {
				t.Errorf("wire message should include qType context; got %q", myErr.Message)
			}
		})
	}
}

// TestHandlerUseDBStoresSchema — the schema set via UseDB is held
// for use by subsequent HandleQuery calls. The end-to-end coverage
// for "UseDB then run flashback" lives in TestEndToEndHandshake; here
// we just validate the storage step in isolation.
func TestHandlerUseDBStoresSchema(t *testing.T) {
	h := NewHandler(nil, nil)
	if err := h.UseDB("myapp"); err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	got := h.db
	h.mu.Unlock()
	if got != "myapp" {
		t.Errorf("stored schema = %q, want %q", got, "myapp")
	}
}

// TestImageToResultColumnOrder — when no DDL order is supplied
// (snapshot missing or table unknown), columns fall back to
// alphabetical order so the wire output stays deterministic.
func TestImageToResultColumnOrder(t *testing.T) {
	res, err := imageToResult(map[string]any{
		"name":  "alice",
		"id":    int64(42),
		"email": "a@b.com",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Resultset == nil {
		t.Fatal("nil resultset")
	}
	want := []string{"email", "id", "name"}
	got := make([]string, len(res.Resultset.Fields))
	for i, f := range res.Resultset.Fields {
		got[i] = string(f.Name)
	}
	if !slices.Equal(got, want) {
		t.Errorf("column order = %v, want %v", got, want)
	}
}

// TestImageToResultRespectsDDLOrder — when ddlOrder is supplied,
// the wire output emits columns in DDL position so customers see
// the same column ordering they'd get from a regular `SELECT *`.
// Without this the time-travel queries return alphabetised columns
// which mismatches the source table's natural order, surprising
// any side-by-side comparison the user might run.
func TestImageToResultRespectsDDLOrder(t *testing.T) {
	res, err := imageToResult(
		map[string]any{
			"id":   int64(42),
			"sku":  "ABC-1",
			"qty":  int64(2),
			"note": "initial",
		},
		[]string{"id", "sku", "qty", "note"},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"id", "sku", "qty", "note"}
	got := make([]string, len(res.Resultset.Fields))
	for i, f := range res.Resultset.Fields {
		got[i] = string(f.Name)
	}
	if !slices.Equal(got, want) {
		t.Errorf("column order = %v, want %v", got, want)
	}
}

// TestOrderColumnsEdgeCases pins the merge behaviour for the
// "image and snapshot disagree" cases. Each branch is a real
// path the production code can hit when an ALTER TABLE happens
// between the binlog event being indexed and the snapshot being
// taken (or vice versa).
func TestOrderColumnsEdgeCases(t *testing.T) {
	cases := []struct {
		name     string
		image    map[string]any
		ddlOrder []string
		want     []string
	}{
		{
			name:     "nil_ddl_order_falls_back_to_alphabetical",
			image:    map[string]any{"sku": 1, "id": 2, "qty": 3},
			ddlOrder: nil,
			want:     []string{"id", "qty", "sku"},
		},
		{
			name:     "empty_ddl_order_falls_back_to_alphabetical",
			image:    map[string]any{"b": 1, "a": 2},
			ddlOrder: []string{},
			want:     []string{"a", "b"},
		},
		{
			name:     "ddl_columns_missing_from_image_are_skipped",
			image:    map[string]any{"id": 1, "qty": 3},
			ddlOrder: []string{"id", "sku", "qty", "note"},
			want:     []string{"id", "qty"},
		},
		{
			name: "image_columns_missing_from_ddl_are_appended_alphabetically",
			image: map[string]any{
				"id": 1, "sku": 2, "qty": 3, "added_after": 4, "another_new": 5,
			},
			ddlOrder: []string{"id", "sku", "qty"},
			want:     []string{"id", "sku", "qty", "added_after", "another_new"},
		},
		{
			name:     "exact_match_preserves_ddl_order",
			image:    map[string]any{"note": 4, "id": 1, "qty": 3, "sku": 2},
			ddlOrder: []string{"id", "sku", "qty", "note"},
			want:     []string{"id", "sku", "qty", "note"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := orderColumns(tc.image, tc.ddlOrder)
			if !slices.Equal(got, tc.want) {
				t.Errorf("orderColumns = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSelectImage covers every branch of the row-image rule used by
// runPointInTime. The function is intentionally pure so it can be
// exercised without sqlmock or a real MySQL: the rest of the
// _flashback / _snapshot pipeline (sort, LimitPerPK, archive merge)
// is covered by the query package's own tests.
//
// A future refactor that swaps the row_after / row_before priority on
// non-DELETE events, or that reintroduces the row_before fallback for
// DELETE (issue #287 regression), would silently return wrong row
// state to the customer. The "delete_*" cases are the tripwires.
// TestExtractFullTableImages pins the two skip rules of the
// full-table reconstruction path (#276): DELETE events are dropped
// (the row did not exist at AS OF) and INSERTs with a nil row_after
// are dropped (corrupted index — emitting an all-null phantom row
// would overstate the table's row count). The single-row _flashback
// path shares the DELETE-skip rule through reconstruct.ApplyAt (a
// DELETE folds to a nil state, #988), replacing the old selectImage
// helper.
func TestExtractFullTableImages(t *testing.T) {
	deleteRow := query.ResultRow{
		EventType: parser.EventDelete,
		RowBefore: map[string]any{"id": int64(1), "qty": int64(5)},
	}
	insertRow := query.ResultRow{
		EventType: parser.EventInsert,
		RowAfter:  map[string]any{"id": int64(2), "qty": int64(7)},
	}
	emptyAfterRow := query.ResultRow{
		EventType: parser.EventInsert,
		RowAfter:  nil,
	}

	if got := extractFullTableImages([]query.ResultRow{deleteRow}); len(got) != 0 {
		t.Errorf("extractFullTableImages([DELETE]) must skip the row; got %v", got)
	}
	if got := extractFullTableImages([]query.ResultRow{emptyAfterRow}); len(got) != 0 {
		t.Errorf("extractFullTableImages([INSERT with empty row_after]) must skip the row; got %v", got)
	}

	mixed := []query.ResultRow{insertRow, deleteRow, emptyAfterRow, insertRow}
	images := extractFullTableImages(mixed)
	if len(images) != 2 {
		t.Errorf("extractFullTableImages: DELETE + empty_after must both be skipped, kept rows = %d, want 2", len(images))
	}
}

// TestRunPointInTimeDispatchesByPKColumn pins the fix for the empty-
// string PK collision: `WHERE id = ”` is a legitimate single-row
// query against a NOT-NULL VARCHAR with empty default, and a dispatch
// on q.PKValue would silently flip it into a 100k-row table scan.
//
// The previous version of this test re-implemented `q.PKColumn == ""`
// inline and asserted the predicate against itself — a tautology that
// would still pass even if runPointInTime ignored its argument
// entirely. This rewrite drives runPointInTime through sqlmock and
// observes which SQL pattern reaches the index DB, which is the
// only thing that proves the dispatch is correct.
//
// Path detection:
//   - Point-lookup SQL contains `pk_hash = SHA2` (the hash + value
//     guard the SQL builder emits when Options.PKValues != "").
//   - Full-table SQL omits that filter entirely. We detect it via
//     the cost-cap behavioural signature: only runFullTable performs
//     the >cap check, so seeding cap+1 rows surfaces 1104 iff
//     dispatch reached runFullTable.
func TestRunPointInTimeDispatchesByPKColumn(t *testing.T) {
	t.Run("PKColumn_set_runs_point_lookup_sql", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.MatchExpectationsInOrder(false)
		mock.ExpectQuery("information_schema.PARTITIONS").
			WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}))
		// Anchor on `pk_hash = SHA2` — unique to the point-lookup SQL.
		// If runPointInTime wrongly dispatched to runFullTable, the
		// emitted SQL would lack pk_hash and ExpectationsWereMet would
		// fail with "expected query was not matched".
		mock.ExpectQuery("pk_hash = SHA2").
			WillReturnRows(emptyBinlogEventsRows())

		h := &Handler{
			indexDB: db,
			cfg:     Config{AllowGaps: true, IndexDBName: "bintrail_index", NoArchive: true},
			logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
			archiveFetcher: func(ctx context.Context, _ query.Options, _ string) ([]query.ResultRow, error) {
				return nil, nil
			},
		}
		h.UseDB("myapp")

		q := TimeTravelQuery{Type: TypeFlashback, Schema: "myapp", Table: "orders",
			PKColumn: "id", PKValue: "42", AsOf: time.Now().UTC()}
		_, _ = h.runPointInTime(q) // result irrelevant; mock matching is the assertion
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("expected point-lookup SQL with pk_hash filter; got: %v", err)
		}
	})

	t.Run("empty_PKColumn_dispatches_to_full_table", func(t *testing.T) {
		// Behavioural proof of dispatch: only runFullTable performs
		// the cap check. Lower the cap to 1 via Config (no global
		// state), seed 2 rows, expect ER_TOO_BIG_SELECT. If
		// runPointInTime stayed on the point-lookup branch despite
		// PKColumn="", no cap check would fire and the test would
		// fail loud.
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		mock.MatchExpectationsInOrder(false)
		mock.ExpectQuery("information_schema.PARTITIONS").
			WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}))
		rows := sqlmock.NewRows(binlogEventsColumns())
		now := time.Now().UTC()
		for i := 0; i < 2; i++ {
			rows.AddRow(int64(i+1), "binlog.000001", int64(100), int64(200), now,
				nil, nil, "myapp", "orders", parser.EventInsert,
				fmt.Sprintf("%d", i+1), nil, nil,
				fmt.Sprintf(`{"id":%d,"sku":"X"}`, i+1), 0, nil, nil, nil)
		}
		mock.ExpectQuery("FROM binlog_events").WillReturnRows(rows)

		h := &Handler{
			indexDB: db,
			cfg: Config{AllowGaps: true, IndexDBName: "bintrail_index",
				NoArchive: true, FullTableRowCap: 1},
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			archiveFetcher: func(ctx context.Context, _ query.Options, _ string) ([]query.ResultRow, error) {
				return nil, nil
			},
		}
		h.UseDB("myapp")

		q := TimeTravelQuery{Type: TypeFlashback, Schema: "myapp", Table: "orders",
			AsOf: now} // PKColumn / PKValue both empty
		_, err = h.runPointInTime(q)
		if err == nil {
			t.Fatal("expected ER_TOO_BIG_SELECT (proves dispatch reached runFullTable); got nil")
		}
		var myErr *gomysql.MyError
		if !errors.As(err, &myErr) || myErr.Code != gomysql.ER_TOO_BIG_SELECT {
			t.Errorf("expected ER_TOO_BIG_SELECT (1104), got %v", err)
		}
	})
}

// binlogEventsColumns is the column list scanned by query.Engine.Fetch.
// Extracted so the cap-overflow tests don't duplicate the literal.
func binlogEventsColumns() []string {
	return []string{
		"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp",
		"gtid", "connection_id", "schema_name", "table_name", "event_type",
		"pk_values", "changed_columns", "row_before", "row_after", "schema_version",
		"query_text", "query_hash", "commit_ts_us",
	}
}

// emptyBinlogEventsRows is the empty-resultset stub for query.Engine.Fetch.
func emptyBinlogEventsRows() *sqlmock.Rows {
	return sqlmock.NewRows(binlogEventsColumns())
}

// TestImagesToResultColumnsFromUnionWhenNoDDLOrder pins the union-
// across-images behavior for the no-snapshot fallback. Using only
// images[0]'s keys would silently drop a column added by a later
// event in the same query (e.g. a row captured pre-ALTER followed
// by a row captured post-ALTER).
// TestImagesToResult_unresolvedToastMarker is the #592 guard on the shim's
// time-travel read surface: a row image carrying the residual unchanged-TOAST
// marker must abort the client query with a loud error, never serve the
// marker's JSON as the column value. The check is cell-level, so a verbatim
// projection that does NOT include the marker column still succeeds — the
// values actually served are real.
func TestImagesToResult_unresolvedToastMarker(t *testing.T) {
	marker := map[string]any{event.UnchangedToastKey: true}
	images := []map[string]any{
		{"id": "1", "body": "fine"},
		{"id": "2", "body": marker},
	}

	assertToastErr := func(t *testing.T, err error) {
		t.Helper()
		if err == nil {
			t.Fatal("expected a loud error for a marker-carrying image")
		}
		for _, want := range []string{"unresolved unchanged-TOAST marker", "capture invariant violated", "body"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error missing %q:\n%s", want, err)
			}
		}
	}

	// SELECT * multi-row (_flashback full-table path).
	if _, err := imagesToResult(images, []string{"id", "body"}); err == nil {
		t.Fatal("imagesToResult: expected error")
	} else {
		assertToastErr(t, err)
	}

	// Verbatim multi-row: marker column projected → refuse.
	_, err := imagesToResultVerbatim(images, []string{"body"})
	assertToastErr(t, err)

	// Verbatim multi-row: marker column NOT projected → the served cells are
	// clean, so the query succeeds.
	if _, err := imagesToResultVerbatim(images, []string{"id"}); err != nil {
		t.Fatalf("marker in an unprojected column must not refuse: %v", err)
	}

	// Single-row siblings (point-lookup path).
	single := map[string]any{"id": "2", "body": marker}
	_, err = imageToResult(single, []string{"id", "body"})
	assertToastErr(t, err)
	_, err = imageToResultVerbatim(single, []string{"id", "body"})
	assertToastErr(t, err)
	if _, err := imageToResultVerbatim(single, []string{"id"}); err != nil {
		t.Fatalf("single-row verbatim: marker in an unprojected column must not refuse: %v", err)
	}
}

func TestImagesToResultColumnsFromUnionWhenNoDDLOrder(t *testing.T) {
	images := []map[string]any{
		{"id": 1, "sku": "A"},                           // pre-ALTER
		{"id": 2, "sku": "B", "added_after_alter": "X"}, // post-ALTER
	}
	res, err := imagesToResult(images, nil)
	if err != nil {
		t.Fatal(err)
	}
	gotCols := make([]string, len(res.Resultset.Fields))
	for i, f := range res.Resultset.Fields {
		gotCols[i] = string(f.Name)
	}
	wantCols := []string{"added_after_alter", "id", "sku"}
	if !slices.Equal(gotCols, wantCols) {
		t.Errorf("cols = %v, want %v (no-ddlOrder fallback must union image keys, "+
			"not pick from images[0])", gotCols, wantCols)
	}
}

// TestImagesToResultDDLOrderStrictWhenSnapshotPresent pins the
// snapshot-driven semantic the docstring describes: when ddlOrder is
// supplied, every column in it appears in the resultset even if no
// image carries it (NULL on the wire). A future refactor that
// reverted to "intersect ddlOrder with images[0] keys" would silently
// elide post-ALTER columns from queries that span the ALTER.
func TestImagesToResultDDLOrderStrictWhenSnapshotPresent(t *testing.T) {
	images := []map[string]any{
		{"id": 1, "sku": "A"}, // missing the post-ALTER column
	}
	ddlOrder := []string{"id", "sku", "qty", "note"}
	res, err := imagesToResult(images, ddlOrder)
	if err != nil {
		t.Fatal(err)
	}
	gotCols := make([]string, len(res.Resultset.Fields))
	for i, f := range res.Resultset.Fields {
		gotCols[i] = string(f.Name)
	}
	if !slices.Equal(gotCols, ddlOrder) {
		t.Errorf("cols = %v, want %v (ddlOrder must be honored verbatim)", gotCols, ddlOrder)
	}
}

// TestImagesToResultBuildsResultset covers the multi-row resultset
// builder added for #276. The single-row imageToResult path is
// covered by TestImageToResultColumnOrder / TestImageToResultRespectsDDLOrder.
func TestImagesToResultBuildsResultset(t *testing.T) {
	cases := []struct {
		name     string
		images   []map[string]any
		ddlOrder []string
		wantRows int
		wantCols []string
	}{
		{
			name:     "empty_input_returns_empty_resultset",
			images:   nil,
			wantRows: 0,
			wantCols: []string{"_flashback"},
		},
		{
			name:     "single_row_uses_ddl_order",
			images:   []map[string]any{{"id": 1, "sku": "ABC", "qty": 3}},
			ddlOrder: []string{"id", "sku", "qty"},
			wantRows: 1,
			wantCols: []string{"id", "sku", "qty"},
		},
		{
			name: "multi_row_uses_first_image_for_columns",
			images: []map[string]any{
				{"id": 1, "sku": "A", "qty": 1},
				{"id": 2, "sku": "B", "qty": 2},
				{"id": 3, "sku": "C", "qty": 3},
			},
			ddlOrder: []string{"id", "sku", "qty"},
			wantRows: 3,
			wantCols: []string{"id", "sku", "qty"},
		},
		{
			// A row missing a column known to ddlOrder gets a NULL
			// in that position rather than failing the whole query —
			// this mirrors how MySQL itself handles a column added
			// after some rows already existed.
			name: "row_missing_column_yields_null",
			images: []map[string]any{
				{"id": 1, "sku": "A", "qty": 1},
				{"id": 2, "sku": "B"},
			},
			ddlOrder: []string{"id", "sku", "qty"},
			wantRows: 2,
			wantCols: []string{"id", "sku", "qty"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := imagesToResult(tc.images, tc.ddlOrder)
			if err != nil {
				t.Fatal(err)
			}
			if got := len(res.Resultset.RowDatas); got != tc.wantRows {
				t.Errorf("rows = %d, want %d", got, tc.wantRows)
			}
			gotCols := make([]string, len(res.Resultset.Fields))
			for i, f := range res.Resultset.Fields {
				gotCols[i] = string(f.Name)
			}
			if !slices.Equal(gotCols, tc.wantCols) {
				t.Errorf("cols = %v, want %v", gotCols, tc.wantCols)
			}
		})
	}
}

// TestFullTableColumns pins the #600 fix: the full-table column list
// keeps ddlOrder verbatim (NULL-filling columns no image carries) AND
// appends image-only keys (sorted) — most importantly a column dropped
// between the AS OF instant and now, whose value is still in the index.
func TestFullTableColumns(t *testing.T) {
	cases := []struct {
		name     string
		images   []map[string]any
		ddlOrder []string
		want     []string
	}{
		{
			// The reported bug: coupon_code was DROPPED after AS OF, so the
			// latest snapshot (ddlOrder) is [id,total] but the captured image
			// still carries coupon_code. Pre-#600 it was strict-projected away.
			name:     "dropped_column_appended",
			images:   []map[string]any{{"id": 1, "total": 100, "coupon_code": "SAVE10"}},
			ddlOrder: []string{"id", "total"},
			want:     []string{"id", "total", "coupon_code"},
		},
		{
			// No schema drift across the window → byte-identical to ddlOrder.
			name:     "no_drift_is_identity",
			images:   []map[string]any{{"id": 1, "sku": "A", "qty": 3}},
			ddlOrder: []string{"id", "sku", "qty"},
			want:     []string{"id", "sku", "qty"},
		},
		{
			// ddlOrder column absent from every image is still emitted (NULL on
			// the wire) — the ADD-column-after semantics locked by
			// TestImagesToResultDDLOrderStrictWhenSnapshotPresent. The fix must
			// not regress this: it only APPENDS, never intersects.
			name:     "missing_ddl_column_retained_plus_extra_appended",
			images:   []map[string]any{{"id": 1, "coupon_code": "SAVE10"}},
			ddlOrder: []string{"id", "total"},
			want:     []string{"id", "total", "coupon_code"},
		},
		{
			// Extras are the UNION across images, deduped and sorted — a column
			// appearing only in a later event must not be dropped.
			name: "extras_union_across_images_sorted",
			images: []map[string]any{
				{"id": 1, "zeta": 1},
				{"id": 2, "alpha": 2},
				{"id": 3, "zeta": 3},
			},
			ddlOrder: []string{"id"},
			want:     []string{"id", "alpha", "zeta"},
		},
		{
			// No resolved snapshot → union of all image keys, sorted.
			name: "no_ddlorder_unions_image_keys",
			images: []map[string]any{
				{"id": 1, "sku": "A"},
				{"id": 2, "sku": "B", "added": "X"},
			},
			ddlOrder: nil,
			want:     []string{"added", "id", "sku"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := fullTableColumns(tc.images, tc.ddlOrder)
			if !slices.Equal(got, tc.want) {
				t.Errorf("fullTableColumns = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestFullTableColumnsDoesNotMutateDDLOrder guards the append-into-caller
// hazard: ddlOrder is shared (it comes from a cached resolver), so appending
// extras must allocate a fresh slice, never scribble into ddlOrder's backing
// array.
func TestFullTableColumnsDoesNotMutateDDLOrder(t *testing.T) {
	ddlOrder := make([]string, 2, 8) // spare capacity → append would reuse it
	ddlOrder[0], ddlOrder[1] = "id", "total"
	_ = fullTableColumns([]map[string]any{{"id": 1, "coupon_code": "X"}}, ddlOrder)
	if !slices.Equal(ddlOrder, []string{"id", "total"}) {
		t.Errorf("ddlOrder was mutated to %v; must stay [id total]", ddlOrder)
	}
}

// TestFullTableColumnsMatchesSingleRowColumnSet pins acceptance criterion #3
// of #600 (asymmetry gone) at the pure-function level for the REPORTED drop
// case: when the image carries every latest-snapshot column plus a
// since-dropped one, the full-table column SET equals the single-row
// (orderColumns) set — so adding/removing a `WHERE pk=` never hides the
// dropped column. (The two are not equal in general: full-table is a superset
// that also NULL-fills snapshot columns no image carries — see
// fullTableColumns' doc. Here every ddlOrder column is in the image, so the
// superset collapses to equality.)
func TestFullTableColumnsMatchesSingleRowColumnSet(t *testing.T) {
	image := map[string]any{"id": 1, "total": 100, "coupon_code": "SAVE10"}
	ddlOrder := []string{"id", "total"} // coupon_code dropped from latest snapshot

	full := fullTableColumns([]map[string]any{image}, ddlOrder)
	single := orderColumns(image, ddlOrder)

	fullSet, singleSet := map[string]bool{}, map[string]bool{}
	for _, c := range full {
		fullSet[c] = true
	}
	for _, c := range single {
		singleSet[c] = true
	}
	if !maps.Equal(fullSet, singleSet) {
		t.Errorf("column set mismatch: full-table=%v single-row=%v", full, single)
	}
}

// TestImagesToResultVerbatimNeverAppendsExtras pins the explicit-projection
// contract: imagesToResultVerbatim projects onto cols EXACTLY, NULL-filling
// missing keys and never surfacing an image-only key the user didn't list.
// This is the multi-row counterpart of imageToResultVerbatim and the builder
// fullTableResult routes #313 projections to.
func TestImagesToResultVerbatimNeverAppendsExtras(t *testing.T) {
	images := []map[string]any{
		{"id": 1, "total": 100, "coupon_code": "SAVE10"}, // carries an off-projection key
		{"id": 2, "total": 200},                          // missing nothing the user asked for
	}
	res, err := imagesToResultVerbatim(images, []string{"id", "total"})
	if err != nil {
		t.Fatal(err)
	}
	gotCols := make([]string, len(res.Resultset.Fields))
	for i, f := range res.Resultset.Fields {
		gotCols[i] = string(f.Name)
	}
	if want := []string{"id", "total"}; !slices.Equal(gotCols, want) {
		t.Errorf("cols = %v, want %v (coupon_code must NOT be appended)", gotCols, want)
	}
	if got := len(res.Resultset.RowDatas); got != 2 {
		t.Errorf("rows = %d, want 2", got)
	}
}

// TestImageToResultEmpty — an empty image (zero-key map) should
// produce a resultset with no rows.
func TestImageToResultEmpty(t *testing.T) {
	res, err := imageToResult(map[string]any{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Resultset == nil {
		t.Fatal("nil resultset")
	}
	if got := len(res.Resultset.RowDatas); got != 0 {
		t.Errorf("expected 0 rows, got %d", got)
	}
}

// TestNewHandlerWiresArchiveFetcher locks in the issue #255 fix at the
// construction boundary. Both NewHandler and NewHandlerWithConfig must
// install a non-nil archiveFetcher; otherwise every virtual-schema
// query crashes with "ArchiveFetcher is required when NoArchive is
// false" because the FetchMerged contract demands either NoArchive=true
// or a non-nil fetcher.
//
// A failure here means a refactor dropped the archiveFetcher wiring
// from the constructor — the same regression class /proxysql-e2e
// would catch end-to-end, but at unit-test speed.
func TestNewHandlerWiresArchiveFetcher(t *testing.T) {
	h := NewHandler(nil, nil)
	if h.archiveFetcher == nil {
		t.Error("NewHandler must wire a non-nil archiveFetcher; got nil")
	}
	h2 := NewHandlerWithConfig(nil, Config{}, nil)
	if h2.archiveFetcher == nil {
		t.Error("NewHandlerWithConfig must wire a non-nil archiveFetcher; got nil")
	}
}

// TestNewHandlerWiresResolverFn — same boundary check as for the
// archive fetcher: both constructors must install a non-nil
// resolverFn or every time-travel query falls back to alphabetical
// column order silently. A failure here means a refactor dropped
// the schema_snapshots wiring; the test/shim test would catch it
// end-to-end but at much higher cost.
func TestNewHandlerWiresResolverFn(t *testing.T) {
	if h := NewHandler(nil, nil); h.resolverFn == nil {
		t.Error("NewHandler must wire a non-nil resolverFn; got nil")
	}
	if h := NewHandlerWithConfig(nil, Config{}, nil); h.resolverFn == nil {
		t.Error("NewHandlerWithConfig must wire a non-nil resolverFn; got nil")
	}
}

// TestColumnOrderForFallsBackOnResolverError pins the resilience
// contract: when the resolver fails to load (no snapshot yet, DB
// blip, ALTER TABLE the snapshot doesn't know about), columnOrderFor
// returns nil so imageToResult silently degrades to alphabetical
// order rather than failing the customer's query. The opposite
// behaviour (hard-failing on resolver error) would make brand-new
// installs that haven't run `bintrail snapshot` yet unable to
// answer any time-travel query.
func TestColumnOrderForFallsBackOnResolverError(t *testing.T) {
	cases := []struct {
		name       string
		resolverFn func() (*metadata.Resolver, error)
		want       []string
	}{
		{
			name:       "resolver_load_fails",
			resolverFn: func() (*metadata.Resolver, error) { return nil, errors.New("snapshot table missing") },
			want:       nil,
		},
		{
			name: "resolver_loads_but_table_unknown",
			resolverFn: func() (*metadata.Resolver, error) {
				return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{}), nil
			},
			want: nil,
		},
		{
			name: "resolver_returns_table_in_ddl_order",
			resolverFn: func() (*metadata.Resolver, error) {
				return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
					"appdb.orders": {
						Schema: "appdb", Table: "orders",
						Columns: []metadata.ColumnMeta{
							{Name: "id", OrdinalPosition: 1},
							{Name: "sku", OrdinalPosition: 2},
							{Name: "qty", OrdinalPosition: 3},
							{Name: "note", OrdinalPosition: 4},
						},
					},
				}), nil
			},
			want: []string{"id", "sku", "qty", "note"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &Handler{
				logger:     slog.Default(),
				resolverFn: tc.resolverFn,
			}
			got := h.columnOrderFor("appdb", "orders")
			if !slices.Equal(got, tc.want) {
				t.Errorf("columnOrderFor = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestResolverCacheBehaviour pins five properties of the resolver
// cache that columnOrderFor relies on. Each is documented in
// handler.go's resolverCache type comment; this test enforces them.
//
//  1. Hit-within-TTL: a second columnOrderFor call within the TTL
//     window must NOT invoke resolverFn — the resolver load is the
//     expensive operation we're caching.
//  2. Expiry-triggers-reload: a call after the TTL window invokes
//     resolverFn again, so a fresh `bintrail snapshot` is picked up
//     without restarting the shim.
//  3. Sticky-fallback: when a refresh fails AND the cache holds a
//     prior good resolver, we keep serving the stale resolver. This
//     prevents transient index-DB blips from oscillating wire-
//     protocol column order between DDL and alphabetical for the
//     same customer connection.
//  4. Sticky-fallback emits a Warn the first time it fires, so a
//     persistent index-DB outage is operator-visible. Without this,
//     a 2-hour outage is invisible because the wire response still
//     looks healthy.
//  5. Sticky-fallback Warns are rate-limited to one per TTL window
//     so a hot shim doesn't spam the log under sustained outage.
func TestResolverCacheBehaviour(t *testing.T) {
	tableMeta := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"appdb.orders": {
			Schema: "appdb", Table: "orders",
			Columns: []metadata.ColumnMeta{
				{Name: "id", OrdinalPosition: 1},
				{Name: "sku", OrdinalPosition: 2},
			},
		},
	})
	silentLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("hit_within_ttl_skips_loader", func(t *testing.T) {
		now := time.Unix(1_700_000_000, 0)
		calls := 0
		c := resolverCache{}
		load := func() (*metadata.Resolver, error) { calls++; return tableMeta, nil }

		if _, err := c.get(func() time.Time { return now }, time.Minute, load, silentLogger); err != nil {
			t.Fatalf("first get: %v", err)
		}
		if _, err := c.get(func() time.Time { return now.Add(30 * time.Second) }, time.Minute, load, silentLogger); err != nil {
			t.Fatalf("second get: %v", err)
		}
		if calls != 1 {
			t.Errorf("expected exactly 1 loader call within TTL, got %d", calls)
		}
	})

	t.Run("ttl_expiry_triggers_reload", func(t *testing.T) {
		now := time.Unix(1_700_000_000, 0)
		calls := 0
		c := resolverCache{}
		load := func() (*metadata.Resolver, error) { calls++; return tableMeta, nil }

		if _, err := c.get(func() time.Time { return now }, time.Minute, load, silentLogger); err != nil {
			t.Fatalf("first get: %v", err)
		}
		if _, err := c.get(func() time.Time { return now.Add(2 * time.Minute) }, time.Minute, load, silentLogger); err != nil {
			t.Fatalf("second get: %v", err)
		}
		if calls != 2 {
			t.Errorf("expected 2 loader calls after TTL expiry, got %d", calls)
		}
	})

	t.Run("sticky_fallback_on_load_error", func(t *testing.T) {
		now := time.Unix(1_700_000_000, 0)
		c := resolverCache{}
		ok := func() (*metadata.Resolver, error) { return tableMeta, nil }
		fail := func() (*metadata.Resolver, error) { return nil, errors.New("transient db blip") }

		if _, err := c.get(func() time.Time { return now }, time.Minute, ok, silentLogger); err != nil {
			t.Fatalf("warm-up: %v", err)
		}
		got, err := c.get(func() time.Time { return now.Add(2 * time.Minute) }, time.Minute, fail, silentLogger)
		if err != nil {
			t.Fatalf("expected sticky fallback to mask error, got: %v", err)
		}
		if got != tableMeta {
			t.Errorf("expected sticky fallback to return prior resolver, got %v", got)
		}
	})

	t.Run("error_with_no_prior_cache_surfaces", func(t *testing.T) {
		c := resolverCache{}
		want := errors.New("first-time db unreachable")
		_, err := c.get(time.Now, time.Minute, func() (*metadata.Resolver, error) { return nil, want }, silentLogger)
		if !errors.Is(err, want) {
			t.Errorf("expected first-time error to surface, got: %v", err)
		}
	})

	t.Run("sticky_fallback_warns_first_time", func(t *testing.T) {
		now := time.Unix(1_700_000_000, 0)
		c := resolverCache{}
		ok := func() (*metadata.Resolver, error) { return tableMeta, nil }
		fail := func() (*metadata.Resolver, error) { return nil, errors.New("db gone") }
		rec := newRecordingHandler()
		logger := slog.New(rec)

		// Warm the cache so the next failure triggers sticky fallback.
		if _, err := c.get(func() time.Time { return now }, time.Minute, ok, logger); err != nil {
			t.Fatalf("warm-up: %v", err)
		}
		// Push past TTL with a failing load. Expect Warn.
		if _, err := c.get(func() time.Time { return now.Add(2 * time.Minute) }, time.Minute, fail, logger); err != nil {
			t.Fatalf("get during outage: %v", err)
		}

		warns := rec.atLevel(slog.LevelWarn)
		if len(warns) != 1 {
			t.Fatalf("expected 1 Warn record on first sticky-fallback, got %d: %v", len(warns), rec.records)
		}
		if !strings.Contains(warns[0].Message, "stale snapshot") {
			t.Errorf("expected Warn about stale snapshot, got %q", warns[0].Message)
		}
	})

	t.Run("sticky_fallback_warn_is_rate_limited_to_one_per_ttl", func(t *testing.T) {
		now := time.Unix(1_700_000_000, 0)
		c := resolverCache{}
		ok := func() (*metadata.Resolver, error) { return tableMeta, nil }
		fail := func() (*metadata.Resolver, error) { return nil, errors.New("db gone") }
		rec := newRecordingHandler()
		logger := slog.New(rec)
		ttl := time.Minute

		if _, err := c.get(func() time.Time { return now }, ttl, ok, logger); err != nil {
			t.Fatalf("warm-up: %v", err)
		}
		// Three failing gets close together — only the first should Warn.
		for i, dt := range []time.Duration{2 * time.Minute, 2*time.Minute + 5*time.Second, 2*time.Minute + 30*time.Second} {
			if _, err := c.get(func() time.Time { return now.Add(dt) }, ttl, fail, logger); err != nil {
				t.Fatalf("get #%d during outage: %v", i, err)
			}
		}

		if got := len(rec.atLevel(slog.LevelWarn)); got != 1 {
			t.Errorf("expected 1 Warn within TTL window, got %d", got)
		}

		// Push past the rate-limit window — expect a second Warn.
		if _, err := c.get(func() time.Time { return now.Add(2*time.Minute + 70*time.Second) }, ttl, fail, logger); err != nil {
			t.Fatalf("get past rate-limit: %v", err)
		}
		if got := len(rec.atLevel(slog.LevelWarn)); got != 2 {
			t.Errorf("expected 2 Warns after TTL window expires, got %d", got)
		}
	})
}

// TestColumnOrderForDistinguishesNoSnapshotFromRealError pins the
// log-level split documented in columnOrderFor: ErrNoSnapshots is
// the benign first-install state (Debug log only) while any other
// resolver-load error is a real config/infra problem (Warn log).
// Both still return nil so the alphabetical fallback path runs.
//
// Without this test a future refactor that collapsed both error
// paths back into the same Debug log would silently un-fix the
// observability gap that motivated the sentinel — the recording
// handler asserts on the actual emitted level rather than reading
// the source.
func TestColumnOrderForDistinguishesNoSnapshotFromRealError(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		wantLevel slog.Level
		wantMsg   string
	}{
		{
			name:      "no_snapshots_logs_debug",
			err:       metadata.ErrNoSnapshots,
			wantLevel: slog.LevelDebug,
			wantMsg:   "no snapshots",
		},
		{
			name:      "real_error_logs_warn",
			err:       errors.New("connection refused"),
			wantLevel: slog.LevelWarn,
			wantMsg:   "schema_snapshots lookup failed",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := newRecordingHandler()
			h := &Handler{
				logger:     slog.New(rec),
				resolverFn: func() (*metadata.Resolver, error) { return nil, tc.err },
			}
			if got := h.columnOrderFor("appdb", "orders"); got != nil {
				t.Errorf("expected nil fallback, got %v", got)
			}
			records := rec.atLevel(tc.wantLevel)
			if len(records) != 1 {
				t.Fatalf("expected exactly 1 record at level %s, got %d (all records: %v)",
					tc.wantLevel, len(records), rec.records)
			}
			if !strings.Contains(records[0].Message, tc.wantMsg) {
				t.Errorf("expected message containing %q, got %q", tc.wantMsg, records[0].Message)
			}
		})
	}
}

// TestColumnOrderForUsesCache pins the wiring between columnOrderFor
// and resolverCache. Without this test, a refactor that bypassed the
// cache (e.g. called h.resolverFn() directly) would invalidate every
// property TestResolverCacheBehaviour pins — the cache subtests would
// still pass because they exercise the cache type directly, not the
// integration. The test counts loader invocations across two
// columnOrderFor calls within the TTL window and asserts the count
// is exactly 1.
func TestColumnOrderForUsesCache(t *testing.T) {
	calls := 0
	tableMeta := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"appdb.orders": {
			Schema: "appdb", Table: "orders",
			Columns: []metadata.ColumnMeta{
				{Name: "id", OrdinalPosition: 1},
				{Name: "sku", OrdinalPosition: 2},
			},
		},
	})
	h := &Handler{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		resolverFn: func() (*metadata.Resolver, error) {
			calls++
			return tableMeta, nil
		},
	}

	if got := h.columnOrderFor("appdb", "orders"); !slices.Equal(got, []string{"id", "sku"}) {
		t.Fatalf("first call: %v", got)
	}
	if got := h.columnOrderFor("appdb", "orders"); !slices.Equal(got, []string{"id", "sku"}) {
		t.Fatalf("second call: %v", got)
	}
	if calls != 1 {
		t.Errorf("expected resolverFn to be invoked exactly once across two columnOrderFor calls "+
			"within the TTL window (cache wiring regression?), got %d calls", calls)
	}
}

// TestMarshalImageOrderedDDL pins the contract that _diff JSON keys
// follow the source table's DDL order — without this, runDiff's
// row_before/row_after columns alphabetise (the json.Marshal(map)
// default), creating an inconsistency with _flashback's reconstructed
// row.
func TestMarshalImageOrderedDDL(t *testing.T) {
	cases := []struct {
		name     string
		image    map[string]any
		ddlOrder []string
		want     string
	}{
		{
			name:     "ddl_order_respected",
			image:    map[string]any{"id": 42, "sku": "ABC", "qty": 1, "note": "init"},
			ddlOrder: []string{"id", "sku", "qty", "note"},
			want:     `{"id":42,"sku":"ABC","qty":1,"note":"init"}`,
		},
		{
			name:     "nil_image_renders_empty_string",
			image:    nil,
			ddlOrder: []string{"id"},
			want:     "",
		},
		{
			name:     "nil_ddl_order_falls_back_to_alphabetical",
			image:    map[string]any{"id": 42, "sku": "ABC"},
			ddlOrder: nil,
			want:     `{"id":42,"sku":"ABC"}`,
		},
		{
			name:     "image_columns_not_in_ddl_appended_alphabetically",
			image:    map[string]any{"id": 1, "sku": "X", "added": "new"},
			ddlOrder: []string{"id", "sku"},
			want:     `{"id":1,"sku":"X","added":"new"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := marshalImageOrdered(tc.image, tc.ddlOrder)
			if got != tc.want {
				t.Errorf("marshalImageOrdered = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestRunPointInTimeInvokesArchiveFetcher exercises the runPointInTime
// → FetchMerged → ArchiveFetcher path with sqlmock, asserting that the
// shim's wiring actually delivers archive rows on virtual-schema
// queries (the issue #255 fix). Uses a stubbed archive_state row so
// FetchMerged calls the injected fetcher.
func TestRunPointInTimeInvokesArchiveFetcher(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// archive_state returns one S3-backed source. The local_path is
	// empty so ResolveArchiveSources falls through to the S3 branch
	// (which doesn't require the directory to exist on disk for the
	// shim host to discover it). The s3_key contains the
	// "bintrail_id=" marker extractBasePath looks for.
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("FROM archive_state").
		WillReturnRows(sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("test-id", "", "test-bucket", "bintrail_id=test-id/event_date=2026/events.parquet"))
	// The planner queries information_schema.PARTITIONS. Stub empty
	// so the planner returns no live hours.
	mock.ExpectQuery("information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}))
	// Live MySQL fetch may or may not run depending on planner output;
	// stub it permissive (no expected rows) so a call is fine.
	mock.ExpectQuery("FROM binlog_events").
		WillReturnRows(sqlmock.NewRows([]string{"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp", "gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values", "changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash", "commit_ts_us"}))

	called := false
	fakeFetcher := func(ctx context.Context, opts query.Options, src string) ([]query.ResultRow, error) {
		called = true
		return nil, nil
	}

	h := &Handler{
		indexDB:        db,
		cfg:            Config{AllowGaps: true, IndexDBName: "bintrail_index"},
		logger:         slog.Default(),
		archiveFetcher: fakeFetcher,
	}

	asof := time.Now()
	q := TimeTravelQuery{
		Type:    TypeFlashback,
		Schema:  "myapp",
		Table:   "orders",
		PKValue: "1",
		AsOf:    asof,
	}
	if _, err := h.runPointInTime(q); err != nil {
		// runPointInTime can succeed (empty resultset) or fail with a
		// scan error from sqlmock; both still prove the fetcher was
		// invoked. The assertion that matters is `called`.
		t.Logf("runPointInTime returned %v (acceptable for sqlmock-stubbed DB)", err)
	}
	if !called {
		t.Error("expected archiveFetcher to be invoked when archive_state has rows; was not called")
	}
}

// TestRunFullTableEnforcesCostCap exercises the load-bearing OOM
// guardrail: when FetchMerged returns more rows than the configured
// cap, runFullTable must surface ER_TOO_BIG_SELECT (1104) on the
// wire, not silently truncate (which would hand the customer a
// partial, unverifiable resultset) and not crash.
//
// The cap is configured per-Handler via Config.FullTableRowCap so
// this test can lower it to 3 on a local Handler instance without
// touching a global var — that keeps the test parallel-safe and
// matches the production path (a future per-tenant override would
// flow through the same field).
//
// A regression that drops the +1 sentinel on Limit (e.g. `Limit:
// cap` without the +1) silently turns the cap into "exactly cap
// rows accepted, no error." This test catches that.
func TestRunFullTableEnforcesCostCap(t *testing.T) {
	const testCap = 3

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Planner queries information_schema.PARTITIONS; stub empty so
	// the planner returns nil. AllowGaps=true below disables strict
	// gap enforcement so the planner-empty path doesn't short-circuit
	// with a *GapError before runFullTable gets a chance to evaluate
	// the cap.
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}))

	// Return cap+1 binlog_events rows. row_after is a JSON image so
	// the scan path produces a non-DELETE non-empty image — ensures
	// extractFullTableImages doesn't filter them out before the cap
	// check sees them.
	rows := sqlmock.NewRows(binlogEventsColumns())
	now := time.Now().UTC()
	for i := 0; i < testCap+1; i++ {
		rows.AddRow(
			int64(i+1), "binlog.000001", int64(100), int64(200), now,
			nil, nil, "myapp", "orders", parser.EventInsert,
			fmt.Sprintf("%d", i+1), nil, nil,
			fmt.Sprintf(`{"id":%d,"sku":"X"}`, i+1), 0, nil, nil, nil,
		)
	}
	mock.ExpectQuery("FROM binlog_events").WillReturnRows(rows)

	h := &Handler{
		indexDB: db,
		cfg: Config{AllowGaps: true, IndexDBName: "bintrail_index",
			NoArchive: true, FullTableRowCap: testCap},
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		archiveFetcher: func(ctx context.Context, _ query.Options, _ string) ([]query.ResultRow, error) {
			return nil, nil
		},
	}
	h.UseDB("myapp")

	q := TimeTravelQuery{
		Type:   TypeFlashback,
		Schema: "myapp",
		Table:  "orders",
		AsOf:   now,
		// PKColumn deliberately empty — that's how runPointInTime
		// dispatches into runFullTable.
	}
	_, err = h.runFullTable(q)
	if err == nil {
		t.Fatal("expected ER_TOO_BIG_SELECT for rows > cap; got nil")
	}
	var myErr *gomysql.MyError
	if !errors.As(err, &myErr) {
		t.Fatalf("expected *mysql.MyError, got %T: %v", err, err)
	}
	if myErr.Code != gomysql.ER_TOO_BIG_SELECT {
		t.Errorf("wire code = %d, want %d (ER_TOO_BIG_SELECT, msg=%q)",
			myErr.Code, gomysql.ER_TOO_BIG_SELECT, myErr.Message)
	}
}

// TestRunFullTable_Limit exercises the #997 LIMIT interaction on the binlog-only
// full-table _flashback path (runFullTable), which is distinct code from the
// _snapshot buffered LIMIT: a LIMIT at or below the cap bounds the fetch so the
// query SUCCEEDS instead of tripping the cap, and a LIMIT above the cap keeps the
// cap+1 overflow probe (a LIMIT never raises the cap). sqlmock returns exactly
// what the real fetch would for the given Limit, since it doesn't honour the SQL
// LIMIT clause itself.
func TestRunFullTable_Limit(t *testing.T) {
	run := func(t *testing.T, cap, limit, nRows int) (*gomysql.Result, error) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		mock.MatchExpectationsInOrder(false)
		mock.ExpectQuery("information_schema.PARTITIONS").
			WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}))
		now := time.Now().UTC()
		events := func(n int) *sqlmock.Rows {
			rows := sqlmock.NewRows(binlogEventsColumns())
			for i := 0; i < n; i++ {
				rows.AddRow(
					int64(i+1), "binlog.000001", int64(100*(i+1)), int64(100*(i+1)+50), now,
					nil, nil, "myapp", "orders", parser.EventInsert,
					fmt.Sprintf("%d", i+1), nil, nil,
					fmt.Sprintf(`{"id":%d,"sku":"X"}`, i+1), 0, nil, nil, nil,
				)
			}
			return rows
		}
		// The read under the cap fixes the keys; the second one (#2156) asks
		// for those keys' latest change in binary log order and runs only
		// when the first one fit.
		if nRows <= cap {
			mock.ExpectQuery("pk_values IN").WillReturnRows(events(nRows))
		}
		mock.ExpectQuery("FROM binlog_events").WillReturnRows(events(nRows))
		h := &Handler{
			indexDB: db,
			cfg: Config{AllowGaps: true, IndexDBName: "bintrail_index",
				NoArchive: true, FullTableRowCap: cap},
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			archiveFetcher: func(ctx context.Context, _ query.Options, _ string) ([]query.ResultRow, error) {
				return nil, nil
			},
		}
		h.UseDB("myapp")
		res, err := h.runFullTable(TimeTravelQuery{
			Type: TypeFlashback, Schema: "myapp", Table: "orders", AsOf: now, Limit: limit,
		})
		if err == nil {
			if merr := mock.ExpectationsWereMet(); merr != nil {
				t.Errorf("reads: %v", merr)
			}
		}
		return res, err
	}

	t.Run("under_cap_succeeds", func(t *testing.T) {
		res, err := run(t, 3, 2, 2) // LIMIT 2 <= cap 3 → fetch bounds to 2, no cap error
		if err != nil {
			t.Fatalf("LIMIT under cap should succeed, got %v", err)
		}
		if n := len(res.Resultset.RowDatas); n != 2 {
			t.Errorf("rows = %d, want 2", n)
		}
	})
	t.Run("at_cap_succeeds", func(t *testing.T) {
		res, err := run(t, 3, 3, 3) // LIMIT == cap boundary → capped=false path
		if err != nil {
			t.Fatalf("LIMIT == cap should succeed, got %v", err)
		}
		if n := len(res.Resultset.RowDatas); n != 3 {
			t.Errorf("rows = %d, want 3", n)
		}
	})
	t.Run("above_cap_trips_cap", func(t *testing.T) {
		_, err := run(t, 3, 5, 4) // LIMIT 5 > cap 3 → probe cap+1=4 → overflow
		var myErr *gomysql.MyError
		if !errors.As(err, &myErr) || myErr.Code != gomysql.ER_TOO_BIG_SELECT {
			t.Fatalf("LIMIT above cap must trip ER_TOO_BIG_SELECT (a LIMIT never raises the cap); got %v", err)
		}
	})
}

// TestWarnCorruptImageDrop pins warnCorruptImageDrop's guard both directions: a
// non-DELETE tail folding to an empty state (corrupt/partial image) Warns so the
// silent drop is operator-visible, while the legitimate empty cases (DELETE tail,
// no events at all) stay silent — a false Warn on every "row deleted before AsOf"
// query would be noise. A refactor that flips the guard passes CI silently
// without this.
func TestWarnCorruptImageDrop(t *testing.T) {
	newH := func() (*Handler, *recordingHandler) {
		rec := newRecordingHandler()
		return &Handler{logger: slog.New(rec)}, rec
	}
	t.Run("non_delete_tail_warns", func(t *testing.T) {
		h, rec := newH()
		h.warnCorruptImageDrop("s", "t", []query.ResultRow{{EventType: parser.EventInsert, EventID: 3}})
		if got := len(rec.atLevel(slog.LevelWarn)); got != 1 {
			t.Errorf("Warn count = %d, want 1 (non-DELETE tail folded to empty)", got)
		}
	})
	t.Run("delete_tail_silent", func(t *testing.T) {
		h, rec := newH()
		h.warnCorruptImageDrop("s", "t", []query.ResultRow{{EventType: parser.EventDelete, EventID: 3}})
		if got := len(rec.atLevel(slog.LevelWarn)); got != 0 {
			t.Errorf("Warn count = %d, want 0 (legit DELETE tail = row absent at AsOf)", got)
		}
	})
	t.Run("no_events_silent", func(t *testing.T) {
		h, rec := newH()
		h.warnCorruptImageDrop("s", "t", nil)
		if got := len(rec.atLevel(slog.LevelWarn)); got != 0 {
			t.Errorf("Warn count = %d, want 0 (never existed = no events)", got)
		}
	})
}

// TestNewHandlerDefaultIsStrict pins the library-side counterpart of the
// CLI default-pin in cmd/bintrail/shim_test.go: NewHandler must return a
// Handler configured with AllowGaps=false. The CLI builds Config directly
// via NewHandlerWithConfig, so a regression that restored the legacy
// AllowGaps=true default in NewHandler would not break the production
// path — but library callers (tests, future embedders) would silently
// pick up the permissive behaviour the issue #257 fix turns off.
func TestNewHandlerDefaultIsStrict(t *testing.T) {
	h := NewHandler(nil, nil)
	if h.cfg.AllowGaps {
		t.Error("NewHandler must default AllowGaps=false (strict); got true (see #257)")
	}
}

// TestRunPointInTimeStrictModePropagatesArchiveError pins the issue #257
// fix: when AllowGaps=false (the new production default) and an archive
// source fails, runPointInTime must return an error rather than silently
// swallowing the failure and returning a partial resultset. Without
// propagation, the MySQL client on the wire sees a successful response
// missing rows it should have received — the exact silent failure the
// PR fixes.
func TestRunPointInTimeStrictModePropagatesArchiveError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("FROM archive_state").
		WillReturnRows(sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}).
			AddRow("test-id", "", "test-bucket", "bintrail_id=test-id/event_date=2026/events.parquet"))
	mock.ExpectQuery("information_schema.PARTITIONS").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME", "PARTITION_DESCRIPTION"}))
	mock.ExpectQuery("FROM binlog_events").
		WillReturnRows(sqlmock.NewRows([]string{"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp", "gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values", "changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash", "commit_ts_us"}))

	archiveErr := errors.New("synthetic archive failure (e.g. S3 throttling)")
	failingFetcher := func(ctx context.Context, opts query.Options, src string) ([]query.ResultRow, error) {
		return nil, archiveErr
	}

	h := &Handler{
		indexDB:        db,
		cfg:            Config{AllowGaps: false, IndexDBName: "bintrail_index"},
		logger:         slog.Default(),
		archiveFetcher: failingFetcher,
	}

	q := TimeTravelQuery{
		Type:    TypeFlashback,
		Schema:  "myapp",
		Table:   "orders",
		PKValue: "1",
		AsOf:    time.Now(),
	}
	_, err = h.runPointInTime(q)
	if err == nil {
		t.Fatal("expected runPointInTime to propagate archive failure under AllowGaps=false; got nil error")
	}
	// errors.Is over substring match: FetchMerged wraps the synthetic
	// archiveErr with %w, so the sentinel is recoverable. Pinning the
	// exact propagation path survives future error-message rewording —
	// a substring check on "archive" would also pass for an unrelated
	// archive-shaped error (e.g. validate-stage rejection) and that's
	// not the contract this test is here to enforce.
	if !errors.Is(err, archiveErr) {
		t.Errorf("expected wrapped archiveErr sentinel, got %v", err)
	}
}

// TestPlannerScopesPartitionsToIndexDB pins issue #259: the planner
// must scope information_schema.PARTITIONS to the index DB, not the
// user query's schema. A regression that re-passes q.Schema causes
// _flashback/_snapshot to return 0 rows (every hour misclassified as
// a coverage gap) and _diff to abort under strict mode.
func TestPlannerScopesPartitionsToIndexDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("FROM archive_state").
		WillReturnRows(sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}))
	mock.ExpectQuery("information_schema.PARTITIONS").
		WithArgs("bintrail_index").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).
			AddRow("p_2026050415"))
	mock.ExpectQuery("FROM binlog_events").
		WillReturnRows(sqlmock.NewRows([]string{"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp", "gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values", "changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash", "commit_ts_us"}))

	h := &Handler{
		indexDB: db,
		cfg: Config{
			AllowGaps:   false,
			NoArchive:   false,
			IndexDBName: "bintrail_index",
		},
		logger:         slog.Default(),
		archiveFetcher: func(ctx context.Context, opts query.Options, src string) ([]query.ResultRow, error) { return nil, nil },
	}

	q := TimeTravelQuery{
		Type:    TypeFlashback,
		Schema:  "e2e_source",
		Table:   "orders",
		PKValue: "1",
		AsOf:    time.Date(2026, 5, 4, 15, 17, 52, 0, time.UTC),
	}
	if _, err := h.runPointInTime(q); err != nil {
		t.Fatalf("runPointInTime: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met (the planner likely scoped to %q instead of %q): %v",
			q.Schema, h.cfg.IndexDBName, err)
	}
}

// TestRunDiffScopesPartitionsToIndexDB is the runDiff sibling of
// TestPlannerScopesPartitionsToIndexDB. The two call sites do the same
// thing today, but a future refactor that splits Config could re-break
// _diff in isolation while leaving _flashback working — pinning each
// call site independently catches that.
func TestRunDiffScopesPartitionsToIndexDB(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery("FROM archive_state").
		WillReturnRows(sqlmock.NewRows([]string{"bintrail_id", "sample_local", "sample_bucket", "sample_key"}))
	mock.ExpectQuery("information_schema.PARTITIONS").
		WithArgs("bintrail_index").
		WillReturnRows(sqlmock.NewRows([]string{"PARTITION_NAME"}).
			AddRow("p_2026050415"))
	mock.ExpectQuery("FROM binlog_events").
		WillReturnRows(sqlmock.NewRows([]string{"event_id", "binlog_file", "start_pos", "end_pos", "event_timestamp", "gtid", "connection_id", "schema_name", "table_name", "event_type", "pk_values", "changed_columns", "row_before", "row_after", "schema_version", "query_text", "query_hash", "commit_ts_us"}))

	h := &Handler{
		indexDB: db,
		cfg: Config{
			AllowGaps:   false,
			NoArchive:   false,
			IndexDBName: "bintrail_index",
		},
		logger:         slog.Default(),
		archiveFetcher: func(ctx context.Context, opts query.Options, src string) ([]query.ResultRow, error) { return nil, nil },
	}

	q := TimeTravelQuery{
		Type:    TypeDiff,
		Schema:  "e2e_source",
		Table:   "orders",
		PKValue: "1",
		Since:   time.Date(2026, 5, 4, 15, 17, 0, 0, time.UTC),
		Until:   time.Date(2026, 5, 4, 15, 18, 0, 0, time.UTC),
	}
	if _, err := h.runDiff(q); err != nil {
		t.Fatalf("runDiff: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations not met (the planner likely scoped to %q instead of %q): %v",
			q.Schema, h.cfg.IndexDBName, err)
	}
}

// TestEndToEndHandshake_AcceptsCorrectPassword boots a real MySQL-protocol
// server with our Handler and asserts that a client connecting with the
// correct username AND password passes the mysql_native_password challenge.
//
// This is the regression guard for issue #254: the handshake exercises
// `compareNativePasswordAuthData(salt, cleartext)` against the value
// `TenantAuth.GetCredential` returns. A regression to the pre-fix
// `("", true, nil)` would only let empty-password clients in — this
// test would fail because the client sends the actual password's
// scrambled response.
func TestEndToEndHandshake_AcceptsCorrectPassword(t *testing.T) {
	if err := runHandshakeTest(t, "alice", "alicepw", "alice", "alicepw"); err != nil {
		t.Fatalf("expected handshake to succeed with matching password: %v", err)
	}
}

// TestEndToEndHandshake_RejectsWrongPassword is the negative half: a
// client sending the wrong password must fail authentication. This
// catches the literal regression of #254 — without it, a pre-fix
// `GetCredential` returning "" would still pass
// TestEndToEndHandshake_AcceptsCorrectPassword if the server happened
// to accept any client response (which it does NOT today, but the
// negative case is what proves real validation is happening).
func TestEndToEndHandshake_RejectsWrongPassword(t *testing.T) {
	err := runHandshakeTest(t, "alice", "alicepw", "alice", "wrongpw")
	if err == nil {
		t.Fatal("expected handshake to fail with wrong password; got nil")
	}
	if !strings.Contains(err.Error(), "Access denied") {
		t.Errorf("expected MySQL 'Access denied' error, got %v", err)
	}
}

// runHandshakeTest spins up one shim listener, configures TenantAuth
// with serverUser/serverPass, and dials with clientUser/clientPass.
// Returns the client's Ping error (nil on success). Used by both the
// positive and negative auth tests above.
func runHandshakeTest(t *testing.T, serverUser, serverPass, clientUser, clientPass string) error {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	addr := listener.Addr().String()

	// Server side: accept one connection, perform handshake, then loop
	// HandleCommand until the client disconnects. SetReadDeadline
	// guarantees the loop unblocks even if the client's TCP close does
	// not propagate immediately, so the test can never hang. It is a
	// BACKSTOP only — the normal exit is the EOF from the client's close —
	// but it also covers every read of the handshake itself, so it must be
	// generous: at 3s a loaded machine could stall the multi-round-trip
	// auth past the deadline, aborting the handshake server-side and
	// handing the client an I/O error instead of "Access denied" (#1164).
	serverErr := make(chan error, 1)
	go func() {
		c, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer c.Close()
		c.SetReadDeadline(time.Now().Add(30 * time.Second))
		h := NewHandler(nil, nil)
		h.UseDB("myapp")
		srv := server.NewDefaultServer()
		auth, _ := NewTenantAuth(map[string]string{serverUser: serverPass})
		mc, err := server.NewCustomizedConn(c, srv, auth, h)
		if err != nil {
			// Auth failure surfaces here as a non-nil error from
			// NewCustomizedConn (handshake fails before the command
			// loop starts). Negative-auth tests rely on this.
			serverErr <- err
			return
		}
		session := NewSession(mc, h)
		for {
			if err := session.HandleCommand(); err != nil {
				serverErr <- nil
				return
			}
		}
	}()

	host, port, _ := net.SplitHostPort(addr)
	clientErr := make(chan error, 1)
	go func() {
		clientErr <- driveClient(host+":"+port, clientUser, clientPass)
	}()

	// Both waits are failure detectors, not paced waits: on the green path
	// the channels are signalled as soon as the handshake resolves. Keep
	// them generous (30s) so a loaded machine cannot turn scheduling delay
	// into a spurious failure (#1164).
	var pingErr error
	select {
	case pingErr = <-clientErr:
	case <-time.After(30 * time.Second):
		t.Fatal("client timed out")
	}

	listener.Close()
	select {
	case <-serverErr:
	case <-time.After(30 * time.Second):
		t.Fatal("server goroutine did not exit")
	}
	return pingErr
}

// driveClient connects to the shim with explicit credentials and
// runs Ping. Returns the Ping error (nil on success).
func driveClient(addr, user, password string) error {
	dsn := user + ":" + password + "@tcp(" + addr + ")/"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return err
	}
	return nil
}

// recordingHandler is a minimal slog.Handler that captures every
// emitted record into an in-memory slice. Used by tests that need
// to assert log levels and messages — without it we'd have to
// either parse a TextHandler's stringly output or skip log-level
// verification entirely (which is what the prior weakened test
// resorted to). Concurrent-safe so it can sit behind a logger
// shared across goroutines if a future test exercises that path.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func newRecordingHandler() *recordingHandler { return &recordingHandler{} }

func (h *recordingHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *recordingHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(_ string) slog.Handler      { return h }

// atLevel returns all captured records at exactly the given level.
func (h *recordingHandler) atLevel(level slog.Level) []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []slog.Record
	for _, r := range h.records {
		if r.Level == level {
			out = append(out, r)
		}
	}
	return out
}

// TestValidatePKColumnRejectsNonPKWhere pins the #296 fix: the shim
// must NOT silently match a WHERE on a non-PK column against
// binlog_events.pk_values (which would return a row whose actual PK
// happens to equal the user's filter value — a correctness bug).
// Validation is wired in HandleQuery before dispatch so every parsed
// shape (TypeFlashback, TypeSnapshot, TypeDiff, hint-comment) is
// covered by one code path.
//
// The subtests below assert one behaviour each:
//
//   - accept: WHERE on the declared PK column passes through and
//     reaches the runX dispatch. We assert "no 1064" without driving
//     the full fetch path (no sqlmock binlog_events expectation) —
//     the test would otherwise reproduce the entire FetchMerged
//     plumbing for no extra signal.
//   - reject single-column PK mismatch / composite PK / no PK
//     declared: each is a distinct 1064 with a message the operator
//     can act on.
//   - reject on missing snapshot / unresolved table / loader failure
//     (#821): the shim can't confirm the WHERE column is the PK, so a
//     column-qualified WHERE fails loud rather than silently answering
//     against pk_values and returning a different row. The no-WHERE
//     full-table path stays permissive.
//   - hint-comment + _snapshot + _diff: per-shape regression guards
//     so a future per-type refactor can't re-introduce the bug for
//     one shape in isolation.
func TestValidatePKColumnRejectsNonPKWhere(t *testing.T) {
	// ordersResolver is a minimal one-table snapshot with id as the
	// single-column PK. PKColumns is the slice the validator consults;
	// Columns is along for the ride so columnOrderFor (called on the
	// runX dispatch path the accept subtest exercises) returns a
	// non-nil order.
	ordersResolver := func() (*metadata.Resolver, error) {
		return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
			"myapp.orders": {
				Schema: "myapp", Table: "orders",
				Columns: []metadata.ColumnMeta{
					{Name: "id", OrdinalPosition: 1, IsPK: true},
					{Name: "customer_id", OrdinalPosition: 2},
					{Name: "status", OrdinalPosition: 3},
				},
				PKColumns: []string{"id"},
			},
		}), nil
	}

	silent := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("accept_pk_column_passes_validation", func(t *testing.T) {
		// Drive the validator alone (not HandleQuery → runX) so we
		// don't have to mock the entire FetchMerged plumbing just to
		// prove the accept branch. A nil error is the full assertion.
		h := &Handler{logger: silent, resolverFn: ordersResolver}
		err := h.validatePKColumn(TimeTravelQuery{
			Type: TypeFlashback, Schema: "myapp", Table: "orders",
			PKColumn: "id", PKValue: "42",
		})
		if err != nil {
			t.Errorf("WHERE on PK column must pass validation; got %v", err)
		}
	})

	t.Run("reject_non_pk_column_returns_1064_with_actionable_message", func(t *testing.T) {
		// This is the literal repro from issue #296: a query that
		// would silently return the row with id=1 because the shim
		// joined the literal 1 against pk_values.
		h := NewHandlerWithConfig(nil, Config{}, silent)
		h.resolverFn = ordersResolver
		h.UseDB("myapp")

		_, err := h.HandleQuery(
			"SELECT * FROM _flashback.orders AS OF '2026-05-23 18:20:13' WHERE customer_id=1",
		)
		if err == nil {
			t.Fatal("expected 1064 for WHERE on non-PK column; got nil")
		}
		var myErr *gomysql.MyError
		if !errors.As(err, &myErr) {
			t.Fatalf("expected *mysql.MyError so wire code is typed, got %T: %v", err, err)
		}
		if myErr.Code != gomysql.ER_PARSE_ERROR {
			t.Errorf("wire code = %d, want %d (ER_PARSE_ERROR)", myErr.Code, gomysql.ER_PARSE_ERROR)
		}
		// The message must name the expected PK and the user-supplied
		// column so the operator can fix the query without reading
		// shim source. A bare "wrong column" would be technically
		// correct but useless in production.
		for _, want := range []string{"customer_id", "id", "primary key", "myapp.orders"} {
			if !strings.Contains(myErr.Message, want) {
				t.Errorf("error message should contain %q for actionability; got %q", want, myErr.Message)
			}
		}
	})

	t.Run("reject_composite_pk_naming_all_pk_columns", func(t *testing.T) {
		compositeResolver := func() (*metadata.Resolver, error) {
			return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
				"myapp.order_items": {
					Schema: "myapp", Table: "order_items",
					Columns: []metadata.ColumnMeta{
						{Name: "order_id", OrdinalPosition: 1, IsPK: true},
						{Name: "line_no", OrdinalPosition: 2, IsPK: true},
					},
					PKColumns: []string{"order_id", "line_no"},
				},
			}), nil
		}
		h := NewHandlerWithConfig(nil, Config{}, silent)
		h.resolverFn = compositeResolver
		h.UseDB("myapp")

		_, err := h.HandleQuery(
			"SELECT * FROM _flashback.order_items AS OF '2026-05-23 18:20:13' WHERE order_id=1",
		)
		if err == nil {
			t.Fatal("expected 1064 for composite PK; got nil")
		}
		var myErr *gomysql.MyError
		if !errors.As(err, &myErr) || myErr.Code != gomysql.ER_PARSE_ERROR {
			t.Fatalf("expected ER_PARSE_ERROR, got %v", err)
		}
		// Must name BOTH composite PK columns so the operator
		// understands the table's actual shape (not just "you used
		// the wrong column").
		for _, want := range []string{"composite", "order_id", "line_no", "single-column"} {
			if !strings.Contains(myErr.Message, want) {
				t.Errorf("composite PK error should contain %q; got %q", want, myErr.Message)
			}
		}
	})

	t.Run("reject_table_with_no_pk_declared", func(t *testing.T) {
		// validateTables would normally reject this at snapshot time,
		// but a snapshot rolled back from a stricter version or a
		// hand-edited schema_snapshots row can still surface it.
		// Rejecting is safer than answering against pk_values with no
		// PK definition (the join semantics are undefined).
		noPKResolver := func() (*metadata.Resolver, error) {
			return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
				"myapp.events": {
					Schema: "myapp", Table: "events",
					Columns: []metadata.ColumnMeta{
						{Name: "ts", OrdinalPosition: 1},
						{Name: "payload", OrdinalPosition: 2},
					},
					PKColumns: nil,
				},
			}), nil
		}
		h := NewHandlerWithConfig(nil, Config{}, silent)
		h.resolverFn = noPKResolver
		h.UseDB("myapp")

		_, err := h.HandleQuery(
			"SELECT * FROM _flashback.events AS OF '2026-05-23 18:20:13' WHERE ts='2026-05-23'",
		)
		if err == nil {
			t.Fatal("expected 1064 for PK-less table; got nil")
		}
		var myErr *gomysql.MyError
		if !errors.As(err, &myErr) || myErr.Code != gomysql.ER_PARSE_ERROR {
			t.Fatalf("expected ER_PARSE_ERROR, got %v", err)
		}
		if !strings.Contains(myErr.Message, "no primary key") {
			t.Errorf("PK-less error should say 'no primary key'; got %q", myErr.Message)
		}
	})

	t.Run("reject_when_table_missing_from_snapshot", func(t *testing.T) {
		// #821: a table created after the latest snapshot can't have
		// its PK verified. A column-qualified WHERE must fail loud —
		// permitting it would join the literal against pk_values and
		// return a DIFFERENT row (re-opening #296 in the no-snapshot
		// window). Re-snapshot converges; the no-WHERE full-table path
		// (asserted below) still works meanwhile.
		emptyResolver := func() (*metadata.Resolver, error) {
			return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{}), nil
		}
		h := &Handler{logger: silent, resolverFn: emptyResolver}
		err := h.validatePKColumn(TimeTravelQuery{
			Type: TypeFlashback, Schema: "myapp", Table: "brand_new_table",
			PKColumn: "customer_id", PKValue: "1",
		})
		if err == nil {
			t.Fatal("unresolved table + column WHERE must reject (documented guarantee); got nil")
		}
		var myErr *gomysql.MyError
		if !errors.As(err, &myErr) || myErr.Code != gomysql.ER_PARSE_ERROR {
			t.Fatalf("expected ER_PARSE_ERROR, got %v", err)
		}
		for _, want := range []string{"customer_id", "myapp.brand_new_table", "snapshot"} {
			if !strings.Contains(myErr.Message, want) {
				t.Errorf("error should contain %q for actionability; got %q", want, myErr.Message)
			}
		}
	})

	t.Run("allow_full_table_when_table_missing_from_snapshot", func(t *testing.T) {
		// The no-WHERE full-table AS OF path must stay permissive even
		// for an unresolved table: it never joins a literal against
		// pk_values, so there's nothing to get wrong (#821).
		emptyResolver := func() (*metadata.Resolver, error) {
			return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{}), nil
		}
		h := &Handler{logger: silent, resolverFn: emptyResolver}
		err := h.validatePKColumn(TimeTravelQuery{
			Type: TypeFlashback, Schema: "myapp", Table: "brand_new_table",
			PKColumn: "", PKValue: "",
		})
		if err != nil {
			t.Errorf("full-table shape (no WHERE) must pass even when unresolved; got %v", err)
		}
	})

	t.Run("reject_when_resolver_load_fails", func(t *testing.T) {
		// #821: a resolver blip can't confirm the PK either. A
		// column-qualified WHERE fails loud rather than risk the
		// wrong-row answer.
		failingResolver := func() (*metadata.Resolver, error) {
			return nil, errors.New("transient db blip")
		}
		h := &Handler{logger: silent, resolverFn: failingResolver}
		err := h.validatePKColumn(TimeTravelQuery{
			Type: TypeFlashback, Schema: "myapp", Table: "orders",
			PKColumn: "customer_id", PKValue: "1",
		})
		if err == nil {
			t.Fatal("resolver-load failure + column WHERE must reject; got nil")
		}
		var myErr *gomysql.MyError
		if !errors.As(err, &myErr) || myErr.Code != gomysql.ER_PARSE_ERROR {
			t.Fatalf("expected ER_PARSE_ERROR, got %v", err)
		}
		if !strings.Contains(myErr.Message, "customer_id") {
			t.Errorf("error should name the user-supplied column; got %q", myErr.Message)
		}
	})

	t.Run("reject_when_no_snapshots_yet", func(t *testing.T) {
		// ErrNoSnapshots (fresh install) also can't verify the PK, so
		// a column-qualified WHERE rejects with a distinct reason.
		noSnapResolver := func() (*metadata.Resolver, error) {
			return nil, metadata.ErrNoSnapshots
		}
		h := &Handler{logger: silent, resolverFn: noSnapResolver}
		err := h.validatePKColumn(TimeTravelQuery{
			Type: TypeFlashback, Schema: "myapp", Table: "orders",
			PKColumn: "customer_id", PKValue: "1",
		})
		if err == nil {
			t.Fatal("no-snapshots + column WHERE must reject; got nil")
		}
		var myErr *gomysql.MyError
		if !errors.As(err, &myErr) || myErr.Code != gomysql.ER_PARSE_ERROR {
			t.Fatalf("expected ER_PARSE_ERROR, got %v", err)
		}
	})

	t.Run("allow_full_table_when_resolver_load_fails", func(t *testing.T) {
		// Full-table path returns before the resolver is consulted, so
		// a resolver blip can't break it.
		failingResolver := func() (*metadata.Resolver, error) {
			return nil, errors.New("transient db blip")
		}
		h := &Handler{logger: silent, resolverFn: failingResolver}
		err := h.validatePKColumn(TimeTravelQuery{
			Type: TypeFlashback, Schema: "myapp", Table: "orders",
			PKColumn: "", PKValue: "",
		})
		if err != nil {
			t.Errorf("full-table shape must pass despite resolver failure; got %v", err)
		}
	})

	t.Run("permissive_when_no_pk_column_in_query", func(t *testing.T) {
		// Full-table reconstruction path (#276). Nothing to validate.
		h := &Handler{logger: silent, resolverFn: ordersResolver}
		err := h.validatePKColumn(TimeTravelQuery{
			Type: TypeFlashback, Schema: "myapp", Table: "orders",
			PKColumn: "", PKValue: "",
		})
		if err != nil {
			t.Errorf("full-table shape (no WHERE) must pass validation; got %v", err)
		}
	})

	t.Run("permissive_when_resolverFn_nil", func(t *testing.T) {
		// Bare &Handler{} (some legacy tests do this). Mirrors
		// columnOrderFor's nil-check at the top.
		h := &Handler{logger: silent}
		err := h.validatePKColumn(TimeTravelQuery{
			Type: TypeFlashback, Schema: "myapp", Table: "orders",
			PKColumn: "anything", PKValue: "1",
		})
		if err != nil {
			t.Errorf("nil resolverFn must NOT panic or reject; got %v", err)
		}
	})

	t.Run("validates_snapshot_shape", func(t *testing.T) {
		h := NewHandlerWithConfig(nil, Config{}, silent)
		h.resolverFn = ordersResolver
		h.UseDB("myapp")

		_, err := h.HandleQuery(
			"SELECT * FROM _snapshot.orders AS OF '2026-05-23 18:20:13' WHERE customer_id=1",
		)
		if err == nil {
			t.Fatal("_snapshot shape: expected 1064; got nil")
		}
		var myErr *gomysql.MyError
		if !errors.As(err, &myErr) || myErr.Code != gomysql.ER_PARSE_ERROR {
			t.Errorf("_snapshot shape: expected ER_PARSE_ERROR, got %v", err)
		}
	})

	t.Run("validates_diff_shape", func(t *testing.T) {
		h := NewHandlerWithConfig(nil, Config{}, silent)
		h.resolverFn = ordersResolver
		h.UseDB("myapp")

		_, err := h.HandleQuery(
			"SELECT * FROM _diff.orders BETWEEN '2026-05-23 18:00:00' AND '2026-05-23 19:00:00' WHERE customer_id=1",
		)
		if err == nil {
			t.Fatal("_diff shape: expected 1064; got nil")
		}
		var myErr *gomysql.MyError
		if !errors.As(err, &myErr) || myErr.Code != gomysql.ER_PARSE_ERROR {
			t.Errorf("_diff shape: expected ER_PARSE_ERROR, got %v", err)
		}
	})

	t.Run("validates_hint_comment_shape", func(t *testing.T) {
		// The hint-comment form (#288) normalises into TypeFlashback
		// but the validator must still see q.PKColumn from the hint
		// regex's capture group 5. A future refactor that splits hint
		// parsing into its own runX would re-introduce the bug for
		// this shape alone — this is the regression guard.
		h := NewHandlerWithConfig(nil, Config{}, silent)
		h.resolverFn = ordersResolver
		h.UseDB("myapp")

		_, err := h.HandleQuery(
			"SELECT /*+ DBTRAIL_AT='2026-05-23 18:20:13' */ * FROM orders WHERE customer_id=1",
		)
		if err == nil {
			t.Fatal("hint-comment shape: expected 1064; got nil")
		}
		var myErr *gomysql.MyError
		if !errors.As(err, &myErr) || myErr.Code != gomysql.ER_PARSE_ERROR {
			t.Errorf("hint-comment shape: expected ER_PARSE_ERROR, got %v", err)
		}
	})
}

// Compile-time check: TenantAuth implements the authentication handler
// interface.
var _ server.AuthenticationHandler = TenantAuth{}

// Compile-time check: nil-safe constructor returns a real Handler.
var _ = func() *Handler {
	return NewHandler(nil, nil)
}

// Compile-time check: emptyResult always returns a resultset.
var _ = emptyResult().Resultset

// Suppress unused-import lint: gomysql is referenced only for the
// compile-time assertion below.
var _ = gomysql.Result{}

// ─── #315 SHOW TABLES FROM _flashback/_diff/_snapshot ─────────────────────────

// TestHandleShowTablesFromVirtual exercises the SHOW TABLES interceptor
// added for #315. Three properties:
//
//  1. Match: SHOW [FULL] TABLES FROM _flashback/_diff/_snapshot returns
//     a resultset of the snapshot's tables in the current DB (sorted).
//  2. Empty currentDB: ER_NO_DB_ERROR with the friendly message pointing
//     at USE <db>.
//  3. ErrNoSnapshots is benign — returns empty resultset, not error,
//     matching MySQL behaviour against a fresh DB with no tables.
func TestHandleShowTablesFromVirtual(t *testing.T) {
	resolverFn := func() (*metadata.Resolver, error) {
		return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
			"appdb.orders":   {Schema: "appdb", Table: "orders"},
			"appdb.users":    {Schema: "appdb", Table: "users"},
			"appdb.products": {Schema: "appdb", Table: "products"},
			// Table in a different schema — must NOT appear in appdb's listing.
			"otherdb.audits": {Schema: "otherdb", Table: "audits"},
		}), nil
	}
	silentLogger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("happy_path_lists_tables_sorted", func(t *testing.T) {
		h := &Handler{logger: silentLogger, resolverFn: resolverFn}
		h.UseDB("appdb")

		res, err := h.HandleQuery("SHOW TABLES FROM _flashback")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if res == nil || res.Resultset == nil {
			t.Fatal("expected resultset, got nil")
		}
		if got := len(res.Resultset.RowDatas); got != 3 {
			t.Fatalf("row count = %d, want 3 (orders/products/users)", got)
		}
		// Column header should be Tables_in_<virtual>.
		if got := res.Resultset.Fields[0].Name; string(got) != "Tables_in__flashback" {
			t.Errorf("column name = %q, want Tables_in__flashback", got)
		}
		// We don't decode RowDatas bytes here — the row count + the
		// Tables() sort guarantee on the metadata side is sufficient
		// (see TestResolverTablesSchemaFilter).
	})

	t.Run("full_tables_keyword_accepted", func(t *testing.T) {
		h := &Handler{logger: silentLogger, resolverFn: resolverFn}
		h.UseDB("appdb")
		res, err := h.HandleQuery("SHOW FULL TABLES FROM _snapshot")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := len(res.Resultset.RowDatas); got != 3 {
			t.Errorf("row count = %d, want 3", got)
		}
		if got := res.Resultset.Fields[0].Name; string(got) != "Tables_in__snapshot" {
			t.Errorf("column name = %q, want Tables_in__snapshot", got)
		}
	})

	t.Run("show_tables_in_keyword_accepted", func(t *testing.T) {
		h := &Handler{logger: silentLogger, resolverFn: resolverFn}
		h.UseDB("appdb")
		res, err := h.HandleQuery("SHOW TABLES IN _diff")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := len(res.Resultset.RowDatas); got != 3 {
			t.Errorf("row count = %d, want 3", got)
		}
	})

	t.Run("backticked_virtual_accepted", func(t *testing.T) {
		h := &Handler{logger: silentLogger, resolverFn: resolverFn}
		h.UseDB("appdb")
		res, err := h.HandleQuery("SHOW TABLES FROM `_flashback`")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got := len(res.Resultset.RowDatas); got != 3 {
			t.Errorf("row count = %d, want 3", got)
		}
	})

	t.Run("no_current_db_friendly_error", func(t *testing.T) {
		h := &Handler{logger: silentLogger, resolverFn: resolverFn}
		_, err := h.HandleQuery("SHOW TABLES FROM _flashback")
		if err == nil {
			t.Fatal("expected error when no DB selected, got nil")
		}
		var myErr *gomysql.MyError
		if !errors.As(err, &myErr) || myErr.Code != gomysql.ER_NO_DB_ERROR {
			t.Errorf("expected ER_NO_DB_ERROR (1046), got %v", err)
		}
		if !strings.Contains(err.Error(), "USE") {
			t.Errorf("error must mention USE; got: %v", err)
		}
	})

	t.Run("no_snapshots_returns_empty_set", func(t *testing.T) {
		emptyResolver := func() (*metadata.Resolver, error) {
			return nil, metadata.ErrNoSnapshots
		}
		h := &Handler{logger: silentLogger, resolverFn: emptyResolver}
		h.UseDB("appdb")
		res, err := h.HandleQuery("SHOW TABLES FROM _flashback")
		if err != nil {
			t.Fatalf("expected empty set, not error: %v", err)
		}
		if res == nil || res.Resultset == nil {
			t.Fatal("expected resultset (empty), got nil")
		}
		if got := len(res.Resultset.RowDatas); got != 0 {
			t.Errorf("row count = %d, want 0", got)
		}
	})

	t.Run("show_tables_from_real_db_falls_through", func(t *testing.T) {
		h := &Handler{logger: silentLogger, resolverFn: resolverFn}
		h.UseDB("appdb")
		// `SHOW TABLES FROM appdb` is not one of our virtual schemas;
		// the SHOW interceptor must NOT fire and the query must fall
		// through to the default unsupported-query path.
		_, err := h.HandleQuery("SHOW TABLES FROM appdb")
		if err == nil {
			t.Fatal("expected ER_NOT_SUPPORTED_YET, got nil")
		}
		var myErr *gomysql.MyError
		if !errors.As(err, &myErr) {
			t.Errorf("expected typed MyError, got %v", err)
		}
		if myErr.Code == gomysql.ER_NO_DB_ERROR {
			t.Errorf("interceptor fired on real DB (ER_NO_DB_ERROR returned); must fall through to existing path")
		}
	})
}

// ─── #313 column projection ───────────────────────────────────────────────────

// TestFullTableResultProjection verifies the branch point #313 added and #600
// re-confirmed: a non-nil q.Columns projects the user's columns VERBATIM (no
// image-only keys appended), while a nil q.Columns (SELECT *) takes the
// snapshot order and UNIONs image-only keys. The explicit-projection subtest
// is the regression guard for #600: when imagesToResult began unioning extras,
// the shared full-table path would have silently widened a user's `SELECT
// id, name` to also include image keys they never asked for — fullTableResult
// routes explicit projections to imagesToResultVerbatim to prevent that.
func TestFullTableResultProjection(t *testing.T) {
	resolverFn := func() (*metadata.Resolver, error) {
		return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
			"appdb.users": {
				Schema: "appdb", Table: "users",
				Columns: []metadata.ColumnMeta{
					{Name: "id", OrdinalPosition: 1},
					{Name: "email", OrdinalPosition: 2},
					{Name: "name", OrdinalPosition: 3},
					{Name: "created_at", OrdinalPosition: 4},
				},
			},
		}), nil
	}
	h := &Handler{
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		resolverFn: resolverFn,
	}
	// Image carries every snapshot column PLUS an off-snapshot key (legacy_col)
	// — the since-dropped-column shape from #600.
	images := []map[string]any{
		{"id": 1, "email": "a@x", "name": "ann", "created_at": "t0", "legacy_col": "L"},
	}
	cols := func(res *gomysql.Result) []string {
		got := make([]string, len(res.Resultset.Fields))
		for i, f := range res.Resultset.Fields {
			got[i] = string(f.Name)
		}
		return got
	}

	t.Run("select_star_unions_image_only_keys", func(t *testing.T) {
		res, err := h.fullTableResult(TimeTravelQuery{Schema: "appdb", Table: "users"}, images)
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"id", "email", "name", "created_at", "legacy_col"}
		if got := cols(res); !slices.Equal(got, want) {
			t.Errorf("SELECT * cols = %v, want %v (snapshot order + appended off-snapshot key)", got, want)
		}
	})

	t.Run("explicit_projection_is_verbatim_no_extras", func(t *testing.T) {
		q := TimeTravelQuery{Schema: "appdb", Table: "users", Columns: []string{"id", "name"}}
		res, err := h.fullTableResult(q, images)
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"id", "name"}
		if got := cols(res); !slices.Equal(got, want) {
			t.Errorf("explicit projection cols = %v, want %v "+
				"(must NOT append email/created_at/legacy_col — user asked for exactly these)", got, want)
		}
	})

	t.Run("explicit_columns_can_include_unknown_as_null", func(t *testing.T) {
		// A listed column absent from the image stays in the projection as NULL.
		q := TimeTravelQuery{Schema: "appdb", Table: "users", Columns: []string{"id", "deleted_column"}}
		res, err := h.fullTableResult(q, images)
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"id", "deleted_column"}
		if got := cols(res); !slices.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}

// TestImageToResultVerbatim pins the single-row user-projection wire
// contract added for #313. The bug this guards against: routing
// q.Columns through imageToResult → orderColumns silently DROPS
// missing-from-image columns and APPENDS image-only keys alphabetically.
// The new imageToResultVerbatim path bypasses orderColumns so wire
// fields stay verbatim and missing keys → NULL.
//
// This is the end-to-end test that should have caught the runPointInTime
// regression. Three properties pinned:
//
//  1. Wire fields are exactly the user's cols, in the user's order.
//  2. Image-only keys (NOT in user's projection) do NOT appear.
//  3. User-listed columns missing from image are kept (NULL on wire).
func TestImageToResultVerbatim(t *testing.T) {
	t.Run("projects_only_listed_cols_in_listed_order", func(t *testing.T) {
		image := map[string]any{
			"id":         1,
			"name":       "alice",
			"email":      "a@b.com",    // image has email, user did NOT ask
			"created_at": "2026-05-02", // image has created_at, user did NOT ask
		}
		res, err := imageToResultVerbatim(image, []string{"id", "name"})
		if err != nil {
			t.Fatalf("imageToResultVerbatim: %v", err)
		}
		// Wire fields are exactly [id, name]. Verifies orderColumns'
		// "append extras alphabetically" misbehaviour is bypassed.
		if got := len(res.Resultset.Fields); got != 2 {
			t.Fatalf("field count = %d, want 2 (id, name only)", got)
		}
		if got := string(res.Resultset.Fields[0].Name); got != "id" {
			t.Errorf("fields[0] = %q, want id", got)
		}
		if got := string(res.Resultset.Fields[1].Name); got != "name" {
			t.Errorf("fields[1] = %q, want name", got)
		}
	})

	t.Run("missing_from_image_stays_in_projection_as_null", func(t *testing.T) {
		image := map[string]any{
			"id":   1,
			"name": "alice",
		}
		res, err := imageToResultVerbatim(image, []string{"id", "deleted_column"})
		if err != nil {
			t.Fatalf("imageToResultVerbatim: %v", err)
		}
		// Wire fields keep [id, deleted_column] verbatim. Verifies
		// orderColumns' "drop missing" misbehaviour is bypassed.
		if got := len(res.Resultset.Fields); got != 2 {
			t.Fatalf("field count = %d, want 2 (id + missing deleted_column)", got)
		}
		if got := string(res.Resultset.Fields[1].Name); got != "deleted_column" {
			t.Errorf("fields[1] = %q, want deleted_column (kept; value NULL on wire)", got)
		}
	})

	t.Run("empty_projection_emits_zero_columns", func(t *testing.T) {
		// Defensive: today the parser invariant guarantees non-empty
		// cols when Columns != nil, but if a future caller passes
		// []string{}, the wire row should be valid (zero columns) and
		// not panic. BuildSimpleTextResultset on empty cols is well-defined.
		_, err := imageToResultVerbatim(map[string]any{"id": 1}, []string{})
		if err != nil {
			t.Errorf("imageToResultVerbatim with empty cols: %v", err)
		}
	})
}

// TestMapEventImagesFallback covers mapEventImages' degradation path in
// a handler with no DB (epochs unavailable): rows must still map via
// the latest-snapshot fallback resolver (#475's pre-existing behavior),
// and a handler without a resolverFn must leave images untouched.
func TestMapEventImagesFallback(t *testing.T) {
	h := &Handler{
		logger: slog.Default(),
		resolverFn: func() (*metadata.Resolver, error) {
			return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
				"myapp.orders": {Schema: "myapp", Table: "orders", Columns: []metadata.ColumnMeta{
					{Name: "id", ColumnType: "int", IsPK: true},
					{Name: "status", ColumnType: "enum('pending','processing','shipped')"},
				}},
			}), nil
		},
	}
	rows := []query.ResultRow{{
		EventTimestamp: time.Now(),
		RowBefore:      map[string]any{"id": float64(1), "status": float64(1)},
		RowAfter:       map[string]any{"id": float64(1), "status": float64(3)},
	}}
	h.mapEventImages("myapp", "orders", rows)
	if rows[0].RowBefore["status"] != "pending" || rows[0].RowAfter["status"] != "shipped" {
		t.Errorf("fallback mapping failed: before=%v after=%v",
			rows[0].RowBefore["status"], rows[0].RowAfter["status"])
	}

	// No resolverFn → untouched ordinals (bare test handlers).
	bare := &Handler{logger: slog.Default()}
	rows2 := []query.ResultRow{{
		EventTimestamp: time.Now(),
		RowAfter:       map[string]any{"status": float64(3)},
	}}
	bare.mapEventImages("myapp", "orders", rows2)
	if rows2[0].RowAfter["status"] != float64(3) {
		t.Errorf("bare handler must pass through, got %v", rows2[0].RowAfter["status"])
	}
}

// TestMapEventImagesDecodesBlobText is the core unit proof for #661: the
// storage-side base64 of BLOB/TEXT event values is decoded back to raw bytes /
// strings in BOTH row images, in place, before emission — while non-BLOB/TEXT
// columns, NULLs, and columns absent from an image are left untouched. Because
// mapEventImages is the single chokepoint every event-sourced path traverses
// before emit/merge (and never sees a baseline row), decoding here gives every
// event-sourced emit path the fix with provenance-correctness for free.
func TestMapEventImagesDecodesBlobText(t *testing.T) {
	silent := slog.New(slog.NewTextHandler(io.Discard, nil))
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	h := &Handler{
		logger: silent,
		resolverFn: func() (*metadata.Resolver, error) {
			return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
				"appdb.docs": {Schema: "appdb", Table: "docs", Columns: []metadata.ColumnMeta{
					{Name: "id", OrdinalPosition: 1, DataType: "int", IsPK: true},
					{Name: "body", OrdinalPosition: 2, DataType: "text"},
					{Name: "payload", OrdinalPosition: 3, DataType: "blob"},
					{Name: "location", OrdinalPosition: 4, DataType: "point"},
					{Name: "embedding", OrdinalPosition: 5, DataType: "vector"},
					{Name: "u", OrdinalPosition: 6, DataType: "uuid"},
					{Name: "ip4", OrdinalPosition: 7, DataType: "inet4"},
					{Name: "ip6", OrdinalPosition: 8, DataType: "inet6"},
				}},
			}), nil
		},
	}
	rawBlob := "\x00\xff\x7f\x80" // arbitrary non-UTF-8 bytes must survive
	// MySQL's internal geometry form: 4-byte little-endian SRID + WKB.
	rawGeom := "\x02\x00\x00\x00\x01\x01\x00\x00\x00\xde\xad\xbe\xef"
	rawVec := "\x00\x00\x80\x3f\x00\x00\x00\x40" // packed float32 LE [1,2]
	rows := []query.ResultRow{{
		SchemaName:     "appdb",
		TableName:      "docs",
		EventTimestamp: time.Unix(1_700_000_000, 0).UTC(),
		RowBefore: map[string]any{
			"id": json.Number("1"), "body": b64("old bio"), "payload": b64("\x01\x02"),
		},
		RowAfter: map[string]any{
			"id": json.Number("1"), "body": b64("hello world"), "payload": b64(rawBlob),
			"location": b64(rawGeom), "embedding": b64(rawVec),
			"u":   b64("\x12\x3e\x45\x67\xe8\x9b\x12\xd3\xa4\x56\x42\x66\x14\x17\x40\x00"),
			"ip4": b64("\x0a\x00\x00\x00"),
			"ip6": "\n", // captured before #1944: no rendering exists, left as it is
		},
	}}
	h.mapEventImages("appdb", "docs", rows)

	// TEXT family → decoded Go string.
	if got := rows[0].RowAfter["body"]; got != "hello world" {
		t.Errorf("RowAfter body = %#v, want decoded string %q", got, "hello world")
	}
	if got := rows[0].RowBefore["body"]; got != "old bio" {
		t.Errorf("RowBefore body = %#v, want decoded string %q (both images decode, for _diff)", got, "old bio")
	}
	// BLOB family → decoded raw []byte, arbitrary bytes intact.
	if got, ok := rows[0].RowAfter["payload"].([]byte); !ok || string(got) != rawBlob {
		t.Errorf("RowAfter payload = %#v, want decoded []byte %q", rows[0].RowAfter["payload"], rawBlob)
	}
	// Spatial family (#1144) → decoded raw []byte (SRID+WKB), what a real
	// server serves over the wire — not the stored base64 text.
	if got, ok := rows[0].RowAfter["location"].([]byte); !ok || string(got) != rawGeom {
		t.Errorf("RowAfter location = %#v, want decoded []byte %q (raw SRID+WKB)", rows[0].RowAfter["location"], rawGeom)
	}
	// VECTOR → decoded raw []byte (packed floats), what a real server serves.
	// The baseline stores VECTOR as bytes too now, so _snapshot's merged rows
	// carry one representation.
	if got, ok := rows[0].RowAfter["embedding"].([]byte); !ok || string(got) != rawVec {
		t.Errorf("RowAfter embedding = %#v, want decoded []byte %q", rows[0].RowAfter["embedding"], rawVec)
	}
	// MariaDB UUID/INET → the server's text, which is what a SELECT returns
	// and what the baseline holds.
	for col, want := range map[string]any{"u": "123e4567-e89b-12d3-a456-426614174000", "ip4": "10.0.0.0", "ip6": "\n"} {
		if got := rows[0].RowAfter[col]; got != want {
			t.Errorf("RowAfter %s = %#v, want %#v", col, got, want)
		}
	}
	// Non-BLOB/TEXT column untouched.
	if got := rows[0].RowAfter["id"]; got != json.Number("1") {
		t.Errorf("RowAfter id = %#v, want untouched json.Number(\"1\")", got)
	}
}

// TestMapEventImagesDecodeEdgeCases pins the defensive branches of the #661
// decode so a future refactor can't silently regress them: NULL values, a
// value that is not a decodable base64 string, and a column absent from the
// image must all be left as-is (no panic, no corruption).
func TestMapEventImagesDecodeEdgeCases(t *testing.T) {
	silent := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := &Handler{
		logger: silent,
		resolverFn: func() (*metadata.Resolver, error) {
			return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
				"appdb.docs": {Schema: "appdb", Table: "docs", Columns: []metadata.ColumnMeta{
					{Name: "id", OrdinalPosition: 1, DataType: "int", IsPK: true},
					{Name: "body", OrdinalPosition: 2, DataType: "text"},
					{Name: "payload", OrdinalPosition: 3, DataType: "blob"},
				}},
			}), nil
		},
	}
	rows := []query.ResultRow{{
		SchemaName:     "appdb",
		TableName:      "docs",
		EventTimestamp: time.Unix(1_700_000_000, 0).UTC(),
		// body is NULL; payload key absent entirely (partial image).
		RowAfter: map[string]any{"id": json.Number("1"), "body": nil},
	}}
	h.mapEventImages("appdb", "docs", rows)
	if got, ok := rows[0].RowAfter["body"]; !ok || got != nil {
		t.Errorf("NULL body = %#v, want nil untouched", got)
	}
	if _, present := rows[0].RowAfter["payload"]; present {
		t.Errorf("absent payload column must not be materialized, got %#v", rows[0].RowAfter["payload"])
	}

	// A TEXT value that is not valid base64 is returned unchanged (the decode
	// is best-effort — DecodeString errors fall through).
	if got := decodeStoredBase64("not base64!!", false); got != "not base64!!" {
		t.Errorf("non-base64 string = %#v, want unchanged", got)
	}
	if got := decodeStoredBase64(nil, true); got != nil {
		t.Errorf("nil value = %#v, want nil", got)
	}
}

// TestBase64StoredKind and TestBase64Cols pin the pure type predicates the
// #661 decode is gated on.
func TestBase64StoredKind(t *testing.T) {
	binaryFamily := []string{
		"blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary",
		// Spatial family decodes to raw SRID+WKB bytes since #1144, mirroring
		// internal/reconstruct (#1136/#1143).
		"geometry", "point", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon",
		"geometrycollection", "geomcollection",
		"vector",
	}
	textFamily := []string{"text", "tinytext", "mediumtext", "longtext"}
	for _, dt := range binaryFamily {
		if binary, ok := base64StoredKind(dt); !ok || !binary {
			t.Errorf("base64StoredKind(%q) = (%v,%v), want (true,true)", dt, binary, ok)
		}
	}
	for _, dt := range textFamily {
		if binary, ok := base64StoredKind(dt); !ok || binary {
			t.Errorf("base64StoredKind(%q) = (%v,%v), want (false,true)", dt, binary, ok)
		}
	}
	// Case-insensitive, and unrelated types are not decoded. MariaDB's
	// UUID/INET are rendered as text by a separate pass, not decoded here.
	if binary, ok := base64StoredKind("LONGTEXT"); !ok || binary {
		t.Errorf("base64StoredKind is not case-insensitive: got (%v,%v)", binary, ok)
	}
	for _, dt := range []string{"int", "varchar", "datetime", "uuid", ""} {
		if _, ok := base64StoredKind(dt); ok {
			t.Errorf("base64StoredKind(%q) reported a decodable column, want none", dt)
		}
	}
	// "json" IS decodable (#736): marshalRow only embeds raw JSON for
	// object/array payloads now, so a JSON column holding a bare scalar
	// top-level value (rare, but legal) takes this same base64 path.
	if binary, ok := base64StoredKind("json"); !ok || binary {
		t.Errorf("base64StoredKind(%q) = (%v,%v), want (false,true)", "json", binary, ok)
	}
}

func TestDecodeStoredBase64_boolAndNumberRepaired(t *testing.T) {
	// #736: a TEXT/BLOB value mis-promoted to a JSON scalar by the pre-fix
	// marshalRow (e.g. the literal string "false" or "123") must be restored
	// to its original textual literal, not left as a stray Go bool/number. A
	// value that decoded to nil (originally "null") stays nil — unrecoverable,
	// indistinguishable from a genuine SQL NULL.
	if got := decodeStoredBase64(false, false); got != "false" {
		t.Errorf("bool text: got %v (%T), want %q", got, got, "false")
	}
	if got := decodeStoredBase64(true, true); string(got.([]byte)) != "true" {
		t.Errorf("bool binary: got %v, want []byte(true)", got)
	}
	if got := decodeStoredBase64(json.Number("123"), false); got != "123" {
		t.Errorf("number text: got %v (%T), want %q", got, got, "123")
	}
	if got := decodeStoredBase64(nil, false); got != nil {
		t.Errorf("nil must pass through unchanged, got %v", got)
	}
}

func TestDecodeStoredBase64_jsonContainerUnaffected(t *testing.T) {
	// #736 added "json" to base64StoredKind, so decodeStoredBase64 is now
	// invoked for JSON-typed columns too — a genuine JSON object/array value
	// (the overwhelmingly common case) must pass through completely
	// unchanged, not just fall into some accidental no-op.
	obj := map[string]any{"a": json.Number("1")}
	if got := decodeStoredBase64(obj, false); fmt.Sprintf("%v", got) != fmt.Sprintf("%v", obj) {
		t.Errorf("expected map[string]any to pass through unchanged, got %v (%T)", got, got)
	}
	arr := []any{json.Number("1"), json.Number("2")}
	if got := decodeStoredBase64(arr, false); fmt.Sprintf("%v", got) != fmt.Sprintf("%v", arr) {
		t.Errorf("expected []any to pass through unchanged, got %v (%T)", got, got)
	}
}

func TestBase64Cols(t *testing.T) {
	// Nil resolver → nil map (no schema = no safe typing, preserves pre-fix base64).
	if got := base64Cols(nil, "appdb", "docs"); got != nil {
		t.Errorf("base64Cols(nil) = %v, want nil", got)
	}
	r := metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
		"appdb.docs": {Schema: "appdb", Table: "docs", Columns: []metadata.ColumnMeta{
			{Name: "id", DataType: "int"},
			{Name: "body", DataType: "text"},
			{Name: "payload", DataType: "blob"},
		}},
	})
	got := base64Cols(r, "appdb", "docs")
	want := map[string]bool{"body": false, "payload": true}
	if len(got) != len(want) || got["body"] != false || got["payload"] != true {
		t.Errorf("base64Cols = %v, want %v", got, want)
	}
	// Unknown table → nil (not a panic).
	if got := base64Cols(r, "appdb", "nope"); got != nil {
		t.Errorf("base64Cols(unknown table) = %v, want nil", got)
	}
}

// TestMapEventImagesDecodesExactlyOnce guards against a double-decode: every
// stored BLOB/TEXT value is base64, but its decoded bytes can THEMSELVES be
// valid base64. A real TEXT value of "SGVsbG8=" is stored as base64("SGVsbG8=");
// one decode yields "SGVsbG8=", a second would yield "Hello". The single decode
// in mapEventImages must return "SGVsbG8=" verbatim.
func TestMapEventImagesDecodesExactlyOnce(t *testing.T) {
	silent := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := &Handler{
		logger: silent,
		resolverFn: func() (*metadata.Resolver, error) {
			return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
				"appdb.docs": {Schema: "appdb", Table: "docs", Columns: []metadata.ColumnMeta{
					{Name: "id", OrdinalPosition: 1, DataType: "int", IsPK: true},
					{Name: "body", OrdinalPosition: 2, DataType: "text"},
				}},
			}), nil
		},
	}
	stored := base64.StdEncoding.EncodeToString([]byte("SGVsbG8=")) // a real value that is itself base64
	rows := []query.ResultRow{{
		SchemaName:     "appdb",
		TableName:      "docs",
		EventTimestamp: time.Unix(1_700_000_000, 0).UTC(),
		RowAfter:       map[string]any{"id": json.Number("1"), "body": stored},
	}}
	h.mapEventImages("appdb", "docs", rows)
	if got := rows[0].RowAfter["body"]; got != "SGVsbG8=" {
		t.Errorf("body = %#v, want \"SGVsbG8=\" (decoded exactly once, not twice → \"Hello\")", got)
	}
}

func TestMapEventImagesDecodesExactlyOnce_boolRepair(t *testing.T) {
	// #736's bool/json.Number repair produces plain text ("true"/"false") that
	// happens to be valid base64 alphabet — e.g. base64.StdEncoding.DecodeString
	// ("true") succeeds (into 3 unrelated bytes) where "false"/"0"/"123" would
	// fail and pass through unchanged. A single call must repair a historically
	// mis-promoted bool to its correct text; every call site is documented and
	// tested elsewhere to invoke this exactly once per event slice — a second
	// call on the same image would re-interpret "true" as base64 and corrupt it,
	// the same non-idempotency class the pre-existing base64-string case has.
	silent := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := &Handler{
		logger: silent,
		resolverFn: func() (*metadata.Resolver, error) {
			return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
				"appdb.docs": {Schema: "appdb", Table: "docs", Columns: []metadata.ColumnMeta{
					{Name: "id", OrdinalPosition: 1, DataType: "int", IsPK: true},
					{Name: "flag", OrdinalPosition: 2, DataType: "text"},
				}},
			}), nil
		},
	}
	rows := []query.ResultRow{{
		SchemaName:     "appdb",
		TableName:      "docs",
		EventTimestamp: time.Unix(1_700_000_000, 0).UTC(),
		RowAfter:       map[string]any{"id": json.Number("1"), "flag": true},
	}}
	h.mapEventImages("appdb", "docs", rows)
	if got := rows[0].RowAfter["flag"]; got != "true" {
		t.Errorf("flag = %#v, want \"true\" (repaired once, not corrupted by a second decode pass)", got)
	}
}

// TestRunSnapshotFullTable_BaselineConfiguredRefusesWireError pins #822: when a
// baseline source IS configured (the operator explicitly opted into full-table
// completeness) but the baseline merge cannot run, full-table _snapshot must
// FAIL LOUD with a wire error instead of silently degrading to the
// binlog-activity-only set — a partial table indistinguishable from a complete
// one. Two DB-free branches are exercised: no baseline at-or-before AsOf (a
// valid PK resolves but the configured dir holds no snapshot → FindBaseline
// returns ErrNoBaseline) and an unresolvable PK (the schema resolver errors).
// Neither branch reaches indexDB before returning, so a nil DB is fine.
func TestRunSnapshotFullTable_BaselineConfiguredRefusesWireError(t *testing.T) {
	silent := slog.New(slog.NewTextHandler(io.Discard, nil))
	usersResolver := func() (*metadata.Resolver, error) {
		return metadata.NewResolverFromTables(1, map[string]*metadata.TableMeta{
			"myapp.users": {Schema: "myapp", Table: "users", PKColumns: []string{"id"},
				Columns: []metadata.ColumnMeta{
					{Name: "id", OrdinalPosition: 1, DataType: "int", IsPK: true},
					{Name: "name", OrdinalPosition: 2, DataType: "varchar"},
				}},
		}), nil
	}
	cases := []struct {
		name       string
		resolverFn func() (*metadata.Resolver, error)
	}{
		{"no baseline at-or-before AsOf", usersResolver},
		{"unresolvable PK", func() (*metadata.Resolver, error) { return nil, errors.New("resolver unavailable") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &Handler{
				logger: silent,
				// A configured (but empty) baseline dir puts us past the
				// no-source binlog-only branch; the empty dir makes FindBaseline
				// return ErrNoBaseline for the valid-PK case.
				cfg:        Config{BaselineDir: t.TempDir()},
				resolverFn: tc.resolverFn,
			}
			_, err := h.runSnapshotFullTable(TimeTravelQuery{
				Type: TypeSnapshot, Schema: "myapp", Table: "users", AsOf: time.Now(),
			})
			if err == nil {
				t.Fatal("baseline configured but merge impossible: want a wire error, got nil (silent degrade)")
			}
			var myErr *gomysql.MyError
			if !errors.As(err, &myErr) {
				t.Fatalf("error = %T %v, want *mysql.MyError", err, err)
			}
			if myErr.Code != gomysql.ER_NO_PARTITION_FOR_GIVEN_VALUE {
				t.Errorf("wire code = %d, want %d (ER_NO_PARTITION_FOR_GIVEN_VALUE)", myErr.Code, gomysql.ER_NO_PARTITION_FOR_GIVEN_VALUE)
			}
		})
	}
}
