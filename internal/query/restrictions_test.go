package query

import (
	"errors"
	"strings"
	"testing"
)

// ─── Session-policy restrictions (#1449): allow-list mode ─────────────────────

func TestBuildQuery_allowTables(t *testing.T) {
	opts := Options{
		AllowTables: []SchemaTable{
			{Schema: "db1", Table: "t1"},
			{Schema: "db2", Table: "t2"},
		},
	}
	q, args := buildQuery(opts)

	// BINARY is load-bearing: the columns' collation is commonly
	// case-insensitive, and a case-insensitive ALLOW fails open (it would
	// also serve a distinct same-name-other-case table).
	want := "((BINARY schema_name = ? AND BINARY table_name = ?) OR (BINARY schema_name = ? AND BINARY table_name = ?))"
	if !strings.Contains(q, want) {
		t.Errorf("expected exact-match allow-list clause %q in query: %s", want, q)
	}
	if len(args) != 4 {
		t.Fatalf("expected 4 args for 2 allow tables, got %d: %v", len(args), args)
	}
	got := []any{args[0], args[1], args[2], args[3]}
	for i, w := range []any{"db1", "t1", "db2", "t2"} {
		if got[i] != w {
			t.Errorf("args[%d] = %v, want %v", i, got[i], w)
		}
	}
}

// TestBuildQuery_allowAndDenyCompose pins that deny composes over allow: a
// table both allowed and denied emits BOTH clauses, and their AND yields
// nothing for that table (the EE compiles a full-table deny this way rather
// than pre-subtracting, so the SQL itself must carry the deny-wins rule).
func TestBuildQuery_allowAndDenyCompose(t *testing.T) {
	opts := Options{
		AllowTables: []SchemaTable{{Schema: "db", Table: "t"}},
		DenyTables:  []SchemaTable{{Schema: "db", Table: "t"}},
	}
	q, _ := buildQuery(opts)
	if !strings.Contains(q, "(BINARY schema_name = ? AND BINARY table_name = ?)") {
		t.Errorf("expected allow clause in query: %s", q)
	}
	// Deny deliberately stays on the column collation (case-insensitive
	// withholds MORE, the safe direction) — no BINARY here.
	if !strings.Contains(q, "NOT (schema_name = ? AND table_name = ?)") {
		t.Errorf("expected collation-matched deny clause in query: %s", q)
	}
}

func TestBuildQuery_noAllowTablesNoClause(t *testing.T) {
	q, _ := buildQuery(Options{Schema: "db"})
	if strings.Contains(q, " OR (schema_name") {
		t.Errorf("no allow tables must emit no allow-list clause: %s", q)
	}
}

// ─── Session-policy restrictions (#1449): column allow list ───────────────────

func TestApplyRedaction_allowColumns(t *testing.T) {
	rows := []ResultRow{
		{
			SchemaName: "mydb",
			TableName:  "employees",
			RowBefore:  map[string]any{"id": float64(1), "name": "ann", "salary": float64(9)},
			RowAfter:   map[string]any{"id": float64(1), "name": "ann", "salary": float64(10)},
		},
		{
			SchemaName: "mydb",
			TableName:  "orders", // no allow entries: untouched by the allow rule
			RowAfter:   map[string]any{"amount": float64(5)},
		},
	}
	allow := []SchemaTableColumn{
		{Schema: "mydb", Table: "employees", Column: "id"},
		{Schema: "mydb", Table: "employees", Column: "name"},
	}
	applyRedaction(rows, nil, allow, false)

	if rows[0].RowBefore["salary"] != nil || rows[0].RowAfter["salary"] != nil {
		t.Errorf("column outside the allow list must be nulled, got before=%v after=%v",
			rows[0].RowBefore["salary"], rows[0].RowAfter["salary"])
	}
	if rows[0].RowBefore["id"] == nil || rows[0].RowAfter["name"] != "ann" {
		t.Errorf("allowed columns must be preserved, got %+v", rows[0])
	}
	if rows[1].RowAfter["amount"] != float64(5) {
		t.Errorf("a table with no allow entries must be untouched, got %v", rows[1].RowAfter["amount"])
	}
}

// TestApplyRedaction_redactWinsOverAllow pins the composition rule: an
// explicit redact entry nulls the column even when the allow list names it
// (deny wins, mirroring the table-level rule).
func TestApplyRedaction_redactWinsOverAllow(t *testing.T) {
	rows := []ResultRow{{
		SchemaName: "mydb",
		TableName:  "employees",
		RowAfter:   map[string]any{"name": "ann"},
	}}
	allow := []SchemaTableColumn{{Schema: "mydb", Table: "employees", Column: "name"}}
	redact := []SchemaTableColumn{{Schema: "mydb", Table: "employees", Column: "name"}}
	applyRedaction(rows, redact, allow, false)
	if rows[0].RowAfter["name"] != nil {
		t.Errorf("redact must win over allow, got %v", rows[0].RowAfter["name"])
	}
}

// ─── RedactionActive must see every restriction field ─────────────────────────

// TestRedactionActive_perField pins that EACH options field carrying a data
// restriction activates the redaction pass on its own. The fields feed
// Fetch's applyRedaction call AND ValidateStatementFilter's digest refusal —
// one forgotten here means row images served with query_text intact and the
// digest filter answering under a restriction.
func TestRedactionActive_perField(t *testing.T) {
	cases := []struct {
		name string
		opts Options
	}{
		{"ProfileActive", Options{ProfileActive: true}},
		{"DenyTables", Options{DenyTables: []SchemaTable{{Schema: "s", Table: "t"}}}},
		{"RedactColumns", Options{RedactColumns: []SchemaTableColumn{{Schema: "s", Table: "t", Column: "c"}}}},
		{"AllowTables", Options{AllowTables: []SchemaTable{{Schema: "s", Table: "t"}}}},
		{"AllowColumns", Options{AllowColumns: []SchemaTableColumn{{Schema: "s", Table: "t", Column: "c"}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.opts.RedactionActive() {
				t.Errorf("Options with %s set must report RedactionActive", tc.name)
			}
			tc.opts.QueryHash = strings.Repeat("a", 64)
			if err := tc.opts.ValidateStatementFilter(); !errors.Is(err, ErrQueryHashUnderProfile) {
				t.Errorf("digest filter under %s: err = %v, want ErrQueryHashUnderProfile", tc.name, err)
			}
		})
	}
	if (Options{}).RedactionActive() {
		t.Error("zero Options must not report RedactionActive")
	}
}

// TestChangedColumnFilterUnderColumnRules pins the #1449 sibling of the
// digest refusal: a changed-column filter under COLUMN-level rules is an
// existence oracle over exactly the hidden columns, so it is refused — while
// deny-table-only rules (no hidden column anywhere) keep the filter.
func TestChangedColumnFilterUnderColumnRules(t *testing.T) {
	base := Options{ChangedColumn: "ssn"}

	refused := []struct {
		name string
		opts Options
	}{
		{"RedactColumns", Options{ChangedColumn: "ssn", RedactColumns: []SchemaTableColumn{{Schema: "s", Table: "t", Column: "ssn"}}}},
		{"AllowColumns", Options{ChangedColumn: "ssn", AllowColumns: []SchemaTableColumn{{Schema: "s", Table: "t", Column: "id"}}}},
	}
	for _, tc := range refused {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.opts.ValidateStatementFilter(); !errors.Is(err, ErrChangedColumnUnderRedaction) {
				t.Errorf("err = %v, want ErrChangedColumnUnderRedaction", err)
			}
		})
	}
	allowedCases := []struct {
		name string
		opts Options
	}{
		{"no rules", base},
		{"deny tables only", Options{ChangedColumn: "ssn", DenyTables: []SchemaTable{{Schema: "s", Table: "t"}}}},
		{"named profile, zero rules", Options{ChangedColumn: "ssn", ProfileActive: true}},
		{"allow tables only", Options{ChangedColumn: "ssn", AllowTables: []SchemaTable{{Schema: "s", Table: "t"}}}},
	}
	for _, tc := range allowedCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.opts.ValidateStatementFilter(); err != nil {
				t.Errorf("err = %v, want nil (no column is hidden here)", err)
			}
		})
	}
}

// TestApplyRedaction_stripsHiddenChangedColumns pins that hidden column NAMES
// are removed from ChangedColumns: values are nulled by the pass, but the
// name list would otherwise enumerate the withheld schema on every UPDATE
// row. Untouched tables keep their list.
func TestApplyRedaction_stripsHiddenChangedColumns(t *testing.T) {
	rows := []ResultRow{
		{
			SchemaName:     "mydb",
			TableName:      "employees",
			ChangedColumns: []string{"name", "salary", "ssn"},
		},
		{
			SchemaName:     "mydb",
			TableName:      "orders",
			ChangedColumns: []string{"amount"},
		},
	}
	allow := []SchemaTableColumn{{Schema: "mydb", Table: "employees", Column: "name"}}
	redact := []SchemaTableColumn{{Schema: "mydb", Table: "employees", Column: "name"}}
	// name is allowed AND redacted: redact wins, so it is stripped too.
	applyRedaction(rows, redact, allow, false)

	if len(rows[0].ChangedColumns) != 0 {
		t.Errorf("hidden column names leaked through changed_columns: %v", rows[0].ChangedColumns)
	}
	if len(rows[1].ChangedColumns) != 1 || rows[1].ChangedColumns[0] != "amount" {
		t.Errorf("a table with no column rules must keep its changed_columns: %v", rows[1].ChangedColumns)
	}
}
