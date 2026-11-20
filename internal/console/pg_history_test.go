package console

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/query"
)

func pgMarkerRow() query.ResultRow {
	return query.ResultRow{EventID: 2, EventTimestamp: time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC),
		SchemaName: "public", TableName: "docs", EventType: event.EventUpdate, PKValues: "2",
		RowBefore: map[string]any{"id": "2", "title": map[string]any{event.NotCapturedKey: true}},
		RowAfter:  map[string]any{"id": "2", "title": "b", "body": map[string]any{event.UnchangedToastKey: true}}}
}

// The two PostgreSQL fields appear only on a PostgreSQL index, and only on a
// row that has them; a MySQL row, marker-shaped JSON and all, keeps exactly
// the allowlisted keys of TestEventDTOFieldAllowlist.
func TestEventDTOPostgresFields(t *testing.T) {
	keys := func(d eventDTO) []string {
		b, err := json.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		var out []string
		for k := range m {
			out = append(out, k)
		}
		slices.Sort(out)
		return out
	}
	pg := toEventDTO(pgMarkerRow(), true)
	if pg.BeforeImage != "key_only" || !slices.Equal(pg.UnchangedColumns, []string{"body"}) {
		t.Errorf("PostgreSQL DTO: before_image %q unchanged_columns %v", pg.BeforeImage, pg.UnchangedColumns)
	}
	my := keys(toEventDTO(pgMarkerRow(), false))
	if slices.Contains(my, "before_image") || slices.Contains(my, "unchanged_columns") {
		t.Errorf("MySQL DTO carries PostgreSQL fields: %v", my)
	}
	whole := query.ResultRow{EventType: event.EventUpdate, ChangedColumns: []string{"a"},
		RowBefore: map[string]any{"a": "1"}, RowAfter: map[string]any{"a": "2"}}
	if got, want := keys(toEventDTO(whole, true)), keys(toEventDTO(whole, false)); !slices.Equal(got, want) {
		t.Errorf("a whole PostgreSQL row gained fields: %v vs %v", got, want)
	}
	if got := keys(pg); !slices.Contains(got, "before_image") || !slices.Contains(got, "unchanged_columns") {
		t.Errorf("PostgreSQL DTO keys: %v", got)
	}
}

func TestPGHistoryWarnings(t *testing.T) {
	rows := []query.ResultRow{pgMarkerRow()}
	got := pgHistoryWarnings(query.Options{ChangedColumn: "title", PostgresHistory: true}, rows)
	if len(got) != 1 || !strings.HasPrefix(got[0], "On this page: 1 UPDATE row may have changed title:") {
		t.Errorf("warnings = %q", got)
	}
	if w := pgHistoryWarnings(query.Options{ChangedColumn: "title"}, rows); w != nil {
		t.Errorf("a MySQL index warned: %q", w)
	}
	if w := pgHistoryWarnings(query.Options{PostgresHistory: true}, rows); w != nil {
		t.Errorf("a warning without a changed-column filter: %q", w)
	}
}

// The Events view decides PostgreSQL the SQL card's way and never reads the
// index for it per request: the boot server from what its index said at
// startup, a registry server from its entry.
func TestHistoryIsPostgres(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s := &Server{}
	for _, tc := range []struct {
		b    *bundle
		want bool
	}{
		{&bundle{serverID: bootServerID, pgSource: true, db: db}, true},
		{&bundle{pgSource: true, db: db}, true},
		{&bundle{serverID: bootServerID, db: db}, false},
		{&bundle{serverID: "pg-1", pgSource: true, db: db}, false}, // not in a registry
		{nil, false},
	} {
		if got := s.historyIsPostgres(tc.b); got != tc.want {
			t.Errorf("historyIsPostgres(%+v) = %v, want %v", tc.b, got, tc.want)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("the index was read: %v", err)
	}
}

// The page's words are the query command's words.
func TestPGHistoryWordsMatchTheQueryCommand(t *testing.T) {
	js := readAsset(t, "app.js")
	for _, want := range []string{
		`const NOT_CAPTURED_TEXT = "` + query.NotCapturedText + `";`,
		`const UNCHANGED_LARGE_TEXT = "` + query.UnchangedLargeValueText + `";`,
		`const CHANGED_UNKNOWN_TEXT = "` + query.ChangedColumnsUnknownText + `";`,
	} {
		if !strings.Contains(js, want) {
			t.Errorf("app.js lacks %s", want)
		}
	}
}

// What the Events page draws, rendered by the real app.js in node from the
// DTOs the server builds: the words in place of the markers on a PostgreSQL
// row, "unknown" for its changed columns, and the raw value, unannotated, on
// a MySQL row whose JSON column holds the same object.
func TestEventsPagePostgresHistoryRender(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		if os.Getenv(requireNodeEnv) != "" {
			t.Fatalf("%s is set and node is not on PATH", requireNodeEnv)
		}
		t.Skip("node is not installed")
	}
	appJS, err := filepath.Abs("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	keyOnlyDelete := query.ResultRow{EventID: 1, EventTimestamp: time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC),
		SchemaName: "public", TableName: "docs", EventType: event.EventDelete, PKValues: "1",
		RowBefore: map[string]any{"id": "1", "title": map[string]any{event.NotCapturedKey: true}}}
	cases := []struct {
		name            string
		dto             eventDTO
		wantBefore      map[string]string
		wantAfter       map[string]string
		wantCols        string
		wantFoot, avoid []string
	}{{
		name:       "key-only DELETE",
		dto:        toEventDTO(keyOnlyDelete, true),
		wantBefore: map[string]string{"id": "1", "title": query.NotCapturedText},
		wantFoot:   []string{"Only the key of the earlier row was captured: the table was not at REPLICA IDENTITY FULL."},
		avoid:      []string{"Which columns changed is unknown", "__bintrail"},
	}, {
		name:       "UPDATE with no old row and a value not resent",
		dto:        toEventDTO(pgMarkerRow(), true),
		wantBefore: map[string]string{"id": "2", "title": query.NotCapturedText},
		wantAfter:  map[string]string{"id": "2", "title": "b", "body": query.UnchangedLargeValueText},
		wantCols:   query.ChangedColumnsUnknownText,
		wantFoot:   []string{"Only the key of the earlier row was captured: the table was not at REPLICA IDENTITY FULL. Which columns changed is unknown."},
		avoid:      []string{"__bintrail"},
	}, {
		name:       "MySQL row whose JSON column holds the marker object",
		dto:        toEventDTO(pgMarkerRow(), false),
		wantBefore: map[string]string{"id": "2", "title": `{"__bintrail_not_captured__":true}`},
		wantAfter:  map[string]string{"body": `{"__bintrail_unchanged_toast__":true}`},
		avoid:      []string{"REPLICA IDENTITY", "unchanged large value", "unknown"},
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev, err := json.Marshal(tc.dto)
			if err != nil {
				t.Fatal(err)
			}
			script := renderHarnessJS + `
const text = (n) => n ? n.textContent : "";
const ev = ` + string(ev) + `;
const frozen = JSON.stringify(ev);
const diff = vm.runInContext("renderDiff", ctx)(ev);
const grid = diff.children[0], foot = diff.children[1];
const cells = grid.children.slice(3).map(text);
const rows = {};
for (let i = 0; i + 2 < cells.length; i += 3) rows[cells[i]] = { before: cells[i + 1], after: cells[i + 2] };
const cols = vm.runInContext("eventColsSummary", ctx)(ev, true).map(text).join("");
console.log(JSON.stringify({ rows, foot: text(foot), cols, all: text(diff), mutated: JSON.stringify(ev) !== frozen }));
`
			path := filepath.Join(t.TempDir(), "diff.js")
			if err := os.WriteFile(path, []byte(script), 0o644); err != nil {
				t.Fatal(err)
			}
			raw, err := exec.Command(node, path, appJS).CombinedOutput()
			if err != nil {
				t.Fatalf("node: %v\n%s", err, raw)
			}
			var got struct {
				Rows            map[string]struct{ Before, After string }
				Foot, Cols, All string
				Mutated         bool
			}
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("decode %q: %v", raw, err)
			}
			t.Logf("rendered: %s", raw)
			for c, w := range tc.wantBefore {
				if got.Rows[c].Before != w {
					t.Errorf("before %s = %q, want %q", c, got.Rows[c].Before, w)
				}
			}
			for c, w := range tc.wantAfter {
				if got.Rows[c].After != w {
					t.Errorf("after %s = %q, want %q", c, got.Rows[c].After, w)
				}
			}
			if tc.wantCols != "" && got.Cols != tc.wantCols {
				t.Errorf("changed columns = %q, want %q", got.Cols, tc.wantCols)
			}
			if tc.wantCols == "" && got.Cols == query.ChangedColumnsUnknownText {
				t.Errorf("changed columns says unknown")
			}
			for _, w := range tc.wantFoot {
				if !strings.Contains(got.Foot, w) {
					t.Errorf("foot %q lacks %q", got.Foot, w)
				}
			}
			for _, a := range tc.avoid {
				if strings.Contains(got.All+got.Cols, a) {
					t.Errorf("rendered text carries %q: %s", a, got.All)
				}
			}
			if got.Mutated {
				t.Error("rendering rewrote the event the page keeps for its exports")
			}
		})
	}
}
