package console

import (
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/views"
)

// #2210: the SQL panel's table list says, per table, how much is waiting to
// be merged and whether a statement naming that table alone is refused. The
// numbers are the refusal's own (same files, same rounding, same line).
func TestSQLUnmergedByView(t *testing.T) {
	const limit = 3 << 20
	sizes := map[string]int64{
		"/snap/shop/a.000000.upserts": 1,                                               // one byte still shows as 1 MB
		"/snap/shop/b.000000.upserts": 2 << 20, "/snap/shop/b.000001.upserts": 1 << 20, // exactly the limit: not over
		"/snap/shop/c.000000.upserts": 3<<20 + 1, // one byte past
		"/snap/shop/d.000000.upserts": 2 << 20,   // ties with e: by name
		"/snap/shop/e.000000.upserts": 2 << 20,
		"/snap/shop/old.upserts":      5 << 20, // a v0.83.0 pair
		"/snap/shop/a.000000.posdel":  1 << 30, // row numbers, not counted
	}
	size := func(p string) (int64, error) {
		if n, ok := sizes[p]; ok {
			return n, nil
		}
		if strings.Contains(p, "denied") {
			return 0, errors.New("permission denied")
		}
		return 0, fs.ErrNotExist
	}
	legacy := views.BaselineTable{Schema: "shop", Table: "old", Path: "/snap/shop/old.parquet", Delta: true, DeltaLegacy: true}
	s3 := views.BaselineTable{Schema: "shop", Table: "s", Path: "s3://b/shop/s.parquet", Delta: true, DeltaLegacy: true}
	in := views.Input{Baselines: []views.BaselineTable{
		chainTable("shop", "plain"), chainTable("shop", "gone", 0), chainTable("shop", "denied", 0),
		chainTable("shop", "e", 0), chainTable("shop", "a", 0), chainTable("shop", "b", 0, 1),
		chainTable("shop", "c", 0), chainTable("shop", "d", 0), legacy, s3,
	}}
	got := sqlUnmergedByView(in, limit, size)
	want := []sqlUnmergedDTO{
		{View: "shop.old", MB: 5, Level: "over"},
		{View: "shop.c", MB: 4, Level: "over"},
		{View: "shop.b", MB: 3, Level: "near"},
		{View: "shop.d", MB: 2},
		{View: "shop.e", MB: 2},
		{View: "shop.a", MB: 1},
		{View: "shop.denied", MB: 1, Unknown: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got  %+v\nwant %+v", got, want)
	}
	if got := sqlUnmergedByView(views.Input{Baselines: []views.BaselineTable{chainTable("shop", "plain")}}, limit, size); got == nil || len(got) != 0 {
		t.Errorf("nothing waiting: %#v, want an empty list", got)
	}

	// A line that is not a whole MB (1000 MiB of memory: 23.4 MiB): under
	// it the MB never reads past the line the page states, past it always.
	odd := sqlChainLimit("1000MiB")
	sizes["/snap/shop/u.000000.upserts"] = odd - 1<<16
	sizes["/snap/shop/v.000000.upserts"] = odd + 1
	got = sqlUnmergedByView(views.Input{Baselines: []views.BaselineTable{chainTable("shop", "u", 0), chainTable("shop", "v", 0)}}, odd, size)
	stated := odd >> 20
	if len(got) != 2 || got[0].Level != "over" || got[0].MB <= stated || got[1].Level != "near" || got[1].MB > stated {
		t.Errorf("line %d bytes (stated %d MB): %+v", odd, stated, got)
	}
}

// Every name the list reports is one the panel lists, renamed twins and
// quoted names included: the page matches them by string.
func TestSQLUnmergedByView_namesAreTheListedViews(t *testing.T) {
	in := views.Input{Baselines: []views.BaselineTable{
		chainTable("shop", "Orders", 0), chainTable("shop", "orders", 0), chainTable("shop", "order line", 0),
	}}
	size := func(string) (int64, error) { return 1 << 20, nil }
	got := sqlUnmergedByView(in, 48<<20, size)
	listed := in.DefinedViews()
	if len(got) != 3 {
		t.Fatalf("got %+v", got)
	}
	for _, u := range got {
		if !slices.Contains(listed, u.View) {
			t.Errorf("%q is not a listed view (%q)", u.View, listed)
		}
	}
}

// The wiring: GET /api/sql carries the list, built from the files on disk
// and against the line the memory in force sets.
func TestSQLAPI_infoListsChangesWaiting(t *testing.T) {
	// The levels against the old 48 MB line, so the fixture's files stay
	// small; the line's own value is pinned in sql_chain_limit_2210_test.go.
	defer func(old int64) { sqlMaxChainBytes = old }(sqlMaxChainBytes)
	sqlMaxChainBytes = 48 << 20
	f := newSQLFixture(t, &fakeSQLRunner{}, false)
	get := func() sqlInfoResponse {
		t.Helper()
		w := httptest.NewRecorder()
		f.s.handleSQLInfo(w, httptest.NewRequest("GET", "/api/sql", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
		}
		var info sqlInfoResponse
		if err := json.Unmarshal(w.Body.Bytes(), &info); err != nil {
			t.Fatal(err)
		}
		return info
	}
	if info := get(); info.Unmerged != nil {
		t.Errorf("no chain: unmerged = %+v, want it omitted", info.Unmerged)
	}
	if err := os.WriteFile(filepath.Join(f.schemaDir, "orders.000000.posdel"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.schemaDir, "orders.000000.upserts"), make([]byte, 50<<20), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := get().Unmerged; !reflect.DeepEqual(got, []sqlUnmergedDTO{{View: "shop.orders", MB: 50, Level: "over"}}) {
		t.Errorf("50 MB at the default memory: %+v", got)
	}
	f.s.sqlMem.savedMiB = 4096
	info := get()
	if !reflect.DeepEqual(info.Unmerged, []sqlUnmergedDTO{{View: "shop.orders", MB: 50}}) || info.Limits.MaxUnmergedMB != 96 {
		t.Errorf("50 MB at 4 GB: %+v, limit %d", info.Unmerged, info.Limits.MaxUnmergedMB)
	}
}
