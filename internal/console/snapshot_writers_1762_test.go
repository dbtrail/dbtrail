package console

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestSharedSnapshotLocations(t *testing.T) {
	const a, b, c = "aaaa-1", "bbbb-2", "cccc-3"
	seen := map[string][]string{
		"/one":       {a},
		"/none":      nil,
		"/two":       {a, b},
		"s3://x/y":   {a, b, c},
		"/strangers": {b, c},
	}
	cases := []struct {
		name    string
		sources []string
		own     string
		want    []snapshotWritersDTO
		asks    int
	}{
		{name: "one writer is not reported and the index is not asked", sources: []string{"/one", "/none"}, own: a},
		{name: "two writers name the other one", sources: []string{"/two"}, own: a, asks: 1,
			want: []snapshotWritersDTO{{Source: "/two", Own: a, Others: []string{b}}}},
		{name: "seen from the other installation, the other one is the first", sources: []string{"/two"}, own: b, asks: 1,
			want: []snapshotWritersDTO{{Source: "/two", Own: b, Others: []string{a}}}},
		{name: "an unknown identity names every writer", sources: []string{"/two"}, own: "", asks: 1,
			want: []snapshotWritersDTO{{Source: "/two", Others: []string{a, b}}}},
		{name: "a server that signed nothing here names every writer", sources: []string{"/strangers"}, own: a, asks: 1,
			want: []snapshotWritersDTO{{Source: "/strangers", Others: []string{b, c}}}},
		{name: "two locations, the index asked once", sources: []string{"/one", "/two", "s3://x/y"}, own: a, asks: 1,
			want: []snapshotWritersDTO{
				{Source: "/two", Own: a, Others: []string{b}},
				{Source: "s3://x/y", Own: a, Others: []string{b, c}},
			}},
		{name: "no sources", own: a},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			asks := 0
			got := sharedSnapshotLocations(tc.sources,
				func(src string) []string { return seen[src] },
				func() string { asks++; return tc.own })
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
			if asks != tc.asks {
				t.Fatalf("the index was asked %d times, want %d", asks, tc.asks)
			}
			// What was seen is the listing's; reporting must not edit it.
			if !reflect.DeepEqual(seen["/two"], []string{a, b}) {
				t.Fatalf("the seen writers were changed: %q", seen["/two"])
			}
		})
	}
}

// The endpoint itself, over real folders: what the page is handed.
func TestBaselinesAPI_sharedWith(t *testing.T) {
	const own = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	const other = "bbbbbbbb-0000-0000-0000-000000000002"
	const d1, d2, d3 = "2026-06-01T00-00-00Z", "2026-06-02T00-00-00Z", "2026-06-03T00-00-00Z"
	folder := func(t *testing.T, markers map[string][]string) string {
		dir := t.TempDir()
		for ts, ms := range markers {
			writeBaselineFixture(t, dir, ts, "shop", "orders.parquet")
			for _, m := range ms {
				writeBaselineFixture(t, dir, ts, m)
			}
		}
		return dir
	}
	get := func(t *testing.T, srv *Server) baselinesResponse {
		t.Helper()
		rec, body := doServersReq(t, srv, "GET", "/api/baselines", "")
		if rec.Code != 200 {
			t.Fatalf("code = %d, body = %s", rec.Code, body)
		}
		var got baselinesResponse
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatal(err)
		}
		if out := os.Getenv("WRITERS_1762_SAMPLES"); out != "" {
			if err := os.WriteFile(filepath.Join(out, strings.ReplaceAll(t.Name(), "/", "__")+".json"), body, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		return got
	}
	withIndex := func(t *testing.T, srv *Server, id any) sqlmock.Sqlmock {
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		mock.MatchExpectationsInOrder(false)
		mock.ExpectQuery("FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"bintrail_id"}).AddRow(id))
		srv.cm.boot.db = db
		return mock
	}

	t.Run("unsigned snapshots and one writer say nothing", func(t *testing.T) {
		dir := folder(t, map[string][]string{d1: nil, d2: {"_SUCCESS"}, d3: {"_SUCCESS", "_WRITER." + own}})
		got := get(t, newBaselineServer(t, dir, true))
		if len(got.SharedWith) != 0 {
			t.Fatalf("shared_with = %+v for a folder with one writer", got.SharedWith)
		}
		if len(got.Snapshots) != 3 {
			t.Fatalf("snapshots = %d, want 3", len(got.Snapshots))
		}
	})
	t.Run("only unsigned snapshots say nothing", func(t *testing.T) {
		dir := folder(t, map[string][]string{d1: nil, d2: {"_SUCCESS"}})
		if got := get(t, newBaselineServer(t, dir, true)); len(got.SharedWith) != 0 {
			t.Fatalf("shared_with = %+v", got.SharedWith)
		}
	})
	t.Run("two writers, this server known, name the other one", func(t *testing.T) {
		dir := folder(t, map[string][]string{d1: {"_SUCCESS", "_WRITER." + own}, d2: {"_SUCCESS", "_WRITER." + other}})
		srv := newBaselineServer(t, dir, true)
		// The index spells its id in upper case; it is the same writer.
		withIndex(t, srv, strings.ToUpper(own))
		got := get(t, srv)
		want := []snapshotWritersDTO{{Source: dir, Own: own, Others: []string{other}}}
		if !reflect.DeepEqual(got.SharedWith, want) {
			t.Fatalf("shared_with = %+v, want %+v", got.SharedWith, want)
		}
		if len(got.Snapshots) != 2 {
			t.Fatalf("snapshots = %d, want 2: the listing is served as before", len(got.Snapshots))
		}
	})
	t.Run("two writers, no index connection, name both", func(t *testing.T) {
		dir := folder(t, map[string][]string{d1: {"_SUCCESS", "_WRITER." + own}, d2: {"_SUCCESS", "_WRITER." + other}})
		got := get(t, newBaselineServer(t, dir, true))
		want := []snapshotWritersDTO{{Source: dir, Others: []string{own, other}}}
		if !reflect.DeepEqual(got.SharedWith, want) {
			t.Fatalf("shared_with = %+v, want %+v", got.SharedWith, want)
		}
	})
	t.Run("an unreadable signature does not fail the listing", func(t *testing.T) {
		dir := folder(t, map[string][]string{d1: {"_SUCCESS", "_WRITER." + own}, d2: {"_SUCCESS", "_WRITER.who?*"}, d3: {"_SUCCESS", "_WRITER."}})
		got := get(t, newBaselineServer(t, dir, true))
		if len(got.SharedWith) != 0 || len(got.Snapshots) != 3 {
			t.Fatalf("shared_with = %+v, snapshots = %d; want none and 3", got.SharedWith, len(got.Snapshots))
		}
	})
}
