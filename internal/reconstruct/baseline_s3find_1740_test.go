package reconstruct

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/dbtrail/dbtrail/internal/duckdbutil"
)

// The table lookup on an s3:// source (#1740) moved off three DuckDB globs
// over the whole prefix onto the directory listing. "Same answer, fewer
// requests" is the whole claim, so every scenario below carries the answer
// written by hand, and both lookups are held to it:
//
//   - the glob lookup (findBaselineGlob, kept in a test file), run by real
//     DuckDB over the scenario written to a local folder. glob() needs httpfs
//     for the s3:// scheme only;
//   - the listing lookup (findBaselineS3), run over the same keys in the fake
//     store.
//
// A scenario the glob lookup answers differently says what it answers and
// why (globNotFound, globHTTPFails, globWhy); one it cannot run on a local
// folder says why in globSkip.

const findRoot = "s3://b/base"

// findDay returns snapshot directory names one hour apart, so a scenario
// reads as "snapshot 3 is newer than snapshot 2".
func findDay(i int) string {
	return time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Hour).Format("2006-01-02T15-04-05Z")
}

func findTime(t *testing.T, dir string) time.Time {
	t.Helper()
	at, ok := parseDirTimestamp(dir)
	if !ok {
		t.Fatalf("%q is not a snapshot directory name", dir)
	}
	return at
}

type findScenario struct {
	name   string
	keys   []string
	slash  string // appended to the source: "" or "/"
	schema string
	table  string
	at     func(t *testing.T) time.Time

	// The answer, by hand. wantDir empty: no baseline.
	wantDir    string
	wantNewest string // the newer snapshot that lacks the table; empty: not stale
	why        string

	// Where the glob lookup answers something else, and why (globWhy).
	globSkip      func() string // non-empty: not run on a local folder
	globNotFound  bool          // it answers "no baseline"
	globHTTPFails string        // over HTTP it fails, with an error holding this
	globWhy       string
}

// findAnswer is one lookup's result with the source's own spelling taken out,
// so a local folder and an s3:// prefix compare equal.
type findAnswer struct {
	path       string
	at         time.Time
	stale      StaleWarning
	noBaseline bool
	err        string
}

func (a findAnswer) String() string {
	return fmt.Sprintf("path=%q at=%s stale={%q using=%s newest=%s unreadable=%v} noBaseline=%v err=%q",
		a.path, a.at.Format(time.RFC3339), a.stale.Message, a.stale.UsingSnapshot.Format(time.RFC3339),
		a.stale.NewestSnapshot.Format(time.RFC3339), a.stale.Unreadable, a.noBaseline, a.err)
}

func answerOf(root, path string, at time.Time, stale StaleWarning, err error) findAnswer {
	a := findAnswer{path: strings.ReplaceAll(path, root, "ROOT"), at: at, stale: stale}
	if err != nil {
		a.noBaseline = errors.Is(err, ErrNoBaseline)
		a.err = strings.ReplaceAll(err.Error(), root, "ROOT")
	}
	return a
}

// wantAnswer writes the expected answer out, every string spelled here and
// not borrowed from the code under test.
func wantAnswer(t *testing.T, sc findScenario, dir, newest string) findAnswer {
	t.Helper()
	at := sc.at(t)
	if dir == "" {
		return findAnswer{noBaseline: true, err: fmt.Sprintf("no baseline snapshot found: %s.%s at or before %s in %q",
			sc.schema, sc.table, at.UTC().Format(time.RFC3339), "ROOT"+sc.slash)}
	}
	a := findAnswer{path: "ROOT/" + dir + "/" + sc.schema + "/" + sc.table + ".parquet", at: findTime(t, dir)}
	if newest != "" {
		a.stale = StaleWarning{
			Message: fmt.Sprintf("baseline for %s.%s is stale: the table is absent from the newest snapshot (%s); reconstructing from an older snapshot (%s) — re-dump to refresh it",
				sc.schema, sc.table, findTime(t, newest).Format(time.RFC3339), findTime(t, dir).Format(time.RFC3339)),
			UsingSnapshot:  findTime(t, dir),
			NewestSnapshot: findTime(t, newest),
		}
	}
	return a
}

func atDir(dir string) func(*testing.T) time.Time {
	return func(t *testing.T) time.Time { return findTime(t, dir) }
}

func atFixed(at time.Time) func(*testing.T) time.Time {
	return func(*testing.T) time.Time { return at }
}

var atLate = atFixed(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))

// caseInsensitiveFS says why a scenario that depends on the case of a name
// cannot be run on this machine's disk, or nothing when it can.
func caseInsensitiveFS() string {
	dir, err := os.MkdirTemp("", "casefs")
	if err != nil {
		return "cannot probe the disk: " + err.Error()
	}
	defer os.RemoveAll(dir)
	if err := os.WriteFile(filepath.Join(dir, "Name"), nil, 0o644); err != nil {
		return "cannot probe the disk: " + err.Error()
	}
	if _, err := os.Stat(filepath.Join(dir, "name")); err == nil {
		return "this disk ignores case; S3 does not"
	}
	return ""
}

func tableKeys(dir string, tables ...string) []string {
	var out []string
	for _, t := range tables {
		out = append(out, dir+"/"+t+".parquet")
	}
	return out
}

func cat(parts ...[]string) []string {
	var out []string
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func findScenarios() []findScenario {
	var many []string // 1,200 snapshots; shop.orders in every one, shop.rare in number 40 only
	for i := range 1200 {
		many = append(many, snapshotKeys(findDay(i), []string{"shop/orders"}, "_SUCCESS")...)
	}
	many = append(many, findDay(40)+"/shop/rare.parquet")

	var wide []string // one snapshot of 1,500 tables: 4,503 objects
	var wideTables []string
	for i := range 1500 {
		wideTables = append(wideTables, fmt.Sprintf("shop/t%04d", i))
	}
	wide = append(wide, snapshotKeys(findDay(1), wideTables, "_SUCCESS")...)
	wide = append(wide, snapshotKeys(findDay(2), []string{"shop/other"}, "_SUCCESS")...)

	return []findScenario{
		{
			name:   "the newest snapshot holds the table",
			keys:   cat(snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS"), snapshotKeys(findDay(2), []string{"shop/orders"}, "_SUCCESS")),
			schema: "shop", table: "orders", at: atLate,
			wantDir: findDay(2),
			why:     "two complete snapshots hold it; the newer wins and nothing is stale",
		},
		{
			name:   "the table is missing from the newer snapshots",
			keys:   cat(snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS"), snapshotKeys(findDay(2), []string{"shop/users"}, "_SUCCESS"), snapshotKeys(findDay(3), []string{"shop/users"}, "_SUCCESS")),
			schema: "shop", table: "orders", at: atLate,
			wantDir: findDay(1), wantNewest: findDay(3),
			why: "only snapshot 1 holds it, and the warning names the NEWEST snapshot that lacks it, not the next one (#466)",
		},
		{
			name:   "a newer incomplete snapshot holds the table",
			keys:   cat(snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS"), snapshotKeys(findDay(2), []string{"shop/orders"}, "_INCOMPLETE")),
			schema: "shop", table: "orders", at: atLate,
			wantDir: findDay(1),
			why:     "_INCOMPLETE with no _SUCCESS is never used, and never counts as the newest snapshot (#467)",
		},
		{
			name:   "only an incomplete snapshot holds the table",
			keys:   snapshotKeys(findDay(2), []string{"shop/orders"}, "_INCOMPLETE"),
			schema: "shop", table: "orders", at: atLate,
			why: "a partial snapshot is no baseline at all (#467)",
		},
		{
			name:   "a snapshot with both markers is complete",
			keys:   cat(snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS"), snapshotKeys(findDay(2), []string{"shop/orders"}, "_INCOMPLETE", "_SUCCESS")),
			schema: "shop", table: "orders", at: atLate,
			wantDir: findDay(2),
			why:     "_SUCCESS wins over a leftover _INCOMPLETE (#467)",
		},
		{
			name:   "a snapshot with no marker is complete",
			keys:   cat(snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS"), snapshotKeys(findDay(2), []string{"shop/orders"})),
			schema: "shop", table: "orders", at: atLate,
			wantDir: findDay(2),
			why:     "a snapshot written before the markers existed has neither (#467)",
		},
		{
			name: "incomplete, then one without the table, then one with it",
			keys: cat(
				snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS"),
				snapshotKeys(findDay(2), []string{"shop/orders"}),
				snapshotKeys(findDay(3), []string{"shop/users"}, "_SUCCESS"),
				snapshotKeys(findDay(4), []string{"shop/orders"}, "_INCOMPLETE"),
				snapshotKeys(findDay(5), []string{"shop/orders"}, "_INCOMPLETE"),
			),
			schema: "shop", table: "orders", at: atLate,
			wantDir: findDay(2), wantNewest: findDay(3),
			why: "4 and 5 are partial, 3 is the newest complete one and lacks the table, 2 is the newest that holds it",
		},
		{
			name:   "at is exactly a snapshot's time",
			keys:   cat(snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS"), snapshotKeys(findDay(2), []string{"shop/orders"}, "_SUCCESS"), snapshotKeys(findDay(3), []string{"shop/orders"}, "_SUCCESS")),
			schema: "shop", table: "orders", at: atDir(findDay(2)),
			wantDir: findDay(2),
			why:     "at-or-before includes the instant itself; snapshot 3 is after it",
		},
		{
			name:   "at is one second before a snapshot's time",
			keys:   cat(snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS"), snapshotKeys(findDay(2), []string{"shop/orders"}, "_SUCCESS")),
			schema: "shop", table: "orders",
			at:      func(t *testing.T) time.Time { return findTime(t, findDay(2)).Add(-time.Second) },
			wantDir: findDay(1),
			why:     "snapshot 2 is one second after at",
		},
		{
			name:   "a newer snapshot after at does not make the pick stale",
			keys:   cat(snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS"), snapshotKeys(findDay(2), []string{"shop/users"}, "_SUCCESS")),
			schema: "shop", table: "orders", at: atDir(findDay(1)),
			wantDir: findDay(1),
			why:     "snapshot 2 lacks the table but is after at, so it is not part of the question",
		},
		{
			name:   "at is before every snapshot",
			keys:   snapshotKeys(findDay(5), []string{"shop/orders"}, "_SUCCESS"),
			schema: "shop", table: "orders", at: atDir(findDay(4)),
			why: "nothing at or before at",
		},
		{
			name:   "at is the zero time",
			keys:   snapshotKeys(findDay(5), []string{"shop/orders"}, "_SUCCESS"),
			schema: "shop", table: "orders", at: atFixed(time.Time{}),
			why: "every snapshot is after year 1",
		},
		{
			name:   "the source ends in a slash",
			keys:   cat(snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS"), snapshotKeys(findDay(2), []string{"shop/users"}, "_SUCCESS")),
			slash:  "/",
			schema: "shop", table: "orders", at: atLate,
			wantDir: findDay(1), wantNewest: findDay(2),
			why: "the path has one slash, the error and the warning are the same",
		},
		{
			name:   "the source ends in a slash and holds nothing",
			keys:   []string{"notes.txt"},
			slash:  "/",
			schema: "shop", table: "orders", at: atLate,
			why: "the error names the source the way it was given",
		},
		{
			name:   "names in capitals",
			keys:   snapshotKeys(findDay(1), []string{"Shop/Orders"}, "_SUCCESS"),
			schema: "Shop", table: "Orders", at: atLate,
			wantDir: findDay(1),
			why:     "the name is matched as written",
		},
		{
			name:   "a name that differs only in case is another table",
			keys:   cat(snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS"), snapshotKeys(findDay(2), []string{"Shop/Orders"}, "_SUCCESS")),
			schema: "shop", table: "orders", at: atLate,
			wantDir: findDay(1), wantNewest: findDay(2),
			why:      "S3 keys are case sensitive: snapshot 2 holds Shop.Orders, not shop.orders",
			globSkip: caseInsensitiveFS,
		},
		{
			name:   "names with a dot",
			keys:   cat(snapshotKeys(findDay(1), []string{"my.db/t.v1"}, "_SUCCESS"), snapshotKeys(findDay(2), []string{"my.db/t"}, "_SUCCESS")),
			schema: "my.db", table: "t.v1", at: atLate,
			wantDir: findDay(1), wantNewest: findDay(2),
			why: "t.v1 is not t",
		},
		{
			name:   "a delta file is not the table",
			keys:   cat(snapshotKeys(findDay(1), []string{"shop/orders.000001"}, "_SUCCESS"), []string{findDay(2) + "/shop/orders.000001.posdel", findDay(2) + "/shop/orders.parquet.000001.upserts", findDay(2) + "/shop/users.parquet", findDay(2) + "/_SUCCESS"}),
			schema: "shop", table: "orders", at: atLate,
			why: "orders.000001.parquet is the table orders.000001, and the delta pair of snapshot 2 is not a table file",
		},
		{
			name:   "names that start with an underscore",
			keys:   cat(snapshotKeys(findDay(1), []string{"_private/_t"}, "_SUCCESS"), snapshotKeys(findDay(2), []string{"_private/_SUCCESS", "_private/_INCOMPLETE"}, "_SUCCESS"), []string{findDay(2) + "/_private/_INCOMPLETE"}),
			schema: "_private", table: "_t", at: atLate,
			wantDir: findDay(1), wantNewest: findDay(2),
			why: "a marker is a file directly under the snapshot; inside a schema folder those names are tables or clutter",
		},
		{
			name:   "a table called like the marker",
			keys:   snapshotKeys(findDay(1), []string{"shop/_INCOMPLETE"}),
			schema: "shop", table: "_INCOMPLETE", at: atLate,
			wantDir: findDay(1),
			why:     "shop/_INCOMPLETE.parquet is a table file of a snapshot with no marker",
		},
		{
			name:   "a schema folder called like the marker",
			keys:   []string{findDay(1) + "/_INCOMPLETE/orders.parquet", findDay(1) + "/_SUCCESS"},
			schema: "_INCOMPLETE", table: "orders", at: atLate,
			wantDir: findDay(1),
			why:     "a folder is not a marker file",
		},
		{
			name:   "the snapshot is signed",
			keys:   cat(snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS", "_WRITER."+writerA), snapshotKeys(findDay(2), []string{"shop/orders"}, "_INCOMPLETE", "_WRITER."+writerA)),
			schema: "shop", table: "orders", at: atLate,
			wantDir: findDay(1),
			why:     "_WRITER.<id> is neither a marker nor a table (#1891): snapshot 2 stays partial, snapshot 1 stays complete",
		},
		{
			name:   "a signature alone does not complete a snapshot",
			keys:   []string{findDay(1) + "/_WRITER." + writerA, findDay(1) + "/_INCOMPLETE", findDay(1) + "/shop/orders.parquet"},
			schema: "shop", table: "orders", at: atLate,
			why: "only _SUCCESS completes a snapshot that has _INCOMPLETE",
		},
		{
			name:   "a table whose name starts like another",
			keys:   cat(snapshotKeys(findDay(1), []string{"shop/order"}, "_SUCCESS"), snapshotKeys(findDay(2), []string{"shop/orders", "shop/orders_archive", "shop2/order", "sho/order"}, "_SUCCESS")),
			schema: "shop", table: "order", at: atLate,
			wantDir: findDay(1), wantNewest: findDay(2),
			why: "orders, orders_archive, shop2.order and sho.order are other tables",
		},
		{
			name:   "the same table name in another schema",
			keys:   cat(snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS"), snapshotKeys(findDay(2), []string{"crm/orders"}, "_SUCCESS")),
			schema: "shop", table: "orders", at: atLate,
			wantDir: findDay(1), wantNewest: findDay(2),
			why: "crm.orders is not shop.orders",
		},
		{
			name: "folders that are not snapshots",
			keys: cat(snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS"), []string{
				"current/shop/orders.parquet", ".compact/shop/orders/x/orders.000000-000003.upserts", "notes.txt",
				"2031-01-01/shop/orders.parquet", "9999/shop/orders.parquet", "shop/orders.parquet",
				findDay(1) + "/deep/er/orders.parquet",
			}),
			schema: "shop", table: "orders", at: func(*testing.T) time.Time { return time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC) },
			wantDir: findDay(1),
			why:     "only a folder named by an instant is a snapshot, and only <schema>/<table>.parquet under it is a table",
		},
		{
			name:   "only folders that are not snapshots",
			keys:   []string{"current/shop/orders.parquet", "notes.txt", "2026-09-01/shop/orders.parquet"},
			schema: "shop", table: "orders", at: atLate,
			why: "none of them is a snapshot",
		},
		{
			name:   "a newer snapshot that holds no table file",
			keys:   cat(snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS"), []string{findDay(2) + "/_SUCCESS", findDay(2) + "/views.sql", findDay(2) + "/_MANIFEST"}),
			schema: "shop", table: "orders", at: atLate,
			wantDir: findDay(1),
			why:     "the newest snapshot is the newest one with a table file in it; an empty one does not make the pick stale",
		},
		{
			name:   "an empty store",
			schema: "shop", table: "orders", at: atLate,
			why: "nothing there",
		},
		{
			name:   "no schema and no table",
			keys:   snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS"),
			schema: "", table: "", at: atLate,
			why: "no file is named .parquet under a folder with no name",
		},
		{
			name:   "a question mark in the name",
			keys:   cat(snapshotKeys(findDay(1), []string{"shop/or?ers"}, "_SUCCESS"), snapshotKeys(findDay(2), []string{"shop/orders"}, "_SUCCESS")),
			schema: "shop", table: "or?ers", at: atLate,
			wantDir: findDay(1), wantNewest: findDay(2),
			why:           "the name is a name, not a pattern: orders in snapshot 2 is another table",
			globHTTPFails: "Invalid query parameters found",
			globWhy:       "on a local folder the glob finds it. On S3 it never could: httpfs reads what follows the ? as the URL's parameters and refuses the glob, so the lookup of such a table failed",
		},
		{
			name:   "a star in the name",
			keys:   cat(snapshotKeys(findDay(1), []string{"shop/t*"}, "_SUCCESS"), snapshotKeys(findDay(2), []string{"shop/table", "shop/t"}, "_SUCCESS")),
			schema: "shop", table: "t*", at: atLate,
			wantDir: findDay(1), wantNewest: findDay(2),
			why: "the name is a name, not a pattern",
		},
		{
			name:   "brackets in the name",
			keys:   cat(snapshotKeys(findDay(1), []string{"shop/a[b]"}, "_SUCCESS"), snapshotKeys(findDay(2), []string{"shop/ab"}, "_SUCCESS")),
			schema: "shop", table: "a[b]", at: atLate,
			wantDir: findDay(1), wantNewest: findDay(2),
			why:          "the table a[b] is in snapshot 1",
			globNotFound: true,
			globWhy:      "the glob reads [b] as a character class, lists ab.parquet, and drops it as another table: it never found a table with brackets in its name. The listing compares names, so it does",
		},
		{
			name:   "a quote in the name",
			keys:   snapshotKeys(findDay(1), []string{"shop/o'brien"}, "_SUCCESS"),
			schema: "shop", table: "o'brien", at: atLate,
			wantDir: findDay(1),
			why:     "the name is matched as written",
		},
		{
			name:   "a slash in the schema name",
			keys:   cat([]string{findDay(1) + "/a/b/t.parquet", findDay(1) + "/_SUCCESS"}, snapshotKeys(findDay(2), []string{"shop/orders"}, "_SUCCESS")),
			schema: "a/b", table: "t", at: atLate,
			wantDir: findDay(1), wantNewest: findDay(2),
			why: "the file is where the name says, one folder deeper than a table file usually is",
		},
		{
			name:   "a slash in the name and an incomplete snapshot",
			keys:   []string{findDay(1) + "/a/b/t.parquet", findDay(1) + "/_INCOMPLETE"},
			schema: "a/b", table: "t", at: atLate,
			why: "the marker rule holds for it too",
		},
		{
			name:   "a directory name written with colons",
			keys:   cat(snapshotKeys(findDay(1), []string{"shop/orders"}, "_SUCCESS"), snapshotKeys("2026-09-01T02:00:00Z", []string{"shop/orders"}, "_SUCCESS")),
			schema: "shop", table: "orders", at: atLate,
			wantDir: "2026-09-01T02:00:00Z",
			why:     "the name parses as an instant either way, and it is the newer one",
		},
		{
			name:   "1,200 snapshots, the table in the newest",
			keys:   many,
			schema: "shop", table: "orders", at: atLate,
			wantDir: findDay(1199),
			why:     "3,600 table and delta files under the prefix; the newest snapshot holds it",
		},
		{
			name:   "1,200 snapshots, the table in an old one",
			keys:   many,
			schema: "shop", table: "rare", at: atLate,
			wantDir: findDay(40), wantNewest: findDay(1199),
			why: "only snapshot 40 holds it, 1,159 newer ones do not",
		},
		{
			name:   "1,200 snapshots, at in the middle",
			keys:   many,
			schema: "shop", table: "orders", at: func(t *testing.T) time.Time { return findTime(t, findDay(600)).Add(30 * time.Minute) },
			wantDir: findDay(600),
			why:     "599 snapshots are after at",
		},
		{
			name:   "1,200 snapshots, a table in none",
			keys:   many,
			schema: "shop", table: "nowhere", at: atLate,
			why: "every snapshot was looked at and none holds it",
		},
		{
			name:   "one snapshot of 1,500 tables",
			keys:   wide,
			schema: "shop", table: "t1499", at: atLate,
			wantDir: findDay(1), wantNewest: findDay(2),
			why: "the last table of a snapshot of 4,503 objects",
		},
	}
}

// writeFindKeys writes a scenario's keys under a local folder.
func writeFindKeys(t *testing.T, keys []string) string {
	t.Helper()
	root := t.TempDir()
	for _, k := range keys {
		p := filepath.Join(root, filepath.FromSlash(k))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return filepath.ToSlash(root)
}

func TestFindBaselineS3_sameAnswerAsTheGlobs(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, sc := range findScenarios() {
		t.Run(sc.name, func(t *testing.T) {
			captureLog(t)
			want := wantAnswer(t, sc, sc.wantDir, sc.wantNewest)
			at := sc.at(t)

			t.Run("listing", func(t *testing.T) {
				f := &fakeS3Snapshots{keys: sc.keys}
				stubS3Snapshots(t, f)
				path, snap, stale, err := findBaselineS3(context.Background(), findRoot+sc.slash, sc.schema, sc.table, at)
				if got := answerOf(findRoot, path, snap, stale, err); got != want {
					t.Fatalf("%s\n got %s\nwant %s", sc.why, got, want)
				}
			})

			// The same two lookups over HTTP, against a store that lists the
			// way S3 does: the real client under the listing, DuckDB's httpfs
			// under the globs. This is where a name's case and S3's own
			// reading of a glob are in play.
			t.Run("listing over http", func(t *testing.T) {
				newHTTPS3(t, "base/", sc.keys)
				path, snap, stale, err := findBaselineS3(context.Background(), findRoot+sc.slash, sc.schema, sc.table, at)
				if got := answerOf(findRoot, path, snap, stale, err); got != want {
					t.Fatalf("%s\n got %s\nwant %s", sc.why, got, want)
				}
			})

			t.Run("globs over http", func(t *testing.T) {
				if !httpfsLoads(t) {
					t.Skip("httpfs does not load here")
				}
				wantGlob := want
				if sc.globNotFound {
					wantGlob = wantAnswer(t, sc, "", "")
				}
				newHTTPS3(t, "base/", sc.keys)
				path, snap, stale, err := findBaselineS3Globs(context.Background(), findRoot+sc.slash, sc.schema, sc.table, at)
				if sc.globHTTPFails != "" {
					if err == nil || errors.Is(err, ErrNoBaseline) || !strings.Contains(err.Error(), sc.globHTTPFails) {
						t.Fatalf("%s\n got %q, %v", sc.globWhy, path, err)
					}
					return
				}
				if got := answerOf(findRoot, path, snap, stale, err); got != wantGlob {
					t.Fatalf("%s\n got %s\nwant %s", sc.why+" "+sc.globWhy, got, wantGlob)
				}
			})

			t.Run("globs", func(t *testing.T) {
				if sc.globSkip != nil {
					if why := sc.globSkip(); why != "" {
						t.Skip(why)
					}
				}
				wantGlob := want
				if sc.globNotFound {
					wantGlob = wantAnswer(t, sc, "", "")
				}
				root := writeFindKeys(t, sc.keys)
				path, snap, stale, err := findBaselineGlob(context.Background(), db, root+sc.slash, sc.schema, sc.table, at)
				if got := answerOf(root, path, snap, stale, err); got != wantGlob {
					t.Fatalf("%s\n got %s\nwant %s", sc.why+" "+sc.globWhy, got, wantGlob)
				}
			})
		})
	}
}

// httpfsLoads says whether DuckDB can load its httpfs extension on this
// machine: it is installed from the network the first time.
func httpfsLoads(t *testing.T) bool {
	t.Helper()
	httpfsOnce.Do(func() {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			return
		}
		defer db.Close()
		httpfsOK = duckdbutil.LoadHTTPFS(context.Background(), db) == nil
	})
	return httpfsOK
}

var (
	httpfsOnce sync.Once
	httpfsOK   bool
)
