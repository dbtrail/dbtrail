//go:build integration

package console

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/ext"
	"github.com/dbtrail/dbtrail/internal/parquetquery"
	"github.com/dbtrail/dbtrail/internal/query"
	"github.com/dbtrail/dbtrail/internal/rotation"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// The archive-heavy default browse (#1353), driven through the REAL handler
// against a fixture built by the REAL rotation: most hours archived to Parquet
// and dropped from live, a thin live remnant on top. The unit tests prove the
// browse plan and the elision flag; this proves the whole claim on the wire:
//
//   - EQUIVALENCE, not just speed: the fast path (archives skipped) returns
//     exactly the rows the slow complete merge would have — the skip is
//     correct by construction, and this test is what fails if the fill-check
//     is ever weakened (a short page that wrongly skips returns different
//     rows than the ground truth, or panics on the trim).
//   - AUDITABILITY: the response SAYS the archives went unread when they were
//     skipped, and does NOT say it when they were read or when they were
//     excluded by a session profile (#1311's exclusion is a different fact
//     and keeps its own notice).
func TestIntegrationEventsBrowseArchiveShortCircuit(t *testing.T) {
	db, dbName := testutil.CreateTestDB(t)
	testutil.InitIndexTables(t, db)

	// Six past hourly partitions, 60 events each. Timestamps are spaced inside
	// each hour so (event_timestamp, event_id) ordering is total.
	now := time.Now().UTC()
	var hours []time.Time
	for h := 6; h >= 1; h-- {
		hours = append(hours, now.Truncate(time.Hour).Add(-time.Duration(h)*time.Hour))
	}
	testutil.SetupPartitionedTable(t, db, dbName, hours)
	const perHour = 60
	for hi, hour := range hours {
		var sb strings.Builder
		sb.WriteString(`INSERT INTO binlog_events
			(binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_after)
			VALUES `)
		for i := range perHour {
			if i > 0 {
				sb.WriteString(",")
			}
			ts := hour.Add(time.Duration(i) * 30 * time.Second).Format("2006-01-02 15:04:05")
			pk := hi*perHour + i
			fmt.Fprintf(&sb, `('bin.000001', %d, %d, '%s', 'shop', 'orders', 1, '%d', '{"id":%d}')`,
				1000+pk, 1200+pk, ts, pk, pk)
		}
		testutil.MustExec(t, db, sb.String())
	}
	total := len(hours) * perHour

	// The real rotation: archive to Parquet, register archive_state, drop the
	// partitions past retention. Retention ages a partition by its label hour,
	// so 3h keeps the newest two of the six, or the newest one when the hour
	// turns between this fixture's clock and rotation's own (2h kept none
	// then, and the premise below failed); the assertions below derive their
	// expectations from the live remnant rather than hardcoding it.
	if _, err := rotation.Perform(context.Background(), db, dbName, rotation.Options{
		RetainDur:          3 * time.Hour,
		RetainRaw:          "3h",
		ArchiveDir:         t.TempDir(),
		ArchiveCompression: "zstd",
		BintrailID:         "browse-sc",
		Format:             "json",
	}); err != nil {
		t.Fatalf("rotation.Perform: %v", err)
	}
	var liveCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM binlog_events`).Scan(&liveCount); err != nil {
		t.Fatalf("count live: %v", err)
	}
	var archived int
	if err := db.QueryRow(`SELECT COUNT(*) FROM archive_state`).Scan(&archived); err != nil {
		t.Fatalf("count archive_state: %v", err)
	}
	// Premises, asserted so a fixture drift fails loudly instead of making the
	// assertions below vacuously pass: the live remnant must FILL a 50-event
	// page (fast-path premise) and must NOT fill a 200-event one (slow-path
	// premise), with real archived hours behind it.
	if liveCount <= 50 || liveCount >= 200 || archived == 0 {
		t.Fatalf("fixture premise broken: live=%d (want 51..199), archived partitions=%d (want >0)", liveCount, archived)
	}
	// Rendered in the eventDTO's timestamp format so the string comparisons
	// below compare like with like.
	var liveFloorT time.Time
	if err := db.QueryRow(`SELECT MIN(event_timestamp) FROM binlog_events`).Scan(&liveFloorT); err != nil {
		t.Fatalf("live floor: %v", err)
	}
	liveFloor := liveFloorT.Format("2006-01-02 15:04:05")

	// Ground truth: the slow, complete, merged read — every live and archived
	// event, fetched with no limit so no short-circuit can fire — sorted
	// newest-first the same way MergeResults sorts.
	truth, _, err := query.FetchMerged(context.Background(), db, query.New(db), query.FetchMergedOptions{
		Opts:           query.Options{Order: "ASC"},
		DBName:         dbName,
		AllowGaps:      true,
		ArchiveFetcher: parquetquery.Fetch,
	})
	if err != nil {
		t.Fatalf("ground-truth FetchMerged: %v", err)
	}
	if len(truth) != total {
		t.Fatalf("ground truth fetched %d events, want %d — the archive tier is not being read, every assertion below would be vacuous", len(truth), total)
	}
	slices.SortFunc(truth, func(a, b query.ResultRow) int {
		if c := b.EventTimestamp.Compare(a.EventTimestamp); c != 0 {
			return c
		}
		switch {
		case b.EventID > a.EventID:
			return 1
		case b.EventID < a.EventID:
			return -1
		}
		return 0
	})

	srv, err := New(Config{DB: db, DBName: dbName, Listen: "127.0.0.1:8090", Token: intToken})
	if err != nil {
		t.Fatal(err)
	}
	type eventsResp struct {
		Events []struct {
			EventID        uint64 `json:"event_id"`
			EventTimestamp string `json:"event_timestamp"`
		} `json:"events"`
		Count    int      `json:"count"`
		HasMore  bool     `json:"has_more"`
		Warnings []string `json:"warnings"`
		Notes    []string `json:"notes"`
	}
	get := func(t *testing.T, params string, profile string) eventsResp {
		t.Helper()
		r := httptest.NewRequest("GET", "http://127.0.0.1:8090/api/events?"+params, nil)
		r.Host = "127.0.0.1:8090"
		if profile != "" {
			r = r.WithContext(context.WithValue(r.Context(),
				policyCtxKey{}, &ext.AccessPolicy{Profile: profile, Permissions: ext.AllPermissions()}))
		}
		w := httptest.NewRecorder()
		srv.handleEvents(w, r)
		if w.Code != 200 {
			t.Fatalf("events code = %d, body = %s", w.Code, w.Body.String())
		}
		var resp eventsResp
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode: %v\n%s", err, w.Body.String())
		}
		return resp
	}
	wantIDs := func(t *testing.T, resp eventsResp, n int) {
		t.Helper()
		if resp.Count != n || len(resp.Events) != n {
			t.Fatalf("count = %d (events %d), want %d", resp.Count, len(resp.Events), n)
		}
		for i, e := range resp.Events {
			if e.EventID != truth[i].EventID {
				t.Fatalf("event[%d].event_id = %d, want %d — the fast path returned different rows than the complete merge", i, e.EventID, truth[i].EventID)
			}
		}
	}

	t.Run("filled page: archives skipped, result identical, skip audited", func(t *testing.T) {
		resp := get(t, "limit=50", "")
		wantIDs(t, resp, 50)
		if !resp.HasMore {
			t.Error("has_more = false with events behind the page")
		}
		// #1365: the elision record is an info NOTE, not a warning — the
		// response must still carry the fact (auditability, #1353), in the
		// notes list, and the warnings list must be clean (routing the note
		// back into warnings fails here; silencing it entirely fails below).
		notes := strings.Join(resp.Notes, "\n")
		if !strings.Contains(notes, "answered from the live index") {
			t.Errorf("the archive skip is not auditable in the response notes: %#v", resp.Notes)
		}
		if strings.Contains(notes, "does not mean nothing happened") {
			t.Errorf("the elision must not be phrased as a scope reduction: %#v", resp.Notes)
		}
		if len(resp.Warnings) != 0 {
			t.Errorf("a clean fast-path browse must carry no warnings — the elision record belongs in notes (#1365): %#v", resp.Warnings)
		}
		for _, e := range resp.Events {
			if e.EventTimestamp < liveFloor {
				t.Fatalf("fast path returned an event below the live floor (%s < %s); the skip premise is broken", e.EventTimestamp, liveFloor)
			}
		}
	})

	t.Run("recover: filled window elides — the note is in notes, positively", func(t *testing.T) {
		// The POSITIVE recover-side pin (#1120 class, PR #1367 review): every
		// other recover-notes assertion in the suite is a negative that holds
		// vacuously on nil, so deleting `Notes: notes` at the plain recover
		// write site — or reverting the split only in handleRecover — passed
		// everything. This fails on both. Recover forces Order=DESC and the
		// live remnant fills the 50-event limit, so the fetch elides the
		// archives exactly like the events fast path above.
		body := strings.NewReader(`{"schema":"shop","table":"orders","limit":50}`)
		r := httptest.NewRequest("POST", "http://127.0.0.1:8090/api/recover", body)
		r.Host = "127.0.0.1:8090"
		w := httptest.NewRecorder()
		srv.handleRecover(w, r)
		if w.Code != 200 {
			t.Fatalf("recover code = %d, body = %s", w.Code, w.Body.String())
		}
		var resp struct {
			SQL      string   `json:"sql"`
			RowCount int      `json:"row_count"`
			Warnings []string `json:"warnings"`
			Notes    []string `json:"notes"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode recover: %v\n%s", err, w.Body.String())
		}
		if resp.RowCount != 50 || resp.SQL == "" {
			t.Fatalf("recover did not process a filled window (row_count=%d, sql empty=%v) — the notes assertion below would be vacuous", resp.RowCount, resp.SQL == "")
		}
		if !strings.Contains(strings.Join(resp.Notes, "\n"), "built from the live index") {
			t.Errorf("the recover elision record is not in notes: %#v", resp.Notes)
		}
		// Warnings must be clean of the elision — but NOT empty: the filled
		// window trips the limit-truncation warning, and asserting it keeps
		// the leak check below from passing on an accidentally empty list.
		joined := strings.Join(resp.Warnings, "\n")
		if !strings.Contains(joined, "truncated at the limit") {
			t.Errorf("expected the limit-truncation warning on a filled window: %#v", resp.Warnings)
		}
		if strings.Contains(joined, "built from the live index") || strings.Contains(joined, "answered from the live index") {
			t.Errorf("the elision record leaked into recover warnings: %#v", resp.Warnings)
		}
	})

	t.Run("short page: archives read, no elision claimed", func(t *testing.T) {
		resp := get(t, "limit=200", "")
		wantIDs(t, resp, 200)
		var belowFloor int
		for _, e := range resp.Events {
			if e.EventTimestamp < liveFloor {
				belowFloor++
			}
		}
		if belowFloor == 0 {
			t.Fatal("no archived (below live floor) events in a 200-event page the live index cannot fill — the archives were not read")
		}
		if joined := strings.Join(append(append([]string{}, resp.Warnings...), resp.Notes...), "\n"); strings.Contains(joined, "answered from the live index") {
			t.Errorf("elision claimed on a page that read the archives: warnings=%#v notes=%#v", resp.Warnings, resp.Notes)
		}
	})

	t.Run("profiled session: exclusion notice, never the elision notice", func(t *testing.T) {
		if _, err := db.Exec(`INSERT INTO profiles (name) VALUES ('analyst')`); err != nil {
			t.Fatalf("seed profile: %v", err)
		}
		resp := get(t, "limit=50", "analyst")
		if resp.Count != 50 {
			t.Fatalf("profiled count = %d, want 50 (live remnant fills the page)", resp.Count)
		}
		joined := strings.Join(resp.Warnings, "\n")
		if !strings.Contains(joined, "LIVE INDEX ONLY") {
			t.Errorf("profiled session lost its scope notice (#1321 regression): %#v", resp.Warnings)
		}
		// The exclusion is a WARNING and must stay one (#1365 split). A
		// profiled session resolves no archives, so nothing can be elided:
		// notes must be empty — an entry here would either demote the
		// exclusion or reframe it as a harmless elision.
		if len(resp.Notes) != 0 {
			t.Errorf("a profiled session must carry no info notes: %#v", resp.Notes)
		}
		if strings.Contains(joined, "answered from the live index") {
			t.Errorf("a profiled session's exclusion must not be reframed as a harmless elision: %#v", resp.Warnings)
		}
		for _, e := range resp.Events {
			if e.EventTimestamp < liveFloor {
				t.Fatalf("profiled session was served an archived event (%s < %s)", e.EventTimestamp, liveFloor)
			}
		}
	})
}
