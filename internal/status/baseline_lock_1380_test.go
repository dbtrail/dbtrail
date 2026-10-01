package status

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// #1380: status says how each snapshot was locked. No record is unknown and
// is printed as unknown, never as consistent.

func lockInfos() []BaselineInfo {
	newest := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	older := newest.Add(-24 * time.Hour)
	return []BaselineInfo{
		{Database: "shop", Table: "orders", SnapshotTime: newest, Lock: LockTorn},
		{Database: "shop", Table: "users", SnapshotTime: newest, Lock: LockConsistent},
		{Database: "shop", Table: "old", SnapshotTime: newest, Lock: LockUnknown},
		{Database: "shop", Table: "later", SnapshotTime: newest, Lock: "locked-v2"},
		{Database: "shop", Table: "unlooked", SnapshotTime: newest},
		// Superseded: its newest snapshot is consistent.
		{Database: "shop", Table: "users", SnapshotTime: older, Lock: LockTorn},
	}
}

func TestWrite_baselinesSayHowTheyWereLocked(t *testing.T) {
	var buf bytes.Buffer
	(&StatusData{Baselines: lockInfos()}).Write(&buf)
	out := buf.String()
	out = out[strings.Index(out, "=== Baselines ==="):]
	t.Logf("\n%s", out)

	header := strings.Fields(strings.Split(out, "\n")[1])
	col := -1
	for i, h := range header {
		if h == "POINT_IN_TIME" {
			col = i
		}
	}
	if col < 0 {
		t.Fatalf("no POINT_IN_TIME column in %v", header)
	}
	// The cell of each table's row at the newest snapshot. The snapshot time
	// is two fields, and "⚠ different points-in-time" is three.
	want := map[string]string{
		"orders":   "⚠ different points-in-time",
		"users":    "consistent",
		"old":      "unknown",
		"later":    "unknown",
		"unlooked": "-",
	}
	seen := map[string]bool{}
	for _, l := range strings.Split(out, "\n") {
		f := strings.Fields(l)
		if len(f) < 9 || f[0] != "2026-09-27" {
			continue
		}
		table := f[3]
		// SIZE, BINLOG_FILE, BINLOG_POS, GTID are "-" here, then the cell.
		cell := strings.Join(f[8:len(f)-2], " ")
		if cell != want[table] {
			t.Errorf("%s: POINT_IN_TIME = %q, want %q (row %q)", table, cell, want[table], l)
		}
		seen[table] = true
	}
	for table := range want {
		if !seen[table] {
			t.Errorf("no row for %s", table)
		}
	}

	for _, say := range []string{
		"⚠ DIFFERENT POINTS-IN-TIME: the newest snapshot of 1 table was read at different points in time, so its rows may not agree with each other.",
		"POINT-IN-TIME UNKNOWN: the newest snapshot of 2 tables does not record whether it is point-in-time.",
	} {
		if !strings.Contains(out, say) {
			t.Errorf("the report does not say %q", say)
		}
	}
	if strings.ContainsAny(out[strings.Index(out, "DIFFERENT POINTS-IN-TIME"):], "\u2014\u2013") {
		t.Errorf("the lines carry a dash that is not a hyphen:\n%s", out)
	}
}

// Nothing is said under the table when every newest snapshot is consistent,
// or when nobody looked.
func TestWrite_baselinesSayNothingWhenLocked(t *testing.T) {
	at := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	for name, infos := range map[string][]BaselineInfo{
		"all consistent": {{Database: "shop", Table: "orders", SnapshotTime: at, Lock: LockConsistent},
			{Database: "shop", Table: "orders", SnapshotTime: at.Add(-time.Hour), Lock: LockTorn}},
		"nobody looked": {{Database: "shop", Table: "orders", SnapshotTime: at}},
	} {
		var buf bytes.Buffer
		(&StatusData{Baselines: infos}).Write(&buf)
		if out := buf.String(); strings.Contains(out, "DIFFERENT POINTS-IN-TIME") || strings.Contains(out, "POINT-IN-TIME UNKNOWN") {
			t.Errorf("%s:\n%s", name, out)
		}
	}
}

func TestWriteJSON_baselinesSayHowTheyWereLocked(t *testing.T) {
	var buf bytes.Buffer
	if err := (&StatusData{Baselines: lockInfos()}).WriteJSON(&buf); err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Baselines []map[string]any `json:"baselines"`
	}
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		t.Fatalf("%v\n%s", err, buf.String())
	}
	got := map[string]any{}
	for _, b := range doc.Baselines {
		if b["snapshot_time"] == "2026-09-27 12:00:00" {
			v, has := b["snapshot_lock"]
			if !has {
				v = "(absent)"
			}
			got[b["table"].(string)] = v
		}
		// What was there before is still there.
		for _, key := range []string{"snapshot_time", "database", "table"} {
			if _, ok := b[key]; !ok {
				t.Errorf("an entry lost %s: %v", key, b)
			}
		}
	}
	for table, want := range map[string]string{
		"orders": "torn", "users": "consistent", "old": "unknown", "later": "unknown", "unlooked": "(absent)",
	} {
		if got[table] != want {
			t.Errorf("%s: snapshot_lock = %v, want %s", table, got[table], want)
		}
	}
}

func TestLockColumn(t *testing.T) {
	for in, want := range map[string]string{
		"": "-", "consistent": "consistent", "torn": "⚠ different points-in-time", "unknown": "unknown",
		"Consistent": "unknown", "consistent ": "unknown", "ok": "unknown", "true": "unknown",
	} {
		if got := lockColumn(in); got != want {
			t.Errorf("lockColumn(%q) = %q, want %q", in, got, want)
		}
	}
	for in, want := range map[string]string{
		"consistent": "consistent", "torn": "torn", "unknown": "unknown", "Consistent": "unknown", "x": "unknown",
	} {
		if got := jsonLock(in); got != want {
			t.Errorf("jsonLock(%q) = %q, want %q", in, got, want)
		}
	}
}
