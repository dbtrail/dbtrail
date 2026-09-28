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
		if h == "LOCKS" {
			col = i
		}
	}
	if col < 0 {
		t.Fatalf("no LOCKS column in %v", header)
	}
	// The cell of each table's row at the newest snapshot. The snapshot time
	// is two fields, and "⚠ no locks" is three.
	want := map[string]string{
		"orders":   "⚠ no locks",
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
			t.Errorf("%s: LOCKS = %q, want %q (row %q)", table, cell, want[table], l)
		}
		seen[table] = true
	}
	for table := range want {
		if !seen[table] {
			t.Errorf("no row for %s", table)
		}
	}

	for _, say := range []string{
		"⚠ NO LOCKS: the newest snapshot of 1 table was read with no locks, so its rows may not agree with each other.",
		"LOCKS NOT RECORDED: the newest snapshot of 2 tables does not say how it was locked.",
	} {
		if !strings.Contains(out, say) {
			t.Errorf("the report does not say %q", say)
		}
	}
	if strings.ContainsAny(out[strings.Index(out, "NO LOCKS"):], "\u2014\u2013") {
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
		if out := buf.String(); strings.Contains(out, "NO LOCKS") || strings.Contains(out, "NOT RECORDED") {
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
		"": "-", "consistent": "consistent", "torn": "⚠ no locks", "unknown": "unknown",
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
