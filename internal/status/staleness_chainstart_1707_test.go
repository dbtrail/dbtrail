package status

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// #1707: a table with table deltas beside it is read from where its chain of
// deltas STARTED, so that instant is what has to sit inside delta coverage.
// The floor here is 10:00 and the snapshot directory 12:00, the shape of the
// report: graded on the directory every case below reads "ok".
func TestGradeTable(t *testing.T) {
	now := time.Date(2026, 9, 27, 13, 0, 0, 0, time.UTC)
	floorAt := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	dir := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	floor := DeltaFloor{Hour: floorAt}
	multi := DeltaFloor{Hour: floorAt, BelowIsUnknown: true}
	cest := time.FixedZone("CEST", 2*60*60)

	cases := []struct {
		name  string
		floor DeltaFloor
		snap  time.Time
		bound ReadBound
		want  BaselineStalenessVerdict
	}{
		{"no chain: the snapshot's own time, as before", floor, dir, ReadBound{}, BaselineOK},
		{"chain started inside coverage", floor, dir, ReadBound{ChainStart: dir.Add(-30 * time.Minute)}, BaselineOK},
		{"chain started before coverage", floor, dir, ReadBound{ChainStart: floorAt.Add(-8 * time.Hour)}, BaselineBroken},
		{"chain started one second before coverage", floor, dir, ReadBound{ChainStart: floorAt.Add(-time.Second)}, BaselineBroken},
		{"chain started exactly at the floor", floor, dir, ReadBound{ChainStart: floorAt}, BaselineAging},
		{"chain start near the floor is aging, the directory was not", floor, dir, ReadBound{ChainStart: floorAt.Add(10 * time.Minute)}, BaselineAging},
		// An empty first pair (a full backup) starts its chain at itself.
		{"chain starts at the snapshot", floor, dir, ReadBound{ChainStart: dir}, BaselineOK},
		// FindBaseline's rule: only ever earlier. A later start is a footer
		// that cannot be right and is ignored, never trusted.
		{"chain start after the snapshot is ignored", floor, floorAt.Add(-time.Hour), ReadBound{ChainStart: now}, BaselineBroken},
		// The chain is capped at a day by the writer, not by the grader: an
		// older start is what the reader would use, so it is graded as is.
		{"chain older than the 24 hour cap", floor, dir, ReadBound{ChainStart: dir.Add(-30 * time.Hour)}, BaselineBroken},
		// 11:30+02:00 is 09:30 UTC: before the floor. Compared as instants.
		{"chain start written with an offset", floor, dir, ReadBound{ChainStart: time.Date(2026, 9, 27, 11, 30, 0, 0, cest)}, BaselineBroken},
		{"snapshot written with an offset", floor, dir.In(cest), ReadBound{ChainStart: dir.Add(-time.Hour)}, BaselineOK},

		{"unread chain is never ok", floor, dir, ReadBound{Unread: true}, BaselineUnknown},
		{"unread chain is never aging", floor, floorAt.Add(time.Minute), ReadBound{Unread: true}, BaselineUnknown},
		// Evidence, not a guess: the chain starts at or before its directory.
		{"unread chain under a snapshot already past coverage", floor, floorAt.Add(-time.Hour), ReadBound{Unread: true}, BaselineBroken},
		{"unread wins over a start that was filled anyway", floor, dir, ReadBound{ChainStart: dir.Add(-time.Minute), Unread: true}, BaselineUnknown},

		// Several sources in one index (#1219): below the live floor is not
		// attributable, so it is unknown, never broken.
		{"several sources: chain started before the live floor", multi, dir, ReadBound{ChainStart: floorAt.Add(-time.Hour)}, BaselineUnknown},
		{"several sources: chain started inside the live window", multi, dir, ReadBound{ChainStart: floorAt.Add(time.Hour + 30*time.Minute)}, BaselineOK},
		{"several sources: unread chain past the live floor", multi, floorAt.Add(-time.Hour), ReadBound{Unread: true}, BaselineUnknown},

		{"no floor", DeltaFloor{}, dir, ReadBound{ChainStart: dir.Add(-time.Hour)}, BaselineUnknown},
		{"no snapshot time", floor, time.Time{}, ReadBound{}, BaselineUnknown},
	}
	for _, tc := range cases {
		if got := tc.floor.GradeTable(tc.snap, tc.bound, now); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
}

// A file with no chain grades exactly as Grade does, over the whole range of
// verdicts: the zero ReadBound changes nothing.
func TestGradeTable_noChainIsGrade(t *testing.T) {
	now := time.Date(2026, 9, 27, 13, 0, 0, 0, time.UTC)
	for _, floor := range []DeltaFloor{{}, {Hour: now.Add(-100 * time.Hour)}, {Hour: now.Add(-100 * time.Hour), BelowIsUnknown: true}} {
		for _, age := range []time.Duration{0, time.Hour, 79 * time.Hour, 80 * time.Hour, 100 * time.Hour, 101 * time.Hour, 500 * time.Hour} {
			snap := now.Add(-age)
			if got, want := floor.GradeTable(snap, ReadBound{}, now), floor.Grade(snap, now); got != want {
				t.Errorf("floor %+v, age %s: GradeTable = %q, Grade = %q", floor, age, got, want)
			}
		}
	}
	var zero DeltaFloor
	if got, want := zero.GradeTable(time.Time{}, ReadBound{}, now), zero.Grade(time.Time{}, now); got != want {
		t.Errorf("zero snapshot: GradeTable = %q, Grade = %q", got, want)
	}
}

func TestReadBoundFrom(t *testing.T) {
	dir := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name  string
		bound ReadBound
		want  time.Time
	}{
		{"no chain", ReadBound{}, dir},
		{"earlier start", ReadBound{ChainStart: dir.Add(-10 * time.Hour)}, dir.Add(-10 * time.Hour)},
		{"same instant", ReadBound{ChainStart: dir}, dir},
		{"later start", ReadBound{ChainStart: dir.Add(time.Second)}, dir},
	} {
		if got := tc.bound.From(dir); !got.Equal(tc.want) {
			t.Errorf("%s: From = %s, want %s", tc.name, got, tc.want)
		}
	}
}

// Each entry is graded on ITS OWN bound: two tables of one snapshot with
// chains that started at different times get different verdicts, and the
// headline is the worst of each table's newest.
func TestAnnotateBaselineStaleness_gradesEachTableOnItsOwnChain(t *testing.T) {
	now := time.Date(2026, 9, 27, 13, 0, 0, 0, time.UTC)
	floor := DeltaFloor{Hour: time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)}
	dir := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	infos := []BaselineInfo{
		{Database: "shop", Table: "orders", SnapshotTime: dir, Bound: ReadBound{ChainStart: time.Date(2026, 9, 27, 2, 0, 0, 0, time.UTC)}},
		{Database: "shop", Table: "users", SnapshotTime: dir, Bound: ReadBound{ChainStart: time.Date(2026, 9, 27, 11, 45, 0, 0, time.UTC)}},
		{Database: "shop", Table: "plain", SnapshotTime: dir},
		{Database: "shop", Table: "damaged", SnapshotTime: dir, Bound: ReadBound{Unread: true}},
	}
	AnnotateBaselineStaleness(infos, floor, now)
	want := []BaselineStalenessVerdict{BaselineBroken, BaselineOK, BaselineOK, BaselineUnknown}
	for i, w := range want {
		if infos[i].Staleness != w {
			t.Errorf("%s: %q, want %q", infos[i].Table, infos[i].Staleness, w)
		}
	}
	if got := OverallBaselineStaleness(infos); got != BaselineBroken {
		t.Errorf("headline = %q, want broken", got)
	}
	if got := OverallBaselineStaleness(infos[1:]); got != BaselineUnknown {
		t.Errorf("headline without the broken table = %q, want unknown (unread outranks ok)", got)
	}
	if got := OverallBaselineStaleness(infos[1:3]); got != BaselineOK {
		t.Errorf("headline of the two covered tables = %q, want ok", got)
	}
}

func TestWorseBaselineStaleness(t *testing.T) {
	order := []BaselineStalenessVerdict{"", BaselineOK, BaselineAging, BaselineUnknown, BaselineBroken}
	for i, a := range order {
		for j, b := range order {
			want := a
			if j > i {
				want = b
			}
			if got := WorseBaselineStaleness(a, b); got != want {
				t.Errorf("Worse(%q, %q) = %q, want %q", a, b, got, want)
			}
		}
	}
}

// The report, as printed: the instant each verdict was graded on is on the
// row, and the not-evaluable banner names the tables whose deltas could not
// be read.
func TestWrite_baselinesShowWhereAReaderStarts(t *testing.T) {
	now := time.Date(2026, 9, 27, 13, 0, 0, 0, time.UTC)
	floor := DeltaFloor{Hour: time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)}
	dir := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	infos := []BaselineInfo{
		{Database: "shop", Table: "users", SnapshotTime: dir, Bound: ReadBound{ChainStart: time.Date(2026, 9, 27, 11, 45, 0, 0, time.UTC)}},
		{Database: "shop", Table: "plain", SnapshotTime: dir},
		{Database: "shop", Table: "damaged", SnapshotTime: dir, Bound: ReadBound{Unread: true}},
	}
	AnnotateBaselineStaleness(infos, floor, now)
	var buf bytes.Buffer
	(&StatusData{Baselines: infos}).Write(&buf)
	out := buf.String()
	t.Logf("\n%s", out[strings.Index(out, "=== Baselines ==="):])
	row := func(table string) []string {
		t.Helper()
		for _, l := range strings.Split(out, "\n") {
			// The snapshot time is two fields, so the table is the fourth.
			if f := strings.Fields(l); len(f) > 4 && f[3] == table {
				return f
			}
		}
		t.Fatalf("no row for %s in:\n%s", table, out)
		return nil
	}
	if !strings.Contains(out, "READS_FROM") {
		t.Fatalf("no READS_FROM column:\n%s", out)
	}
	for table, want := range map[string][2]string{
		"users":   {dir.Add(-15 * time.Minute).Format(TSFmt), "ok"},
		"plain":   {"-", "ok"},
		"damaged": {"unreadable", "unknown"},
	} {
		// TSFmt has a space in it, so the two cells are matched as one string.
		if got := strings.Join(row(table), " "); !strings.HasSuffix(got, want[0]+" "+want[1]) {
			t.Errorf("%s row = %q, want it to end in %q %q", table, got, want[0], want[1])
		}
	}
	if !strings.Contains(out, "BASELINE STALENESS NOT EVALUABLE") ||
		!strings.Contains(out, "The deltas beside the newest snapshot could not be read for:\n  shop.damaged\n") {
		t.Errorf("the banner does not name the table whose deltas could not be read:\n%s", out)
	}
	if strings.Contains(out, "shop.users") || strings.Contains(out, "shop.plain") {
		t.Errorf("the banner names a table that was graded:\n%s", out)
	}
	// The floor was read: the banner must not send the operator to it.
	if strings.Contains(out, "delta-coverage floor could not be established") {
		t.Errorf("the banner blames the floor, which is known here:\n%s", out)
	}

	// An unknown floor keeps its own text, and both causes can show at once.
	both := []BaselineInfo{
		{Database: "shop", Table: "damaged", SnapshotTime: dir, Bound: ReadBound{Unread: true}},
		{Database: "shop", Table: "plain", SnapshotTime: dir},
	}
	AnnotateBaselineStaleness(both, DeltaFloor{}, now)
	buf.Reset()
	(&StatusData{Baselines: both}).Write(&buf)
	if out := buf.String(); !strings.Contains(out, "delta-coverage floor could not be established") ||
		!strings.Contains(out, "could not be read for:\n  shop.damaged\n") {
		t.Errorf("unknown floor and unread deltas:\n%s", out)
	}

	// Broken: the row says from where, so "12:00 snapshot, broken" is not a
	// verdict with no visible reason.
	infos = []BaselineInfo{{Database: "shop", Table: "orders", SnapshotTime: dir, Bound: ReadBound{ChainStart: time.Date(2026, 9, 27, 2, 0, 0, 0, time.UTC)}}}
	AnnotateBaselineStaleness(infos, floor, now)
	buf.Reset()
	(&StatusData{Baselines: infos}).Write(&buf)
	out = buf.String()
	t.Logf("\n%s", out[strings.Index(out, "=== Baselines ==="):])
	if got := strings.Join(row("orders"), " "); !strings.HasSuffix(got, time.Date(2026, 9, 27, 2, 0, 0, 0, time.UTC).Format(TSFmt)+" ⚠ broken") {
		t.Errorf("orders row = %q", got)
	}
	if strings.Contains(out, "NOT EVALUABLE") || !strings.Contains(out, "FULL-TABLE RESTORE BROKEN") {
		t.Errorf("banners:\n%s", out)
	}

	var js bytes.Buffer
	infos = append(infos, BaselineInfo{Database: "shop", Table: "plain", SnapshotTime: dir},
		BaselineInfo{Database: "shop", Table: "damaged", SnapshotTime: dir, Bound: ReadBound{Unread: true}})
	AnnotateBaselineStaleness(infos, floor, now)
	if err := (&StatusData{Baselines: infos}).WriteJSON(&js); err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Baselines []struct {
			Table            string `json:"table"`
			Staleness        string `json:"staleness"`
			ReadsFrom        string `json:"reads_from"`
			ReadsFromUnknown bool   `json:"reads_from_unknown"`
		} `json:"baselines"`
		BaselineStaleness string `json:"baseline_staleness"`
	}
	if err := json.Unmarshal(js.Bytes(), &doc); err != nil {
		t.Fatalf("%v\n%s", err, js.String())
	}
	if len(doc.Baselines) != 3 || doc.BaselineStaleness != "broken" {
		t.Fatalf("json: %+v", doc)
	}
	if b := doc.Baselines[0]; b.Staleness != "broken" || b.ReadsFrom != time.Date(2026, 9, 27, 2, 0, 0, 0, time.UTC).Format(TSFmt) || b.ReadsFromUnknown {
		t.Errorf("orders: %+v", b)
	}
	if b := doc.Baselines[1]; b.Staleness != "ok" || b.ReadsFrom != "" || b.ReadsFromUnknown {
		t.Errorf("plain: %+v", b)
	}
	if b := doc.Baselines[2]; b.Staleness != "unknown" || b.ReadsFrom != "" || !b.ReadsFromUnknown {
		t.Errorf("damaged: %+v", b)
	}
	if strings.Contains(js.String(), `"reads_from": ""`) || strings.Count(js.String(), "reads_from_unknown") != 1 {
		t.Errorf("empty fields must be omitted:\n%s", js.String())
	}
}
