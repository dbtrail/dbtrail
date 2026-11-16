package cascade

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/event"
	"github.com/dbtrail/dbtrail/internal/metadata"
	"github.com/dbtrail/dbtrail/internal/query"
)

// memFetcher answers Fetch from a fixed list of events, filtered the way the
// logged path asks: schema, table, event type and the one-second window.
type memFetcher struct {
	rows  []query.ResultRow
	err   error
	calls int
}

func (m *memFetcher) Fetch(_ context.Context, o query.Options) ([]query.ResultRow, error) {
	m.calls++
	if m.err != nil {
		return nil, m.err
	}
	var out []query.ResultRow
	for _, r := range m.rows {
		if o.Schema != "" && r.SchemaName != o.Schema || o.Table != "" && r.TableName != o.Table {
			continue
		}
		if o.EventType != nil && r.EventType != *o.EventType {
			continue
		}
		if o.Since != nil && r.EventTimestamp.Before(*o.Since) || o.Until != nil && r.EventTimestamp.After(*o.Until) {
			continue
		}
		out = append(out, r)
		if o.Limit > 0 && len(out) == o.Limit {
			break
		}
	}
	return out, nil
}

var t0 = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

func u64(v uint64) *uint64 { return &v }
func u32(v uint32) *uint32 { return &v }

// ev is an event of the parent's transaction unless the test changes it.
func ev(id uint64, table string, typ event.EventType, pk string, before, after map[string]any) query.ResultRow {
	return query.ResultRow{
		EventID: id, SchemaName: "s", TableName: table, EventType: typ, PKValues: pk,
		RowBefore: before, RowAfter: after, EventTimestamp: t0,
		BinlogFile: "binlog.000007", CommitTsUS: u64(1_000_001), ConnectionID: u32(42),
	}
}

func fk(table, col, refTable, refCol, del, upd string) CascadeFK {
	return CascadeFK{Schema: "s", Table: table, ConstraintName: "fk_" + table + "_" + col, Column: col,
		ReferencedSchema: "s", ReferencedTable: refTable, ReferencedColumn: refCol, DeleteRule: del, UpdateRule: upd}
}

func pks(rows []query.ResultRow) string {
	var s []string
	for _, r := range rows {
		s = append(s, r.TableName+":"+r.PKValues)
	}
	return strings.Join(s, ",")
}

// A change is judged by the start whose proven span (start to last reading)
// holds it, and by nothing else.
func TestLoggingAt(t *testing.T) {
	// An 8.4, then the 9.7 it was upgraded to.
	a := metadata.SourceFKLogging{ServerUUID: "u1", ServerStarted: t0.Add(-48 * time.Hour), LastSeen: t0.Add(-25 * time.Hour)}
	b := metadata.SourceFKLogging{ServerUUID: "u1", ServerStarted: t0.Add(-24 * time.Hour), LastSeen: t0.Add(time.Hour), NativeForeignKeys: "OFF"}
	l := NewLogging([]metadata.SourceFKLogging{b, a})
	for _, tc := range []struct {
		name string
		at   time.Time
		want loggingMode
	}{
		{"before the first recorded start", a.ServerStarted.Add(-time.Hour), modeUnknown},
		{"the first seconds of a start: maybe the server before", a.ServerStarted.Add(time.Second), modeUnknown},
		{"inside the 8.4 span", a.ServerStarted.Add(time.Hour), modeNotLogged},
		{"at the 8.4's last reading", a.LastSeen, modeNotLogged},
		{"after the 8.4's last reading, before the 9.7 started: nobody read it", a.LastSeen.Add(30 * time.Minute), modeUnknown},
		{"inside the 9.7 span", t0, modeLogged},
		{"after the last reading of all", b.LastSeen.Add(time.Minute), modeUnknown},
	} {
		if got := l.at(tc.at); got != tc.want {
			t.Errorf("%s: at = %v, want %v", tc.name, got, tc.want)
		}
	}
	// A restart nobody read between two recorded starts: its changes are not
	// credited to the start before it.
	gap := NewLogging([]metadata.SourceFKLogging{
		{ServerStarted: t0.Add(-48 * time.Hour), LastSeen: t0.Add(-40 * time.Hour)},
		{ServerStarted: t0.Add(-10 * time.Hour), LastSeen: t0.Add(time.Hour), NativeForeignKeys: "OFF"},
	})
	if got := gap.at(t0.Add(-20 * time.Hour)); got != modeUnknown {
		t.Errorf("between two recorded starts: at = %v, want unknown", got)
	}
	// Two servers recorded over the same time (a failover) that disagree.
	both := NewLogging([]metadata.SourceFKLogging{
		{ServerUUID: "primary", ServerStarted: t0.Add(-10 * time.Hour), LastSeen: t0.Add(time.Hour)},
		{ServerUUID: "replica", ServerStarted: t0.Add(-30 * time.Hour), LastSeen: t0.Add(time.Hour), NativeForeignKeys: "OFF"},
	})
	if got := both.at(t0); got != modeUnknown {
		t.Errorf("two servers that disagree over the same time: at = %v, want unknown", got)
	}
	on := NewLogging([]metadata.SourceFKLogging{{ServerStarted: t0.Add(-time.Hour), LastSeen: t0.Add(time.Hour), NativeForeignKeys: "ON"}})
	if got := on.at(t0); got != modeNotLogged {
		t.Errorf("innodb_native_foreign_keys=ON: at = %v, want not logged", got)
	}
	if got := (*Logging)(nil).at(t0); got != modeUnknown {
		t.Errorf("nil Logging: at = %v, want unknown", got)
	}
}

func TestSameKey(t *testing.T) {
	for _, tc := range []struct {
		a, b any
		want bool
	}{
		{float64(1), float64(1), true},
		{float64(1), "1", true},
		{"abc", "ABC  ", true},
		{"abc", "abd", false},
		{"", nil, false},
		{float64(1), float64(2), false},
	} {
		if got := sameKey(tc.a, tc.b); got != tc.want {
			t.Errorf("sameKey(%#v, %#v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestCollectLogged(t *testing.T) {
	ctx := context.Background()
	parent := ev(10, "p", event.EventDelete, "1", map[string]any{"id": float64(1)}, nil)
	fks := []CascadeFK{
		fk("c", "pid", "p", "id", "CASCADE", "RESTRICT"),
		fk("g", "cid", "c", "id", "CASCADE", "RESTRICT"),
		fk("n", "pid", "p", "id", "SET NULL", "RESTRICT"),
	}
	child := ev(7, "c", event.EventDelete, "100", map[string]any{"id": float64(100), "pid": float64(1)}, nil)
	grand := ev(6, "g", event.EventDelete, "900", map[string]any{"id": float64(900), "cid": float64(100)}, nil)
	nulled := ev(8, "n", event.EventUpdate, "50", map[string]any{"id": float64(50), "pid": float64(1)}, map[string]any{"id": float64(50), "pid": nil})

	otherTrx := ev(11, "c", event.EventDelete, "101", map[string]any{"id": float64(101), "pid": float64(1)}, nil)
	otherTrx.CommitTsUS = u64(2_000_002) // the same second, another transaction
	otherKey := ev(12, "c", event.EventDelete, "102", map[string]any{"id": float64(102), "pid": float64(2)}, nil)
	otherConn := ev(13, "c", event.EventDelete, "103", map[string]any{"id": float64(103), "pid": float64(1)}, nil)
	otherConn.ConnectionID = u32(43)
	otherFile := ev(14, "c", event.EventDelete, "104", map[string]any{"id": float64(104), "pid": float64(1)}, nil)
	otherFile.BinlogFile = "binlog.000008"
	notNulled := ev(15, "n", event.EventUpdate, "51", map[string]any{"id": float64(51), "pid": float64(1)}, map[string]any{"id": float64(51), "pid": float64(3)})

	t.Run("the cascade's own rows, down two levels, and nothing else", func(t *testing.T) {
		f := &memFetcher{rows: []query.ResultRow{parent, child, grand, nulled, otherTrx, otherKey, otherConn, otherFile, notNulled}}
		res, err := collectLogged(ctx, f, fks, []query.ResultRow{parent}, Options{}, false)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := pks(res.Victims), "g:900,c:100,n:50"; got != want {
			t.Errorf("victims %s, want %s (oldest first)", got, want)
		}
		if len(res.Incomplete) != 0 {
			t.Errorf("incomplete: %v", res.Incomplete)
		}
	})

	t.Run("no commit time: refused for that parent, said", func(t *testing.T) {
		p := parent
		p.CommitTsUS = nil
		res, err := collectLogged(ctx, &memFetcher{rows: []query.ResultRow{child}}, fks, []query.ResultRow{p}, Options{}, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Victims) != 0 || len(res.Incomplete) != 1 || !strings.Contains(res.Incomplete[0], "no commit time") {
			t.Errorf("victims %s, incomplete %v; want none and the commit-time caveat", pks(res.Victims), res.Incomplete)
		}
	})

	t.Run("a parent key update rewrites the children: the parent is a key root", func(t *testing.T) {
		upFKs := []CascadeFK{fk("c", "pid", "p", "id", "RESTRICT", "CASCADE")}
		up := ev(20, "p", event.EventUpdate, "1", map[string]any{"id": float64(1)}, map[string]any{"id": float64(9)})
		moved := ev(19, "c", event.EventUpdate, "100", map[string]any{"id": float64(100), "pid": float64(1)}, map[string]any{"id": float64(100), "pid": float64(9)})
		other := ev(21, "p", event.EventUpdate, "2", map[string]any{"id": float64(2), "x": "a"}, map[string]any{"id": float64(2), "x": "b"})
		res, err := collectLogged(ctx, &memFetcher{rows: []query.ResultRow{up, moved}}, upFKs, []query.ResultRow{up, other}, Options{}, false)
		if err != nil {
			t.Fatal(err)
		}
		if got := pks(res.Victims); got != "c:100" {
			t.Errorf("victims %s, want c:100", got)
		}
		if len(res.KeyUpdateParents) != 1 || res.KeyUpdateParents[0].EventID != 20 {
			t.Errorf("key roots %v, want only the update that moved the key", res.KeyUpdateParents)
		}
	})

	t.Run("more rows than the limit: said", func(t *testing.T) {
		res, _ := collectLogged(ctx, &memFetcher{rows: []query.ResultRow{child, otherConn, otherFile}}, fks[:1], []query.ResultRow{parent}, Options{CandidateLimit: 1}, false)
		if len(res.Incomplete) != 1 || !strings.Contains(res.Incomplete[0], "more than 1") {
			t.Errorf("incomplete %v, want the limit caveat", res.Incomplete)
		}
	})

	t.Run("the unknown case's search keeps only read failures and truncation", func(t *testing.T) {
		p := parent
		p.CommitTsUS = nil
		res, _ := collectLogged(ctx, &memFetcher{rows: []query.ResultRow{child}}, fks, []query.ResultRow{p}, Options{}, true)
		if len(res.Incomplete) != 0 || pks(res.Victims) != "c:100" {
			t.Errorf("probe without a commit time: victims %s, incomplete %v; want c:100 found by file, connection and second, and no caveat", pks(res.Victims), res.Incomplete)
		}
		res, _ = collectLogged(ctx, &memFetcher{rows: []query.ResultRow{child, otherConn, otherFile}}, fks[:1], []query.ResultRow{parent}, Options{CandidateLimit: 1}, true)
		if len(res.Incomplete) != 1 {
			t.Errorf("probe over the limit: incomplete %v, want the limit caveat", res.Incomplete)
		}
	})

	t.Run("a failed read is an error and a caveat", func(t *testing.T) {
		res, err := collectLogged(ctx, &memFetcher{err: errors.New("boom")}, fks[:1], []query.ResultRow{parent}, Options{}, false)
		if err == nil || len(res.Incomplete) != 1 {
			t.Errorf("err %v, incomplete %v; want both", err, res.Incomplete)
		}
	})
}

// Through SynthesizeVictims: a parent written by a server that logs its
// cascades gets the logged rows; one older than every record gets synthesis
// and, when the index holds child changes of its transaction, a caveat that
// names them instead of returning fewer rows in silence.
func TestSynthesizeVictims_byLogging(t *testing.T) {
	ctx := context.Background()
	fks := []CascadeFK{fk("c", "pid", "p", "id", "CASCADE", "RESTRICT")}
	parent := ev(10, "p", event.EventDelete, "1", map[string]any{"id": float64(1)}, nil)
	child := ev(7, "c", event.EventDelete, "100", map[string]any{"id": float64(100), "pid": float64(1)}, nil)
	f := &memFetcher{rows: []query.ResultRow{parent, child}}

	logged := NewLogging([]metadata.SourceFKLogging{{ServerStarted: t0.Add(-time.Hour), LastSeen: t0.Add(time.Hour), NativeForeignKeys: "OFF"}})
	res, err := SynthesizeVictims(ctx, f, fks, []query.ResultRow{parent}, Options{Logging: logged})
	if err != nil {
		t.Fatal(err)
	}
	if got := pks(res.Victims); got != "c:100" || len(res.Incomplete) != 0 {
		t.Errorf("logged: victims %s, incomplete %v; want c:100 and none", got, res.Incomplete)
	}
	if res.Victims[0].EventID != 7 {
		t.Errorf("logged: the victim is not the logged event: %+v", res.Victims[0])
	}

	res, _ = SynthesizeVictims(ctx, f, fks, []query.ResultRow{parent}, Options{Logging: NewLogging(nil)})
	found := false
	for _, m := range res.Incomplete {
		if strings.Contains(m, "does not record whether the source wrote its foreign-key cascades") && strings.Contains(m, "s.c 100") {
			found = true
		}
	}
	if !found {
		t.Errorf("unknown: incomplete %v, want the caveat naming s.c 100", res.Incomplete)
	}

	// The record says logged, but the cascade is not in the index: the
	// children still referenced the parent at its delete (a borrowed
	// timestamp, a wrong record). The synthesis beside the logged walk keeps
	// them, with a warning.
	alive := ev(5, "c", event.EventInsert, "200", nil, map[string]any{"id": float64(200), "pid": float64(1)})
	alive.EventTimestamp = t0.Add(-time.Minute)
	alive.CommitTsUS = u64(999)
	res, err = SynthesizeVictims(ctx, &memFetcher{rows: []query.ResultRow{parent, alive}}, fks, []query.ResultRow{parent}, Options{Logging: logged})
	if err != nil {
		t.Fatal(err)
	}
	if got := pks(res.Victims); got != "c:200" {
		t.Errorf("wrong record: victims %s, want the synthesized c:200", got)
	}
	warned := false
	for _, w := range res.Warnings {
		if strings.Contains(w, "reconstructed as well") {
			warned = true
		}
	}
	if !warned {
		t.Errorf("wrong record: warnings %v, want the one about rows reconstructed as well", res.Warnings)
	}

	// One call with a root of each kind: each arm's rows come back together.
	notLogged := NewLogging([]metadata.SourceFKLogging{
		{ServerStarted: t0.Add(-10 * time.Hour), LastSeen: t0.Add(-5 * time.Hour)},
		{ServerStarted: t0.Add(-time.Hour), LastSeen: t0.Add(time.Hour), NativeForeignKeys: "OFF"},
	})
	old := ev(30, "p", event.EventDelete, "7", map[string]any{"id": float64(7)}, nil)
	old.EventTimestamp = t0.Add(-6 * time.Hour)
	oldChild := ev(29, "c", event.EventInsert, "700", nil, map[string]any{"id": float64(700), "pid": float64(7)})
	oldChild.EventTimestamp = t0.Add(-7 * time.Hour)
	res, err = SynthesizeVictims(ctx, &memFetcher{rows: []query.ResultRow{parent, child, old, oldChild}}, fks,
		[]query.ResultRow{old, parent}, Options{Logging: notLogged})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, v := range res.Victims {
		got[v.TableName+":"+v.PKValues] = true
	}
	if !got["c:700"] || !got["c:100"] || len(res.Victims) != 2 {
		t.Errorf("mixed roots: victims %s, want c:700 (synthesized) and c:100 (logged)", pks(res.Victims))
	}
}

// A child row deleted twice under two roots keeps one restore (no double
// INSERT), while every logged UPDATE of one row is its own reversal.
func TestDedupVictimsNewest_loggedUpdatesStay(t *testing.T) {
	a := ev(1, "c", event.EventUpdate, "5", nil, nil)
	b := ev(2, "c", event.EventUpdate, "5", nil, nil)
	d1 := ev(3, "c", event.EventDelete, "6", nil, nil)
	d2 := ev(4, "c", event.EventDelete, "6", nil, nil)
	d2.EventTimestamp = t0.Add(time.Minute)
	got := dedupVictimsNewest([]query.ResultRow{a, b, d1, d2})
	if len(got) != 3 || got[2].EventID != 4 {
		t.Errorf("got %v, want both updates and the newest delete", got)
	}
}
