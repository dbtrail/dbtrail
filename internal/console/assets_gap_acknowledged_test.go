package console

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/status"
)

// gapAckHarnessJS runs the real covCard, statusFlowModel and continuityBox
// with the payloads /api/coverage and /api/status send for a capture loss,
// before and after an operator acknowledged it.
const gapAckHarnessJS = `
const f = (name) => vm.runInContext(name, ctx);
const flat = (node, cls) => { const out = []; const walk = (n) => { if (!n || !n.children) return; if ((" " + n.className + " ").includes(" " + cls + " ")) out.push({ cls: n.className, text: n.textContent }); n.children.forEach(walk); }; walk(node); return out; };
const cases = JSON.parse(process.argv[3]);
const out = cases.map((c) => {
  const card = f("covCard")(c.cov, { at: "12:44:33 UTC" });
  const m = f("statusFlowModel")({ stream: c.stream }, null, {});
  const station = m.pieces.find((x) => x.title === "DBTrail");
  const box = f("continuityBox")(c.stream, false);
  return {
    chips: flat(card, "cov-chip"), lines: flat(card, "cov-line"),
    station: station ? { tone: station.tone, line: station.line } : null,
    box: box ? { cls: box.className, text: box.children.map((k) => k.textContent) } : null,
  };
});
process.stdout.write(JSON.stringify(out));
`

type gapAckRendered struct {
	Chips, Lines []struct{ Cls, Text string }
	Station      *struct{ Tone, Line string }
	Box          *struct {
		Cls  string
		Text []string
	}
}

// TestAcknowledgedGapIsOnRecordNotGreen pins what the web interface shows for
// a capture loss an operator acknowledged: on record, neutral, with the
// sentence that says what is still refused. Never the green "continuity ok"
// chip and never the green "no gaps" station, which is what it showed while
// an acknowledged loss was reported as "ok".
func TestAcknowledgedGapIsOnRecordNotGreen(t *testing.T) {
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
	type c = map[string]any
	window := func(extra c) c {
		out := c{"delta_from": "2026-06-20 00:00:00", "delta_to": "2026-06-22 14:00:00", "freshness": "current", "lag_seconds": 3}
		for k, v := range extra {
			out[k] = v
		}
		return out
	}
	acked := c{"at": "2026-06-22 12:00:00", "acknowledged_at": "2026-06-22 13:00:00"}
	ackedStream := c{"at": "2026-06-22 12:00:00", "acknowledged_at": "2026-06-22 13:00:00", "detail": "binlogs purged past the saved position"}
	cases := []c{
		// 0: acknowledged, as both endpoints send it
		{"cov": window(c{"continuity": "gap_acknowledged", "gap_acknowledged": acked}),
			"stream": c{"continuity": c{"status": "gap_acknowledged"}, "gap_acknowledged": ackedStream}},
		// 1: the same loss, not acknowledged
		{"cov": window(c{"continuity": "gap_lost"}),
			"stream": c{"continuity": c{"status": "gap_lost"}, "gap_lost": c{"at": "2026-06-22 12:00:00", "detail": "binlogs purged past the saved position"}}},
		// 2: no loss
		{"cov": window(c{"continuity": "ok"}), "stream": c{"continuity": c{"status": "ok"}}},
		// 3: acknowledged, the verdict word alone (no object beside it)
		{"cov": window(c{"continuity": "gap_acknowledged"}), "stream": c{"continuity": c{"status": "gap_acknowledged"}}},
		// 4: the object alone, under a verdict a stale reader left at "ok"
		{"cov": window(c{"continuity": "gap_acknowledged", "gap_acknowledged": acked}),
			"stream": c{"continuity": c{"status": "ok"}, "gap_acknowledged": ackedStream}},
	}
	// 5 and 6 (#2664): the stream object GET /api/status really sends for an
	// acknowledged loss, produced by the status writer and not typed by hand,
	// beside the coverage document GET /api/coverage sends for the same index.
	// 5: the changes still held start AFTER the loss. 6: they still reach it.
	lost := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	realStream := func(floor status.DeltaFloor) (c, string) {
		t.Helper()
		st := &status.StreamStateInfo{Mode: "gtid", ServerID: 7, LastCheckpoint: lost.Add(2 * time.Hour), GapColumnsPresent: true,
			GapLostAt:     sql.NullTime{Time: lost, Valid: true},
			GapAckedAt:    sql.NullTime{Time: lost.Add(time.Hour), Valid: true},
			GapLostDetail: sql.NullString{String: "binlogs purged past the saved position", Valid: true},
			Floor:         floor}
		var buf bytes.Buffer
		if err := (&status.StatusData{Stream: st}).WriteJSON(&buf); err != nil {
			t.Fatal(err)
		}
		var doc struct {
			Stream c `json:"stream"`
		}
		if err := json.Unmarshal(buf.Bytes(), &doc); err != nil || doc.Stream == nil {
			t.Fatalf("status JSON: %v\n%s", err, buf.String())
		}
		return doc.Stream, status.ContinuityStatus(st, nil)
	}
	outStream, outVerdict := realStream(status.DeltaFloor{Hour: lost.Add(time.Hour)})
	inStream, inVerdict := realStream(status.DeltaFloor{Hour: lost.Add(-48 * time.Hour)})
	if outVerdict != status.ContinuityOK || inVerdict != status.ContinuityGapAcknowledged {
		t.Fatalf("fixture: verdicts out=%q in=%q, want ok and gap_acknowledged", outVerdict, inVerdict)
	}
	cases = append(cases,
		c{"cov": window(c{"continuity": outVerdict, "delta_from": "2026-06-22 13:00:00"}), "stream": outStream},
		c{"cov": window(c{"continuity": inVerdict, "gap_acknowledged": acked}), "stream": inStream})

	payload, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "h.js")
	if err := os.WriteFile(path, []byte(renderHarnessJS+gapAckHarnessJS), 0o644); err != nil {
		t.Fatal(err)
	}
	raw, err := exec.Command(node, path, appJS, string(payload)).CombinedOutput()
	if err != nil {
		t.Fatalf("node: %v\n%s", err, raw)
	}
	var got []gapAckRendered
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("parse: %v\n%s", err, raw)
	}
	chipOf := func(r gapAckRendered, sub string) (cls, text string) {
		for _, ch := range r.Chips {
			if strings.Contains(ch.Text, sub) {
				return ch.Cls, ch.Text
			}
		}
		return "", ""
	}
	lineOf := func(r gapAckRendered, sub string) string {
		for _, l := range r.Lines {
			if strings.Contains(l.Text, sub) {
				return l.Cls + " | " + l.Text
			}
		}
		return ""
	}
	show := func(name string, r gapAckRendered) {
		t.Helper()
		for _, ch := range r.Chips {
			t.Logf("%s | Restore coverage chip [%s]: %s", name, ch.Cls, ch.Text)
		}
		for _, l := range r.Lines {
			t.Logf("%s | Restore coverage line [%s]: %s", name, l.Cls, l.Text)
		}
		if r.Station != nil {
			t.Logf("%s | Status, DBTrail station [%s]: %s", name, r.Station.Tone, r.Station.Line)
		}
		if r.Box != nil {
			t.Logf("%s | Status box [%s]:\n    %s", name, r.Box.Cls, strings.Join(r.Box.Text, "\n    "))
		}
	}
	names := []string{"acknowledged", "not acknowledged", "no loss", "acknowledged, verdict only", "acknowledged, object under a stale ok",
		"acknowledged, older than the changes still held (real status JSON)", "acknowledged, inside the changes still held (real status JSON)"}
	for i, r := range got {
		show(names[i], r)
	}

	const sentence = "Changes were lost at 2026-06-22 12:00:00 UTC and that was acknowledged at 2026-06-22 13:00:00 UTC. " +
		"Updates of the copy and time travel that start from a full read of the database older than that moment and cross it are refused. " +
		"A full read taken after it (Read database now, on the Snapshots page) is not affected."

	for _, i := range []int{0, 3, 4, 6} {
		r := got[i]
		for _, ch := range r.Chips {
			if strings.Contains(ch.Text, "continuity") {
				t.Errorf("%s: the card shows a continuity verdict chip %q; an acknowledged loss is said in words", names[i], ch.Text)
			}
		}
		cls, text := chipOf(r, "changes lost, on record")
		if text != "changes lost, on record" {
			t.Errorf("%s: chips = %+v, want one that reads \"changes lost, on record\"", names[i], r.Chips)
		}
		for _, tone := range []string{" ok", " bad"} {
			if strings.Contains(cls+" ", tone+" ") {
				t.Errorf("%s: the on-record chip is class %q; it must be neutral, never green or red", names[i], cls)
			}
		}
		if r.Station == nil || r.Station.Tone != "none" || r.Station.Line != "changes lost, on record" {
			t.Errorf("%s: DBTrail station = %+v, want the neutral \"changes lost, on record\"", names[i], r.Station)
		}
		if r.Station != nil && (r.Station.Tone == "ok" || strings.Contains(r.Station.Line, "no gaps")) {
			t.Errorf("%s: the station says %q in %q: green \"no gaps\" over a known loss", names[i], r.Station.Line, r.Station.Tone)
		}
		if r.Box == nil || !strings.Contains(" "+r.Box.Cls+" ", " muted-box ") {
			t.Fatalf("%s: Status box = %+v, want a muted box", names[i], r.Box)
		}
		if r.Box.Text[0] != "Changes lost, on record (acknowledged)" {
			t.Errorf("%s: box title = %q", names[i], r.Box.Text[0])
		}
	}
	// With the two instants, the sentence is exact, on both pages.
	for _, i := range []int{0, 4, 6} {
		if l := lineOf(got[i], "Changes were lost"); l != "cov-line warn | "+sentence {
			t.Errorf("%s: card line = %q\nwant %q", names[i], l, "cov-line warn | "+sentence)
		}
		if got[i].Box.Text[1] != sentence {
			t.Errorf("%s: box text = %q\nwant %q", names[i], got[i].Box.Text[1], sentence)
		}
		if len(got[i].Box.Text) != 3 || got[i].Box.Text[2] != "What capture recorded: binlogs purged past the saved position" {
			t.Errorf("%s: box detail = %q", names[i], got[i].Box.Text)
		}
	}
	// Without them (verdict word only) it still says what is refused, with no "undefined".
	if l := lineOf(got[3], "Changes were lost"); !strings.Contains(l, "Changes were lost and that was acknowledged. Updates of the copy") || strings.Contains(l, "undefined") {
		t.Errorf("verdict only: card line = %q", l)
	}
	for _, r := range got {
		for _, l := range r.Lines {
			if strings.ContainsAny(l.Text, "—–") {
				t.Errorf("a dash in operator text: %q", l.Text)
			}
			// The sentence is shown before and after the operator read the
			// database in full, so it must not ask for that read (#2664).
			if strings.Contains(l.Text, "until the database is read in full again") {
				t.Errorf("the card gives an instruction that may already be followed: %q", l.Text)
			}
		}
		if r.Box != nil && strings.Contains(strings.Join(r.Box.Text, " "), "until the database is read in full again") {
			t.Errorf("the Status box gives an instruction that may already be followed: %q", r.Box.Text)
		}
	}

	// #2664: once the changes still held start after an acknowledged loss,
	// both pages read like an index with no loss. The Status page used to
	// keep "changes lost, on record" there forever while the Overview's card
	// said "continuity ok".
	gone := got[5]
	if cls, _ := chipOf(gone, "continuity ok"); !strings.Contains(cls, "ok") {
		t.Errorf("older than the window: chips = %+v, want a green \"continuity ok\"", gone.Chips)
	}
	if gone.Station == nil || gone.Station.Tone != "ok" || gone.Station.Line != "no gaps" {
		t.Errorf("older than the window: DBTrail station = %+v, want the green \"no gaps\" the card agrees with", gone.Station)
	}
	if gone.Box != nil {
		t.Errorf("older than the window: the Status page still shows a box: %+v", gone.Box)
	}
	if l := lineOf(gone, "Changes were lost"); l != "" {
		t.Errorf("older than the window: the card still says %q", l)
	}

	// The twins: an unacknowledged loss stays red, no loss stays green.
	if cls, _ := chipOf(got[1], "continuity gap_lost"); !strings.Contains(cls, "bad") {
		t.Errorf("not acknowledged: chips = %+v, want a red \"continuity gap_lost\"", got[1].Chips)
	}
	if got[1].Station == nil || got[1].Station.Tone != "bad" || got[1].Box == nil || !strings.Contains(got[1].Box.Cls, "error-box") {
		t.Errorf("not acknowledged: station = %+v box = %+v, want red", got[1].Station, got[1].Box)
	}
	if cls, _ := chipOf(got[2], "continuity ok"); !strings.Contains(cls, "ok") {
		t.Errorf("no loss: chips = %+v, want a green \"continuity ok\"", got[2].Chips)
	}
	if got[2].Station == nil || got[2].Station.Tone != "ok" || got[2].Station.Line != "no gaps" || got[2].Box != nil {
		t.Errorf("no loss: station = %+v box = %+v, want the green \"no gaps\" and no box", got[2].Station, got[2].Box)
	}
}
