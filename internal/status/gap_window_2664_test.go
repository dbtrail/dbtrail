package status

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// #2664. An acknowledged capture loss is reported while the changes still
// held reach it, and no longer once they start after it. The coverage summary
// already said so; `bintrail status` (text and JSON) kept the loss on record
// forever, so the two surfaces disagreed about the same index.

// gapWindowFloors are the starts of the window of changes still held, around
// a loss at 2026-06-22 12:00:00 (gapLostStream). onRecord is whether an
// ACKNOWLEDGED loss is still reported under that floor.
func gapWindowFloors() []struct {
	name     string
	floor    DeltaFloor
	onRecord bool
} {
	lost := gapLostStream().GapLostAt.Time
	return []struct {
		name     string
		floor    DeltaFloor
		onRecord bool
	}{
		{"window starts before the loss", DeltaFloor{Hour: lost.Add(-48 * time.Hour)}, true},
		{"window starts at the loss", DeltaFloor{Hour: lost}, true},
		{"window starts one hour after the loss", DeltaFloor{Hour: lost.Add(time.Hour)}, false},
		{"window starts months after the loss", DeltaFloor{Hour: lost.Add(90 * 24 * time.Hour)}, false},
		{"window start unknown", DeltaFloor{}, true},
		{"archives may reach further back than the floor", DeltaFloor{Hour: lost.Add(time.Hour), BelowIsUnknown: true}, true},
	}
}

type gapWindowDoc struct {
	Stream struct {
		Continuity struct {
			Status string `json:"status"`
		} `json:"continuity"`
		GapLost *struct {
			At string `json:"at"`
		} `json:"gap_lost"`
		GapAcknowledged *struct {
			At             string `json:"at"`
			AcknowledgedAt string `json:"acknowledged_at"`
		} `json:"gap_acknowledged"`
	} `json:"stream"`
}

func renderGapWindow(t *testing.T, s *StreamStateInfo) (text string, doc gapWindowDoc, raw string) {
	t.Helper()
	var tb, jb bytes.Buffer
	WriteStatus(&tb, nil, nil, nil, nil, nil, s)
	if err := WriteStatusJSON(&jb, nil, nil, nil, nil, nil, s); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(jb.Bytes(), &doc); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, jb.String())
	}
	return tb.String(), doc, jb.String()
}

// streamLines is the Stream section's continuity lines, for the test log.
func streamLines(text string) string {
	var out []string
	for _, l := range strings.Split(text, "\n") {
		if strings.Contains(l, "Continuity:") || strings.Contains(l, "Acknowledged loss") ||
			strings.Contains(l, "Snapshot updates and") || strings.Contains(l, "refused.") ||
			strings.Contains(l, "while the index still holds") || strings.Contains(l, "PERMANENTLY LOST") {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}

func TestAcknowledgedLoss_reportedWhileTheWindowReachesIt(t *testing.T) {
	for _, c := range gapWindowFloors() {
		t.Run(c.name, func(t *testing.T) {
			s := ackedLostStream()
			s.Floor = c.floor
			wantVerdict := ContinuityOK
			if c.onRecord {
				wantVerdict = ContinuityGapAcknowledged
			}
			if got := s.GapOnRecord(); got != c.onRecord {
				t.Errorf("GapOnRecord = %v, want %v", got, c.onRecord)
			}
			if got := ContinuityStatus(s, nil); got != wantVerdict {
				t.Errorf("ContinuityStatus = %q, want %q", got, wantVerdict)
			}
			// The record is a fact and the alarm never comes back, in or out
			// of the window.
			if !s.GapAcknowledged() || s.GapUnacknowledged() || !s.GapLostAt.Valid {
				t.Errorf("the window changed the record: acked=%v unacked=%v lost=%v", s.GapAcknowledged(), s.GapUnacknowledged(), s.GapLostAt)
			}

			text, doc, raw := renderGapWindow(t, s)
			t.Logf("status text:\n%s", streamLines(text))
			t.Logf("status JSON: continuity=%q gap_acknowledged=%v", doc.Stream.Continuity.Status, doc.Stream.GapAcknowledged != nil)
			if doc.Stream.Continuity.Status != wantVerdict {
				t.Errorf("JSON continuity = %q, want %q\n%s", doc.Stream.Continuity.Status, wantVerdict, raw)
			}
			if doc.Stream.GapLost != nil {
				t.Errorf("an acknowledged loss printed gap_lost:\n%s", raw)
			}
			if strings.Contains(text, "GAP LOST") || strings.Contains(text, "PERMANENTLY LOST") {
				t.Errorf("an acknowledged loss raised the alarm:\n%s", text)
			}
			if c.onRecord {
				if doc.Stream.GapAcknowledged == nil || doc.Stream.GapAcknowledged.At != "2026-06-22 12:00:00" ||
					doc.Stream.GapAcknowledged.AcknowledgedAt != "2026-06-22 13:00:00" {
					t.Errorf("JSON gap_acknowledged = %+v, want both instants\n%s", doc.Stream.GapAcknowledged, raw)
				}
				for _, want := range []string{
					"  Continuity:      no gaps since the loss acknowledged below (not a liveness check)\n",
					"  Acknowledged loss: capture permanently lost events at 2026-06-22 12:00:00 (acknowledged 2026-06-22 13:00:00).\n",
					"  Snapshot updates and reconstructions whose window spans that moment are\n",
					"  refused. A full snapshot taken after it is not affected. This is reported\n",
					"  while the index or its archives still hold the hour it happened in.\n",
				} {
					if !strings.Contains(text, want) {
						t.Errorf("status text missing %q:\n%s", want, text)
					}
				}
				if strings.Contains(text, "no gaps in the captured range") {
					t.Errorf("status text claims no gaps over a loss the window reaches:\n%s", text)
				}
				return
			}
			// Out of the window: exactly what a stream with no loss prints.
			if doc.Stream.GapAcknowledged != nil || strings.Contains(raw, "gap_acknowledged") {
				t.Errorf("JSON still carries gap_acknowledged for a loss older than the window:\n%s", raw)
			}
			if !strings.Contains(text, "  Continuity:      no gaps in the captured range (not a liveness check)\n") {
				t.Errorf("status text is not the plain no-gaps line:\n%s", text)
			}
			for _, banned := range []string{"Acknowledged loss", "refused", "loss acknowledged below"} {
				if strings.Contains(text, banned) {
					t.Errorf("status text still says %q about a loss older than the window:\n%s", banned, text)
				}
			}
		})
	}
}

// The sentence must be true before AND after the operator took the full
// snapshot: it used to say "still refused until a full snapshot is taken
// after it", an instruction that stayed on screen once followed.
func TestAcknowledgedLoss_textGivesNoInstructionAlreadyFollowed(t *testing.T) {
	text, _, _ := renderGapWindow(t, ackedLostStream())
	for _, banned := range []string{"until", "still refused"} {
		if strings.Contains(streamLines(text), banned) {
			t.Errorf("the acknowledged-loss lines say %q:\n%s", banned, streamLines(text))
		}
	}
	for _, l := range strings.Split(streamLines(text), "\n") {
		if strings.Contains(l, "Acknowledged loss") || strings.Contains(l, "refused") || strings.Contains(l, "while the index") {
			if strings.ContainsAny(l, "—–") {
				t.Errorf("a dash in the acknowledged-loss text: %q", l)
			}
		}
	}
}

// The window NEVER applies to a loss nobody acknowledged: however old, it
// stays gap_lost with the alarm.
func TestUnacknowledgedLoss_neverNarrowedByTheWindow(t *testing.T) {
	for _, c := range gapWindowFloors() {
		t.Run(c.name, func(t *testing.T) {
			s := gapLostStream()
			s.GapColumnsPresent = true
			s.Floor = c.floor
			if s.GapOnRecord() {
				t.Error("GapOnRecord is true for an unacknowledged loss")
			}
			if !s.GapUnacknowledged() {
				t.Error("the window silenced an unacknowledged loss")
			}
			if got := ContinuityStatus(s, nil); got != ContinuityGapLost {
				t.Errorf("ContinuityStatus = %q, want %q", got, ContinuityGapLost)
			}
			text, doc, raw := renderGapWindow(t, s)
			t.Logf("status text:\n%s", streamLines(text))
			if doc.Stream.Continuity.Status != ContinuityGapLost || doc.Stream.GapLost == nil || doc.Stream.GapAcknowledged != nil {
				t.Errorf("JSON = continuity %q gap_lost %v gap_acknowledged %v\n%s",
					doc.Stream.Continuity.Status, doc.Stream.GapLost, doc.Stream.GapAcknowledged, raw)
			}
			for _, want := range []string{"Continuity:      ⚠ GAP LOST at 2026-06-22 12:00:00", "=== ⚠ EVENTS PERMANENTLY LOST ==="} {
				if !strings.Contains(text, want) {
					t.Errorf("status text missing %q:\n%s", want, text)
				}
			}
		})
	}
	// A second loss after an acknowledgement, older than the window: the
	// stamp withdrew the acknowledgement, so it alarms.
	again := ackedLostStream()
	again.GapLostAt = nt(again.GapAckedAt.Time.Add(time.Hour))
	again.GapAckedAt = sql.NullTime{}
	again.Floor = DeltaFloor{Hour: again.GapLostAt.Time.Add(24 * time.Hour)}
	if got := ContinuityStatus(again, nil); got != ContinuityGapLost {
		t.Errorf("a loss after an acknowledgement, older than the window = %q, want %q", got, ContinuityGapLost)
	}
}

// The other verdicts do not move with the window either.
func TestGapWindow_otherVerdictsUnchanged(t *testing.T) {
	for _, c := range gapWindowFloors() {
		clean := &StreamStateInfo{Mode: "gtid", LastCheckpoint: time.Now(), GapColumnsPresent: true, Floor: c.floor}
		if got := ContinuityStatus(clean, nil); got != ContinuityOK {
			t.Errorf("%s: no loss = %q, want ok", c.name, got)
		}
		text, doc, _ := renderGapWindow(t, clean)
		if doc.Stream.Continuity.Status != ContinuityOK || !strings.Contains(text, "no gaps in the captured range") {
			t.Errorf("%s: no loss renders %q:\n%s", c.name, doc.Stream.Continuity.Status, text)
		}
		// A legacy index cannot say whether a loss happened; a floor after
		// "the loss" must not turn that into ok.
		legacy := &StreamStateInfo{Mode: "gtid", LastCheckpoint: time.Now(), GapColumnsPresent: false, Floor: c.floor}
		if got := ContinuityStatus(legacy, nil); got != ContinuityUnknown {
			t.Errorf("%s: legacy index = %q, want unknown", c.name, got)
		}
		text, doc, _ = renderGapWindow(t, legacy)
		if doc.Stream.Continuity.Status != ContinuityUnknown || !strings.Contains(text, "not evaluated (legacy index") {
			t.Errorf("%s: legacy index renders %q:\n%s", c.name, doc.Stream.Continuity.Status, text)
		}
	}
	var nilStream *StreamStateInfo
	if nilStream.GapOnRecord() {
		t.Error("GapOnRecord on a nil stream")
	}
	if got := ContinuityStatus(nil, nil); got != ContinuityNone {
		t.Errorf("no stream row = %q, want none", got)
	}
	if got := ContinuityStatus(nil, errors.New("boom")); got != ContinuityUnavailable {
		t.Errorf("unreadable stream = %q, want unavailable", got)
	}
}

// LoadGapWindow reads the floor for an acknowledged loss and for nothing else.
func TestLoadGapWindow(t *testing.T) {
	newDB := func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		return db, mock
	}
	noArchives := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"min", "max", "sources"}).AddRow(nil, nil, 0)
	}
	ctx := context.Background()

	t.Run("acknowledged loss older than the oldest partition: floor read, verdict ok", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_2026062300", "p_future"))
		mock.ExpectQuery(covArchQ).WillReturnRows(noArchives())
		s := ackedLostStream()
		LoadGapWindow(ctx, db, "binlog_index", s)
		if !s.Floor.Hour.Equal(time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC)) {
			t.Fatalf("Floor = %+v, want the oldest partition's hour", s.Floor)
		}
		if got := ContinuityStatus(s, nil); got != ContinuityOK {
			t.Errorf("verdict = %q, want ok", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	})

	t.Run("acknowledged loss inside the oldest partition: on record", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_2026062212", "p_2026062213", "p_future"))
		mock.ExpectQuery(covArchQ).WillReturnRows(noArchives())
		s := ackedLostStream()
		LoadGapWindow(ctx, db, "binlog_index", s)
		if got := ContinuityStatus(s, nil); got != ContinuityGapAcknowledged {
			t.Errorf("verdict = %q, want gap_acknowledged", got)
		}
	})

	t.Run("archives reach back past the loss: on record", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_2026062300", "p_future"))
		mock.ExpectQuery(covArchQ).WillReturnRows(sqlmock.NewRows([]string{"min", "max", "sources"}).
			AddRow("p_2026062200", "p_2026062223", 1))
		s := ackedLostStream()
		LoadGapWindow(ctx, db, "binlog_index", s)
		if got := ContinuityStatus(s, nil); got != ContinuityGapAcknowledged {
			t.Errorf("verdict = %q (floor %+v), want gap_acknowledged: the archived hours still hold the loss", got, s.Floor)
		}
	})

	t.Run("floor unreadable: stays on record", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery(covPartsQ).WillReturnError(errors.New("conn reset"))
		s := ackedLostStream()
		LoadGapWindow(ctx, db, "binlog_index", s)
		if !s.Floor.Hour.IsZero() {
			t.Errorf("Floor = %+v after a failed read, want unknown", s.Floor)
		}
		if got := ContinuityStatus(s, nil); got != ContinuityGapAcknowledged {
			t.Errorf("verdict = %q, want gap_acknowledged: ok is never asserted from an unread floor", got)
		}
	})

	// For every other stream the floor is not read at all: the expected
	// queries stay unconsumed, which is what ExpectationsWereMet reports
	// (sqlmock does not report a surplus query, so this is the usable check).
	notAsked := func(name string, s *StreamStateInfo) {
		t.Run(name+": floor not read", func(t *testing.T) {
			db, mock := newDB(t)
			mock.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_2026062300", "p_future"))
			mock.ExpectQuery(covArchQ).WillReturnRows(noArchives())
			LoadGapWindow(ctx, db, "binlog_index", s)
			if err := mock.ExpectationsWereMet(); err == nil {
				t.Error("the delta floor was read for a stream with no acknowledged loss")
			}
			if s != nil && !s.Floor.Hour.IsZero() {
				t.Errorf("Floor = %+v, want untouched", s.Floor)
			}
		})
	}
	unacked := gapLostStream()
	unacked.GapColumnsPresent = true
	notAsked("unacknowledged loss", unacked)
	notAsked("no loss", &StreamStateInfo{GapColumnsPresent: true})
	notAsked("legacy index", &StreamStateInfo{GapColumnsPresent: false})
	notAsked("no stream row", nil)
}

// The coverage summary and the status report grade one stream against one
// floor and say the same word.
func TestCoverageSummaryAndStatusAgree(t *testing.T) {
	latest := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		name string
		lost time.Time
		want string
	}{
		{"loss before the oldest partition", time.Date(2026, 7, 1, 9, 30, 0, 0, time.UTC), ContinuityOK},
		{"loss inside the oldest partition", time.Date(2026, 8, 1, 10, 30, 0, 0, time.UTC), ContinuityGapAcknowledged},
		{"loss after the newest indexed event", latest.Add(20 * time.Second), ContinuityGapAcknowledged},
	} {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.MatchExpectationsInOrder(false)
			mock.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_2026080110", "p_future"))
			mock.ExpectQuery(covArchQ).WillReturnRows(sqlmock.NewRows([]string{"min", "max", "sources"}).AddRow(nil, nil, 0))
			mock.ExpectQuery(covPartsQ).WillReturnRows(covPartRows("p_2026080110", "p_future"))
			mock.ExpectQuery(covProbeQ).WillReturnRows(covProbeRow(nil))
			mock.ExpectQuery(covProbeQ).WillReturnRows(covProbeRow(latest))
			mock.ExpectQuery("SELECT mode, binlog_file").WillReturnRows(sqlmock.NewRows(covStreamCols).
				AddRow("gtid", "", 0, "uuid:1-9", 100, latest, latest, 7, "id-1", c.lost, "auto-advance"))
			mock.ExpectQuery("SELECT gap_acked_at FROM stream_state").
				WillReturnRows(sqlmock.NewRows([]string{"gap_acked_at"}).AddRow(c.lost.Add(10 * time.Minute)))
			sum, err := CollectCoverageSummary(context.Background(), db, "binlog_index", latest.Add(42*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			if sum.Continuity != c.want {
				t.Fatalf("coverage summary = %q, want %q", sum.Continuity, c.want)
			}
			if onRecord := c.want == ContinuityGapAcknowledged; onRecord != !sum.GapLostAt.IsZero() || onRecord != !sum.GapAckedAt.IsZero() {
				t.Errorf("summary instants lost=%v acked=%v under %q", sum.GapLostAt, sum.GapAckedAt, sum.Continuity)
			}
			// The status report, from the same record and the same floor.
			s := &StreamStateInfo{Mode: "gtid", LastCheckpoint: latest, GapColumnsPresent: true,
				GapLostAt: nt(c.lost), GapAckedAt: nt(c.lost.Add(10 * time.Minute)), Floor: sum.Floor}
			_, doc, raw := renderGapWindow(t, s)
			if doc.Stream.Continuity.Status != sum.Continuity {
				t.Errorf("status JSON says %q, the coverage summary %q\n%s", doc.Stream.Continuity.Status, sum.Continuity, raw)
			}
		})
	}
}
