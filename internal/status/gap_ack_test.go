package status

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// The acknowledgement of a permanent capture loss is recorded BESIDE the loss
// (stream_state.gap_acked_at), never by erasing it: the alarm surfaces go
// quiet, and every reader that decides whether a window can be trusted keeps
// seeing gap_lost_at.

func nt(t time.Time) sql.NullTime { return sql.NullTime{Time: t, Valid: true} }

func TestGapLossAcknowledged(t *testing.T) {
	lost := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		name      string
		lost, ack sql.NullTime
		wantAcked bool
		wantLoud  bool // GapUnacknowledged: the alarm surfaces
	}{
		{"no loss, no acknowledgement", sql.NullTime{}, sql.NullTime{}, false, false},
		{"acknowledgement with no loss stamped acknowledges nothing", sql.NullTime{}, nt(lost), false, false},
		{"loss, never acknowledged", nt(lost), sql.NullTime{}, false, true},
		{"loss, acknowledged later", nt(lost), nt(lost.Add(time.Hour)), true, false},
		{"loss and acknowledgement in the same second", nt(lost), nt(lost), true, false},
		{"acknowledgement older than the loss is of an earlier loss", nt(lost), nt(lost.Add(-time.Second)), false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := GapLossAcknowledged(c.lost, c.ack); got != c.wantAcked {
				t.Errorf("GapLossAcknowledged = %v, want %v", got, c.wantAcked)
			}
			s := &StreamStateInfo{GapColumnsPresent: true, GapLostAt: c.lost, GapAckedAt: c.ack}
			if got := s.GapAcknowledged(); got != c.wantAcked {
				t.Errorf("GapAcknowledged() = %v, want %v", got, c.wantAcked)
			}
			if got := s.GapUnacknowledged(); got != c.wantLoud {
				t.Errorf("GapUnacknowledged() = %v, want %v", got, c.wantLoud)
			}
		})
	}
	// A nil state has no loss on record on either side.
	var none *StreamStateInfo
	if none.GapAcknowledged() || none.GapUnacknowledged() {
		t.Error("a nil stream state must be neither acknowledged nor unacknowledged")
	}
}

var gapAckCoreCols = []string{"mode", "binlog_file", "binlog_position", "gtid_set",
	"events_indexed", "last_event_time", "last_checkpoint",
	"server_id", "bintrail_id", "gap_lost_at", "gap_lost_detail"}

func gapAckCoreRow(lostAt, detail any) *sqlmock.Rows {
	return sqlmock.NewRows(gapAckCoreCols).AddRow(
		"gtid", "", 0, "uuid:1-9", 100, nil, time.Date(2026, 11, 3, 9, 0, 0, 0, time.UTC), 7, "id-1", lostAt, detail)
}

func TestLoadStreamState_gapAckedAt(t *testing.T) {
	lost := time.Date(2026, 11, 3, 10, 0, 0, 0, time.UTC)
	newDB := func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		// The best-effort augmenting queries the test does not care about fail
		// against the mock and are tolerated by LoadStreamState; only the
		// queries named below must be answered.
		mock.MatchExpectationsInOrder(false)
		t.Cleanup(func() { db.Close() })
		return db, mock
	}

	t.Run("acknowledged loss: the loss stays, the acknowledgement is beside it", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery("SELECT mode, binlog_file").WillReturnRows(gapAckCoreRow(lost, "purged"))
		mock.ExpectQuery("SELECT gap_acked_at FROM stream_state").
			WillReturnRows(sqlmock.NewRows([]string{"gap_acked_at"}).AddRow(lost.Add(time.Minute)))
		s, err := LoadStreamState(context.Background(), db)
		if err != nil || s == nil {
			t.Fatalf("LoadStreamState = (%v, %v)", s, err)
		}
		if !s.GapLostAt.Valid || !s.GapLostAt.Time.Equal(lost) {
			t.Errorf("an acknowledged loss must still be loaded: GapLostAt = %v", s.GapLostAt)
		}
		if !s.GapColumnsPresent || !s.GapAcknowledged() || s.GapUnacknowledged() {
			t.Errorf("want evaluable + acknowledged, got present=%v acked=%v unacked=%v",
				s.GapColumnsPresent, s.GapAcknowledged(), s.GapUnacknowledged())
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("gap_acked_at must be read: %v", err)
		}
	})

	t.Run("loss with a NULL acknowledgement is unacknowledged", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery("SELECT mode, binlog_file").WillReturnRows(gapAckCoreRow(lost, "purged"))
		mock.ExpectQuery("SELECT gap_acked_at FROM stream_state").
			WillReturnRows(sqlmock.NewRows([]string{"gap_acked_at"}).AddRow(nil))
		s, err := LoadStreamState(context.Background(), db)
		if err != nil || s == nil {
			t.Fatalf("LoadStreamState = (%v, %v)", s, err)
		}
		if s.GapAcknowledged() || !s.GapUnacknowledged() {
			t.Errorf("want unacknowledged, got acked=%v unacked=%v", s.GapAcknowledged(), s.GapUnacknowledged())
		}
	})

	t.Run("index with gap_lost_* but no gap_acked_at column: evaluable, unacknowledged", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery("SELECT mode, binlog_file").WillReturnRows(gapAckCoreRow(lost, "purged"))
		mock.ExpectQuery("SELECT gap_acked_at FROM stream_state").
			WillReturnError(&mysql.MySQLError{Number: 1054, Message: "Unknown column 'gap_acked_at' in 'field list'"})
		s, err := LoadStreamState(context.Background(), db)
		if err != nil || s == nil {
			t.Fatalf("an index without gap_acked_at must still load: (%v, %v)", s, err)
		}
		if !s.GapColumnsPresent {
			t.Error("a missing gap_acked_at must NOT make the gap state unevaluable")
		}
		if !s.GapLostAt.Valid || s.GapAcknowledged() || !s.GapUnacknowledged() {
			t.Errorf("want the loss loaded and unacknowledged, got lost=%v acked=%v", s.GapLostAt, s.GapAcknowledged())
		}
		if got := ContinuityStatus(s, nil); got != ContinuityGapLost {
			t.Errorf("continuity = %q, want %q (as before the column existed)", got, ContinuityGapLost)
		}
	})

	t.Run("an unreadable acknowledgement leaves the alarm up", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery("SELECT mode, binlog_file").WillReturnRows(gapAckCoreRow(lost, "purged"))
		mock.ExpectQuery("SELECT gap_acked_at FROM stream_state").WillReturnError(sql.ErrConnDone)
		s, err := LoadStreamState(context.Background(), db)
		if err != nil || s == nil {
			t.Fatalf("LoadStreamState = (%v, %v)", s, err)
		}
		if !s.GapUnacknowledged() {
			t.Error("a failed read of gap_acked_at must read as unacknowledged, never as acknowledged")
		}
	})

	t.Run("legacy index with neither column: unchanged, unevaluable", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery("SELECT mode, binlog_file").
			WillReturnError(&mysql.MySQLError{Number: 1054, Message: "Unknown column 'gap_lost_at' in 'field list'"})
		mock.ExpectQuery("SELECT mode, binlog_file").WillReturnRows(sqlmock.NewRows(gapAckCoreCols[:9]).
			AddRow("gtid", "", 0, "uuid:1-9", 100, nil, lost, 7, "id-1"))
		s, err := LoadStreamState(context.Background(), db)
		if err != nil || s == nil {
			t.Fatalf("LoadStreamState = (%v, %v)", s, err)
		}
		if s.GapColumnsPresent || s.GapLostAt.Valid || s.GapAcknowledged() || s.GapUnacknowledged() {
			t.Errorf("legacy index must stay unevaluable with no loss on either side: %+v", s)
		}
		if got := ContinuityStatus(s, nil); got != ContinuityUnknown {
			t.Errorf("continuity = %q, want %q", got, ContinuityUnknown)
		}
	})

	t.Run("empty stream_state: unchanged, nil", func(t *testing.T) {
		db, mock := newDB(t)
		mock.ExpectQuery("SELECT mode, binlog_file").WillReturnError(sql.ErrNoRows)
		s, err := LoadStreamState(context.Background(), db)
		if err != nil || s != nil {
			t.Fatalf("LoadStreamState = (%v, %v), want (nil, nil)", s, err)
		}
	})
}

// ackedLostStream is gapLostStream with the loss acknowledged an hour later.
func ackedLostStream() *StreamStateInfo {
	s := gapLostStream()
	s.GapColumnsPresent = true
	s.GapAckedAt = nt(s.GapLostAt.Time.Add(time.Hour))
	return s
}

func TestContinuityStatus_acknowledgedLoss(t *testing.T) {
	// Its own verdict. "ok" over a hole the index knows about is the one
	// answer an acknowledged loss must never give.
	if got := ContinuityStatus(ackedLostStream(), nil); got != "gap_acknowledged" {
		t.Errorf("acknowledged loss = %q, want %q", got, "gap_acknowledged")
	}
	sameSecond := ackedLostStream()
	sameSecond.GapAckedAt = sameSecond.GapLostAt
	if got := ContinuityStatus(sameSecond, nil); got != ContinuityGapAcknowledged {
		t.Errorf("loss acknowledged in the same second = %q, want %q", got, ContinuityGapAcknowledged)
	}
	loud := gapLostStream()
	loud.GapColumnsPresent = true
	if got := ContinuityStatus(loud, nil); got != ContinuityGapLost {
		t.Errorf("unacknowledged loss = %q, want %q", got, ContinuityGapLost)
	}
	// A second loss after the acknowledgement: the stamp nulled gap_acked_at.
	again := ackedLostStream()
	again.GapLostAt = nt(again.GapAckedAt.Time.Add(time.Hour))
	again.GapAckedAt = sql.NullTime{}
	if got := ContinuityStatus(again, nil); got != ContinuityGapLost {
		t.Errorf("a loss after an acknowledgement = %q, want %q", got, ContinuityGapLost)
	}
}

func TestWriteStatus_acknowledgedLoss(t *testing.T) {
	var quiet bytes.Buffer
	WriteStatus(&quiet, nil, nil, nil, nil, nil, ackedLostStream())
	out := quiet.String()
	t.Logf("acknowledged loss, status text:\n%s", out)
	for _, banned := range []string{"GAP LOST", "EVENTS PERMANENTLY LOST"} {
		if strings.Contains(out, banned) {
			t.Errorf("an acknowledged loss must not raise %q:\n%s", banned, out)
		}
	}
	for _, want := range []string{
		"Continuity:      no gaps since the loss acknowledged below",
		"Acknowledged loss: capture permanently lost events at 2026-06-22 12:00:00",
		"acknowledged 2026-06-22 13:00:00",
		"Snapshot updates and reconstructions whose window spans that moment are",
		"refused. A full snapshot taken after it is not affected. This is reported",
		"while the index still holds the hour it happened in.",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("acknowledged status text missing %q:\n%s", want, out)
		}
	}
	// "no gaps in the captured range" over a line that says events were lost
	// reads as a contradiction.
	if strings.Contains(out, "no gaps in the captured range") {
		t.Errorf("acknowledged status text claims no gaps in the captured range:\n%s", out)
	}

	var loud bytes.Buffer
	unacked := gapLostStream()
	unacked.GapColumnsPresent = true
	WriteStatus(&loud, nil, nil, nil, nil, nil, unacked)
	t.Logf("unacknowledged loss, status text:\n%s", loud.String())
	for _, want := range []string{"Continuity:      ⚠ GAP LOST at 2026-06-22 12:00:00", "=== ⚠ EVENTS PERMANENTLY LOST ==="} {
		if !strings.Contains(loud.String(), want) {
			t.Errorf("unacknowledged status text missing %q:\n%s", want, loud.String())
		}
	}
	if strings.Contains(loud.String(), "Acknowledged loss") {
		t.Errorf("an unacknowledged loss must not print the acknowledged line:\n%s", loud.String())
	}

	// No loss at all: neither the alarm nor the acknowledged line.
	var clean bytes.Buffer
	WriteStatus(&clean, nil, nil, nil, nil, nil, &StreamStateInfo{Mode: "gtid", ServerID: 7,
		LastCheckpoint: time.Now(), GapColumnsPresent: true, GapAckedAt: nt(time.Now())})
	if strings.Contains(clean.String(), "Acknowledged loss") || strings.Contains(clean.String(), "GAP LOST") {
		t.Errorf("an acknowledgement with no loss stamped must print nothing about a loss:\n%s", clean.String())
	}
}

func TestWriteStatusJSON_acknowledgedLoss(t *testing.T) {
	type doc struct {
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
				Detail         string `json:"detail"`
			} `json:"gap_acknowledged"`
		} `json:"stream"`
	}
	render := func(s *StreamStateInfo) (doc, string) {
		t.Helper()
		var buf bytes.Buffer
		if err := WriteStatusJSON(&buf, nil, nil, nil, nil, nil, s); err != nil {
			t.Fatal(err)
		}
		var d doc
		if err := json.Unmarshal(buf.Bytes(), &d); err != nil {
			t.Fatalf("invalid JSON: %v\n%s", err, buf.String())
		}
		return d, buf.String()
	}

	d, raw := render(ackedLostStream())
	if d.Stream.Continuity.Status != "gap_acknowledged" || d.Stream.GapLost != nil {
		t.Errorf("acknowledged loss: continuity=%q gap_lost=%v, want gap_acknowledged and no gap_lost\n%s",
			d.Stream.Continuity.Status, d.Stream.GapLost, raw)
	}
	if d.Stream.GapAcknowledged == nil {
		t.Fatalf("acknowledged loss: stream.gap_acknowledged missing\n%s", raw)
	}
	t.Logf("acknowledged loss, status JSON gap_acknowledged: %+v", *d.Stream.GapAcknowledged)
	if d.Stream.GapAcknowledged.At != "2026-06-22 12:00:00" ||
		d.Stream.GapAcknowledged.AcknowledgedAt != "2026-06-22 13:00:00" ||
		d.Stream.GapAcknowledged.Detail != "replication slot invalidated (wal_status=lost)" {
		t.Errorf("gap_acknowledged = %+v", *d.Stream.GapAcknowledged)
	}

	unacked := gapLostStream()
	unacked.GapColumnsPresent = true
	d, raw = render(unacked)
	if d.Stream.Continuity.Status != ContinuityGapLost || d.Stream.GapLost == nil || d.Stream.GapAcknowledged != nil {
		t.Errorf("unacknowledged loss: continuity=%q gap_lost=%v gap_acknowledged=%v\n%s",
			d.Stream.Continuity.Status, d.Stream.GapLost, d.Stream.GapAcknowledged, raw)
	}

	_, raw = render(&StreamStateInfo{Mode: "gtid", LastCheckpoint: time.Now(), GapColumnsPresent: true})
	if strings.Contains(raw, "gap_acknowledged") {
		t.Errorf("a stream with no loss must omit gap_acknowledged:\n%s", raw)
	}
}

// An acknowledged loss stays on record in a coverage summary only while the
// summary's window still contains it. The record is never erased, so the
// window's start is what lets a summary say ok again.
func TestAcknowledgedLossInWindow(t *testing.T) {
	lost := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name  string
		floor DeltaFloor
		want  string
	}{
		{"window starts before the loss", DeltaFloor{Hour: lost.Add(-48 * time.Hour)}, ContinuityGapAcknowledged},
		{"window starts at the loss", DeltaFloor{Hour: lost}, ContinuityGapAcknowledged},
		{"window starts one second after the loss", DeltaFloor{Hour: lost.Add(time.Second)}, ContinuityOK},
		{"window starts long after the loss", DeltaFloor{Hour: lost.Add(90 * 24 * time.Hour)}, ContinuityOK},
		{"window start unknown", DeltaFloor{}, ContinuityGapAcknowledged},
		{"archives may reach further back than the floor", DeltaFloor{Hour: lost.Add(time.Hour), BelowIsUnknown: true}, ContinuityGapAcknowledged},
	} {
		if got := acknowledgedLossInWindow(lost, tc.floor); got != tc.want {
			t.Errorf("%s: %s, want %s", tc.name, got, tc.want)
		}
	}
}
