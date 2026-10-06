package verify

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/consistency"
	"github.com/dbtrail/dbtrail/internal/query"
)

// #2150: the live-source read is cut at the snapshot's GTID position. These
// pin how one indexed change's GTID is judged against that position, and how
// the index is walked to find the last change the snapshot holds.

const (
	uuidA = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	uuidB = "4f22ab58-82db-22f2-af44-d91bb0530673"
)

func TestSnapshotMembership_MySQL(t *testing.T) {
	in, err := newSnapshotMembership(consistency.GTIDFlavorMySQL,
		" "+uuidA+":1-10:12,\n"+uuidB+":1-3 ")
	if err != nil {
		t.Fatalf("newSnapshotMembership: %v", err)
	}
	for _, c := range []struct {
		gtid      string
		want, err bool
	}{
		{uuidA + ":1", true, false},
		{uuidA + ":10", true, false},
		{uuidA + ":11", false, false}, // a hole: committed after the snapshot
		{uuidA + ":12", true, false},
		{uuidA + ":13", false, false},
		{uuidB + ":3", true, false},
		{uuidB + ":4", false, false},
		{strings.ToUpper(uuidA) + ":5", true, false}, // case is not identity
		{"5a33bc69-93db-33f3-b055-ea2cc1641784:1", false, false},
		{"", false, true},
		{"not-a-gtid", false, true},
		{uuidA + ":1-3", false, true}, // a range is not one change's GTID
	} {
		got, err := in(c.gtid)
		if (err != nil) != c.err || (err == nil && got != c.want) {
			t.Errorf("member(%q) = %v, %v; want %v, error %v", c.gtid, got, err, c.want, c.err)
		}
	}
}

func TestSnapshotMembership_MySQLTaggedSetRefused(t *testing.T) {
	// The index stores a change's GTID without its tag, so against a set
	// that carries tags membership cannot be told.
	_, err := newSnapshotMembership(consistency.GTIDFlavorMySQL, uuidA+":1-5,"+uuidA+":batch:1-2")
	if !errors.Is(err, errTaggedGTIDSet) {
		t.Fatalf("err = %v, want errTaggedGTIDSet", err)
	}
}

func TestSnapshotMembership_MariaDB(t *testing.T) {
	in, err := newSnapshotMembership(consistency.GTIDFlavorMariaDB, "0-1-100,\n 7-2-5")
	if err != nil {
		t.Fatalf("newSnapshotMembership: %v", err)
	}
	for _, c := range []struct {
		gtid      string
		want, err bool
	}{
		{"0-1-100", true, false},
		{"0-1-101", false, false},
		{"0-3-99", true, false},   // another server id, same domain: the sequence decides
		{"0-3-101", false, false}, // a failover after the snapshot
		{"7-2-5", true, false},
		{"7-2-6", false, false},
		{"9-1-1", false, false}, // a domain the snapshot had not seen
		{"", false, true},
		{"0-1", false, true},
	} {
		got, err := in(c.gtid)
		if (err != nil) != c.err || (err == nil && got != c.want) {
			t.Errorf("member(%q) = %v, %v; want %v, error %v", c.gtid, got, err, c.want, c.err)
		}
	}
	for _, bad := range []string{"", "0-1-5,,1-2-3", "0-1-5,0-2-6"} {
		if _, err := newSnapshotMembership(consistency.GTIDFlavorMariaDB, bad); err == nil {
			t.Errorf("newSnapshotMembership(mariadb, %q) accepted", bad)
		}
	}
	if _, err := newSnapshotMembership("", uuidA+":1"); err == nil {
		t.Error("an unknown flavor was accepted")
	}
}

// cutRows is one page of the descending walk.
func cutRows(rows ...[4]any) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{"event_id", "gtid", "binlog_file", "end_pos"})
	for _, x := range rows {
		r.AddRow(x[0], x[1], x[2], x[3])
	}
	return r
}

func TestSnapshotCut(t *testing.T) {
	set := uuidA + ":1-10"
	ckpt := func(m sqlmock.Sqlmock, file any, pos any) {
		m.ExpectQuery("SELECT binlog_file, binlog_position FROM stream_state").
			WillReturnRows(sqlmock.NewRows([]string{"binlog_file", "binlog_position"}).AddRow(file, pos))
	}
	cases := []struct {
		name     string
		page     int
		mock     func(m sqlmock.Sqlmock)
		want     *query.BinlogPos
		unplaced string // substring of the reason; "" = placed
	}{
		{
			name: "changes after the snapshot are skipped; the cut is the end of the last one it holds",
			page: 2,
			mock: func(m sqlmock.Sqlmock) {
				ckpt(m, "binlog.000002", 900)
				m.ExpectQuery("ORDER BY event_id DESC").WillReturnRows(cutRows(
					[4]any{int64(9), uuidA + ":12", "binlog.000002", int64(800)},
					[4]any{int64(8), uuidA + ":11", "binlog.000002", int64(700)}))
				m.ExpectQuery("ORDER BY event_id DESC").WithArgs(int64(8)).WillReturnRows(cutRows(
					[4]any{int64(7), uuidA + ":10", "binlog.000002", int64(600)},
					[4]any{int64(6), uuidA + ":10", "binlog.000002", int64(500)}))
			},
			want: &query.BinlogPos{File: "binlog.000002", Pos: 600},
		},
		{
			name: "everything indexed is in the snapshot: the cut is the newest change's end",
			page: 1000,
			mock: func(m sqlmock.Sqlmock) {
				ckpt(m, "binlog.000002", 900)
				m.ExpectQuery("ORDER BY event_id DESC").WillReturnRows(cutRows(
					[4]any{int64(3), uuidA + ":9", "binlog.000001", int64(300)}))
			},
			want: &query.BinlogPos{File: "binlog.000001", Pos: 300},
		},
		{
			name: "an empty live index: the checkpoint read before the walk",
			page: 1000,
			mock: func(m sqlmock.Sqlmock) {
				ckpt(m, "binlog.000004", 154)
				m.ExpectQuery("ORDER BY event_id DESC").WillReturnRows(cutRows())
			},
			want: &query.BinlogPos{File: "binlog.000004", Pos: 154},
		},
		{
			name: "an empty live index and no checkpoint position",
			page: 1000,
			mock: func(m sqlmock.Sqlmock) {
				ckpt(m, nil, nil)
				m.ExpectQuery("ORDER BY event_id DESC").WillReturnRows(cutRows())
			},
			unplaced: "no position",
		},
		{
			name: "every live change is newer than the snapshot",
			page: 1000,
			mock: func(m sqlmock.Sqlmock) {
				ckpt(m, "binlog.000002", 900)
				m.ExpectQuery("ORDER BY event_id DESC").WillReturnRows(cutRows(
					[4]any{int64(2), uuidA + ":11", "binlog.000002", int64(700)}))
			},
			unplaced: "newer than the snapshot",
		},
		{
			name: "a change with no GTID above the cut cannot be placed",
			page: 1000,
			mock: func(m sqlmock.Sqlmock) {
				ckpt(m, "binlog.000002", 900)
				m.ExpectQuery("ORDER BY event_id DESC").WillReturnRows(cutRows(
					[4]any{int64(5), nil, "binlog.000002", int64(700)},
					[4]any{int64(4), uuidA + ":3", "binlog.000002", int64(600)}))
			},
			unplaced: "no GTID",
		},
		{
			name: "a GTID that does not parse cannot be placed",
			page: 1000,
			mock: func(m sqlmock.Sqlmock) {
				ckpt(m, "binlog.000002", 900)
				m.ExpectQuery("ORDER BY event_id DESC").WillReturnRows(cutRows(
					[4]any{int64(5), "garbage", "binlog.000002", int64(700)}))
			},
			unplaced: "garbage",
		},
		{
			name: "no stream state row",
			page: 1000,
			mock: func(m sqlmock.Sqlmock) {
				m.ExpectQuery("SELECT binlog_file, binlog_position FROM stream_state").
					WillReturnRows(sqlmock.NewRows([]string{"binlog_file", "binlog_position"}))
				m.ExpectQuery("ORDER BY event_id DESC").WillReturnRows(cutRows(
					[4]any{int64(3), uuidA + ":9", "binlog.000001", int64(300)}))
			},
			want: &query.BinlogPos{File: "binlog.000001", Pos: 300},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, m, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			c.mock(m)
			in, err := newSnapshotMembership(consistency.GTIDFlavorMySQL, set)
			if err != nil {
				t.Fatal(err)
			}
			got, unplaced, err := snapshotCutPaged(context.Background(), db, in, c.page)
			if err != nil {
				t.Fatalf("snapshotCut: %v", err)
			}
			if c.unplaced != "" {
				if got != nil || !strings.Contains(unplaced, c.unplaced) {
					t.Fatalf("got %v, %q; want unplaced with %q", got, unplaced, c.unplaced)
				}
			} else if unplaced != "" || got == nil || *got != *c.want {
				t.Fatalf("got %v, %q; want %v", got, unplaced, c.want)
			}
			if err := m.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestResolveLiveCut_unanchored(t *testing.T) {
	for _, c := range []struct {
		name string
		src  consistency.TableChecksum
		want string
	}{
		{"gtid off", consistency.TableChecksum{GTIDFlavor: consistency.GTIDFlavorMySQL}, "GTIDs disabled"},
		{"no lock grant", consistency.TableChecksum{GTIDFlavor: consistency.GTIDFlavorMySQL, GTIDSet: uuidA + ":1-3", AnchorLockRefused: true}, "LOCK TABLES"},
		{"mariadb without a coordinate", consistency.TableChecksum{GTIDFlavor: consistency.GTIDFlavorMariaDB, GTIDSet: "0-1-3"}, "binary log position"},
		{"tagged set", consistency.TableChecksum{GTIDFlavor: consistency.GTIDFlavorMySQL, GTIDSet: uuidA + ":t:1-3", Anchor: consistency.AnchorTableLock}, "tagged"},
	} {
		t.Run(c.name, func(t *testing.T) {
			db, m, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			cut, err := resolveLiveCut(context.Background(), db, c.src, time.Time{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if cut.pos != nil || cut.inconclusive != "" || !strings.Contains(cut.note, "not cut at the snapshot") || !strings.Contains(cut.note, c.want) {
				t.Fatalf("got %+v; want an unanchored note naming %q", cut, c.want)
			}
			if err := m.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestResolveLiveCut_anchored(t *testing.T) {
	src := consistency.TableChecksum{GTIDFlavor: consistency.GTIDFlavorMySQL, GTIDSet: uuidA + ":1-10", Anchor: consistency.AnchorTableLock}
	stream := func(m sqlmock.Sqlmock) {
		m.ExpectQuery("SELECT 1 FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
		m.ExpectQuery("FROM index_state").WillReturnRows(sqlmock.NewRows([]string{"m", "n"}).AddRow(nil, 0))
	}
	walk := func(m sqlmock.Sqlmock) {
		m.ExpectQuery("SELECT binlog_file, binlog_position FROM stream_state").
			WillReturnRows(sqlmock.NewRows([]string{"binlog_file", "binlog_position"}).AddRow("binlog.000003", 900))
		m.ExpectQuery("ORDER BY event_id DESC").WillReturnRows(cutRows(
			[4]any{int64(9), uuidA + ":11", "binlog.000003", int64(800)},
			[4]any{int64(8), uuidA + ":10", "binlog.000003", int64(700)}))
	}
	t.Run("cut at the last change the snapshot holds", func(t *testing.T) {
		db, m, _ := sqlmock.New()
		defer db.Close()
		stream(m)
		walk(m)
		cut, err := resolveLiveCut(context.Background(), db, src, time.Now(), &query.BinlogPos{File: "binlog.000003", Pos: 100})
		if err != nil || cut.pos == nil || *cut.pos != (query.BinlogPos{File: "binlog.000003", Pos: 700}) || cut.note != "" {
			t.Fatalf("got %+v, %v", cut, err)
		}
	})
	t.Run("a baseline newer than the read", func(t *testing.T) {
		db, m, _ := sqlmock.New()
		defer db.Close()
		stream(m)
		walk(m)
		cut, err := resolveLiveCut(context.Background(), db, src, time.Now(), &query.BinlogPos{File: "binlog.000003", Pos: 750})
		if err != nil || cut.pos != nil || !strings.Contains(cut.inconclusive, "newer than the read") {
			t.Fatalf("got %+v, %v; want inconclusive", cut, err)
		}
	})
	t.Run("ids not proven to be binlog order", func(t *testing.T) {
		db, m, _ := sqlmock.New()
		defer db.Close()
		m.ExpectQuery("SELECT 1 FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
		m.ExpectQuery("FROM index_state").WillReturnRows(sqlmock.NewRows([]string{"m", "n"}).AddRow(nil, 1))
		cut, err := resolveLiveCut(context.Background(), db, src, time.Now(), nil)
		if err != nil || cut.pos != nil || !strings.Contains(cut.note, "binary log order") {
			t.Fatalf("got %+v, %v; want unanchored", cut, err)
		}
	})
}

func TestWaitIndexCovers(t *testing.T) {
	defer func(p time.Duration) { coveragePoll = p }(coveragePoll)
	coveragePoll = time.Millisecond
	src := uuidA + ":1-10"
	behind := func(m sqlmock.Sqlmock) {
		m.ExpectQuery("SELECT gtid_set FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"g"}).AddRow(uuidA + ":1-8"))
	}
	age := func(m sqlmock.Sqlmock, secs int) {
		m.ExpectQuery("TIMESTAMPDIFF").WillReturnRows(sqlmock.NewRows([]string{"a"}).AddRow(secs))
	}
	t.Run("waits for a running capture to catch up", func(t *testing.T) {
		db, m, _ := sqlmock.New()
		defer db.Close()
		behind(m)
		age(m, 1)
		m.ExpectQuery("SELECT gtid_set FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"g"}).AddRow(uuidA + ":1-12"))
		ok, note := waitIndexCovers(context.Background(), db, src, consistency.GTIDFlavorMySQL, time.Minute)
		if !ok || note != "" {
			t.Fatalf("got %v %q", ok, note)
		}
		if err := m.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("a stopped capture is not waited for", func(t *testing.T) {
		db, m, _ := sqlmock.New()
		defer db.Close()
		behind(m)
		age(m, 3600)
		ok, note := waitIndexCovers(context.Background(), db, src, consistency.GTIDFlavorMySQL, time.Minute)
		if ok || !strings.HasPrefix(note, indexBehind) {
			t.Fatalf("got %v %q", ok, note)
		}
		if err := m.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("another verdict is not waited for", func(t *testing.T) {
		db, m, _ := sqlmock.New()
		defer db.Close()
		m.ExpectQuery("SELECT gtid_set FROM stream_state").WillReturnRows(sqlmock.NewRows([]string{"g"}).AddRow("garbage"))
		ok, note := waitIndexCovers(context.Background(), db, src, consistency.GTIDFlavorMySQL, time.Minute)
		if ok || !strings.Contains(note, "unparseable") {
			t.Fatalf("got %v %q", ok, note)
		}
		if err := m.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("the wait is bounded", func(t *testing.T) {
		db, m, _ := sqlmock.New()
		defer db.Close()
		behind(m)
		ok, _ := waitIndexCovers(context.Background(), db, src, consistency.GTIDFlavorMySQL, -1)
		if ok {
			t.Fatal("covered")
		}
		if err := m.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}
