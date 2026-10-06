//go:build integration

package reconstruct_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/indexer"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2160: the source's binlog numbering starts over (RESET MASTER, a failover
// to a server whose files are also binlog.N, a shorter log_bin base name) while
// capture keeps writing the same index. The events written after that sort
// BELOW the snapshot's anchor, so a window bounded by position cannot see
// them. A refresh must refuse, by its own sentinel and whatever --allow-gaps
// says; the full snapshot that the refusal calls for must then let the next
// refresh fold again.

// insertEventAt is insertTableEvent with the binlog file chosen by the caller.
func insertEventAt(t *testing.T, db *sql.DB, schema, table, file string, id, start uint64, at time.Time, pk, after string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO binlog_events
		(event_id, binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values, row_before, row_after)
		VALUES (?, ?, ?, ?, ?, ?, ?, 2, ?, ?, ?)`,
		id, file, start, start+100, at.Format("2006-01-02 15:04:05"), schema, table, pk,
		[]byte(`{"id":`+pk+`,"status":"before"}`), []byte(after)); err != nil {
		t.Fatalf("insert event %d: %v", id, err)
	}
}

// refreshWith is floorRig.refresh returning the run's error instead of
// failing, with the config edited by edit.
func (r *floorRig) refreshWith(at time.Time, deltas, carry bool, edit func(*reconstruct.FullTableConfig)) (string, *reconstruct.TableReport, error) {
	cfg := reconstruct.FullTableConfig{
		IndexDSN: r.dsn, BaselineSrc: r.root, Tables: []string{r.schema + ".orders"},
		At: at, OutputDir: r.root, OutputFormat: reconstruct.OutputFormatParquet,
		TableDeltas: deltas, CarryForwardUnchanged: carry,
	}
	if edit != nil {
		edit(&cfg)
	}
	reps, err := reconstruct.ReconstructTables(r.ctx, cfg)
	if err != nil {
		return "", nil, err
	}
	p, _, _, ferr := reconstruct.FindBaseline(r.ctx, r.root, r.schema, "orders", at)
	if ferr != nil {
		return "", nil, ferr
	}
	return p, reps[0], nil
}

func (r *floorRig) refreshErr(at time.Time, deltas, carry bool) (string, *reconstruct.TableReport, error) {
	return r.refreshWith(at, deltas, carry, nil)
}

// seedFullRead writes what the daemon's full backup writes: a snapshot of
// orders read from the source at at, anchored where the source stood, with
// the event mark read from the index before the dump.
func seedFullRead(t *testing.T, r *floorRig, at time.Time, file string, pos uint64, mark string, rows [][]string) {
	t.Helper()
	snapDir := filepath.Join(r.root, strings.ReplaceAll(at.UTC().Format(time.RFC3339), ":", "-"))
	cols, err := baseline.ParseSchemaText(ordersCreateSQL)
	if err != nil {
		t.Fatal(err)
	}
	md := map[string]string{
		baseline.MetaKeyCreateTableSQL: ordersCreateSQL,
		baseline.MetaKeyBinlogFile:     file,
		baseline.MetaKeyBinlogPos:      strconv.FormatUint(pos, 10),
		"bintrail.snapshot_timestamp":  at.UTC().Format(time.RFC3339),
	}
	if mark != "" {
		md[baseline.MetaKeyEventMark] = mark
	}
	w, err := baseline.NewWriter(filepath.Join(snapDir, r.schema, "orders.parquet"), cols, baseline.WriterConfig{Compression: "none", RowGroupSize: 100, Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := w.WriteRow(row, []bool{false, false}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := baseline.WriteSuccessMarker(snapDir); err != nil {
		t.Fatal(err)
	}
}

// renumberRig is newLagRig with a stream-written index and a first refresh
// that folded one change at binlog.000007 and anchored the snapshot there.
func renumberRig(t *testing.T, deltas, carry bool) (*floorRig, time.Time) {
	t.Helper()
	var T time.Time
	r, _ := newLagRig(t, func(first time.Time) time.Time {
		T = first.Add(6*time.Hour + 30*time.Minute)
		return T.Add(-4 * time.Hour)
	})
	markStreamCaptured(t, r.db)
	insertEventAt(t, r.db, r.schema, "items", "binlog.000006", 5, 100, T.Add(-2*time.Hour), "9", `{"id":9,"status":"x"}`)
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000007", 10, 100, T.Add(-time.Hour), "1", `{"id":1,"status":"A"}`)
	if _, _, err := r.refreshErr(T, deltas, carry); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	return r, T
}

func TestRefresh_afterTheBinlogNumberingStartsOver_refusesUntilAFullSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name          string
		deltas, carry bool
	}{
		{"table rewritten", false, false},
		{"table carried forward when unchanged", false, true},
		{"delta pairs", true, false},
		{"delta pairs, carried forward when unchanged", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, T := renumberRig(t, tc.deltas, tc.carry)
			if !tc.deltas {
				// The first refresh stamped the run's mark beside its cut: the
				// newest event before the cut, binlog.000007:100-200 (id 10).
				p, _, _, err := reconstruct.FindBaseline(r.ctx, r.root, r.schema, "orders", T)
				if err != nil {
					t.Fatal(err)
				}
				md, err := baseline.ReadParquetMetadata(p)
				if err != nil {
					t.Fatal(err)
				}
				m := reconstruct.ParseEventMark(md.EventMark)
				if m == nil || *m != (reconstruct.EventMark{ID: 10, File: "binlog.000007", End: 200}) || md.BinlogFile != "binlog.000007" || md.BinlogPos != 200 {
					t.Fatalf("first refresh's footer: anchor %s:%d, event mark %q; want binlog.000007:200 and the mark of event 10", md.BinlogFile, md.BinlogPos, md.EventMark)
				}
			}

			// The numbering starts over: the next change is in binlog.000001.
			insertEventAt(t, r.db, r.schema, "orders", "binlog.000001", 20, 300, T.Add(20*time.Minute), "1", `{"id":1,"status":"B"}`)
			for _, allow := range []bool{false, true} {
				_, _, err := r.refreshWith(T.Add(time.Hour), tc.deltas, tc.carry, func(c *reconstruct.FullTableConfig) { c.AllowGaps = allow })
				if !errors.Is(err, reconstruct.ErrBinlogRenumbered) {
					t.Fatalf("refresh after the numbering started over (allow gaps %v): err = %v, want ErrBinlogRenumbered", allow, err)
				}
				if errors.Is(err, reconstruct.ErrCaptureGap) {
					t.Fatalf("the refusal reads as a capture gap, whose remedy is a flag: %v", err)
				}
				for _, want := range []string{"shop.orders", "binlog.000001:300", "binlog.000007:200", "a new full snapshot is needed"} {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("the refusal does not say %q: %v", want, err)
					}
				}
			}

			// The remedy: a full snapshot read from the source in the new
			// numbering, which already holds the change. The next refresh folds
			// what came after it.
			seedFullRead(t, r, T.Add(90*time.Minute), "binlog.000001", 400,
				reconstruct.EventMark{ID: 20, File: "binlog.000001", End: 400}.Encode(),
				[][]string{{"1", "B"}, {"2", "paid"}, {"3", "shipped"}})
			insertEventAt(t, r.db, r.schema, "orders", "binlog.000001", 30, 500, T.Add(100*time.Minute), "3", `{"id":3,"status":"C"}`)
			base, rep, err := r.refreshErr(T.Add(2*time.Hour), tc.deltas, tc.carry)
			if err != nil {
				t.Fatalf("refresh after the full snapshot: %v", err)
			}
			if got, want := lagState(t, base, tc.deltas), []string{"1=B", "2=paid", "3=C"}; !equalStrings(got, want) {
				t.Fatalf("refresh after the full snapshot published %v (events applied %d), want %v", got, rep.EventsApplied, want)
			}
			// And the one after that, from the refresh's own mark.
			insertEventAt(t, r.db, r.schema, "orders", "binlog.000001", 40, 700, T.Add(130*time.Minute), "2", `{"id":2,"status":"D"}`)
			base, _, err = r.refreshErr(T.Add(3*time.Hour), tc.deltas, tc.carry)
			if err != nil {
				t.Fatalf("second refresh after the full snapshot: %v", err)
			}
			if got, want := lagState(t, base, tc.deltas), []string{"1=B", "2=D", "3=C"}; !equalStrings(got, want) {
				t.Fatalf("second refresh after the full snapshot published %v, want %v", got, want)
			}
		})
	}
}

// A numbering that started over, then grew past the snapshot's position
// before the refresh ran: the newest event sorts after the anchor, so only
// the first event indexed after the mark shows it.
func TestRefresh_aNumberingThatStartedOverAndGrewPastTheAnchorRefuses(t *testing.T) {
	r, T := renumberRig(t, false, false)
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000001", 20, 300, T.Add(20*time.Minute), "1", `{"id":1,"status":"B"}`)
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000009", 21, 300, T.Add(40*time.Minute), "3", `{"id":3,"status":"C"}`)
	if _, _, err := r.refreshErr(T.Add(time.Hour), false, false); !errors.Is(err, reconstruct.ErrBinlogRenumbered) {
		t.Fatalf("err = %v, want ErrBinlogRenumbered", err)
	}
}

// The shape a position-mode stream leaves when it restarts after a RESET
// MASTER (observed on MySQL 8.4): its saved file is gone, it starts again at
// the oldest file the source has, stamps a capture loss, and its resume
// cleanup deletes the indexed rows that sort at or after that start, which
// takes the event mark's own row. The stamped loss refuses; with the loss
// accepted, capture back below the anchor still refuses.
func TestRefresh_afterAPositionModeRestartBelowTheAnchor(t *testing.T) {
	r, T := renumberRig(t, false, false)
	testutil.MustExec(t, r.db, `DELETE FROM binlog_events WHERE event_id = 10`)
	testutil.MustExec(t, r.db, `UPDATE stream_state SET gap_lost_at = ?, gap_lost_detail = 'binlog gap detected but CANNOT be filled: required file binlog.000007 has been purged'
		WHERE id = 1`, T.Add(10*time.Minute).Format("2006-01-02 15:04:05"))
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000001", 20, 300, T.Add(20*time.Minute), "1", `{"id":1,"status":"B"}`)
	_, _, err := r.refreshErr(T.Add(time.Hour), false, false)
	if !errors.Is(err, reconstruct.ErrCaptureGap) {
		t.Fatalf("refresh over a stamped capture loss: err = %v, want ErrCaptureGap", err)
	}
	_, _, err = r.refreshWith(T.Add(time.Hour), false, false, func(c *reconstruct.FullTableConfig) { c.AllowGaps = true })
	if !errors.Is(err, reconstruct.ErrBinlogRenumbered) {
		t.Fatalf("refresh with the loss accepted: err = %v, want ErrBinlogRenumbered", err)
	}
}

// A failover to a server whose files are numbered HIGHER: every new event
// sorts after the anchor, so positions show nothing. Capture recorded the new
// server_uuid at the same address, and that refuses. A host change of the
// same server does not.
func TestRefresh_aReplacedSourceRefusesEvenWithHigherNumbers(t *testing.T) {
	r, T := renumberRig(t, false, false)
	testutil.MustExec(t, r.db, serverChangeSQL, "host", "db-a", "db-b", T.Add(5*time.Minute).Unix())
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000009", 20, 300, T.Add(20*time.Minute), "1", `{"id":1,"status":"B"}`)
	base, _, err := r.refreshErr(T.Add(time.Hour), false, false)
	if err != nil {
		t.Fatalf("refresh after a host change of the same server: %v", err)
	}
	if got, want := lagState(t, base, false), []string{"1=B", "2=paid", "3=shipped"}; !equalStrings(got, want) {
		t.Fatalf("refresh after a host change published %v, want %v", got, want)
	}

	testutil.MustExec(t, r.db, serverChangeSQL, "server_uuid", "3e11fa47-71ca-11e1-9e33-c80aa9429562", "4f22ab58-71ca-11e1-9e33-c80aa9429563", T.Add(70*time.Minute).Unix())
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000009", 30, 500, T.Add(80*time.Minute), "3", `{"id":3,"status":"C"}`)
	_, _, err = r.refreshErr(T.Add(2*time.Hour), false, false)
	if !errors.Is(err, reconstruct.ErrBinlogRenumbered) {
		t.Fatalf("refresh after the source was replaced: err = %v, want ErrBinlogRenumbered", err)
	}
	if !strings.Contains(err.Error(), "4f22ab58-71ca-11e1-9e33-c80aa9429563") {
		t.Errorf("the refusal does not name the new server: %v", err)
	}
}

const serverChangeSQL = `INSERT INTO bintrail_server_changes (bintrail_id, field_changed, old_value, new_value, detected_at)
	VALUES ('b1', ?, ?, ?, FROM_UNIXTIME(?))`

// A full snapshot taken while capture was behind it (#1689): its mark is the
// newest event before the dump, below its anchor, and capture catching up
// fills the gap between the two. That is one numbering read late, not a new
// one: the window is empty, then folds once capture passes the anchor.
func TestRefresh_captureCatchingUpOnAFullSnapshotIsNotARenumbering(t *testing.T) {
	var T time.Time
	r, _ := newLagRig(t, func(first time.Time) time.Time {
		T = first.Add(6*time.Hour + 30*time.Minute)
		return T.Add(-4 * time.Hour)
	})
	markStreamCaptured(t, r.db)
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000007", 5, 100, T.Add(-5*time.Hour), "2", `{"id":2,"status":"paid"}`)
	seedFullRead(t, r, T.Add(-3*time.Hour), "binlog.000007", 900,
		reconstruct.EventMark{ID: 5, File: "binlog.000007", End: 200}.Encode(),
		[][]string{{"1", "A"}, {"2", "paid"}, {"3", "shipped"}})
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000007", 6, 400, T.Add(-4*time.Hour), "1", `{"id":1,"status":"A"}`)
	base, rep, err := r.refreshErr(T, false, false)
	if err != nil {
		t.Fatalf("refresh while capture catches up: %v", err)
	}
	if rep.EventsApplied != 0 {
		t.Fatalf("refresh while capture catches up applied %d events, want 0: they are in the full snapshot already", rep.EventsApplied)
	}
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000007", 7, 900, T.Add(-2*time.Hour), "3", `{"id":3,"status":"C"}`)
	base, _, err = r.refreshErr(T.Add(time.Hour), false, false)
	if err != nil {
		t.Fatalf("refresh once capture passed the snapshot: %v", err)
	}
	if got, want := lagState(t, base, false), []string{"1=A", "2=paid", "3=C"}; !equalStrings(got, want) {
		t.Fatalf("published %v, want %v", got, want)
	}
}

// CheckNumberingContinues against a real index, one index per case.
func TestCheckNumberingContinues_2160(t *testing.T) {
	mark := &reconstruct.EventMark{ID: 10, File: "binlog.000007", End: 200}
	type ev struct {
		id    uint64
		file  string
		start uint64
	}
	for _, tc := range []struct {
		name   string
		events []ev // inserted with end = start + 100
		want   bool
	}{
		{"nothing after the mark", []ev{{10, "binlog.000007", 100}}, false},
		{"the same numbering", []ev{{10, "binlog.000007", 100}, {11, "binlog.000007", 200}, {12, "binlog.000008", 4}}, false},
		{"RESET MASTER: binlog.000001 again", []ev{{10, "binlog.000007", 100}, {11, "binlog.000001", 300}}, true},
		{"the same file, lower", []ev{{10, "binlog.000007", 100}, {11, "binlog.000007", 150}}, true},
		{"a shorter base name", []ev{{10, "binlog.000007", 100}, {11, "bin.000001", 300}}, true},
		{"a longer base name", []ev{{10, "binlog.000007", 100}, {11, "mysql-bin.000001", 300}}, true},
		{"started over, then grew past the mark", []ev{{10, "binlog.000007", 100}, {11, "binlog.000001", 300}, {12, "binlog.000009", 4}}, true},
		{"fine at first, started over later", []ev{{10, "binlog.000007", 100}, {11, "binlog.000008", 4}, {12, "binlog.000001", 300}}, true},
		{"the mark's id names another event: an index rebuilt", []ev{{10, "binlog.000003", 100}, {11, "binlog.000001", 300}}, false},
		{"the mark's row deleted by a resume cleanup, older rows kept", []ev{{9, "binlog.000007", 4}, {11, "binlog.000007", 150}}, false},
		{"the mark's row rotated away with every older one", []ev{{11, "binlog.000001", 300}}, true},
		{"rotated away, the same numbering", []ev{{11, "binlog.000008", 300}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testutil.SkipIfNoMySQL(t)
			db, _ := testutil.CreateTestDB(t)
			if err := indexer.CreateIndexTables(context.Background(), db, 48, false, nil); err != nil {
				t.Fatal(err)
			}
			for _, e := range tc.events {
				testutil.MustExec(t, db, `INSERT INTO binlog_events
					(event_id, binlog_file, start_pos, end_pos, event_timestamp, schema_name, table_name, event_type, pk_values)
					VALUES (?, ?, ?, ?, UTC_TIMESTAMP(), 'shop', 'orders', 2, '1')`, e.id, e.file, e.start, e.start+100)
			}
			err := reconstruct.CheckNumberingContinues(context.Background(), db, mark)
			if got := errors.Is(err, reconstruct.ErrBinlogRenumbered); got != tc.want || (err != nil && !got) {
				t.Fatalf("CheckNumberingContinues = %v, want renumbered %v", err, tc.want)
			}
		})
	}
}
