//go:build integration

package reconstruct_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/reconstruct"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// #2182: `bintrail reconstruct --at <past>` reads by position from a snapshot
// up to --at. A binlog numbering that started over, or capture moving to
// another server, AFTER --at leaves that read whole, so it must not refuse;
// one at or before --at still must. A refresh (ExplicitAt unset) keeps the
// unbounded check: it refuses on anything indexed after the mark, whatever
// its At.

const (
	uuidA = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	uuidB = "4f22ab58-71ca-11e1-9e33-c80aa9429563"
)

// explicitAt runs a reconstruct to a dump (the command's default output) at
// at with ExplicitAt set as given, and returns every file it wrote, joined.
func (r *floorRig) explicitAt(t *testing.T, at time.Time, explicit bool) (string, error) {
	t.Helper()
	out := t.TempDir()
	_, err := reconstruct.ReconstructTables(r.ctx, reconstruct.FullTableConfig{
		IndexDSN: r.dsn, BaselineSrc: r.root, Tables: []string{r.schema + ".orders"},
		At: at, ExplicitAt: explicit, OutputDir: out, OutputFormat: reconstruct.OutputFormatMydumper,
	})
	if err != nil {
		return "", err
	}
	var all strings.Builder
	if werr := filepath.WalkDir(out, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		all.Write(b)
		return err
	}); werr != nil {
		t.Fatal(werr)
	}
	return all.String(), nil
}

// holds fails unless dump holds every value in want and none in bad.
func holds(t *testing.T, dump string, want, bad []string) {
	t.Helper()
	for _, v := range want {
		if !strings.Contains(dump, "'"+v+"'") {
			t.Errorf("the dump does not hold %q", v)
		}
	}
	for _, v := range bad {
		if strings.Contains(dump, "'"+v+"'") {
			t.Errorf("the dump holds %q", v)
		}
	}
}

// renumberRigNamingServer is renumberRig with capture reading server uuidA
// from the start, so the first refresh's event mark names it.
func renumberRigNamingServer(t *testing.T) (*floorRig, time.Time) {
	t.Helper()
	var T time.Time
	r, _ := newLagRig(t, func(first time.Time) time.Time {
		T = first.Add(6*time.Hour + 30*time.Minute)
		return T.Add(-4 * time.Hour)
	})
	markStreamCaptured(t, r.db)
	testutil.MustExec(t, r.db, `UPDATE stream_state SET bintrail_id = 'b1' WHERE id = 1`)
	testutil.MustExec(t, r.db, `INSERT INTO bintrail_servers (bintrail_id, server_uuid, host, port, username) VALUES ('b1', ?, 'db', 3306, 'u')`, uuidA)
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000007", 10, 100, T.Add(-time.Hour), "1", `{"id":1,"status":"A"}`)
	if _, _, err := r.refreshErr(T, false, false); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	p, _, _, err := reconstruct.FindBaseline(r.ctx, r.root, r.schema, "orders", T)
	if err != nil {
		t.Fatal(err)
	}
	md, err := baseline.ReadParquetMetadata(p)
	if err != nil {
		t.Fatal(err)
	}
	if m := reconstruct.ParseEventMark(md.EventMark); m == nil || m.ServerUUID != uuidA || md.Producer != baseline.ProducerReconstruct {
		t.Fatalf("first refresh's footer: event mark %q, producer %q; want a mark naming %s from the refresh", md.EventMark, md.Producer, uuidA)
	}
	return r, T
}

// failover moves capture from uuidA to uuidB at when, as capture records it:
// the server record updated in place and a server_uuid change row.
func failover(t *testing.T, r *floorRig, when time.Time) {
	t.Helper()
	testutil.MustExec(t, r.db, `UPDATE bintrail_servers SET server_uuid = ? WHERE bintrail_id = 'b1'`, uuidB)
	testutil.MustExec(t, r.db, serverChangeSQL, "server_uuid", uuidA, uuidB, when.Unix())
}

func TestReconstructAt_aRenumberingAfterTheTargetDoesNotRefuse_2182(t *testing.T) {
	r, T := renumberRig(t, false, false)
	// One change in the snapshot's numbering, then the numbering starts over.
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000007", 15, 300, T.Add(10*time.Minute), "2", `{"id":2,"status":"X"}`)
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000001", 20, 300, T.Add(20*time.Minute), "1", `{"id":1,"status":"B"}`)

	for _, tc := range []struct {
		name     string
		at       time.Time
		explicit bool
	}{
		// Today's check, which a refresh keeps: unbounded, so it refuses even
		// though the target is before the restart.
		{"refresh before the restart", T.Add(15 * time.Minute), false},
		{"--at the restart's own second", T.Add(20 * time.Minute), true},
		{"--at after the restart", T.Add(30 * time.Minute), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := r.explicitAt(t, tc.at, tc.explicit); !errors.Is(err, reconstruct.ErrBinlogRenumbered) {
				t.Fatalf("err = %v, want ErrBinlogRenumbered", err)
			}
		})
	}

	// A Parquet snapshot at --at keeps the whole-index check: its window ends
	// at the position of the first change past --at, here binlog.000001:300,
	// which sorts before the change at binlog.000007:300 it needs.
	t.Run("Parquet snapshot --at before the restart", func(t *testing.T) {
		_, _, err := r.refreshWith(T.Add(15*time.Minute), false, false, func(c *reconstruct.FullTableConfig) { c.ExplicitAt = true })
		if !errors.Is(err, reconstruct.ErrBinlogRenumbered) {
			t.Fatalf("err = %v, want ErrBinlogRenumbered", err)
		}
	})

	t.Run("--at before the restart", func(t *testing.T) {
		dump, err := r.explicitAt(t, T.Add(15*time.Minute), true)
		if err != nil {
			t.Fatalf("--at before the restart: %v", err)
		}
		// The change before --at applied, the one after it not.
		holds(t, dump, []string{"A", "X", "shipped"}, []string{"B", "paid"})
	})
}

// A renumbering in another table only: the read of orders is whole.
func TestReconstructAt_aRenumberingInAnotherTableDoesNotRefuse_2182(t *testing.T) {
	r, T := renumberRig(t, false, false)
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000007", 15, 300, T.Add(10*time.Minute), "2", `{"id":2,"status":"X"}`)
	insertEventAt(t, r.db, r.schema, "items", "binlog.000001", 20, 300, T.Add(12*time.Minute), "9", `{"id":9,"status":"y"}`)
	if _, err := r.explicitAt(t, T.Add(15*time.Minute), false); !errors.Is(err, reconstruct.ErrBinlogRenumbered) {
		t.Fatalf("without --at: err = %v, want ErrBinlogRenumbered (unchanged)", err)
	}
	dump, err := r.explicitAt(t, T.Add(15*time.Minute), true)
	if err != nil {
		t.Fatalf("--at with the renumbering in another table: %v", err)
	}
	holds(t, dump, []string{"A", "X", "shipped"}, []string{"paid"})
}

// A snapshot whose mark names the server: capture moving to another server
// after --at leaves the read whole; at or before it, refused.
func TestReconstructAt_aServerMoveAfterTheTargetDoesNotRefuse_2182(t *testing.T) {
	r, T := renumberRigNamingServer(t)
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000007", 15, 300, T.Add(10*time.Minute), "2", `{"id":2,"status":"X"}`)
	failover(t, r, T.Add(50*time.Minute))
	// The new server's files are numbered higher: positions show nothing.
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000009", 30, 100, T.Add(55*time.Minute), "3", `{"id":3,"status":"D"}`)

	for _, tc := range []struct {
		name     string
		at       time.Time
		explicit bool
	}{
		{"refresh before the move", T.Add(40 * time.Minute), false},
		{"--at the move's own second", T.Add(50 * time.Minute), true},
		{"--at after the move", T.Add(time.Hour), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := r.explicitAt(t, tc.at, tc.explicit); !errors.Is(err, reconstruct.ErrBinlogRenumbered) {
				t.Fatalf("err = %v, want ErrBinlogRenumbered", err)
			}
		})
	}

	t.Run("--at before the move", func(t *testing.T) {
		dump, err := r.explicitAt(t, T.Add(40*time.Minute), true)
		if err != nil {
			t.Fatalf("--at before the move: %v", err)
		}
		holds(t, dump, []string{"A", "X", "shipped"}, []string{"D", "paid"})
	})
}

// A failover and a failback both before --at: capture reads the mark's
// server again, so only the change rows show the other server's leg, whose
// changes the read by position could not order. Refused.
func TestReconstructAt_aFailoverAndFailbackBeforeTheTargetRefuses_2182(t *testing.T) {
	r, T := renumberRigNamingServer(t)
	testutil.MustExec(t, r.db, serverChangeSQL, "server_uuid", uuidA, uuidB, T.Add(5*time.Minute).Unix())
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000009", 15, 300, T.Add(6*time.Minute), "2", `{"id":2,"status":"Y"}`)
	testutil.MustExec(t, r.db, serverChangeSQL, "server_uuid", uuidB, uuidA, T.Add(8*time.Minute).Unix())
	if _, err := r.explicitAt(t, T.Add(40*time.Minute), true); !errors.Is(err, reconstruct.ErrBinlogRenumbered) {
		t.Fatalf("err = %v, want ErrBinlogRenumbered", err)
	}
}

// A refresh-produced snapshot whose mark names no server (written before
// marks did): nothing says which server its position belongs to, so the
// replaced-source check keeps today's behavior and refuses even a move after
// --at.
func TestReconstructAt_aMarkWithoutAServerKeepsTodaysCheck_2182(t *testing.T) {
	r, T := renumberRig(t, false, false)
	insertEventAt(t, r.db, r.schema, "orders", "binlog.000007", 15, 300, T.Add(10*time.Minute), "2", `{"id":2,"status":"X"}`)
	testutil.MustExec(t, r.db, serverChangeSQL, "server_uuid", uuidA, uuidB, T.Add(50*time.Minute).Unix())
	if _, err := r.explicitAt(t, T.Add(40*time.Minute), true); !errors.Is(err, reconstruct.ErrBinlogRenumbered) {
		t.Fatalf("err = %v, want ErrBinlogRenumbered", err)
	}
}
