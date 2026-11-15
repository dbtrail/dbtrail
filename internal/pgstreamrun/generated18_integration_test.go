//go:build integration

package pgstreamrun_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
	"github.com/dbtrail/dbtrail/internal/pgstreamrun"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

// PostgreSQL 18 and a generated column the publication does not publish:
// the doctor fails with the statements to run, and the capture refuses to
// start, before it creates a slot or writes anything. On 17 and older the
// same setup passes and captures.
func TestGeneratedColumnsStopDoctorAndCapture(t *testing.T) {
	pgDSN := testutil.SkipIfNoPostgres(t)
	testutil.SkipIfNoMySQL(t)
	ctx := context.Background()
	pg, err := pgx.Connect(ctx, pgDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { pg.Close(context.Background()) })
	var version int
	if err := pg.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&version); err != nil {
		t.Fatal(err)
	}
	pg18 := version >= 180000

	const slot, pub, tbl = "bintrail_gen18sr_it", "bintrail_gen18sr_it_pub", "gen18sr_it_t"
	drop := func() {
		bg := context.Background()
		_, _ = pg.Exec(bg, "DROP PUBLICATION IF EXISTS "+pub)
		_, _ = pg.Exec(bg, "DROP TABLE IF EXISTS "+tbl)
		for range 50 {
			_, _ = pg.Exec(bg, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1 AND NOT active", slot)
			var n int
			if err := pg.QueryRow(bg, "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&n); err != nil || n == 0 {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	drop()
	t.Cleanup(drop)
	for _, q := range []string{
		fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text, vlen int GENERATED ALWAYS AS (length(v)) STORED)", tbl),
		fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", tbl),
		fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pub, tbl),
	} {
		if _, err := pg.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	_, dbName := testutil.CreateTestDB(t)
	indexDSN := testutil.BaseDSN() + "/" + dbName
	repl := pgDSN + "?replication=database"
	if strings.Contains(pgDSN, "?") {
		repl = pgDSN + "&replication=database"
	}
	const check = "Generated columns (PostgreSQL 18)"
	report := func() doctor.CheckResult {
		t.Helper()
		r := pgstreamrun.BuildPGReport(ctx, pgstreamrun.PGDoctorConfig{QueryDSN: pgDSN, SlotName: slot, Publication: pub})
		for _, c := range r.Checks {
			if c.Name == check {
				return c
			}
		}
		t.Fatalf("the report has no check named %q: %+v", check, r.Checks)
		return doctor.CheckResult{}
	}
	// start runs the capture and returns its error if it ends within a few
	// seconds, or nil once the slot is active (it is capturing).
	start := func() error {
		t.Helper()
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			done <- pgstreamrun.One(runCtx, pgstreamrun.Config{
				IndexDSN: indexDSN, ReplDSN: repl, QueryDSN: pgDSN, SlotName: slot, Publication: pub,
				ServerID: 2818, Tables: "public." + tbl, BatchSize: 100, Checkpoint: 200 * time.Millisecond,
			})
		}()
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			select {
			case err := <-done:
				return err
			default:
			}
			var active bool
			if pg.QueryRow(ctx, "SELECT active FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&active) == nil && active {
				cancel()
				<-done
				return nil
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal("the capture neither started nor ended")
		return nil
	}

	c := report()
	if !pg18 {
		if c.Status != doctor.StatusPass {
			t.Fatalf("PostgreSQL %d has no such rule: %+v", version, c)
		}
		if err := start(); err != nil {
			t.Fatalf("the capture did not start on PostgreSQL %d: %v", version, err)
		}
		return
	}
	if c.Status != doctor.StatusFail || !strings.Contains(c.Detail, fmt.Sprintf(`ALTER TABLE "public".%q REPLICA IDENTITY DEFAULT;`, tbl)) ||
		!strings.Contains(c.Detail, fmt.Sprintf(`ALTER PUBLICATION %q SET (publish_generated_columns = stored);`, pub)) {
		t.Fatalf("the doctor on PostgreSQL 18: %+v", c)
	}
	err = start()
	if !errors.Is(err, pgcapture.ErrGeneratedBlocksWrites) {
		t.Fatalf("the capture on PostgreSQL 18 with an unpublished generated column: %v, want a refusal", err)
	}
	// It refused before it touched the source: no slot was created.
	var slots int
	if err := pg.QueryRow(ctx, "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = $1", slot).Scan(&slots); err != nil || slots != 0 {
		t.Fatalf("the refused capture left %d slot(s) (err %v)", slots, err)
	}
	// The statement the message gives makes both pass.
	if _, err := pg.Exec(ctx, "ALTER PUBLICATION "+pub+" SET (publish_generated_columns = stored)"); err != nil {
		t.Fatal(err)
	}
	if c := report(); c.Status != doctor.StatusPass {
		t.Fatalf("the doctor after the publication publishes the column: %+v", c)
	}
	if err := start(); err != nil {
		t.Fatalf("the capture after the publication publishes the column: %v", err)
	}
}
