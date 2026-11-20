//go:build integration

package pgcompare

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/dbtrail/dbtrail/internal/pgbaseline"
	"github.com/dbtrail/dbtrail/internal/pgshim"
	"github.com/dbtrail/dbtrail/internal/snapshotdir"
	"github.com/dbtrail/dbtrail/internal/sqlcompare"
	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/testutil"
)

func TestZZMeasureNumeric(t *testing.T) {
	baseDSN := testutil.SkipIfNoPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	setup, err := pgx.Connect(ctx, baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer setup.Close(context.Background())
	drop := func() {
		bg := context.Background()
		_, _ = setup.Exec(bg, "DROP PUBLICATION IF EXISTS zzm_pub")
		_, _ = setup.Exec(bg, "DROP TABLE IF EXISTS zzm_t")
		_, _ = setup.Exec(bg, "SELECT pg_drop_replication_slot('zzm_slot') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='zzm_slot')")
	}
	drop()
	defer drop()
	for _, s := range []string{
		`CREATE TABLE zzm_t (id int PRIMARY KEY, g int, amt numeric(10,2), q int, r double precision, b bigint, n numeric(20,6))`,
		`INSERT INTO zzm_t VALUES
			(1, 1, 500.00, 5, 2.5, 5, 0.000001),
			(2, 1, 400.00, -3, -2.5, 6, 12.345678),
			(3, 1, 600.00, 0, 0.5, 7, -1.5),
			(4, 2, 12.50, 7, 1.005, 8, 99999.999999),
			(5, 2, -7.25, 2, 3.5, 9, 0),
			(6, 2, NULL, NULL, NULL, NULL, NULL),
			(7, 3, 0.00, 1, 0.125, 1, 1),
			(8, 4, 0.10, 1, 1e-7, 1, 0.5),
			(9, 4, 0.20, 2, 123456.789, 2, 0.25)`,
		`CREATE PUBLICATION zzm_pub FOR TABLE zzm_t`,
	} {
		if _, err := setup.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	outDir := t.TempDir()
	repl := baseDSN + "?replication=database"
	if strings.Contains(baseDSN, "?") {
		repl = baseDSN + "&replication=database"
	}
	if _, err := pgbaseline.Run(ctx, pgbaseline.Config{QueryDSN: baseDSN, ReplDSN: repl, SlotName: "zzm_slot", Publication: "zzm_pub", OutputDir: outDir, Compression: "none", Parallelism: 1}); err != nil {
		t.Fatal(err)
	}
	snapshot := ""
	entries, _ := os.ReadDir(outDir)
	for _, e := range entries {
		if _, ok := snapshotdir.ParseTime(e.Name()); ok && e.IsDir() {
			snapshot = filepath.Join(outDir, e.Name())
		}
	}
	exe, _ := os.Executable()
	runner := sqlsandbox.New(sqlsandbox.Config{Exe: exe, Args: []string{}})
	raw, err := OpenSnapshotCopy(ctx, runner, snapshot, "public")
	if err != nil {
		t.Fatal(err)
	}
	rw, _ := OpenSnapshotCopy(ctx, runner, snapshot, "public")
	rw.Rewrite = pgshim.RewriteForCopy
	src, err := OpenSource(ctx, baseDSN, "public")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close(context.Background())
	stmts := defaultZZ
	if f := os.Getenv("ZZ_FILE"); f != "" {
		b, _ := os.ReadFile(f)
		stmts = strings.Split(strings.TrimSpace(string(b)), "\n")
	}
	show := func(r sqlcompare.Rows, err error) string {
		if err != nil {
			return "ERROR: " + err.Error()
		}
		var out []string
		for _, row := range r.Rows {
			var cells []string
			for _, c := range row {
				if c == nil {
					cells = append(cells, "NULL")
				} else {
					cells = append(cells, "'"+*c+"'")
				}
			}
			out = append(out, strings.Join(cells, ","))
		}
		return strings.Join(out, " | ")
	}
	for _, s := range stmts {
		a, _, e1 := src.Query(ctx, s, 100)
		b, _, e2 := raw.Query(ctx, s, 100)
		c, _, e3 := rw.Query(ctx, s, 100)
		t.Logf("\nSTMT %s\n  PG16 : %s\n  RAW  : %s\n  RWR  : %s", s, show(a, e1), show(b, e2), show(c, e3))
	}
}

var defaultZZ = []string{
	`SELECT round(500.00, 2), round(500, 2), round(0, 2), round(0.0, 2), round(-12.345, 2), round(-0.004, 2)`,
	`SELECT round(12.5, 0), round(15, -1), round(14.99, -1), round(12.5), round(-12.5), round(500), round(0.5)`,
	`SELECT round(2.5::float8), round(3.5::float8), round(-2.5::float8), round(2.4::float8)`,
	`SELECT round(1.005::float8, 2)`,
	`SELECT id, amt FROM zzm_t ORDER BY id`,
	`SELECT id, round(amt, 2), round(amt, 3), round(amt, 1), round(amt, 0), round(amt, -1), round(amt) FROM zzm_t ORDER BY id`,
	`SELECT id, round(q, 2), round(q), round(q, -1), round(b, 2) FROM zzm_t ORDER BY id`,
	`SELECT id, round(r) FROM zzm_t ORDER BY id`,
	`SELECT id, round(r, 2) FROM zzm_t ORDER BY id`,
	`SELECT id, round(n, 2), round(n, 8), round(n) FROM zzm_t ORDER BY id`,
	`SELECT g, sum(amt), avg(amt), round(avg(amt), 2), round(avg(amt)), round(avg(amt), 0), round(avg(amt), -1) FROM zzm_t GROUP BY g ORDER BY g`,
	`SELECT g, round(sum(amt), 2), round(sum(amt), 3), round(sum(amt)), sum(q), avg(q), round(avg(q), 2), avg(b) FROM zzm_t GROUP BY g ORDER BY g`,
	`SELECT g, avg(r), round(avg(r)) FROM zzm_t GROUP BY g ORDER BY g`,
	`SELECT g, avg(n), round(avg(n), 2), sum(n) FROM zzm_t GROUP BY g ORDER BY g`,
	`SELECT round(avg(amt), 2), avg(amt), sum(amt) FROM zzm_t WHERE id < 0`,
	`SELECT round(NULL::numeric, 2), round(amt, 2) FROM zzm_t WHERE id = 6`,
	`SELECT g, CASE WHEN g = 1 THEN round(avg(amt), 2) ELSE 0 END FROM zzm_t GROUP BY g ORDER BY g`,
	`SELECT id, CASE WHEN id < 3 THEN amt ELSE 0 END, coalesce(amt, 0), CASE WHEN id < 3 THEN 1.5 ELSE 2.25 END FROM zzm_t ORDER BY id`,
	`SELECT id, amt * 2, amt + 1, amt * q, amt * 1.5, -amt FROM zzm_t ORDER BY id`,
	`SELECT avg(x) FROM (VALUES (0), (1)) v(x)`,
	`SELECT avg(x) FROM (VALUES (5.00), (10.00), (10.00)) v(x)`,
	`SELECT 100 * avg(amt), round(100 * avg(amt), 1) FROM zzm_t`,
}
