//go:build integration

package consoleapp

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

// #3022 through `server add --flavor postgres` itself: a real PostgreSQL, a
// real index, the daemon's own supervisor and handlers, and the role of the
// issue, which may replicate and has no SELECT on the published table.
// Before the check that role was added and its first snapshot failed with
// 42501. Now the add is refused, with the statement to run; nothing is
// saved and no slot is created; and after that statement the same command
// adds the server.
func TestIntegrationServerAddPostgresRefusesARoleWithoutSelect_3022(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const table, slot, pub, role, rolePW = "add3022_customers", "add3022_slot", "add3022_pub", "add3022_capture", "capture pw/3022"
	baseDSN, source, exec1 := pgTestSource(t, ctx, table, slot, pub, "id int PRIMARY KEY, name text")
	exec1("INSERT INTO " + table + " VALUES (1, 'Ada')")
	dropRole := func() {
		bg := context.Background()
		_, _ = source.Exec(bg, `DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '`+role+`') THEN EXECUTE 'DROP OWNED BY `+role+`'; END IF; END $$`).ReadAll()
		_, _ = source.Exec(bg, "DROP ROLE IF EXISTS "+role).ReadAll()
	}
	dropRole()
	t.Cleanup(dropRole)
	exec1("CREATE ROLE " + role + " LOGIN REPLICATION PASSWORD '" + rolePW + "'")

	mon, bootDSN := pgTestMonitor(t, ctx, time.Second)
	reg, err := console.LoadRegistry(filepath.Join(t.TempDir(), "console-servers.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	srv, err := console.New(console.Config{Listen: "127.0.0.1:0", Token: "tok", Registry: reg, MonitorCtrl: mon})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	c := &ctlClient{base: ts.URL, token: "tok", hc: ts.Client()}
	t.Cleanup(func() {
		for _, e := range reg.List() {
			_ = mon.Stop(context.Background(), e.ID)
			if db, err := sql.Open("mysql", bootDSN); err == nil {
				_, _ = db.Exec("DROP DATABASE IF EXISTS bintrail_idx_" + e.ID)
				_ = db.Close()
			}
		}
	})

	base, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	in := addInput{Host: base.Hostname(), Port: base.Port(), User: role, Password: rolePW,
		Name: "add3022", Flavor: console.FlavorPostgres, Database: strings.TrimPrefix(base.Path, "/"), Slot: slot, Publication: pub}

	var out bytes.Buffer
	err = runServerAdd(ctx, c, in, false, &out)
	t.Logf("server add, as a role without SELECT:\n%s\nerror: %v", out.String(), err)
	if !errors.Is(err, errNotAdded) || err.Error() != "the database was not added: 1 check failed. Fix it and run the command again" {
		t.Fatalf("err = %v, want the add refused on one check\n%s", err, out.String())
	}
	const grant = `GRANT SELECT ON TABLE "public"."add3022_customers" TO "add3022_capture";`
	want := "  FAIL  Read access to the published tables\n" +
		`        The role "add3022_capture" cannot read 1 of the 1 table the copy reads: "public"."add3022_customers" (no SELECT)` + "\n" +
		"        Recording changes does not need these permissions. The first copy of the tables does: it reads each one as this role, " +
		"and would stop with \"permission denied\" (42501). DBTrail grants nothing by itself. " +
		"Run this on the database, as the tables' owner or a superuser:\n" +
		"          " + grant + "\n"
	if !strings.Contains(out.String(), want) {
		t.Errorf("output lacks:\n%s\ngot:\n%s", want, out.String())
	}
	if strings.Count(out.String(), "FAIL") != 1 {
		t.Errorf("more than one failed check:\n%s", out.String())
	}
	if strings.Contains(out.String(), rolePW) {
		t.Errorf("the password in the output:\n%s", out.String())
	}
	if n := len(reg.List()); n != 0 {
		t.Fatalf("a refused add saved %d server(s)", n)
	}
	if _, rows, err := pgText(t, source, "SELECT count(*)::text FROM pg_replication_slots WHERE slot_name = '"+slot+"'"); err != nil || len(rows) != 1 || rows[0][0] != "0" {
		t.Fatalf("a refused add left a replication slot: rows %v, err %v", rows, err)
	}

	// The statement shown, as printed, and the same command again.
	exec1(grant)
	out.Reset()
	if err := runServerAdd(ctx, c, in, false, &out); err != nil {
		t.Fatalf("after the statement shown: err = %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "  ok    Read access to the published tables\n") ||
		!strings.Contains(out.String(), `Added "add3022". DBTrail is recording its changes.`) {
		t.Errorf("after the statement shown:\n%s", out.String())
	}
}
