//go:build integration

package consoleapp

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/baseline"
	"github.com/dbtrail/dbtrail/internal/console"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// #3022 through `server add --flavor postgres` itself: a real PostgreSQL, a
// real index, the daemon's own supervisor and handlers, and the role of the
// issue, which may replicate and has no SELECT on the published table.
//
// What is missing for the copy does not stop capture. The add goes through
// and says, with the statement to run, that the first copy will be refused;
// a server saved with that role can be stopped and started again; the copy
// is refused with the same statement; and after that statement it works.
func TestIntegrationServerAddPostgresWarnsAboutARoleWithoutSelect_3022(t *testing.T) {
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
	if err != nil {
		t.Fatalf("err = %v: a role that replicates and cannot read must still be added\n%s", err, out.String())
	}
	const grant = `GRANT SELECT ON TABLE "public"."add3022_customers" TO "add3022_capture";`
	want := "  warn  Read access to the published tables\n" +
		`        The role "add3022_capture" cannot read 1 of the 1 table the copy reads: "public"."add3022_customers" (no SELECT)` + "\n" +
		"        Recording changes does not need these permissions, so this does not stop the server from being added or started. " +
		"The first copy of the tables needs them: it reads each one as this role, and DBTrail refuses to start it until they are there. " +
		"DBTrail grants nothing by itself. Run this on the database, as the tables' owner or a superuser:\n" +
		"          " + grant + "\n"
	for _, w := range []string{want, `Added "add3022". DBTrail is recording its changes.`} {
		if !strings.Contains(out.String(), w) {
			t.Errorf("output lacks:\n%s\ngot:\n%s", w, out.String())
		}
	}
	if strings.Contains(out.String(), "FAIL") || strings.Contains(out.String(), "was not added") {
		t.Errorf("the add reads as refused:\n%s", out.String())
	}
	if strings.Contains(out.String(), rolePW) {
		t.Errorf("the password in the output:\n%s", out.String())
	}
	entries := reg.List()
	if len(entries) != 1 {
		t.Fatalf("servers saved: %d, want 1", len(entries))
	}
	e := entries[0]
	attached := func(what string) {
		t.Helper()
		pgTestWait(t, what, 60*time.Second, func() (bool, string) {
			_, rows, err := pgText(t, source, "SELECT active::text FROM pg_replication_slots WHERE slot_name = '"+slot+"'")
			if err != nil || len(rows) != 1 {
				return false, "no slot yet"
			}
			return rows[0][0] == "true", "active=" + rows[0][0]
		})
	}
	attached("capture attached to its slot after the add")

	// Start of a saved server reads the same report: with a failure in it,
	// a server that records today could not be started again.
	if err := mon.Stop(ctx, e.ID); err != nil {
		t.Fatalf("stop: %v", err)
	}
	out.Reset()
	started, err := startRecording(ctx, c, e.ID, &out)
	if err != nil || !started {
		t.Fatalf("start of the saved server: started %v, err %v\n%s", started, err, out.String())
	}
	attached("capture attached to its slot after Start")

	// The copy is what is refused, with the same statement, and nothing of
	// it is written.
	local := t.TempDir()
	sup := newBaselineSupervisor(ctx, t.TempDir(), baseline.DefaultLockMode)
	sup.jobs[e.ID] = &console.BaselineStatus{State: "running"}
	req := console.BaselineRequestFor(e)
	req.LocalDir = local
	_, err = sup.executePG(req)
	t.Logf("the first copy, as that role:\n%v", err)
	if err == nil || !errors.Is(err, pgcapture.ErrNoReadAccess) || !strings.Contains(err.Error(), grant) {
		t.Fatalf("the copy: err = %v, want it refused with %s", err, grant)
	}
	if left, _ := os.ReadDir(local); len(left) != 0 {
		t.Errorf("the refused copy wrote %v", left)
	}

	// The statement shown, as printed, and the copy works.
	exec1(grant)
	if out, err := sup.executePG(req); err != nil || out.stats.RowsWritten != 1 {
		t.Fatalf("the copy after the statement shown: rows %d, err %v", out.stats.RowsWritten, err)
	}
}

// Start of a server that is already saved with a role that replicates and
// cannot read its tables (#3022): it starts. Start reads the report the add
// reads, and a failure in it would leave a server that records today
// impossible to start again after a stop or an upgrade.
func TestIntegrationServerStartPostgresWithARoleWithoutSelect_3022(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const table, slot, pub, role, rolePW = "start3022_customers", "start3022_slot", "start3022_pub", "start3022_capture", "capture pw/3022"
	baseDSN, source, exec1 := pgTestSource(t, ctx, table, slot, pub, "id int PRIMARY KEY, name text")
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

	// Saved as an earlier version saved it: no check of this kind ran.
	u, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(role, rolePW)
	e, err := reg.Add(console.ServerEntry{Name: "start3022", Flavor: console.FlavorPostgres,
		SourceDSN: u.String(), SourceSlot: slot, SourcePublication: pub, BaselineDir: t.TempDir()})
	if err != nil {
		t.Fatalf("save the server: %v", err)
	}
	if e.DSN, err = mon.DeriveIndexDSN(e.ID); err != nil {
		t.Fatal(err)
	}
	if err := reg.Update(e); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = mon.Stop(context.Background(), e.ID)
		if db, err := sql.Open("mysql", bootDSN); err == nil {
			_, _ = db.Exec("DROP DATABASE IF EXISTS bintrail_idx_" + e.ID)
			_ = db.Close()
		}
	})

	// What `server start` and the web's Start button send.
	var res struct {
		Doctor  *console.DoctorReport `json:"doctor"`
		Started bool                  `json:"started"`
	}
	if err := c.do(ctx, "POST", "/api/servers/"+e.ID+"/monitor/start", nil, &res); err != nil {
		t.Fatalf("start: %v", err)
	}
	var out bytes.Buffer
	failed := printChecks(&out, res.Doctor, false)
	t.Logf("start of a saved server, as a role without SELECT: started %v\n%s", res.Started, out.String())
	if !res.Started || failed != 0 {
		t.Fatalf("started %v with %d failed check(s): a server that records today must start", res.Started, failed)
	}
	// The answer carries the warning with the statement, for the screen.
	if res.Doctor != nil && (!strings.Contains(out.String(), "  warn  Read access to the published tables\n") ||
		!strings.Contains(out.String(), `GRANT SELECT ON TABLE "public"."start3022_customers" TO "start3022_capture";`)) {
		t.Errorf("the start's checks lack the warning:\n%s", out.String())
	}
	pgTestWait(t, "capture attached to its slot", 60*time.Second, func() (bool, string) {
		_, rows, err := pgText(t, source, "SELECT active::text FROM pg_replication_slots WHERE slot_name = '"+slot+"'")
		if err != nil || len(rows) != 1 {
			return false, "no slot yet"
		}
		return rows[0][0] == "true", "active=" + rows[0][0]
	})
}
