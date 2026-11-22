//go:build integration

package consoleapp

import (
	"bytes"
	"context"
	"database/sql"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/console"
)

// #3108 through `server add --flavor postgres` itself: a publication that
// does not publish TRUNCATE. The add goes through and says, with the
// statement to run, that a TRUNCATE is not recorded; the saved server stops
// and starts again with the same warning, and capture attaches each time.
func TestIntegrationServerAddPostgresWarnsAboutAPublicationWithoutTruncate_3108(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const table, slot, pub = "add3108_orders", "add3108_slot", "add3108_pub"
	baseDSN, source, exec1 := pgTestSource(t, ctx, table, slot, pub, "id int PRIMARY KEY, total int")
	exec1("ALTER PUBLICATION " + pub + " SET (publish = 'insert, update, delete')")
	exec1("INSERT INTO " + table + " VALUES (1, 10)")

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
	pw, _ := base.User.Password()
	in := addInput{Host: base.Hostname(), Port: base.Port(), User: base.User.Username(), Password: pw,
		Name: "add3108", Flavor: console.FlavorPostgres, Database: strings.TrimPrefix(base.Path, "/"), Slot: slot, Publication: pub}

	const want = "  warn  Publication publishes TRUNCATE\n" +
		`        Publication "add3108_pub" does not publish TRUNCATE, so a TRUNCATE on the source is not recorded: the rows it removed stay in the copy and come back in every rebuild of the table, with nothing to say they are gone` + "\n"
	const fix = `          ALTER PUBLICATION "add3108_pub" SET (publish = 'insert, update, delete, truncate');` + "\n"
	var out bytes.Buffer
	err = runServerAdd(ctx, c, in, false, &out)
	t.Logf("server add, publication without TRUNCATE:\n%s\nerror: %v", out.String(), err)
	if err != nil {
		t.Fatalf("err = %v: a publication without TRUNCATE must not stop the add\n%s", err, out.String())
	}
	for _, w := range []string{want, fix, `Added "add3108". DBTrail is recording its changes.`} {
		if !strings.Contains(out.String(), w) {
			t.Errorf("output lacks:\n%s\ngot:\n%s", w, out.String())
		}
	}
	if strings.Contains(out.String(), "FAIL") || strings.Contains(out.String(), "was not added") {
		t.Errorf("the add reads as refused:\n%s", out.String())
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

	if err := mon.Stop(ctx, e.ID); err != nil {
		t.Fatalf("stop: %v", err)
	}
	var res struct {
		Doctor  *console.DoctorReport `json:"doctor"`
		Started bool                  `json:"started"`
	}
	if err := c.do(ctx, "POST", "/api/servers/"+e.ID+"/monitor/start", nil, &res); err != nil {
		t.Fatalf("start: %v", err)
	}
	out.Reset()
	failed := printChecks(&out, res.Doctor, false)
	t.Logf("start of the saved server: started %v\n%s", res.Started, out.String())
	if !res.Started || failed != 0 {
		t.Fatalf("started %v with %d failed check(s): a publication without TRUNCATE must not stop a start", res.Started, failed)
	}
	if res.Doctor != nil && (!strings.Contains(out.String(), want) || !strings.Contains(out.String(), fix)) {
		t.Errorf("the start's checks lack the warning:\n%s", out.String())
	}
	attached("capture attached to its slot after Start")
}
