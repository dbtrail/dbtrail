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

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/dbtrail/dbtrail/internal/console"
)

// #3020 against a real PostgreSQL, a real index and the daemon's own
// supervisor and handlers: `server add --flavor postgres` with a forwarding
// user runs the real checks, saves the server, starts capture and saves the
// forwarding account; and what it saved logs in to the source as that role
// and reads the table. The login is made here with the PostgreSQL driver the
// port uses, which is the reader of that saved URL.
func TestIntegrationServerAddPostgresSetsTheForwardingUser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	const table, slot, pub, role, rolePW = "add3020_t", "add3020_slot", "add3020_pub", "add3020_ro", "fwd pw/3020"
	baseDSN, source, exec1 := pgTestSource(t, ctx, table, slot, pub, "id int PRIMARY KEY, v text")
	exec1("INSERT INTO " + table + " VALUES (1, 'one')")
	dropRole := func() {
		bg := context.Background()
		_, _ = source.Exec(bg, `DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '`+role+`') THEN EXECUTE 'DROP OWNED BY `+role+`'; END IF; END $$`).ReadAll()
		_, _ = source.Exec(bg, "DROP ROLE IF EXISTS "+role).ReadAll()
	}
	dropRole()
	t.Cleanup(dropRole)
	exec1("CREATE ROLE " + role + " LOGIN PASSWORD '" + rolePW + "'")
	exec1("GRANT SELECT ON " + table + " TO " + role)

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

	capture, err := url.Parse(baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	capturePW, _ := capture.User.Password()
	in := addInput{Host: capture.Hostname(), Port: capture.Port(), User: capture.User.Username(), Password: capturePW,
		Name: "add3020", Flavor: console.FlavorPostgres, Database: strings.TrimPrefix(capture.Path, "/"), Slot: slot, Publication: pub,
		FwdUser: role, FwdPassword: rolePW}
	// Whatever the add leaves behind is stopped and dropped, before the
	// source's own cleanup drops the slot.
	t.Cleanup(func() {
		for _, e := range reg.List() {
			_ = mon.Stop(context.Background(), e.ID)
			if db, err := sql.Open("mysql", bootDSN); err == nil {
				_, _ = db.Exec("DROP DATABASE IF EXISTS bintrail_idx_" + e.ID)
				_ = db.Close()
			}
		}
	})

	var out bytes.Buffer
	if err := runServerAdd(ctx, c, in, false, &out); err != nil {
		t.Fatalf("err = %v\n%s", err, out.String())
	}
	for _, want := range []string{
		`Added "add3020". DBTrail is recording its changes.`,
		`"add3020" forwards as ` + role + `: the PostgreSQL port sends to your PostgreSQL, as that user and read-only, every statement the copy is not known to answer as PostgreSQL does, and a client's transaction blocks.`,
		"Next, the first copy of its tables:  bintrail-console snapshot run add3020\n",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), rolePW) || (capturePW != "" && strings.Contains(out.String(), capturePW)) {
		t.Errorf("a password in the output:\n%s", out.String())
	}

	entries := reg.List()
	if len(entries) != 1 {
		t.Fatalf("servers saved: %d, want 1", len(entries))
	}
	e := entries[0]
	if !e.IsPostgres() || e.SourceSlot != slot || e.SourcePublication != pub || e.RouteDSN == "" {
		t.Fatalf("saved entry: flavor %q slot %q publication %q, forwarding account set: %v", e.Flavor, e.SourceSlot, e.SourcePublication, e.RouteDSN != "")
	}
	if dsn, _, refused := srv.PostgresForwardAccount(e.ID); dsn != e.RouteDSN || refused != "" {
		t.Fatalf("the port would not forward with what was saved: refused %q", refused)
	}

	// What was saved logs in as the role, on the database that was added.
	fwd, err := pgconn.Connect(ctx, e.RouteDSN)
	if err != nil {
		t.Fatalf("the saved forwarding account does not log in: %v", err)
	}
	defer fwd.Close(context.Background())
	_, rows, err := pgText(t, fwd, "SELECT current_user::text, current_database()::text, (SELECT v FROM "+table+" WHERE id = 1)")
	if err != nil || len(rows) != 1 || rows[0][0] != role || rows[0][1] != in.Database || rows[0][2] != "one" {
		t.Fatalf("as the saved forwarding account: rows %v, err %v", rows, err)
	}

	// Capture was started by the add, on the slot it was given.
	pgTestWait(t, "capture attached to its slot", 60*time.Second, func() (bool, string) {
		_, rows, err := pgText(t, source, "SELECT active::text FROM pg_replication_slots WHERE slot_name = '"+slot+"'")
		if err != nil || len(rows) != 1 {
			return false, "no slot yet"
		}
		return rows[0][0] == "true", "active=" + rows[0][0]
	})

	// `server forward-user` removes it afterwards, as before.
	out.Reset()
	if err := runServerForwardUser(ctx, c, "add3020", fwdFlags{Remove: true}, nil, strings.NewReader(""), &out); err != nil {
		t.Fatal(err)
	}
	if got, ok := reg.Get(e.ID); !ok || got.RouteDSN != "" {
		t.Fatalf("after --remove: found %v, forwarding account still set: %v", ok, got.RouteDSN != "")
	}
}
