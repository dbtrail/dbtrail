package consoleapp

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

// `server add --flavor postgres` (#2773): what a PostgreSQL needs beyond its
// address and login is settled before anything is sent, and what belongs to
// one family is refused for the other.
func TestResolveAddInputPostgres(t *testing.T) {
	pgcnf := "[client]\nhost=pghost\nuser=dbtrail\npassword=pw\n"
	cases := []struct {
		name    string
		flags   addFlags
		cnf     string
		ask     fakeAsker
		want    addInput
		wantErr string
	}{
		{
			name:  "everything on the command line",
			flags: addFlags{Host: "pg1", User: "u", Flavor: "postgres", Database: " shop ", Publication: " shop_pub ", Slot: " shop_slot "},
			ask:   fakeAsker{tty: true, secret: "pw"},
			want: addInput{Host: "pg1", Port: "5432", User: "u", Password: "pw", Flavor: "postgres",
				Database: "shop", Publication: "shop_pub", Slot: "shop_slot", FwdAsk: true},
		},
		{
			name:  "postgresql, in capitals, is postgres",
			flags: addFlags{Host: "pg1", Port: "6432", User: "u", Flavor: " PostgreSQL ", Database: "shop", Publication: "p", Slot: "s"},
			ask:   fakeAsker{tty: true, secret: "pw"},
			want: addInput{Host: "pg1", Port: "6432", User: "u", Password: "pw", Flavor: "postgres",
				Database: "shop", Publication: "p", Slot: "s", FwdAsk: true},
		},
		{
			name:  "the slot is named after the database when none is given, without a terminal",
			flags: addFlags{Flavor: "postgres", Database: "Shop-EU.2", Publication: "p"},
			cnf:   pgcnf,
			want: addInput{Host: "pghost", Port: "5432", User: "dbtrail", Password: "pw", Flavor: "postgres",
				Database: "Shop-EU.2", Publication: "p", Slot: "bintrail_shop_eu_2"},
		},
		{
			name:  "on a terminal, what is left out is asked for, with the slot's default offered",
			flags: addFlags{Host: "pg1", User: "u", Flavor: "postgres"},
			ask: fakeAsker{tty: true, secret: "pw", lines: map[string]string{
				"Database": "shop", "Publication (you create it; see docs/postgres.md)": "", "Replication slot (DBTrail creates it)": ""}},
			want: addInput{Host: "pg1", Port: "5432", User: "u", Password: "pw", Flavor: "postgres",
				Database: "shop", Publication: "bintrail_pub", Slot: "bintrail_shop", FwdAsk: true},
		},
		{
			name:  "a host asked for offers PostgreSQL's port",
			flags: addFlags{User: "u", Flavor: "postgres", Database: "shop", Publication: "p", Slot: "s"},
			ask:   fakeAsker{tty: true, secret: "pw", lines: map[string]string{"Host": "pg1", "Port": ""}},
			want: addInput{Host: "pg1", Port: "5432", User: "u", Password: "pw", Flavor: "postgres",
				Database: "shop", Publication: "p", Slot: "s", FwdAsk: true},
		},
		{name: "no database, no terminal", flags: addFlags{Flavor: "postgres", Publication: "p"}, cnf: pgcnf, wantErr: "-d"},
		{name: "no publication, no terminal", flags: addFlags{Flavor: "postgres", Database: "shop"}, cnf: pgcnf, wantErr: "--publication"},
		{name: "a database typed as nothing", flags: addFlags{Host: "pg1", User: "u", Flavor: "postgres"},
			ask: fakeAsker{tty: true, secret: "pw", lines: map[string]string{"Database": ""}}, wantErr: "no database"},
		{name: "a slot PostgreSQL would refuse", flags: addFlags{Flavor: "postgres", Database: "shop", Publication: "p", Slot: "Shop-Slot"}, cnf: pgcnf, wantErr: "cannot name a replication slot"},
		{name: "a slot of 64 characters", flags: addFlags{Flavor: "postgres", Database: "shop", Publication: "p", Slot: strings.Repeat("a", 64)}, cnf: pgcnf, wantErr: "cannot name a replication slot"},
		// The forwarding user (#3020) has its own tests, in
		// server_add_forward_pg_3020_test.go.
		// A PostgreSQL's options on a MySQL say the flavor was left out.
		{name: "--database without the flavor", flags: addFlags{Host: "db1", User: "u", Database: "shop"}, wantErr: "--flavor postgres"},
		{name: "--publication on a mariadb", flags: addFlags{Host: "db1", User: "u", Flavor: "mariadb", Publication: "p"}, wantErr: "--flavor postgres"},
		{name: "--slot without the flavor", flags: addFlags{Host: "db1", User: "u", Slot: "s"}, wantErr: "--flavor postgres"},
		// PostgreSQL's port with no flavor: said before a password is asked.
		{name: "5432 and no flavor", flags: addFlags{Host: "db1", Port: "5432", User: "u"}, ask: fakeAsker{tty: true, secret: "pw"}, wantErr: "5432 is PostgreSQL's port"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := c.flags
			if c.cnf != "" {
				f.DefaultsFile = writeCnf(t, c.cnf)
			}
			ask := c.ask
			ask.t = t
			got, err := resolveAddInput(f, &ask, strings.NewReader(""))
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("got %+v, %v; want an error with %q", got, err, c.wantErr)
				}
				if c.name == "5432 and no flavor" && ask.secrets != 0 {
					t.Error("the password was asked for before the flavor was refused")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.FwdAsk != c.ask.tty || got.FwdUser != "" {
				t.Errorf("the forwarding user is asked for on a terminal and nowhere else: %+v", got)
			}
			got.ask = nil
			if got != c.want {
				t.Fatalf("got %+v\nwant %+v", got, c.want)
			}
		})
	}
}

func TestPGDefaultSlot(t *testing.T) {
	for in, want := range map[string]string{
		"shop":                  "bintrail_shop",
		"Shop":                  "bintrail_shop",
		"my-db.v2":              "bintrail_my_db_v2",
		"ünï":                   "bintrail__n_",
		strings.Repeat("d", 80): "bintrail_" + strings.Repeat("d", 54),
		"":                      "bintrail_",
	} {
		got := pgDefaultSlot(in)
		if got != want || !pgSlotNameRE.MatchString(got) {
			t.Errorf("pgDefaultSlot(%q) = %q, want %q (a name PostgreSQL takes)", in, got, want)
		}
	}
}

// What is sent for a PostgreSQL: its three fields, no question about what
// answers at the address (it is asked in MySQL's protocol), no forwarding
// account when none was named, and the psql line in place of the mysql one.
func TestServerAddPostgres(t *testing.T) {
	on := true
	d, c := newFakeDaemon(t)
	d.on("POST /api/servers/check", 200, map[string]any{"ok": true, "name": "shop", "started": true, "doctor": passReport(),
		"server": map[string]any{"id": "abc", "name": "shop"},
		"port":   map[string]any{"enabled": on, "turned_on": true, "routing_on": true, "host": "", "port": "3309", "password": "bfp_abc123"}})
	var out bytes.Buffer
	in := addInput{Host: "pg1", Port: "5432", User: "dbtrail", Password: "s3cret#1", Name: "shop", Flavor: "postgres",
		Database: "shopdb", Slot: "bintrail_shopdb", Publication: "bintrail_pub", Schemas: "public"}
	if err := runServerAdd(context.Background(), c, in, false, &out); err != nil {
		t.Fatalf("err = %v\n%s", err, out.String())
	}
	if len(d.calls) != 1 || d.calls[0] != "POST /api/servers/check" {
		t.Fatalf("calls = %v, want the check alone", d.calls)
	}
	got := d.bodies["POST /api/servers/check"]
	for k, v := range map[string]any{
		"flavor": "postgres", "name": "shop", "source_host": "pg1", "source_port": "5432", "source_user": "dbtrail",
		"source_password": "s3cret#1", "schemas": "public", "no_draft": true,
		"source_database": "shopdb", "source_slot": "bintrail_shopdb", "source_publication": "bintrail_pub",
	} {
		if got[k] != v {
			t.Errorf("check body %s = %v, want %v", k, got[k], v)
		}
	}
	text := out.String()
	for _, s := range []string{`Added "shop"`, "The port for SQL clients is on now.",
		`psql "host=127.0.0.1 port=5433 dbname=shop user=app sslmode=require"`,
		"Password (shown only now, keep it): bfp_abc123", "snapshot run shop"} {
		if !strings.Contains(text, s) {
			t.Errorf("output lacks %q:\n%s", s, text)
		}
	}
	for _, s := range []string{"mysql -h", "Engine Routing", "forwards as", "3309", "s3cret"} {
		if strings.Contains(text, s) {
			t.Errorf("output of a PostgreSQL add says %q:\n%s", s, text)
		}
	}
}

// A MySQL add sends none of a PostgreSQL's fields.
func TestServerAddMySQLSendsNoPostgresFields(t *testing.T) {
	d, c := newFakeDaemon(t)
	d.on("POST /api/servers/identify", 200, map[string]any{"addr": "db1:3306", "flavor": "mysql"})
	d.on("POST /api/servers/check", 200, map[string]any{"ok": true, "name": "shop", "started": true, "doctor": passReport()})
	in := addInput{Host: "db1", Port: "3306", User: "dbtrail", Password: "p", Name: "shop"}
	if err := runServerAdd(context.Background(), c, in, false, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"source_database", "source_slot", "source_publication"} {
		if _, has := d.bodies["POST /api/servers/check"][k]; has {
			t.Errorf("a MySQL add sent %s", k)
		}
	}
}

func TestPrintNewServerPortPG(t *testing.T) {
	on, off := true, false
	for _, tc := range []struct {
		name         string
		port         *ctlNewServerPort
		noForwardYet bool
		want         []string
		wantNot      []string
	}{
		{"an older DBTrail says nothing", nil, true, nil, []string{"port", "forward"}},
		{"already on, on another address", &ctlNewServerPort{ctlPort: ctlPort{Enabled: &on, Host: "10.0.0.5", Port: "3309"}}, false,
			[]string{"is on, and takes the password it already has", `psql "host=10.0.0.5 port=5433 dbname=shop `}, []string{"shown only now", "3309", "forward"}},
		{"already on, and no forwarding user named", &ctlNewServerPort{ctlPort: ctlPort{Enabled: &on, Host: "10.0.0.5", Port: "3309"}}, true,
			[]string{"sslmode=require\"\n\nUntil \"shop\" has a forwarding user, the copy answers its statements alone, and a statement it cannot answer gets an error:\n  bintrail-console server forward-user shop -u <user> -p\n\n"}, []string{"shown only now", "3309"}},
		{"turned on and no password in the answer", &ctlNewServerPort{ctlPort: ctlPort{Enabled: &on}, TurnedOn: true}, false,
			[]string{"is on now", "port new-password"}, []string{"shown only now", "forward"}},
		{"turned on, and no forwarding user named", &ctlNewServerPort{ctlPort: ctlPort{Enabled: &on}, TurnedOn: true}, true,
			[]string{"is on now", "ready.\nUntil \"shop\" has a forwarding user, the copy answers its statements alone, and a statement it cannot answer gets an error:\n  bintrail-console server forward-user shop -u <user> -p\n\n"}, nil},
		{"not turned on", &ctlNewServerPort{ctlPort: ctlPort{Enabled: &off}, NotTurnedOn: "DBTrail already had servers."}, true,
			[]string{"was not turned on: DBTrail already had servers.\n"}, []string{"psql", "forward"}},
		{"off, as saved", &ctlNewServerPort{ctlPort: ctlPort{Enabled: &off}}, true, []string{"is off, as it was saved", "port on"}, []string{"psql", "forward"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			printNewServerPortPG(&out, tc.port, "shop", tc.noForwardYet)
			for _, s := range tc.want {
				if !strings.Contains(out.String(), s) {
					t.Errorf("lacks %q:\n%s", s, out.String())
				}
			}
			for _, s := range tc.wantNot {
				if strings.Contains(out.String(), s) {
					t.Errorf("says %q:\n%s", s, out.String())
				}
			}
		})
	}
	// A name that is not safe to paste is left to the person.
	var out bytes.Buffer
	printNewServerPortPG(&out, &ctlNewServerPort{ctlPort: ctlPort{Enabled: &on}}, `shop"; rm`, false)
	if !strings.Contains(out.String(), "dbname=<server name>") {
		t.Errorf("an unsafe name went into the psql line:\n%s", out.String())
	}
}
