package consoleapp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"
)

func pgServer(name string) map[string]any {
	return map[string]any{"id": "id-" + name, "name": name, "kind": "registry", "flavor": "postgres", "has_source": true}
}

func myServer(name string) map[string]any {
	return map[string]any{"id": "id-" + name, "name": name, "kind": "registry", "flavor": "mysql", "has_source": true}
}

func TestPostgresInstall(t *testing.T) {
	pg := ctlServer{Flavor: "postgres", HasSource: true}
	my := ctlServer{Flavor: "mysql", HasSource: true}
	for _, tc := range []struct {
		name    string
		servers []ctlServer
		err     error
		want    bool
	}{
		{"PostgreSQL servers", []ctlServer{pg, pg}, nil, true},
		{"one PostgreSQL and an entry with no source", []ctlServer{pg, {Flavor: "mysql"}}, nil, true},
		{"MySQL servers", []ctlServer{my}, nil, false},
		// From before one family per installation: MySQL's lines, as always.
		{"both families", []ctlServer{pg, my}, nil, false},
		{"no server", nil, nil, false},
		{"only entries with no source", []ctlServer{{Flavor: "postgres"}}, nil, false},
		{"a DBTrail that does not say the family", []ctlServer{{HasSource: true}}, nil, false},
		{"the list did not load", []ctlServer{pg}, errors.New("no answer"), false},
	} {
		if got := postgresInstall(tc.servers, tc.err); got != tc.want {
			t.Errorf("%s: postgresInstall = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// With PostgreSQL servers, `port on` and `port status` print the psql line
// for the PostgreSQL port and nothing of the MySQL port's.
func TestPortOnAndStatusWithPostgresServers(t *testing.T) {
	for _, verb := range []string{"on", "status"} {
		t.Run(verb, func(t *testing.T) {
			d, c := newFakeDaemon(t)
			d.on("GET /api/servers", 200, map[string]any{"servers": []map[string]any{pgServer("shop")}})
			answer := map[string]any{"enabled": true, "host": "10.0.0.5", "port": "3309", "has_password": true, "password": "pw-shown-once",
				"error": "nothing answers on 3309"}
			var out bytes.Buffer
			var err error
			if verb == "on" {
				d.on("PUT /api/flashback", 200, answer)
				err = runPortSet(context.Background(), c, true, &out)
			} else {
				d.on("GET /api/flashback", 200, answer)
				err = runPortStatus(context.Background(), c, &out)
			}
			// The MySQL router not answering is not an error of the
			// PostgreSQL port, which another service answers.
			if err != nil {
				t.Fatalf("err = %v\n%s", err, out.String())
			}
			text := out.String()
			for _, s := range []string{"The port for SQL clients is on.", `psql "host=10.0.0.5 port=5433 dbname=shop user=app sslmode=require"`,
				"The database is the server's name", "goes to your PostgreSQL as the server's forwarding user",
				"fresh lays the changes captured since the last snapshot over it", "port mode <server> [fresh|fast]"} {
				if !strings.Contains(text, s) {
					t.Errorf("output lacks %q:\n%s", s, text)
				}
			}
			if verb == "on" && !strings.Contains(text, "pw-shown-once") {
				t.Errorf("the password made now is not shown:\n%s", text)
			}
			for _, s := range []string{"mysql -h", "Engine Routing", "3309", "lag limit", "MySQL port"} {
				if strings.Contains(text, s) {
					t.Errorf("output for PostgreSQL servers says %q:\n%s", s, text)
				}
			}
		})
	}
	t.Run("off", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("GET /api/servers", 200, map[string]any{"servers": []map[string]any{pgServer("shop")}})
		d.on("PUT /api/flashback", 200, map[string]any{"enabled": false})
		var out bytes.Buffer
		if err := runPortSet(context.Background(), c, false, &out); err != nil {
			t.Fatal(err)
		}
		if got := out.String(); !strings.Contains(got, "The port for SQL clients is off") || strings.Contains(got, "MySQL") {
			t.Errorf("output:\n%s", got)
		}
	})
	t.Run("two servers: the database is left to the person", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("GET /api/servers", 200, map[string]any{"servers": []map[string]any{pgServer("shop"), pgServer("crm")}})
		d.on("GET /api/flashback", 200, map[string]any{"enabled": true, "has_password": true})
		var out bytes.Buffer
		if err := runPortStatus(context.Background(), c, &out); err != nil {
			t.Fatal(err)
		}
		if got := out.String(); !strings.Contains(got, "dbname=<server name>") || !strings.Contains(got, "host=127.0.0.1") {
			t.Errorf("output:\n%s", got)
		}
	})
}

// With MySQL servers nothing changes: the mysql line, Engine Routing, and
// the error when the port is on and not answering.
func TestPortOnWithMySQLServersIsAsBefore(t *testing.T) {
	d, c := newFakeDaemon(t)
	d.on("GET /api/servers", 200, map[string]any{"servers": []map[string]any{myServer("shop")}})
	d.on("PUT /api/flashback", 200, map[string]any{"enabled": true, "host": "127.0.0.1", "port": "3309", "has_password": true, "password": "pw", "error": "address already in use"})
	var out bytes.Buffer
	err := runPortSet(context.Background(), c, true, &out)
	if err == nil || !strings.Contains(err.Error(), "the MySQL port is saved as on and is not answering") {
		t.Fatalf("err = %v", err)
	}
	if got := out.String(); !strings.Contains(got, "mysql -h 127.0.0.1 -P 3309 -u shop -p") || !strings.Contains(got, "Engine Routing is off") || strings.Contains(got, "psql") {
		t.Errorf("output:\n%s", got)
	}
}

// #2756: a PostgreSQL server has fresh and fast mode on the PostgreSQL port,
// so `port mode` says and saves its choice. It never reads GET /api/router,
// the MySQL router's status, which says nothing of the PostgreSQL port's.
func TestPortModeOfAPostgresServer(t *testing.T) {
	for _, tc := range []struct {
		name, want   string
		fresh, saved bool
		put          map[string]any
		says         []string
	}{
		{"a look, nothing saved", "", true, false, nil,
			[]string{`Nothing is saved for "shop": it is fresh, the default.`, pgPortModeNote}},
		{"fast saved", "fast", true, false, map[string]any{"fresh": false, "fresh_saved": true},
			[]string{`Saved for "shop": fast.`, pgPortModeNote}},
		{"fresh saved", "fresh", false, true, map[string]any{"fresh": true, "fresh_saved": true},
			[]string{`Saved for "shop": fresh.`, pgPortModeNote}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, c := newFakeDaemon(t)
			srv := pgServer("shop")
			srv["sql_fresh"], srv["sql_fresh_saved"] = tc.fresh, tc.saved
			d.on("GET /api/servers", 200, map[string]any{"servers": []map[string]any{srv}})
			if tc.put != nil {
				d.on("PUT /api/servers/id-shop/sql-mode", 200, tc.put)
			}
			var out bytes.Buffer
			if err := runPortMode(context.Background(), c, "shop", tc.want, time.Second, &out); err != nil {
				t.Fatalf("err = %v\n%s", err, out.String())
			}
			t.Logf("port mode shop %s:\n%s", tc.want, out.String())
			for _, s := range tc.says {
				if !strings.Contains(out.String(), s) {
					t.Errorf("output lacks %q:\n%s", s, out.String())
				}
			}
			for _, s := range []string{"no fresh mode", "is a PostgreSQL", "The router answers it", "MySQL"} {
				if strings.Contains(out.String(), s) {
					t.Errorf("output says %q:\n%s", s, out.String())
				}
			}
			if slices.Contains(d.calls, "GET /api/router") {
				t.Errorf("the MySQL router's status was read: %v", d.calls)
			}
			if saved := slices.Contains(d.calls, "PUT /api/servers/id-shop/sql-mode"); saved != (tc.want != "") {
				t.Errorf("saved = %v for mode %q: %v", saved, tc.want, d.calls)
			}
			if tc.want != "" {
				if b := d.bodies["PUT /api/servers/id-shop/sql-mode"]; len(b) != 1 || b["fresh"] != (tc.want == "fresh") {
					t.Errorf("body = %v", b)
				}
			}
		})
	}
}

// What only a MySQL or MariaDB source has is refused for PostgreSQL before
// anything is changed.
func TestPortSettingsOfTheMySQLPortRefusePostgres(t *testing.T) {
	servers := map[string]any{"servers": []map[string]any{pgServer("shop")}}
	t.Run("routing on", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("GET /api/servers", 200, servers)
		err := runPortRouting(context.Background(), c, true, io.Discard)
		if err == nil || !strings.Contains(err.Error(), "The servers here are PostgreSQL") || strings.Contains(err.Error(), "fresh mode") {
			t.Fatalf("err = %v", err)
		}
		for _, call := range d.calls {
			if strings.HasPrefix(call, "PUT") {
				t.Errorf("the setting was changed (%s) before the refusal", call)
			}
		}
	})
	t.Run("routing off is let through", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("PUT /api/flashback", 200, map[string]any{"enabled": true, "routing_on": false})
		if err := runPortRouting(context.Background(), c, false, io.Discard); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("max-behind", func(t *testing.T) {
		d, c := newFakeDaemon(t)
		d.on("GET /api/servers", 200, servers)
		five := int64(300)
		for _, want := range []*int64{nil, &five} {
			err := runPortMaxBehind(context.Background(), c, "shop", want, io.Discard)
			if err == nil || !strings.Contains(err.Error(), `"shop" is a PostgreSQL`) || strings.Contains(err.Error(), "fresh mode") {
				t.Fatalf("err = %v", err)
			}
		}
		if len(d.calls) != 2 {
			t.Errorf("calls = %v, want the two reads of the list and nothing saved", d.calls)
		}
	})
}
