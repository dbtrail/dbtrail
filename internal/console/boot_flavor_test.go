package console

import (
	"encoding/json"
	"testing"
)

// The daemon's own source (watch --source-dsn) has no saved Source type: the
// server list shows the flavor its capture detected, read from
// Config.BootSourceFlavor on every list, so a MariaDB main source is labeled
// MariaDB once capture has asked it.
func TestServersAPIBootEntryShowsTheDetectedFlavor(t *testing.T) {
	cases := []struct {
		name     string
		reported string
		nilFunc  bool
		want     string
	}{
		{name: "mariadb detected", reported: "mariadb", want: "mariadb"},
		{name: "mysql detected", reported: "mysql", want: "mysql"},
		{name: "not detected yet", reported: "", want: ""},
		{name: "no capture in this process", nilFunc: true, want: ""},
		{name: "a value that is not a MySQL-family flavor is not shown", reported: "postgres", want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, _, closeFn := newSQLMock(t)
			defer closeFn()
			reg, err := LoadRegistry(t.TempDir() + "/console-servers.yaml")
			if err != nil {
				t.Fatal(err)
			}
			cfg := Config{
				Listen: "127.0.0.1:8090", Token: "t", Registry: reg,
				DB: db, DBName: "binlog_index", BootDSN: "cli:pw@tcp(127.0.0.1:3306)/binlog_index",
			}
			if !tc.nilFunc {
				reported := tc.reported
				cfg.BootSourceFlavor = func() string { return reported }
			}
			srv, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			rec, body := doServersReq(t, srv, "GET", "/api/servers", "")
			if rec.Code != 200 {
				t.Fatalf("list code = %d", rec.Code)
			}
			var resp serversResponse
			if err := json.Unmarshal(body, &resp); err != nil {
				t.Fatal(err)
			}
			if len(resp.Servers) != 1 || resp.Servers[0].ID != bootServerID {
				t.Fatalf("want the boot entry only, got %+v", resp.Servers)
			}
			if got := resp.Servers[0].Flavor; got != tc.want {
				t.Errorf("boot entry flavor = %q, want %q", got, tc.want)
			}
		})
	}
}
