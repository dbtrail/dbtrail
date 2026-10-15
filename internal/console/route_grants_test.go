package console

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
)

func TestClassifyGrants(t *testing.T) {
	cases := []struct {
		name                 string
		lines                []string
		writes, other, roles []string
		execute              bool
	}{
		{name: "select on one schema", lines: []string{"GRANT USAGE ON *.* TO `r`@`%`", "GRANT SELECT ON `shop`.* TO `r`@`%`"}},
		{name: "all privileges", lines: []string{"GRANT ALL PRIVILEGES ON *.* TO `r`@`%` WITH GRANT OPTION"}, writes: []string{"ALL PRIVILEGES"}},
		{name: "lower case", lines: []string{"grant select, insert on shop.* to r@'%'"}, writes: []string{"INSERT"}},
		{name: "column list", lines: []string{"GRANT SELECT, UPDATE (`a`, `b`), INSERT ON `shop`.`t` TO `r`@`%`"}, writes: []string{"UPDATE", "INSERT"}},
		{name: "each once", lines: []string{"GRANT UPDATE ON `a`.* TO `r`@`%`", "GRANT UPDATE, DELETE ON `b`.* TO `r`@`%`"}, writes: []string{"UPDATE", "DELETE"}},
		{name: "usage only", lines: []string{"GRANT USAGE ON *.* TO `r`@`%`"}},
		{name: "revoke adds nothing", lines: []string{"GRANT SELECT ON *.* TO `r`@`%`", "REVOKE INSERT ON `mysql`.* FROM `r`@`%`"}},
		{name: "role", lines: []string{"GRANT USAGE ON *.* TO `r`@`%`", "GRANT `reader`@`%`,`audit` TO `r`@`%`"}, roles: []string{"`reader`@`%`", "`audit`"}},
		{name: "a role whose name holds ' on '", lines: []string{"GRANT `role on x`@`%` TO `u`@`%`"}, roles: []string{"`role on x`@`%`"}},
		{name: "a column whose name holds ' on '", lines: []string{"GRANT SELECT (`note on x`), UPDATE (`id`) ON `db`.`t` TO `u`@`%`"}, writes: []string{"UPDATE"}},
		{name: "a schema whose name holds ' to '", lines: []string{"GRANT SELECT ON `a to b`.* TO `u`@`%`"}},
		{name: "server administration is never read-only", lines: []string{"GRANT SELECT ON *.* TO `r`@`%`", "GRANT BINLOG_ADMIN,SYSTEM_VARIABLES_ADMIN ON *.* TO `r`@`%`"}, other: []string{"BINLOG_ADMIN", "SYSTEM_VARIABLES_ADMIN"}},
		{name: "super, create user, shutdown", lines: []string{"GRANT SUPER, CREATE USER, SHUTDOWN ON *.* TO `r`@`%`"}, other: []string{"SUPER", "CREATE USER", "SHUTDOWN"}},
		{name: "a privilege this build does not know", lines: []string{"GRANT SELECT, FUTURE_THING ON *.* TO `r`@`%`"}, other: []string{"FUTURE_THING"}},
		{name: "create tablespace changes the schema", lines: []string{"GRANT CREATE TABLESPACE ON *.* TO `r`@`%`"}, writes: []string{"CREATE TABLESPACE"}},
		{name: "execute", lines: []string{"GRANT SELECT, EXECUTE ON `app`.* TO `u`@`%`"}, execute: true},
		{name: "proxy", lines: []string{"GRANT PROXY ON ''@'' TO 'r'@'%' WITH GRANT OPTION"}, other: []string{"PROXY"}},
		{name: "read-only kinds", lines: []string{"GRANT SELECT, SHOW VIEW, PROCESS, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO `r`@`%`"}},
		{name: "multi-word privileges", lines: []string{"GRANT CREATE TEMPORARY TABLES, LOCK TABLES, CREATE VIEW ON `s`.* TO `r`@`%`"}, writes: []string{"CREATE TEMPORARY TABLES", "LOCK TABLES", "CREATE VIEW"}},
		{name: "empty lines", lines: []string{"", "  "}},
	}
	for _, c := range cases {
		g := classifyGrants(c.lines)
		if !reflect.DeepEqual(g.writes, c.writes) || !reflect.DeepEqual(g.other, c.other) || !reflect.DeepEqual(g.roles, c.roles) || g.execute != c.execute {
			t.Errorf("%s: writes %q other %q roles %q execute %v, want %q %q %q %v", c.name, g.writes, g.other, g.roles, g.execute, c.writes, c.other, c.roles, c.execute)
		}
	}
}

// MariaDB prints the password hash in SHOW GRANTS; it never reaches the page.
func TestClassifyGrantsHidesAuthentication(t *testing.T) {
	lines := []string{
		"GRANT SELECT ON *.* TO `r`@`%` IDENTIFIED BY PASSWORD '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'",
		"GRANT USAGE ON *.* TO `r`@`%` IDENTIFIED VIA ed25519 USING 'ZIgUREUg5PVgQ6LskhXmO+eZLS0nC8be6HPjYWR4YJY' OR unix_socket",
		"GRANT USAGE ON *.* TO 'r'@'%' IDENTIFIED WITH 'mysql_native_password' AS '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19'",
		"GRANT USAGE ON *.* TO 'r'@'%' IDENTIFIED BY 'it''s secret'",
	}
	shown := classifyGrants(lines).shown
	for _, s := range shown {
		for _, secret := range []string{"2470C0C06", "ZIgUREUg5", "secret"} {
			if strings.Contains(s, secret) {
				t.Errorf("%q still shows %q", s, secret)
			}
		}
		if !strings.Contains(s, "'<hidden>'") {
			t.Errorf("%q does not say what was removed", s)
		}
	}
	if !strings.HasSuffix(shown[1], "OR unix_socket") {
		t.Errorf("the line after the secret was lost: %q", shown[1])
	}
}

// grantsServer is a Server with only the grants reader: routeGrants reads
// nothing else.
func grantsServer(_ *testing.T, _ ServerEntry, grants func(context.Context, string, config.SSL, time.Duration) ([]string, error)) *Server {
	return &Server{routeAccountGrants: grants}
}

func TestRouteGrantsStates(t *testing.T) {
	src := "cap:cpw@tcp(db:3306)/"
	route := "fwd:fpw@tcp(db:3306)/"
	ok := func(context.Context, string, config.SSL, time.Duration) ([]string, error) {
		return []string{"GRANT SELECT, INSERT ON `shop`.* TO `fwd`@`%`"}, nil
	}
	t.Run("no forwarding account", func(t *testing.T) {
		r := grantsServer(t, ServerEntry{ID: "a", Name: "a", SourceDSN: src}, ok).routeGrants(context.Background(), ServerEntry{ID: "a", SourceDSN: src})
		if r.None == "" || r.Grants != nil {
			t.Errorf("%+v", r)
		}
	})
	t.Run("the capture account", func(t *testing.T) {
		e := ServerEntry{ID: "a", SourceDSN: src, RouteDSN: src}
		r := grantsServer(t, e, ok).routeGrants(context.Background(), e)
		if r.Error == "" || r.Grants != nil {
			t.Errorf("%+v", r)
		}
	})
	t.Run("no client in this process", func(t *testing.T) {
		e := ServerEntry{ID: "a", SourceDSN: src, RouteDSN: route}
		r := grantsServer(t, e, nil).routeGrants(context.Background(), e)
		if r.Skipped == "" || r.User != "fwd" {
			t.Errorf("%+v", r)
		}
	})
	t.Run("read", func(t *testing.T) {
		e := ServerEntry{ID: "a", SourceDSN: src, RouteDSN: route}
		r := grantsServer(t, e, ok).routeGrants(context.Background(), e)
		if r.User != "fwd" || len(r.Grants) != 1 || !reflect.DeepEqual(r.Writes, []string{"INSERT"}) {
			t.Errorf("%+v", r)
		}
	})
	t.Run("no lines is an error, never read-only", func(t *testing.T) {
		e := ServerEntry{ID: "a", SourceDSN: src, RouteDSN: route}
		r := grantsServer(t, e, func(context.Context, string, config.SSL, time.Duration) ([]string, error) {
			return []string{"", " "}, nil
		}).routeGrants(context.Background(), e)
		if r.Error == "" || r.Grants != nil {
			t.Errorf("%+v", r)
		}
	})
	t.Run("login refused, no password in the error", func(t *testing.T) {
		e := ServerEntry{ID: "a", SourceDSN: src, RouteDSN: route}
		r := grantsServer(t, e, func(context.Context, string, config.SSL, time.Duration) ([]string, error) {
			return nil, errors.New("dial " + route + ": Access denied for user 'fwd'")
		}).routeGrants(context.Background(), e)
		if r.Error == "" || strings.Contains(r.Error, "fpw") {
			t.Errorf("%+v", r)
		}
	})
}
