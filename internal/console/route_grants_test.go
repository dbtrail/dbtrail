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
		name          string
		lines         []string
		writes, roles []string
	}{
		{"select on one schema", []string{"GRANT USAGE ON *.* TO `r`@`%`", "GRANT SELECT ON `shop`.* TO `r`@`%`"}, nil, nil},
		{"all privileges", []string{"GRANT ALL PRIVILEGES ON *.* TO `r`@`%` WITH GRANT OPTION"}, []string{"ALL PRIVILEGES"}, nil},
		{"lower case", []string{"grant select, insert on shop.* to r@'%'"}, []string{"INSERT"}, nil},
		{"column list", []string{"GRANT SELECT, UPDATE (`a`, `b`), INSERT ON `shop`.`t` TO `r`@`%`"}, []string{"UPDATE", "INSERT"}, nil},
		{"each once", []string{"GRANT UPDATE ON `a`.* TO `r`@`%`", "GRANT UPDATE, DELETE ON `b`.* TO `r`@`%`"}, []string{"UPDATE", "DELETE"}, nil},
		{"usage only", []string{"GRANT USAGE ON *.* TO `r`@`%`"}, nil, nil},
		{"revoke adds nothing", []string{"GRANT SELECT ON *.* TO `r`@`%`", "REVOKE INSERT ON `mysql`.* FROM `r`@`%`"}, nil, nil},
		{"role", []string{"GRANT USAGE ON *.* TO `r`@`%`", "GRANT `reader`@`%`,`audit` TO `r`@`%`"}, nil, []string{"`reader`@`%`", "`audit`"}},
		{"dynamic admin privileges are not data", []string{"GRANT BACKUP_ADMIN,REPLICATION_SLAVE_ADMIN ON *.* TO `r`@`%`"}, nil, nil},
		{"proxy is not a role", []string{"GRANT PROXY ON ''@'' TO 'r'@'%' WITH GRANT OPTION"}, nil, nil},
		{"multi-word privileges", []string{"GRANT CREATE TEMPORARY TABLES, LOCK TABLES, CREATE VIEW ON `s`.* TO `r`@`%`"}, []string{"CREATE TEMPORARY TABLES", "LOCK TABLES", "CREATE VIEW"}, nil},
		{"empty lines", []string{"", "  "}, nil, nil},
	}
	for _, c := range cases {
		_, writes, roles := classifyGrants(c.lines)
		if !reflect.DeepEqual(writes, c.writes) || !reflect.DeepEqual(roles, c.roles) {
			t.Errorf("%s: writes %q roles %q, want %q %q", c.name, writes, roles, c.writes, c.roles)
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
	shown, _, _ := classifyGrants(lines)
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
