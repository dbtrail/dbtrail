package console

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dbtrail/dbtrail/internal/config"
	"github.com/dbtrail/dbtrail/internal/hiderules"
)

func TestHiddenExposures(t *testing.T) {
	set, err := hiderules.Parse(hiderules.Rules{
		Tables:  []string{"shop.audit_log", "hr.*"},
		Columns: []string{"shop.customers.email", "billing.*.card"},
	})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		grants  []string
		exposed []string // rules that must be named
		safe    []string // rules that must NOT be named
		unsure  []string // substrings that must appear among the unsure notes
	}{
		{name: "usage only", grants: []string{"GRANT USAGE ON *.* TO `r`@`%`"},
			safe: []string{"shop.audit_log", "hr.*", "shop.customers.email", "billing.*.card"}},
		{name: "global select", grants: []string{"GRANT SELECT ON *.* TO `r`@`%`"},
			exposed: []string{"shop.audit_log", "hr.*", "shop.customers.email", "billing.*.card"}},
		{name: "all privileges on the schema", grants: []string{"GRANT ALL PRIVILEGES ON `shop`.* TO `r`@`%` WITH GRANT OPTION"},
			exposed: []string{"shop.audit_log", "shop.customers.email"}, safe: []string{"hr.*", "billing.*.card"}},
		{name: "case and no backticks", grants: []string{"grant select on SHOP.* to r@'%'"},
			exposed: []string{"shop.audit_log", "shop.customers.email"}},
		{name: "one table", grants: []string{"GRANT SELECT ON `shop`.`customers` TO `r`@`%`"},
			exposed: []string{"shop.customers.email"}, safe: []string{"shop.audit_log", "hr.*"}},
		{name: "a table in a hidden schema", grants: []string{"GRANT SELECT ON `hr`.`salaries` TO `r`@`%`"},
			exposed: []string{"hr.*"}},
		{name: "a table of a schema-wide column rule", grants: []string{"GRANT SELECT ON `billing`.`invoices` TO `r`@`%`"},
			exposed: []string{"billing.*.card"}},
		{name: "column grant without the hidden column", grants: []string{"GRANT SELECT (`id`, `name`) ON `shop`.`customers` TO `r`@`%`"},
			safe: []string{"shop.customers.email"}},
		{name: "column grant with it", grants: []string{"GRANT SELECT (`id`, `Email`), UPDATE (`name`) ON `shop`.`customers` TO `r`@`%`"},
			exposed: []string{"shop.customers.email"}},
		// A write does not read, so nothing is EXPOSED; it can confirm a
		// value through a duplicate-key error, so it is named.
		{name: "write only", grants: []string{"GRANT INSERT, UPDATE ON `shop`.* TO `r`@`%`"},
			safe: []string{"shop.audit_log", "shop.customers.email"}, unsure: []string{"GRANT INSERT, UPDATE ON `shop`.*"}},
		// Privileges no list of dangerous ones would have kept up with.
		{name: "definer and role administration", grants: []string{"GRANT SET_USER_ID, ROLE_ADMIN, AUDIT_ADMIN ON *.* TO `r`@`%`"},
			unsure: []string{"GRANT SET_USER_ID, ROLE_ADMIN, AUDIT_ADMIN"}},
		{name: "a privilege this build has never heard of", grants: []string{"GRANT FUTURE_THING ON *.* TO `r`@`%`"},
			unsure: []string{"FUTURE_THING"}},
		// Plain reads and harmless metadata name nothing beyond the view note.
		{name: "harmless metadata", grants: []string{"GRANT USAGE, SHOW DATABASES, REPLICATION CLIENT ON *.* TO `r`@`%`"},
			safe: []string{"shop.audit_log"}},
		// A routine runs with its creator's rights: not "safe", unsure.
		{name: "routine grant", grants: []string{"GRANT EXECUTE ON PROCEDURE `shop`.`p` TO `r`@`%`"},
			safe: []string{"shop.audit_log"}, unsure: []string{"EXECUTE"}},
		// A view elsewhere may select a hidden column with its creator's rights.
		{name: "a read on another schema", grants: []string{"GRANT SELECT ON `reporting`.* TO `r`@`%`"},
			safe: []string{"shop.customers.email"}, unsure: []string{"VIEW"}},
		{name: "server-wide privileges", grants: []string{"GRANT FILE, PROCESS ON *.* TO `r`@`%`"},
			unsure: []string{"GRANT FILE, PROCESS ON *.*"}},
		// The binary log holds statement text with its literals.
		{name: "replication", grants: []string{"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO `r`@`%`"},
			unsure: []string{"binlog privileges", "GRANT REPLICATION SLAVE"}},
		{name: "mariadb binlog monitor", grants: []string{"GRANT BINLOG MONITOR ON *.* TO `r`@`%`"},
			unsure: []string{"GRANT BINLOG MONITOR"}},
		{name: "mariadb replica monitor", grants: []string{"GRANT REPLICA MONITOR ON *.* TO `r`@`%`"},
			unsure: []string{"GRANT REPLICA MONITOR"}},
		// MySQL's own log tables hold other sessions' statements.
		{name: "general log", grants: []string{"GRANT SELECT ON `mysql`.`general_log` TO `r`@`%`"},
			unsure: []string{"mysql.general_log"}},
		{name: "statement history", grants: []string{"GRANT SELECT ON `performance_schema`.* TO `r`@`%`"},
			unsure: []string{"performance_schema"}},
		// In a database-level grant, _ and % are wildcards (LIKE), unless escaped.
		{name: "wildcard database name", grants: []string{"GRANT SELECT ON `sh_p`.* TO `r`@`%`"},
			exposed: []string{"shop.audit_log", "shop.customers.email"}, safe: []string{"hr.*"}},
		{name: "escaped underscore is literal", grants: []string{"GRANT SELECT ON `sh\\_p`.* TO `r`@`%`"},
			safe: []string{"shop.audit_log"}},
		{name: "percent database name", grants: []string{"GRANT SELECT ON `%`.* TO `r`@`%`"},
			exposed: []string{"shop.audit_log", "hr.*", "billing.*.card"}, unsure: []string{"mysql.general_log"}},
		{name: "wildcard on the statement history", grants: []string{"GRANT SELECT ON `perf%`.* TO `r`@`%`"},
			unsure: []string{"performance_schema"}},
		{name: "escaped wildcard on the statement history", grants: []string{"GRANT SELECT ON `performance\\_schema`.* TO `r`@`%`"},
			unsure: []string{"performance_schema"}},
		{name: "roles", grants: []string{"GRANT USAGE ON *.* TO `r`@`%`", "GRANT `reader`@`%` TO `r`@`%`"}, unsure: []string{"reader"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := hiddenExposures(c.grants, set)
			text := strings.Join(got.Exposed, "\n")
			for _, r := range c.exposed {
				if !strings.Contains(text, r) {
					t.Errorf("%q not named as readable:\n%s", r, text)
				}
			}
			for _, r := range c.safe {
				if strings.Contains(text, r) {
					t.Errorf("%q named as readable, it is not:\n%s", r, text)
				}
			}
			notes := strings.Join(got.Unsure, "\n")
			for _, u := range c.unsure {
				if !strings.Contains(notes, u) {
					t.Errorf("unsure notes do not mention %q:\n%s", u, notes)
				}
			}
			// What no grant list settles is always said.
			if !strings.Contains(notes, "mandatory_roles") {
				t.Errorf("the always-unsure note is missing:\n%s", notes)
			}
			if len(c.unsure) == 0 && len(got.Unsure) != 1 && !strings.Contains(notes, "VIEW") {
				t.Errorf("unexpected unsure notes:\n%s", notes)
			}
			// One note per grant line, however many of its privileges count.
			seen := map[string]bool{}
			for _, n := range got.Unsure {
				if seen[n] {
					t.Errorf("note repeated: %s", n)
				}
				seen[n] = true
			}
		})
	}
	if got := hiddenExposures([]string{"GRANT SELECT ON *.* TO `r`@`%`"}, nil); len(got.Exposed) != 0 || got.Unsure != nil {
		t.Errorf("no rules: %+v", got)
	}
}

// The route-grants answer carries the check on a server with rules, and
// nothing on one without; rules this build cannot read are said, not skipped.
func TestRouteGrantsCarryHiddenCheck(t *testing.T) {
	src := "cap:cpw@tcp(db:3306)/"
	route := "fwd:fpw@tcp(db:3306)/"
	ok := func(context.Context, string, config.SSL, time.Duration) ([]string, error) {
		return []string{"GRANT SELECT, INSERT ON `shop`.* TO `fwd`@`%`"}, nil
	}
	e := ServerEntry{ID: "a", SourceDSN: src, RouteDSN: route}
	if r := grantsServer(t, e, ok).routeGrants(context.Background(), e); r.Hidden != nil {
		t.Errorf("no rules: hidden = %+v", r.Hidden)
	}
	e.Hidden = &hiderules.Rules{Columns: []string{"shop.customers.email"}}
	r := grantsServer(t, e, ok).routeGrants(context.Background(), e)
	if r.Hidden == nil || len(r.Hidden.Exposed) != 1 || !strings.Contains(r.Hidden.Exposed[0], "shop.customers.email") ||
		!strings.Contains(r.Hidden.Exposed[0], "GRANT SELECT, INSERT ON `shop`.*") {
		t.Errorf("rules: hidden = %+v", r.Hidden)
	}
	e.Hidden = &hiderules.Rules{Tables: []string{"orders"}}
	if r := grantsServer(t, e, ok).routeGrants(context.Background(), e); r.Hidden == nil || len(r.Hidden.Unsure) == 0 || !strings.Contains(r.Hidden.Unsure[0], "do not parse") {
		t.Errorf("unreadable rules: hidden = %+v", r.Hidden)
	}
}

// In a database-level grant _ and % are wildcards and \ escapes them.
func TestLikeFold(t *testing.T) {
	for _, c := range []struct {
		pattern, name string
		want          bool
	}{
		{`sh\_p`, "sh_p", true},
		{`sh\_p`, "SH_P", true},
		{`sh\_p`, "shop", false},
		{`sh_p`, "shop", true},
		{`sh%`, "shop", true},
		{`%`, "", true},
		{`shop`, "shop2", false},
		{`s%p`, "sp", true},
		{`100\%%`, "100%_x", true},
		{`100\%%`, "100x", false},
		{`a%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%b`, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaac", false},
	} {
		if got := likeFold(c.pattern, c.name); got != c.want {
			t.Errorf("likeFold(%q, %q) = %v, want %v", c.pattern, c.name, got, c.want)
		}
	}
}
