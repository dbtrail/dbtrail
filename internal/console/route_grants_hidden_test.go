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
		unsure  bool
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
		{name: "write only", grants: []string{"GRANT INSERT, UPDATE ON `shop`.* TO `r`@`%`"},
			safe: []string{"shop.audit_log", "shop.customers.email"}},
		{name: "routine grant", grants: []string{"GRANT EXECUTE ON PROCEDURE `shop`.`p` TO `r`@`%`"},
			safe: []string{"shop.audit_log"}},
		// In a database-level grant, _ and % are wildcards (LIKE), unless escaped.
		{name: "wildcard database name", grants: []string{"GRANT SELECT ON `sh_p`.* TO `r`@`%`"},
			exposed: []string{"shop.audit_log", "shop.customers.email"}, safe: []string{"hr.*"}},
		{name: "escaped underscore is literal", grants: []string{"GRANT SELECT ON `sh\\_p`.* TO `r`@`%`"},
			safe: []string{"shop.audit_log"}},
		{name: "percent database name", grants: []string{"GRANT SELECT ON `%`.* TO `r`@`%`"},
			exposed: []string{"shop.audit_log", "hr.*", "billing.*.card"}},
		{name: "roles", grants: []string{"GRANT USAGE ON *.* TO `r`@`%`", "GRANT `reader`@`%` TO `r`@`%`"}, unsure: true},
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
			if (got.Unsure != "") != c.unsure {
				t.Errorf("unsure = %q, want %v", got.Unsure, c.unsure)
			}
		})
	}
	if got := hiddenExposures([]string{"GRANT SELECT ON *.* TO `r`@`%`"}, nil); len(got.Exposed) != 0 || got.Unsure != "" {
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
	if r := grantsServer(t, e, ok).routeGrants(context.Background(), e); r.Hidden == nil || r.Hidden.Unsure == "" {
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
	} {
		if got := likeFold(c.pattern, c.name); got != c.want {
			t.Errorf("likeFold(%q, %q) = %v, want %v", c.pattern, c.name, got, c.want)
		}
	}
}
