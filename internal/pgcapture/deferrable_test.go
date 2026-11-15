package pgcapture

import (
	"strings"
	"testing"
)

func TestDeferrableKeysProblem(t *testing.T) {
	one := DeferrableKeysProblem([]DeferrableKey{{Schema: "public", Table: "orders", Constraint: "orders_pkey"}})
	for _, want := range []string{"1 table of the publication has a primary key declared DEFERRABLE",
		`"public"."orders" (constraint "orders_pkey")`,
		`ALTER TABLE "public"."orders" DROP CONSTRAINT "orders_pkey", ADD PRIMARY KEY (<the same columns>);`,
		"take the table out of the publication", "locks the table"} {
		if !strings.Contains(one, want) {
			t.Errorf("missing %q in:\n%s", want, one)
		}
	}
	two := DeferrableKeysProblem([]DeferrableKey{{Schema: "public", Table: "orders", Constraint: "orders_pkey"},
		{Schema: "Sales", Table: `it"ems`, Constraint: "My Key"}})
	for _, want := range []string{"2 tables of the publication have a primary key", `ALTER TABLE "Sales"."it""ems" DROP CONSTRAINT "My Key"`} {
		if !strings.Contains(two, want) {
			t.Errorf("missing %q in:\n%s", want, two)
		}
	}
}
