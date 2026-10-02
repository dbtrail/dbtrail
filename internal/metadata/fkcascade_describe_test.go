package metadata

import (
	"errors"
	"strings"
	"testing"
)

func fk(schema, table, refSchema, refTable, del, upd string) FKCascadeConstraint {
	return FKCascadeConstraint{Schema: schema, Table: table, Name: "fk_" + table,
		ReferencedSchema: refSchema, ReferencedTable: refTable, DeleteRule: del, UpdateRule: upd}
}

// Each constraint is named child → parent with the rules that cascade, and
// only those: a RESTRICT or NO ACTION side is not a cascade and is not shown.
func TestDescribeFKCascades(t *testing.T) {
	for name, tc := range map[string]struct {
		in   []FKCascadeConstraint
		want string
	}{
		"one on delete": {
			[]FKCascadeConstraint{fk("demo", "orders", "demo", "customers", "CASCADE", "RESTRICT")},
			"demo.orders → demo.customers, ON DELETE CASCADE",
		},
		"on update only": {
			[]FKCascadeConstraint{fk("shop", "products", "shop", "categories", "NO ACTION", "CASCADE")},
			"shop.products → shop.categories, ON UPDATE CASCADE",
		},
		"both rules": {
			[]FKCascadeConstraint{fk("shop", "items", "shop", "orders", "CASCADE", "SET NULL")},
			"shop.items → shop.orders, ON DELETE CASCADE and ON UPDATE SET NULL",
		},
		"set null": {
			[]FKCascadeConstraint{fk("blog", "posts", "blog", "users", "SET NULL", "RESTRICT")},
			"blog.posts → blog.users, ON DELETE SET NULL",
		},
		"self reference": {
			[]FKCascadeConstraint{fk("hr", "employees", "hr", "employees", "CASCADE", "RESTRICT")},
			"hr.employees → hr.employees, ON DELETE CASCADE",
		},
		"parent in another schema": {
			[]FKCascadeConstraint{fk("app", "orders", "crm", "customers", "CASCADE", "RESTRICT")},
			"app.orders → crm.customers, ON DELETE CASCADE",
		},
		"exactly the cap": {
			[]FKCascadeConstraint{
				fk("d", "a", "d", "p", "CASCADE", ""), fk("d", "b", "d", "p", "CASCADE", ""),
				fk("d", "c", "d", "p", "CASCADE", ""), fk("d", "e", "d", "p", "CASCADE", ""),
				fk("d", "f", "d", "p", "CASCADE", ""),
			},
			"d.a → d.p, ON DELETE CASCADE; d.b → d.p, ON DELETE CASCADE; d.c → d.p, ON DELETE CASCADE; " +
				"d.e → d.p, ON DELETE CASCADE; d.f → d.p, ON DELETE CASCADE",
		},
		"over the cap": {
			[]FKCascadeConstraint{
				fk("d", "a", "d", "p", "CASCADE", ""), fk("d", "b", "d", "p", "CASCADE", ""),
				fk("d", "c", "d", "p", "CASCADE", ""), fk("d", "e", "d", "p", "CASCADE", ""),
				fk("d", "f", "d", "p", "CASCADE", ""), fk("d", "g", "d", "p", "CASCADE", ""),
				fk("d", "h", "d", "p", "CASCADE", ""),
			},
			"d.a → d.p, ON DELETE CASCADE; d.b → d.p, ON DELETE CASCADE; d.c → d.p, ON DELETE CASCADE; " +
				"d.e → d.p, ON DELETE CASCADE; d.f → d.p, ON DELETE CASCADE; and 2 more",
		},
		"empty": {nil, ""},
	} {
		t.Run(name, func(t *testing.T) {
			if got := DescribeFKCascades(tc.in, FKCascadeListLimit); got != tc.want {
				t.Errorf("got  %q\nwant %q", got, tc.want)
			}
		})
	}
}

// The error the capture pre-flight logs names the constraints too, keeps the
// sentinel the callers branch on, and carries no history or jargon.
func TestFKCascadesErrorText(t *testing.T) {
	err := error(&FKCascadesError{Found: []FKCascadeConstraint{fk("demo", "orders", "demo", "customers", "CASCADE", "RESTRICT")}})
	if !errors.Is(err, ErrFKCascadesFound) {
		t.Fatal("FKCascadesError must match ErrFKCascadesFound")
	}
	want := "foreign keys that cascade: demo.orders → demo.customers, ON DELETE CASCADE"
	if err.Error() != want {
		t.Errorf("got  %q\nwant %q", err.Error(), want)
	}
	if strings.ContainsAny(err.Error(), "#—") {
		t.Errorf("issue number or em dash in %q", err.Error())
	}
}
