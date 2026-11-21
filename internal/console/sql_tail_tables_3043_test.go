package console

import (
	"reflect"
	"testing"

	"github.com/dbtrail/dbtrail/internal/sqlsandbox"
	"github.com/dbtrail/dbtrail/internal/views"
)

// #3043: which tables a fresh-mode statement is given tails for. A bare
// name that matched no view of the copy is no table of the copy, so it does
// not keep the tables the statement names from their tails; whatever could
// still be a table of the copy read without one keeps every table from them,
// since the answer would say every table was read with its changes.
func TestSQLTailTables(t *testing.T) {
	table := func(schema, name string) views.BaselineTable {
		return views.BaselineTable{Schema: schema, Table: name, Path: "/c/" + schema + "/" + name + ".parquet"}
	}
	in := views.Input{Baselines: []views.BaselineTable{
		table("shop", "orders"), table("shop", "customers"), table("shop", "items"), table("crm", "orders"),
	}}
	ref := func(parts ...string) sqlsandbox.TableRef {
		switch len(parts) {
		case 1:
			return sqlsandbox.TableRef{Name: parts[0]}
		case 2:
			return sqlsandbox.TableRef{Schema: parts[0], Name: parts[1]}
		}
		return sqlsandbox.TableRef{Catalog: parts[0], Schema: parts[1], Name: parts[2]}
	}
	for _, c := range []struct {
		name   string
		refs   sqlsandbox.Refs
		want   []string // schema.table, in the views' order
		known  bool
		narrow bool // sqlWantedViews narrows the views
	}{
		{"one table", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("shop", "orders")}}, []string{"shop.orders"}, true, true},
		{"no table at all", sqlsandbox.Refs{}, nil, true, true},
		{"a bare name in two schemas reads both", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("orders")}}, []string{"crm.orders", "shop.orders"}, true, true},
		{"a table and the name of a WITH", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("shop", "orders"), ref("x")}}, []string{"shop.orders"}, true, false},
		{"upper case, and the name twice", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("SHOP", "Orders"), ref("X"), ref("x")}}, []string{"shop.orders"}, true, false},
		{"two tables of many and an unknown name", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("shop", "orders"), ref("y"), ref("shop", "items")}}, []string{"shop.items", "shop.orders"}, true, false},
		{"a WITH named as a table of the copy counts as the table", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("shop", "orders"), ref("customers")}}, []string{"shop.customers", "shop.orders"}, true, true},
		{"only unknown names", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("x"), ref("duckdb_views")}}, nil, false, false},
		{"a table function beside the table", sqlsandbox.Refs{Unsure: true, Tables: []sqlsandbox.TableRef{ref("shop", "orders")}}, nil, false, false},
		{"a table function and a WITH", sqlsandbox.Refs{Unsure: true, Tables: []sqlsandbox.TableRef{ref("shop", "orders"), ref("x")}}, nil, false, false},
		{"a name in a schema the copy does not have", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("shop", "orders"), ref("memory", "customers")}}, nil, false, false},
		{"a table the copy does not have, in a schema it has", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("shop", "orders"), ref("shop", "nope")}}, nil, false, false},
		{"the engine's catalog and a table, no schema: the table in every schema", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("shop", "orders"), {Catalog: "memory", Name: "customers"}}}, []string{"shop.customers", "shop.orders"}, true, true},
		{"the same beside the name of a WITH", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("shop", "orders"), {Catalog: "memory", Name: "customers"}, ref("x")}}, nil, false, false},
		{"another catalog", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("shop", "orders"), ref("other", "shop", "customers")}}, nil, false, false},
		{"the engine's catalog, written out", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("memory", "shop", "orders"), ref("x")}}, []string{"shop.orders"}, true, false},
		{"a file of the copy by its path", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("shop", "orders"), ref("/c/shop/customers.parquet")}}, nil, false, false},
		{"files by a pattern", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("shop", "orders"), ref("/c/shop/*")}}, nil, false, false},
		{"a file in the working directory", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("shop", "orders"), ref("customers.parquet")}}, nil, false, false},
		{"a name with a space", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("shop", "orders"), ref("my block")}}, nil, false, false},
		{"a WITH named in another alphabet", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("shop", "orders"), ref("año_2026$")}}, []string{"shop.orders"}, true, false},
		{"a system table in a schema", sqlsandbox.Refs{Tables: []sqlsandbox.TableRef{ref("shop", "orders"), ref("information_schema", "tables")}}, nil, false, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			wanted := sqlWantedViews(in, c.refs)
			if (wanted != nil) != c.narrow {
				t.Fatalf("the views are narrowed: %v, and the case is written for %v", wanted != nil, c.narrow)
			}
			got, known := sqlTailTables(in, c.refs, wanted, sqlNamedTables(in, c.refs))
			var names []string
			for _, b := range got {
				names = append(names, b.Schema+"."+b.Table)
			}
			if known != c.known || !reflect.DeepEqual(names, c.want) {
				t.Errorf("tails for %v (known %v), want %v (known %v)", names, known, c.want, c.known)
			}
		})
	}
}
