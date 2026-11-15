package pgcapture

import (
	"strings"
	"testing"
)

// What an operator is told about generated columns on PostgreSQL 18: the
// statement that lets the application write again comes first, for every
// table, and then what each kind of column allows.
func TestGeneratedColumnsProblem(t *testing.T) {
	stored := UnpublishedGenerated{Schema: "public", Table: "orders", Column: "total", Publication: "pub"}
	virtual := UnpublishedGenerated{Schema: "public", Table: "items", Column: "label", Virtual: true, Publication: "pub"}
	for _, tc := range []struct {
		name  string
		found []UnpublishedGenerated
		has   []string
		lacks []string
	}{
		{name: "a stored column", found: []UnpublishedGenerated{stored},
			has: []string{"1 table", `ALTER TABLE "public"."orders" REPLICA IDENTITY DEFAULT;`,
				`ALTER PUBLICATION "pub" SET (publish_generated_columns = stored);`, `public.orders.total (publication "pub")`},
			lacks: []string{"VIRTUAL", "DROP TABLE"}},
		{name: "a virtual column", found: []UnpublishedGenerated{virtual},
			has: []string{`ALTER TABLE "public"."items" REPLICA IDENTITY DEFAULT;`, "VIRTUAL", "cannot be captured",
				`ALTER PUBLICATION "pub" DROP TABLE "public"."items";`, "FOR ALL TABLES"},
			lacks: []string{"publish_generated_columns"}},
		{name: "one of each", found: []UnpublishedGenerated{virtual, stored},
			has: []string{"2 tables", `ALTER TABLE "public"."items" REPLICA IDENTITY DEFAULT; ALTER TABLE "public"."orders" REPLICA IDENTITY DEFAULT;`,
				`SET (publish_generated_columns = stored);`, `DROP TABLE "public"."items";`}},
		{name: "two columns of one table count the table once", found: []UnpublishedGenerated{stored,
			{Schema: "public", Table: "orders", Column: "tax", Publication: "pub"}},
			has: []string{"1 table", "public.orders.total", "public.orders.tax"}},
		{name: "another publication than the capture's leaves the column out", found: []UnpublishedGenerated{
			{Schema: "public", Table: "orders", Column: "total", Publication: "reporting"}},
			has: []string{`ALTER PUBLICATION "reporting" SET (publish_generated_columns = stored);`, `(publication "reporting")`}},
		{name: "names that need quoting", found: []UnpublishedGenerated{
			{Schema: "Sales", Table: `o"rders`, Column: "total", Virtual: true, Publication: "My Pub"}},
			has: []string{`ALTER TABLE "Sales"."o""rders" REPLICA IDENTITY DEFAULT;`, `ALTER PUBLICATION "My Pub" DROP TABLE "Sales"."o""rders";`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := GeneratedColumnsProblem(tc.found)
			for _, want := range tc.has {
				if !strings.Contains(got, want) {
					t.Errorf("missing %q in:\n%s", want, got)
				}
			}
			for _, not := range tc.lacks {
				if strings.Contains(got, not) {
					t.Errorf("unexpected %q in:\n%s", not, got)
				}
			}
			// The way out for the application comes before anything about capture.
			if i, j := strings.Index(got, "REPLICA IDENTITY DEFAULT"), strings.Index(got, "ALTER PUBLICATION"); i < 0 || (j >= 0 && j < i) {
				t.Errorf("the statement that lets the application write again is not first:\n%s", got)
			}
		})
	}
}
