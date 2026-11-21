package pgstreamrun

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/dbtrail/dbtrail/internal/doctor"
	"github.com/dbtrail/dbtrail/internal/pgcapture"
)

// What `server add` and `bintrail-pg doctor` say about a role that cannot
// read a published table (#3022).
func TestReadAccessResult(t *testing.T) {
	t.Run("every table readable", func(t *testing.T) {
		got := readAccessResult(pgcapture.ReadAccess{Role: "dbtrail", Tables: 3})
		if got.Name != readAccessCheck || got.Status != doctor.StatusPass || got.Detail != "" || got.Remediation != "" || got.Kind != "" {
			t.Errorf("got %+v, want a bare pass", got)
		}
	})
	t.Run("nothing to check is not a pass", func(t *testing.T) {
		got := readAccessResult(pgcapture.ReadAccess{Role: "dbtrail"})
		if got.Status != doctor.StatusSkip || got.Detail != "the publication holds no table the first copy would read, so there was nothing to check" {
			t.Errorf("got %+v", got)
		}
	})
	t.Run("the issue: one table without SELECT", func(t *testing.T) {
		got := readAccessResult(pgcapture.ReadAccess{Role: "dbtrail", Tables: 3, Unreadable: []pgcapture.UnreadableTable{
			{Schema: "public", Table: "customers", NoSelect: true},
		}})
		// A warning: a failure here would keep capture from starting, and
		// capture does not need what is missing.
		want := doctor.CheckResult{
			Name:   readAccessCheck,
			Status: doctor.StatusWarn,
			Detail: `The role "dbtrail" cannot read 1 of the 3 tables the copy reads: "public"."customers" (no SELECT)`,
			Remediation: "Recording changes does not need these permissions, so this does not stop the server from being added or started. " +
				"The first copy of the tables needs them: it reads each one as this role, and DBTrail refuses to start it until they are there. " +
				"DBTrail grants nothing by itself. Run this on the database, as the tables' owner or a superuser:\n\n" +
				"  GRANT SELECT ON TABLE \"public\".\"customers\" TO \"dbtrail\";\n",
		}
		// No kind: a screen then shows the whole fix, prose included. Not
		// optional either: it is not a tip, the copy will be refused.
		if got.Optional || got.Name != want.Name || got.Status != want.Status || got.Detail != want.Detail || got.Remediation != want.Remediation ||
			got.Kind != "" || len(got.Subjects) != 0 || len(got.Statements) != 0 {
			t.Errorf("got  %+v\nwant %+v", got, want)
		}
	})
	t.Run("another schema, quoted names", func(t *testing.T) {
		got := readAccessResult(pgcapture.ReadAccess{Role: "Db Trail", Tables: 2, Unreadable: []pgcapture.UnreadableTable{
			{Schema: "Sales Data", Table: `Order "Items"`, NoUsage: true, NoSelect: true},
		}})
		for _, want := range []string{
			"\n\n  GRANT USAGE ON SCHEMA \"Sales Data\" TO \"Db Trail\";\n  GRANT SELECT ON TABLE \"Sales Data\".\"Order \"\"Items\"\"\" TO \"Db Trail\";\n",
		} {
			if !strings.Contains(got.Remediation, want) {
				t.Errorf("remediation lacks %q:\n%s", want, got.Remediation)
			}
		}
	})
	t.Run("hundreds of tables: a bounded message and one statement per schema", func(t *testing.T) {
		acc := pgcapture.ReadAccess{Role: "dbtrail", Tables: 500}
		for i := range 500 {
			acc.Unreadable = append(acc.Unreadable, pgcapture.UnreadableTable{Schema: "public", Table: fmt.Sprintf("t%03d", i), NoSelect: true})
		}
		got := readAccessResult(acc)
		if !strings.HasSuffix(got.Detail, `"public"."t009" (no SELECT), and 490 more`) || len(got.Detail) > 600 {
			t.Errorf("detail (%d bytes): %s", len(got.Detail), got.Detail)
		}
		if !strings.Contains(got.Remediation, "\n\n  GRANT SELECT ON ALL TABLES IN SCHEMA \"public\" TO \"dbtrail\";\n\n"+
			"That is one statement per schema and not one per table, since many tables lack SELECT: it gives SELECT on every table of the schema, published or not.") {
			t.Errorf("remediation:\n%s", got.Remediation)
		}
		if strings.Count(got.Remediation, "GRANT ") != 1 || len(got.Remediation) > 700 {
			t.Errorf("remediation is %d bytes with %d statements", len(got.Remediation), strings.Count(got.Remediation, "GRANT "))
		}
	})
	// The statements are the lines a screen offers to copy: every one of them
	// indented under a paragraph, and nothing else indented.
	t.Run("the statements are the only indented lines", func(t *testing.T) {
		acc := pgcapture.ReadAccess{Role: "dbtrail", Tables: 2, Unreadable: []pgcapture.UnreadableTable{
			{Schema: "app", Table: "a", NoUsage: true, NoSelect: true}, {Schema: "app", Table: "b", NoUsage: true, NoSelect: true},
		}}
		var indented []string
		for line := range strings.SplitSeq(readAccessResult(acc).Remediation, "\n") {
			if strings.HasPrefix(line, " ") {
				indented = append(indented, strings.TrimSpace(line))
			}
		}
		if !slices.Equal(indented, acc.Grants()) {
			t.Errorf("indented lines %q, want the statements %q", indented, acc.Grants())
		}
	})
}
