package console

import "testing"

// The build request carries the server's flavor, which is what makes a
// PostgreSQL server's .sql build the files psql loads (#3157). A server
// added without one is a MySQL server.
func TestSQLExportTrigger_carriesTheServersFlavor(t *testing.T) {
	for _, tc := range []struct {
		entry ServerEntry
		want  string
	}{
		{ServerEntry{Name: "pg", Flavor: FlavorPostgres, DSN: "i:p@tcp(h:3306)/idx_pg",
			SourceDSN: "postgres://u:p@h:5432/app", BaselineDir: "/var/lib/dbtrail/pg"}, FlavorPostgres},
		{ServerEntry{Name: "maria", Flavor: FlavorMariaDB, DSN: "i:p@tcp(h:3306)/idx_maria",
			SourceDSN: "src:pw@tcp(h:3306)/", BaselineDir: "/var/lib/dbtrail/maria"}, FlavorMariaDB},
		{ServerEntry{Name: "plain", DSN: "i:p@tcp(h:3306)/idx_plain",
			SourceDSN: "src:pw@tcp(h2:3306)/", BaselineDir: "/var/lib/dbtrail/plain"}, FlavorMySQL},
	} {
		// A server of its own for each: one installation serves one family.
		stub := &stubSQLExporter{}
		srv := newSQLExportServer(t, stub)
		e, err := srv.cm.reg.Add(tc.entry)
		if err != nil {
			t.Fatalf("add %s: %v", tc.entry.Name, err)
		}
		rec, body := doServersReq(t, srv, "POST", "/api/servers/"+e.ID+"/sql-export", `{"at":"2026-06-10 12:00:00"}`)
		if rec.Code != 202 || stub.last == nil {
			t.Fatalf("%s: code=%d body=%s, want 202 and a request", tc.entry.Name, rec.Code, body)
		}
		if stub.last.Flavor != tc.want {
			t.Errorf("%s: the request's flavor is %q, want %q", tc.entry.Name, stub.last.Flavor, tc.want)
		}
	}
}
