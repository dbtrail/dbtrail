package doctor

import (
	"regexp"
	"strconv"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/dbtrail/dbtrail/internal/serverid"
)

// #1783: the console shows these checks when a server is added from the
// browser, where there is no --source-dsn to fix and no `bintrail init` to
// run. The text names the source or index connection instead, and says when
// the index database gets created instead of only which command creates it.
// `bintrail init` / `bintrail up` may still appear when the same text says
// it is the command-line way ("on the command line"): `bintrail stream` does
// not create the database, so a text that dropped init would be wrong there.
var consoleOnlyWrong = regexp.MustCompile("--source-dsn|--index-dsn|`bintrail (init|up|query)`|`query --|port 3306|`stream`")

var labeledCommand = regexp.MustCompile("^`bintrail (init|up)`$")

func assertConsoleWording(t *testing.T, where string, c CheckResult) {
	t.Helper()
	for field, s := range map[string]string{"detail": c.Detail, "remediation": c.Remediation} {
		for _, m := range consoleOnlyWrong.FindAllString(s, -1) {
			if labeledCommand.MatchString(m) && strings.Contains(s, "on the command line") {
				continue
			}
			t.Errorf("%s %s names %q, a command-line spelling the console cannot act on:\n%s", where, field, m, s)
		}
	}
}

// An address nothing listens on, so the connection is refused at once.
const refusedDSN = "u:p@tcp(127.0.0.1:1)/idx"

func TestSourceUnreachableWording(t *testing.T) {
	r := Build(t.Context(), refusedDSN, "", "", 0)
	for _, c := range r.Checks {
		if c.Name == SourceConnectionCheckName {
			if c.Status != StatusFail || c.Remediation == "" {
				t.Fatalf("source connection = %s with remediation %q", c.Status, c.Remediation)
			}
			assertConsoleWording(t, "source connection", c)
			return
		}
	}
	t.Fatal("no source connection check in the report")
}

func TestIndexUnreachableWording(t *testing.T) {
	for name, c := range map[string]CheckResult{
		"index connection":   checkIndexConnection(t.Context(), refusedDSN, "idx"),
		"index write access": checkIndexWriteAccess(t.Context(), refusedDSN, "idx"),
	} {
		if c.Status != StatusFail || c.Remediation == "" {
			t.Fatalf("%s = %s with remediation %q", name, c.Status, c.Remediation)
		}
		// `bintrail stream` does not create the database, so the command-line
		// way to create it must stay named beside the console's.
		if !strings.Contains(c.Remediation, "`bintrail init`") || !strings.Contains(c.Remediation, "the console creates it") {
			t.Errorf("%s remediation does not say who creates the database on each surface:\n%s", name, c.Remediation)
		}
		assertConsoleWording(t, name, c)
	}
}

func TestIndexNotInitializedWording(t *testing.T) {
	c := capacityCheckResult(CapacityMeasurement{Status: StatusSkip, Reason: CapacityNotInitialized}, "idx", "")
	if c.Status != StatusSkip {
		t.Fatalf("status = %s", c.Status)
	}
	assertConsoleWording(t, "index capacity", c)
}

func TestStatementCaptureWording(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mysql      mockSQLScalar
		maria      *mockSQLScalar
		wantStatus CheckStatus
	}{
		{"mysql OFF", row("0"), nil, StatusWarn},
		{"mariadb OFF", mysqlErrResp(1193, "Unknown system variable 'binlog_rows_query_log_events'"), ptr(row("OFF")), StatusWarn},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			tc.mysql.apply(mock.ExpectQuery("SELECT @@binlog_rows_query_log_events"), "@@binlog_rows_query_log_events")
			if tc.maria != nil {
				tc.maria.apply(mock.ExpectQuery("SELECT @@binlog_annotate_row_events"), "@@binlog_annotate_row_events")
			}
			c := checkStatementCapture(t.Context(), db)
			if c.Status != tc.wantStatus {
				t.Fatalf("status = %s: %s", c.Status, c.Detail)
			}
			assertConsoleWording(t, tc.name, c)
		})
	}
}

func TestServerIDCollisionWording(t *testing.T) {
	const dsn = "repl:pw@tcp(db.example.com:3306)/"
	derived, err := serverid.DeriveServerID(dsn)
	if err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		srcID string
		want  CheckStatus
	}{
		"no collision": {"1", StatusPass},
		"collision":    {strconv.FormatUint(uint64(derived), 10), StatusWarn},
	} {
		t.Run(name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("SELECT @@server_id").WillReturnRows(sqlmock.NewRows([]string{"@@server_id"}).AddRow(tc.srcID))
			c := checkServerIDCollision(t.Context(), db, dsn)
			if c.Status != tc.want || (tc.want == StatusWarn && c.Remediation == "") {
				t.Fatalf("status = %s with remediation %q, want %s", c.Status, c.Remediation, tc.want)
			}
			assertConsoleWording(t, name, c)
		})
	}
	// A DSN that cannot be parsed never reaches the query.
	c := checkServerIDCollision(t.Context(), nil, "not a dsn")
	if c.Status != StatusWarn {
		t.Fatalf("underivable = %s: %s", c.Status, c.Detail)
	}
	assertConsoleWording(t, "underivable", c)
}
