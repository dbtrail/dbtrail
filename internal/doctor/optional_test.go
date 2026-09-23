package doctor

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

// An optional improvement is a WARN a script can still read as "warn", marked
// optional so nothing counts it as a warning: not the Warnings total, not the
// exit code, not the console's "with N warnings" sentence.
func TestOptionalChecksNeverCountAsWarnings(t *testing.T) {
	r := &Report{}
	r.add(CheckResult{Name: "p", Status: StatusPass})
	r.add(CheckResult{Name: "w", Status: StatusWarn})
	r.add(CheckResult{Name: "o1", Status: StatusWarn, Optional: true})
	r.add(CheckResult{Name: "o2", Status: StatusWarn, Optional: true})
	if r.Warnings != 1 || r.Optional != 2 || r.Passed != 1 || r.Failed != 0 {
		t.Errorf("counts = %+v; want 1 warning and 2 optional", r)
	}
	if got := r.Passed + r.Failed + r.Warnings + r.Skipped + r.Optional; got != len(r.Checks) {
		t.Errorf("the counters add up to %d over %d checks", got, len(r.Checks))
	}
	if err := r.Err(); err != nil {
		t.Errorf("optional items changed the exit code: %v", err)
	}
	if err := r.ErrExcluding(CapacityCheckName); err != nil {
		t.Errorf("optional items changed the boot decision: %v", err)
	}

	// A FAIL is never hidden by the flag: it still counts and still refuses.
	f := &Report{}
	f.add(CheckResult{Name: "x", Status: StatusFail, Optional: true})
	if f.Failed != 1 || f.Optional != 0 || f.Err() == nil {
		t.Errorf("a failure marked optional was hidden: %+v err=%v", f, f.Err())
	}
	// Optional on a pass or a skip is counted as what it is.
	s := &Report{}
	s.add(CheckResult{Name: "a", Status: StatusPass, Optional: true})
	s.add(CheckResult{Name: "b", Status: StatusSkip, Optional: true})
	if s.Passed != 1 || s.Skipped != 1 || s.Optional != 0 {
		t.Errorf("optional pass/skip miscounted: %+v", s)
	}
}

// The JSON keeps status "warn" for existing scripts and adds the optional
// flag and count. The text marks the line optional and names the count in the
// summary only when there is one, so a report without optional items prints
// the same bytes as before.
func TestOptionalChecksAreLabeledInOutput(t *testing.T) {
	r := &Report{}
	r.add(CheckResult{Name: "Statement capture (query_text)", Status: StatusWarn, Optional: true, Detail: "binlog_rows_query_log_events=OFF"})
	r.add(CheckResult{Name: "binlog_format=ROW", Status: StatusPass})

	var js bytes.Buffer
	if err := r.Write(&js, "json"); err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Checks []struct {
			Status   string `json:"status"`
			Optional *bool  `json:"optional"`
		} `json:"checks"`
		Warnings int  `json:"warnings"`
		Optional *int `json:"optional"`
	}
	if err := json.Unmarshal(js.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Checks[0].Status != "warn" || decoded.Checks[0].Optional == nil || !*decoded.Checks[0].Optional {
		t.Errorf("optional check JSON = %+v; want status warn and optional true", decoded.Checks[0])
	}
	if decoded.Checks[1].Optional != nil {
		t.Errorf("a required check carries an optional field: %s", js.String())
	}
	if decoded.Warnings != 0 || decoded.Optional == nil || *decoded.Optional != 1 {
		t.Errorf("JSON counts: warnings %d optional %v; want 0 and 1", decoded.Warnings, decoded.Optional)
	}

	var txt bytes.Buffer
	if err := r.Write(&txt, "text"); err != nil {
		t.Fatal(err)
	}
	out := txt.String()
	t.Log("\n" + out)
	if !strings.Contains(out, "~ Statement capture (query_text) [optional] (binlog_rows_query_log_events=OFF)") {
		t.Errorf("text does not label the optional check:\n%s", out)
	}
	if !strings.Contains(out, "Passed: 1  Failed: 0  Warnings: 0  Skipped: 0  Optional: 1\n") {
		t.Errorf("summary does not count the optional item:\n%s", out)
	}

	var plain bytes.Buffer
	(&Report{Checks: []CheckResult{{Name: "a", Status: StatusPass}}, Passed: 1}).Write(&plain, "text")
	if !strings.Contains(plain.String(), "Passed: 1  Failed: 0  Warnings: 0  Skipped: 0\n") {
		t.Errorf("a report with no optional items changed its summary line:\n%s", plain.String())
	}
}

// optionalHeadline is the first paragraph of a fix: the words a person reads
// before the statement.
func optionalHeadline(rem string) string {
	head, _, _ := strings.Cut(rem, "\n\n")
	return head
}

// The two optional findings speak plain English: one short line, then the
// statement, with no jargon and no em dash, and every outcome of these two
// checks that is a WARN is marked optional.
func TestOptionalCheckTexts(t *testing.T) {
	type tc struct {
		name     string
		run      func(t *testing.T) CheckResult
		headline string
		stmt     string
		after    string // must appear after the statement
	}
	mysqlVar := func(q, col string, s mockSQLScalar) func(*testing.T) CheckResult {
		return func(t *testing.T) CheckResult {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			s.apply(mock.ExpectQuery(regexp.QuoteMeta(q)), col)
			if strings.Contains(q, "binlog_rows_query_log_events") {
				return checkStatementCapture(t.Context(), db)
			}
			return checkRowMetadata(t.Context(), db)
		}
	}
	mariaStmt := func(t *testing.T) CheckResult {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		mysqlErrResp(1193, "Unknown system variable").apply(mock.ExpectQuery("SELECT @@binlog_rows_query_log_events"), "v")
		row("OFF").apply(mock.ExpectQuery("SELECT @@binlog_annotate_row_events"), "v")
		return checkStatementCapture(t.Context(), db)
	}
	cases := []tc{
		{"statement capture OFF", mysqlVar("SELECT @@binlog_rows_query_log_events", "v", row("0")),
			"Show the SQL statement behind each change. To turn it on:",
			"  SET PERSIST binlog_rows_query_log_events = ON;", "Only changes made after this carry it."},
		{"statement capture MariaDB OFF", mariaStmt,
			"Show the SQL statement behind each change. To turn it on:",
			"  SET GLOBAL binlog_annotate_row_events = ON;", "Only changes made after this carry it."},
		{"row metadata MINIMAL", mysqlVar("SELECT @@binlog_row_metadata", "v", row("MINIMAL")),
			"Notice if someone renames a column. To turn it on:",
			"  SET PERSIST binlog_row_metadata = 'FULL';", "  SET GLOBAL binlog_row_metadata = 'FULL';"},
		{"row metadata MariaDB NO_LOG", mysqlVar("SELECT @@binlog_row_metadata", "v", row("NO_LOG")),
			"Notice if someone renames a column. To turn it on:",
			"  SET PERSIST binlog_row_metadata = 'FULL';", "MariaDB"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := c.run(t)
			t.Logf("detail=%q\n%s", got.Detail, got.Remediation)
			if got.Status != StatusWarn || !got.Optional {
				t.Fatalf("status %q optional %v; want an optional warn", got.Status, got.Optional)
			}
			if h := optionalHeadline(got.Remediation); h != c.headline {
				t.Errorf("headline = %q, want %q", h, c.headline)
			}
			if n := len(strings.Fields(optionalHeadline(got.Remediation))); n > 25 {
				t.Errorf("%d words before the statement, over 25", n)
			}
			i := strings.Index(got.Remediation, "\n\n"+c.stmt+"\n")
			j := strings.LastIndex(got.Remediation, c.after)
			if i < 0 || j < i {
				t.Errorf("statement %q missing, or %q not after it:\n%s", c.stmt, c.after, got.Remediation)
			}
			for _, s := range []string{got.Detail, got.Remediation} {
				if strings.Contains(s, "—") {
					t.Errorf("em dash in %q", s)
				}
			}
			for _, w := range []string{"TABLE_MAP", "snapshot", "metadata", "query_text", "Optional:"} {
				if strings.Contains(strings.ToLower(optionalHeadline(got.Remediation)), strings.ToLower(w)) {
					t.Errorf("headline uses %q: %q", w, optionalHeadline(got.Remediation))
				}
			}
		})
	}

	// A failed read of an optional setting is still optional: it can never
	// be the reason a start reads "with warnings".
	for _, run := range []func(*testing.T) CheckResult{
		mysqlVar("SELECT @@binlog_rows_query_log_events", "v", errResp("driver: bad connection")),
		mysqlVar("SELECT @@binlog_row_metadata", "v", errResp("driver: bad connection")),
	} {
		if got := run(t); got.Status != StatusWarn || !got.Optional {
			t.Errorf("read failure: status %q optional %v (%s)", got.Status, got.Optional, got.Detail)
		}
	}
	// A pass stays a plain pass.
	if got := mysqlVar("SELECT @@binlog_row_metadata", "v", row("FULL"))(t); got.Status != StatusPass || got.Optional {
		t.Errorf("FULL: %+v", got)
	}
}

// grantAccount turns what CURRENT_USER() returned into the account a GRANT
// can name. The user part may hold a quote, a backquote, an @ or a space; the
// host may be % or an IPv6 address. Anything it cannot name safely is "", and
// the caller keeps its placeholder.
func TestGrantAccount(t *testing.T) {
	cases := []struct{ in, want string }{
		{"bintrail@%", "`bintrail`@`%`"},
		{"Admin@%", "`Admin`@`%`"},
		{"o'brien@10.0.%", "`o'brien`@`10.0.%`"},
		{"we`ird@%", "`we``ird`@`%`"},
		{"a@b@%", "`a@b`@`%`"},
		{"my user@localhost", "`my user`@`localhost`"},
		{"u6@::1", "`u6`@`::1`"},
		{"app@2001:db8::1", "`app`@`2001:db8::1`"},
		{"@localhost", ""},
		{"root@", ""},
		{"nohost", ""},
		{"", ""},
		{"two\nlines@%", ""},
	}
	for _, c := range cases {
		if got := grantAccount(c.in); got != c.want {
			t.Errorf("grantAccount(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// The RDS retention fix names the account the check connected as, asked of
// the server with CURRENT_USER(). If that query fails, the placeholder stays.
// Both paths register the CURRENT_USER() expectation: sqlmock errors a query
// nobody expected, which would fall back to the placeholder and pass a test
// that only looked for it.
func TestRDSRetentionGrantNamesTheRealAccount(t *testing.T) {
	cases := []struct {
		name  string
		user  mockSQLScalar
		grant string
	}{
		{"plain", row("bintrail@%"), "  GRANT SELECT ON mysql.rds_configuration TO `bintrail`@`%`;"},
		{"quote and backquote", row("o'b`q@%"), "  GRANT SELECT ON mysql.rds_configuration TO `o'b``q`@`%`;"},
		{"ipv6 host", row("app@2001:db8::1"), "  GRANT SELECT ON mysql.rds_configuration TO `app`@`2001:db8::1`;"},
		{"current_user fails", errResp("driver: bad connection"), "  GRANT SELECT ON mysql.rds_configuration TO '<user>'@'%';"},
		{"anonymous", row("@localhost"), "  GRANT SELECT ON mysql.rds_configuration TO '<user>'@'%';"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectQuery("mysql.rds_configuration").
				WillReturnError(&mysql.MySQLError{Number: 1142, Message: "SELECT command denied"})
			c.user.apply(mock.ExpectQuery(regexp.QuoteMeta("SELECT CURRENT_USER()")), "CURRENT_USER()")

			got := checkBinlogRetention(t.Context(), db)
			if got.Status != StatusWarn {
				t.Errorf("status %q", got.Status)
			}
			if !strings.HasSuffix(got.Remediation, "\n\n"+c.grant) {
				t.Errorf("remediation = %q, want it to end with %q", got.Remediation, c.grant)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("CURRENT_USER() was not asked: %v", err)
			}
		})
	}
}
